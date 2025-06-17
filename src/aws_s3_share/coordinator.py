from contextlib import contextmanager
from pathlib import Path
import queue
import threading
from typing import Iterator

from aws_s3_share.compress import DEFAULT_CHUNK_SIZE, ChunkWriter, Compressor
from aws_s3_share.errors import S3UploadMultipartError
from aws_s3_share.progress import ProgressReporter
from aws_s3_share.upload import S3UploadPartResponse, Uploader


class Coordinator:
    """Coordinates compression and upload operations."""

    def __init__(
        self,
        compressor: Compressor,
        uploader: Uploader,
        progress_reporter: ProgressReporter,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ):
        """
        Initialize the Coordinator.

        Args:
            compressor (Compressor): The compressor instance to use for file compression.
            uploader (Uploader): The uploader instance to use for S3 uploads.
            progress_reporter (ProgressReporter): Progress reporter for tracking operations.
            chunk_size (int): Size of chunks for compression and upload. Defaults to DEFAULT_CHUNK_SIZE.
        """
        self._compressor = compressor
        self._uploader = uploader
        self._progress_reporter = progress_reporter
        self._chunk_size = chunk_size
        self._uploaded_bytes = 0
        self._upload_lock = threading.Lock()
        self._compression_done = threading.Event()

    @contextmanager
    def _managed_multipart_upload(self, bucket: str, key: str) -> Iterator[str]:
        """
        Context manager for multipart upload with automatic cleanup.

        Starts a multipart upload and ensures it's properly aborted if an exception occurs.

        Args:
            bucket (str): The S3 bucket name.
            key (str): The S3 object key.

        Yields:
            str: The upload ID for the multipart upload.

        Raises:
            Exception: Re-raises any exceptions that occur during the upload process.
        """
        upload_id = self._uploader.start_multipart_upload(bucket, key)
        try:
            yield upload_id
        except Exception:
            try:
                self._uploader.abort_multipart_upload(bucket, key, upload_id)
            except Exception:
                pass
            raise

    def archive_and_upload(self, input_path: Path, bucket: str, key: str | None = None) -> str:
        """
        Archive a file or directory by compressing it and uploading via S3 multipart upload.

        Args:
            input_path (Path): Local file or directory to compress and upload.
            bucket (str): Name of the target S3 bucket.
            key (str | None): Optional S3 object key; if None, a key is generated based on path and compressor.

        Returns:
            str: The S3 object key under which the data was uploaded.

        Raises:
            S3UploadMultipartError: If the upload thread times out or other multipart errors occur.
            Exception: Propagates any exceptions from compression or upload operations.
        """
        chunk_queue: queue.Queue = queue.Queue(maxsize=10)

        if key is None:
            if self._compressor.__class__.__name__ == "GzipCompressor":
                key = input_path.name + ".gz"
            elif self._compressor.__class__.__name__ == "TarGzipCompressor":
                key = input_path.name + ".tar.gz"
            else:
                key = input_path.name

        with self._managed_multipart_upload(bucket, key) as upload_id:
            chunk_writer = ChunkWriter(chunk_queue, self._chunk_size)
            parts: list[S3UploadPartResponse] = []
            upload_exception = None

            uploader_thread = threading.Thread(
                target=self._upload_chunks, args=(chunk_queue, bucket, key, upload_id, parts), daemon=True
            )
            uploader_thread.start()

            try:
                self._compressor.compress(input_path, chunk_writer, self._chunk_size, self._progress_reporter)

                chunk_writer.close()
                total_compressed_size = chunk_writer.total_enqueued
                self._compression_done.set()

                with self._upload_lock:
                    initial_uploaded = self._uploaded_bytes
                self._progress_reporter.start_upload(total_compressed_size, initial_uploaded)

                uploader_thread.join(timeout=300)
                if uploader_thread.is_alive():
                    raise S3UploadMultipartError("S3 multipart upload thread timed out.")

                self._progress_reporter.finish_upload()

            except Exception as e:
                upload_exception = e
                raise

            if upload_exception is None:
                self._uploader.complete_multipart_upload(bucket, key, upload_id, parts)

        return key

    def _upload_chunks(
        self, chunk_queue: queue.Queue, bucket: str, key: str, upload_id: str, parts: list[S3UploadPartResponse]
    ) -> None:
        """
        Upload chunks from the compression queue to S3 as multipart parts.

        Continuously reads compressed data chunks from the queue and uploads them using the provided Uploader.
        Updates the progress reporter and collects part metadata for the final completion call.

        Args:
            chunk_queue (queue.Queue): Queue from which to retrieve compressed data chunks.
            bucket (str): Target S3 bucket name.
            key (str): S3 object key under upload.
            upload_id (str): Multipart upload session ID.
            parts (list[S3UploadPartResponse]): List to append each part's response for completion.

        Raises:
            Exception: Any exception during upload is propagated up to abort and cleanup.
        """
        part_number = 1

        try:
            while True:
                try:
                    chunk = chunk_queue.get(timeout=30)
                    if chunk is None:
                        break

                    part = self._uploader.upload_part(chunk, bucket, key, part_number, upload_id)
                    parts.append(part)

                    with self._upload_lock:
                        self._uploaded_bytes += len(chunk)

                    if self._compression_done.is_set():
                        self._progress_reporter.update_upload(len(chunk))

                    part_number += 1
                    chunk_queue.task_done()

                except queue.Empty:
                    break

        except Exception:
            raise
