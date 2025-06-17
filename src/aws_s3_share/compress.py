import io
from abc import ABC, abstractmethod
import gzip
from pathlib import Path
import queue
import tarfile
import threading
from typing import IO

from aws_s3_share.errors import CompressorCalculateTotalSizeError, CompressorInputPathError
from aws_s3_share.progress import ProgressReporter


DEFAULT_CHUNK_SIZE = 5 * 1024 * 1024  # 5MB (AWS minimum multipart size)


class Compressor(ABC):
    """Abstract base class for file compression."""

    @property
    @abstractmethod
    def file_extension(self) -> str:
        """
        The file extension used for compressed files.
        Returns:
            str: The file extension for the compressed files, e.g., '.gz' or '.tar.gz'.
        """
        pass  # pragma: no cover

    @abstractmethod
    def compress(
        self, input_path: Path, fileobj: IO[bytes], chunk_size: int, progress_reporter: ProgressReporter
    ) -> None:
        """
        Compresses the content of `input_path` and writes it to `fileobj`.

        Args:
            input_path: The path to the file or directory to compress.
            fileobj: A file-like object to write the compressed data to.
            chunk_size: The size of chunks to read from the input file.
            progress_reporter: An instance of ProgressReporter to report compression progress.

        Raises:
            CompressorInputPathError: If there's an error reading the input_path or during the compression process.
        """
        pass  # pragma: no cover


class GzipCompressor(Compressor):
    """Compressor for single files using gzip."""

    @property
    def file_extension(self) -> str:
        """
        The file extension used for compressed files.

        Returns:
            str: The file extension for gzip compressed files, which is '.gz'.
        """
        return ".gz"

    def compress(
        self, input_path: Path, fileobj: IO[bytes], chunk_size: int, progress_reporter: ProgressReporter
    ) -> None:
        """
        Compresses a single file using gzip and writes it to `fileobj`.

        Reads the `input_path` file in chunks, compresses each chunk, and writes it to the `fileobj`.  Progress is
        reported via the provided `progress_reporter`.

        Args:
            input_path (pathlib.Path): The path to the file to compress. Must be a file.
            fileobj (IO[bytes]): A file-like object to write the gzipped data to.
            chunk_size (int): The size of chunks to read from the input file.
            progress_reporter (ProgressReporter): An instance of ProgressReporter to report compression progress.

        Raises:
            CompressorInputPathError: If `input_path` is not a file or if an OSError occurs during file reading or gzip
                                      operations.
        """
        total_size = input_path.stat().st_size
        progress_reporter.start_compression(total_size)

        try:
            with open(input_path, "rb") as input_file:
                with gzip.GzipFile(fileobj=fileobj, mode="wb") as gz:
                    while chunk := input_file.read(chunk_size):
                        gz.write(chunk)
                        progress_reporter.update_compression(len(chunk))
        except OSError as e:
            raise CompressorInputPathError(f"Error reading file {input_path}: {e}")
        finally:
            progress_reporter.finish_compression()


class TarGzipCompressor(Compressor):
    """Compressor for directories using tar.gz."""

    @property
    def file_extension(self) -> str:
        """
        The file extension used for compressed files.

        Returns:
            str: The file extension for tar.gz compressed files, which is '.tar.gz'.
        """
        return ".tar.gz"

    def compress(
        self, input_path: Path, fileobj: IO[bytes], chunk_size: int, progress_reporter: ProgressReporter
    ) -> None:
        """
        Compresses a directory into a tar.gz archive and writes it to `fileobj`.

        Calculates the total size of files in the directory for progress reporting and uses a filter during tar
        creation to update progress for each file added.

        Args:
            input_path (pathlib.Path): The path to the directory to compress. Must be a directory.
            fileobj (IO[bytes]): A file-like object (binary mode) to write the tar.gz data to.
            chunk_size (int): This parameter is ignored as the tarfile module does not support a chunked interface.
                              It is included for compatibility with the Compressor interface.
            progress_reporter (ProgressReporter): An instance of ProgressReporter to report compression progress.

        Raises:
            CompressorInputPathError: If `input_path` is not a directory or if an OSError or tarfile.TarError occurs
                                      during archive creation.
        """
        total_size = self._calculate_total_size(input_path)
        progress_reporter.start_compression(total_size)

        try:
            with tarfile.open(fileobj=fileobj, mode="w:gz") as tar:

                def progress_filter(tarinfo: tarfile.TarInfo) -> tarfile.TarInfo:
                    if tarinfo.isfile():
                        progress_reporter.update_compression(tarinfo.size)
                    return tarinfo

                tar.add(input_path, arcname=input_path.name, filter=progress_filter)
        except (OSError, tarfile.TarError) as e:
            raise CompressorInputPathError(f"Error creating tar archive: {e}")
        finally:
            progress_reporter.finish_compression()

    def _calculate_total_size(self, input_path: Path) -> int:
        """
        Calculate total size of all files in directory.

        Recursively walks through the directory and sums up the size of all regular files.

        Args:
            input_path (pathlib.Path): The directory path to calculate total size for.

        Returns:
            int: Total size in bytes of all files in the directory.

        Raises:
            CompressorCalculateTotalSizeError: If an OSError occurs while accessing file information.
        """
        total_size = 0
        try:
            for file_path in input_path.rglob("*"):
                if file_path.is_file():
                    total_size += file_path.stat().st_size
        except OSError as e:
            raise CompressorCalculateTotalSizeError(f"Error calculating total size of directory {input_path}: {e}")
        return total_size


class ChunkWriter(io.RawIOBase):
    """A file-like object that writes data in chunks to a queue."""

    def __init__(self, chunk_queue: queue.Queue, chunk_size: int = DEFAULT_CHUNK_SIZE):
        """
        Initialize the ChunkWriter.

        Args:
            chunk_queue (queue.Queue): Queue to write chunks to.
            chunk_size (int): Size of chunks to write. Defaults to DEFAULT_CHUNK_SIZE.
        """
        self._queue = chunk_queue
        self._chunk_size = chunk_size
        self._buffer = bytearray()
        self._total_enqueued = 0
        self._closed = False
        self._lock = threading.Lock()

    @property
    def total_enqueued(self) -> int:
        """
        Total bytes enqueued.

        Returns:
            int: The total number of bytes that have been enqueued as chunks.
        """
        return self._total_enqueued

    def write(self, data: bytes) -> int:
        """
        Write data to the chunk writer.

        Data is buffered and written to the queue in chunks of the specified size.

        Args:
            data (bytes): Bytes to write.

        Returns:
            int: Number of bytes written.

        Raises:
            ValueError: If the writer is closed.
        """
        with self._lock:
            if self._closed:
                raise ValueError("I/O operation on closed file")

            self._buffer.extend(data)
            bytes_written = len(data)

            while len(self._buffer) >= self._chunk_size:
                chunk = bytes(self._buffer[: self._chunk_size])
                self._queue.put(chunk)
                self._total_enqueued += len(chunk)
                del self._buffer[: self._chunk_size]

            return bytes_written

    def flush(self) -> None:
        """
        Flush remaining buffer and signal end of chunks.

        Writes any remaining buffered data as a final chunk and puts a sentinel
        value (None) in the queue to indicate end of data.
        """
        if self._buffer:
            chunk = bytes(self._buffer)
            self._queue.put(chunk)
            self._total_enqueued += len(chunk)
            self._buffer = bytearray()
        self._queue.put(None)  # Sentinel to indicate end of chunks

    def close(self) -> None:
        """
        Close the chunk writer.  Flushes any remaining data and marks the writer as closed.

        Returns:
            None
        """
        if not self._closed:
            self.flush()
            self._closed = True

    def __enter__(self) -> "ChunkWriter":
        """
        Context manager entry.  Allows using ChunkWriter in a with statement.

        Returns:
            ChunkWriter: The instance itself for use within the context.
        """
        return self

    def __exit__(self, exc_type: any, exc_val: any, exc_tb: any) -> None:
        """
        Context manager exit.  Ensures the writer is properly closed when exiting the context.

        Args:
            exc_type (any): Exception type if an exception occurred.
            exc_val (any): Exception value if an exception occurred.
            exc_tb (any): Exception traceback if an exception occurred.

        Returns:
            None
        """
        self.close()

    def read(self, size: int = -1) -> bytes:
        """
        Read operation not supported.  Always raises NotImplementedError.

        Args:
            size (int): Number of bytes to read (ignored).

        Raises:
            NotImplementedError: ChunkWriter is write-only.

        """
        raise NotImplementedError("ChunkWriter is write-only")

    def readline(self, size: int = -1) -> bytes:
        """
        Readline operation not supported.  Always raises NotImplementedError.

        Args:
            size (int): Maximum number of bytes to read (ignored).

        Raises:
            NotImplementedError: ChunkWriter is write-only.
        """
        raise NotImplementedError("ChunkWriter is write-only")

    def readable(self) -> bool:
        """
        Check if the stream is readable.  Always returns `False`.

        Returns:
            bool: Always `False`, as ChunkWriter is write-only.
        """
        return False

    def writable(self) -> bool:
        """
        Check if the stream is writable.

        Returns:
            bool: `True` if not closed, `False` otherwise.
        """
        return not self._closed

    def seekable(self) -> bool:
        """
        Check if the stream supports seeking.  Always returns `False`.

        Returns:
            bool: Always `False`, as ChunkWriter does not support seeking.
        """
        return False
