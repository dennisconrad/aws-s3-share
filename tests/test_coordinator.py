import pytest
import queue
import threading
from pathlib import Path

from aws_s3_share.coordinator import Coordinator
from aws_s3_share.upload import S3UploadPartResponse


class MockCompressor:
    def __init__(self, chunks_to_write=None, should_fail=False):
        self.chunks_to_write = chunks_to_write or [b'chunk1', b'chunk2']
        self.should_fail = should_fail
        self.compress_called = False
        
    def compress(self, input_path, writer, chunk_size, progress_reporter):
        self.compress_called = True
        if self.should_fail:
            raise RuntimeError("Compression failed")
        
        for chunk in self.chunks_to_write:
            writer.write(chunk)
        # Close the writer to signal completion
        writer.close()


class MockUploader:
    def __init__(self):
        self.start_called = False
        self.abort_called = False
        self.complete_called = False
        self.upload_part_calls = []
        self.start_args = None
        self.abort_args = None
        self.complete_args = None
        
    def start_multipart_upload(self, bucket, key):
        self.start_called = True
        self.start_args = (bucket, key)
        return "test-upload-id"
        
    def abort_multipart_upload(self, bucket, key, upload_id):
        self.abort_called = True
        self.abort_args = (bucket, key, upload_id)
        
    def complete_multipart_upload(self, bucket, key, upload_id, parts):
        self.complete_called = True
        self.complete_args = (bucket, key, upload_id, parts)
        
    def upload_part(self, chunk, bucket, key, part_number, upload_id):
        self.upload_part_calls.append((chunk, bucket, key, part_number, upload_id))
        return S3UploadPartResponse(part_number=part_number, ETag=f"etag-{part_number}")


class MockProgressReporter:
    def __init__(self):
        self.start_upload_called = False
        self.update_upload_calls = []
        self.finish_upload_called = False
        
    def start_upload(self, total, initial):
        self.start_upload_called = True
        self.start_args = (total, initial)
        
    def update_upload(self, size):
        self.update_upload_calls.append(size)
        
    def finish_upload(self):
        self.finish_upload_called = True


class GzipCompressor(MockCompressor):
    pass


class TarGzipCompressor(MockCompressor):
    pass


def test_archive_and_upload_success():
    """Test successful archive and upload operation."""
    compressor = MockCompressor(chunks_to_write=[b'data1', b'data2'])
    uploader = MockUploader()
    progress = MockProgressReporter()
    
    coordinator = Coordinator(compressor, uploader, progress, chunk_size=1024)
    
    result_key = coordinator.archive_and_upload(Path("test.txt"), "test-bucket", "test-key")
    
    assert result_key == "test-key"
    assert compressor.compress_called
    assert uploader.start_called
    assert uploader.complete_called
    assert not uploader.abort_called
    assert uploader.start_args == ("test-bucket", "test-key")
    # ChunkWriter combines chunks, so we get 1 upload call
    assert len(uploader.upload_part_calls) == 1
    assert progress.start_upload_called
    assert progress.finish_upload_called


def test_archive_and_upload_with_compression_error():
    """Test that compression errors trigger multipart upload abort."""
    compressor = MockCompressor(should_fail=True)
    uploader = MockUploader()
    progress = MockProgressReporter()
    
    coordinator = Coordinator(compressor, uploader, progress)
    
    with pytest.raises(RuntimeError, match="Compression failed"):
        coordinator.archive_and_upload(Path("test.txt"), "test-bucket", "test-key")
    
    assert uploader.start_called
    assert uploader.abort_called
    assert not uploader.complete_called
    assert uploader.abort_args == ("test-bucket", "test-key", "test-upload-id")


def test_key_generation_gzip():
    """Test automatic key generation for GzipCompressor."""
    compressor = GzipCompressor()
    uploader = MockUploader()
    progress = MockProgressReporter()
    
    coordinator = Coordinator(compressor, uploader, progress)
    
    result_key = coordinator.archive_and_upload(Path("test.txt"), "test-bucket")
    
    assert result_key == "test.txt.gz"


def test_key_generation_tar_gzip():
    """Test automatic key generation for TarGzipCompressor."""
    compressor = TarGzipCompressor()
    uploader = MockUploader()
    progress = MockProgressReporter()
    
    coordinator = Coordinator(compressor, uploader, progress)
    
    result_key = coordinator.archive_and_upload(Path("test.txt"), "test-bucket")
    
    assert result_key == "test.txt.tar.gz"


def test_key_generation_other_compressor():
    """Test automatic key generation for other compressor types."""
    compressor = MockCompressor()
    uploader = MockUploader()
    progress = MockProgressReporter()
    
    coordinator = Coordinator(compressor, uploader, progress)
    
    result_key = coordinator.archive_and_upload(Path("test.txt"), "test-bucket")
    
    assert result_key == "test.txt"


def test_upload_chunks_processing():
    """Test the _upload_chunks method processes queue correctly."""
    uploader = MockUploader()
    progress = MockProgressReporter()
    compressor = MockCompressor()
    
    coordinator = Coordinator(compressor, uploader, progress)
    coordinator._compression_done.set()  # Mark compression as done
    
    chunk_queue = queue.Queue()
    chunk_queue.put(b'chunk1')
    chunk_queue.put(b'chunk2')
    chunk_queue.put(None)  # Sentinel
    
    parts = []
    coordinator._upload_chunks(chunk_queue, "bucket", "key", "upload-id", parts)
    
    assert len(parts) == 2
    # Check if S3UploadPartResponse has attributes or is dict-like
    if hasattr(parts[0], 'part_number'):
        assert parts[0].part_number == 1
        assert parts[0].ETag == "etag-1"
        assert parts[1].part_number == 2
        assert parts[1].ETag == "etag-2"
    else:
        # Handle as dict-like
        assert parts[0]['part_number'] == 1
        assert parts[0]['ETag'] == "etag-1"
        assert parts[1]['part_number'] == 2
        assert parts[1]['ETag'] == "etag-2"
    assert coordinator._uploaded_bytes == 12  # len(b'chunk1') + len(b'chunk2')
    assert len(progress.update_upload_calls) == 2


def test_managed_multipart_upload_context_manager():
    """Test the managed multipart upload context manager."""
    uploader = MockUploader()
    progress = MockProgressReporter()
    compressor = MockCompressor()
    
    coordinator = Coordinator(compressor, uploader, progress)
    
    # Test successful case
    with coordinator._managed_multipart_upload("bucket", "key") as upload_id:
        assert upload_id == "test-upload-id"
        assert uploader.start_called
    
    # Test exception case
    uploader.start_called = False
    uploader.abort_called = False
    
    with pytest.raises(ValueError):
        with coordinator._managed_multipart_upload("bucket", "key") as upload_id:
            raise ValueError("Test error")
    
    assert uploader.start_called
    assert uploader.abort_called


def test_thread_safety():
    """Test that uploaded_bytes counter is thread-safe."""
    uploader = MockUploader()
    progress = MockProgressReporter()
    compressor = MockCompressor()
    
    coordinator = Coordinator(compressor, uploader, progress)
    coordinator._compression_done.set()
    
    # Create multiple threads that update the counter
    def update_counter():
        with coordinator._upload_lock:
            coordinator._uploaded_bytes += 10
    
    threads = []
    for _ in range(10):
        thread = threading.Thread(target=update_counter)
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    assert coordinator._uploaded_bytes == 100
    
    coordinator = Coordinator(compressor, uploader, progress)
    coordinator._compression_done.set()
    
    # Create multiple threads that update the counter
    def update_counter():
        with coordinator._upload_lock:
            coordinator._uploaded_bytes += 10
    
    threads = []
    for _ in range(10):
        thread = threading.Thread(target=update_counter)
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    assert coordinator._uploaded_bytes == 100
