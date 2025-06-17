import gzip
import io
import queue
import tarfile
import threading
from pathlib import Path
from unittest.mock import Mock, patch, mock_open

import math
import pytest

from s3_share.compress import (
    GzipCompressor,
    TarGzipCompressor,
    ChunkWriter,
    DEFAULT_CHUNK_SIZE,
)
from s3_share.errors import CompressorCalculateTotalSizeError, CompressorInputPathError



class TestGzipCompressor:
    """Test the GzipCompressor class."""

    @pytest.fixture
    def compressor(self):
        """Create a GzipCompressor instance."""
        return GzipCompressor()

    @pytest.fixture
    def mock_path(self):
        """Create a mock Path object."""
        return Mock(spec=Path)
    
    @pytest.fixture
    def mock_progress_reporter(self):
        """Create a mock progress reporter."""
        return Mock()

    def test_gzip_compress_file(self, compressor, mock_path, mock_progress_reporter):
        """Test compressing a single file."""
        uncompressed_data = b"Test data for compression."
        mock_path.stat.return_value.st_size = len(uncompressed_data)
        expected_update_compression_call_count = math.ceil(len(uncompressed_data) / DEFAULT_CHUNK_SIZE)

        with patch("builtins.open", mock_open(read_data=uncompressed_data)):
            output = io.BytesIO()
            compressor.compress(mock_path, output, DEFAULT_CHUNK_SIZE, mock_progress_reporter)
            
        output.seek(0)
        with gzip.GzipFile(fileobj=output, mode="rb") as gz:
            decompressed_data = gz.read()

        assert decompressed_data == uncompressed_data
        mock_progress_reporter.start_compression.assert_called_once_with(len(uncompressed_data))
        assert mock_progress_reporter.update_compression.call_count == expected_update_compression_call_count
        mock_progress_reporter.finish_compression.assert_called_once()

    def test_gzip_compress_empty_file(self, compressor, mock_path, mock_progress_reporter):
        """Test compressing an empty file."""
        uncompressed_data = b""
        mock_path.stat.return_value.st_size = len(uncompressed_data)

        with patch("builtins.open", mock_open(read_data=uncompressed_data)):
            output = io.BytesIO()
            compressor.compress(mock_path, output, DEFAULT_CHUNK_SIZE, mock_progress_reporter)
            
        output.seek(0)
        with gzip.GzipFile(fileobj=output, mode="rb") as gz:
            decompressed_data = gz.read()

        assert decompressed_data == uncompressed_data
        mock_progress_reporter.start_compression.assert_called_once_with(len(uncompressed_data))
        mock_progress_reporter.update_compression.assert_not_called()
        mock_progress_reporter.finish_compression.assert_called_once()

    def test_compress_nonexistent_file(self, compressor, mock_path, mock_progress_reporter):
        """Test compressing a nonexistent file raises error."""
        output = io.BytesIO()
        
        with patch("builtins.open", side_effect=OSError("Error reading file")):
            with pytest.raises(CompressorInputPathError, match="Error reading file"):
                compressor.compress(mock_path, output, DEFAULT_CHUNK_SIZE, mock_progress_reporter)

        mock_progress_reporter.start_compression.assert_called_once()
        mock_progress_reporter.update_compression.assert_not_called()
        mock_progress_reporter.finish_compression.assert_called_once()

    def test_gzip_compress_with_custom_chunk_size(self, compressor, mock_path, mock_progress_reporter):
        """Test compression with custom chunk size."""
        uncompressed_data = b"Test data for compression."
        mock_path.stat.return_value.st_size = len(uncompressed_data)
        chunk_size = 5
        expected_update_compression_call_count = math.ceil(len(uncompressed_data) / chunk_size)

        with patch("builtins.open", mock_open(read_data=uncompressed_data)):
            output = io.BytesIO()
            compressor.compress(mock_path, output, chunk_size, mock_progress_reporter)
            
        output.seek(0)
        with gzip.GzipFile(fileobj=output, mode="rb") as gz:
            decompressed_data = gz.read()
            
        assert decompressed_data == uncompressed_data
        mock_progress_reporter.start_compression.assert_called_once_with(len(uncompressed_data))
        assert mock_progress_reporter.update_compression.call_count == expected_update_compression_call_count
        mock_progress_reporter.finish_compression.assert_called_once()


class TestTarGzipCompressor:
    """Test the TarGzipCompressor class."""

    @pytest.fixture
    def compressor(self):
        """Create a TarGzipCompressor instance."""
        return TarGzipCompressor()

    @pytest.fixture
    def mock_progress_reporter(self):
        """Create a mock progress reporter."""
        return Mock()

    @pytest.fixture
    def mock_directory(self):
        """Create a mock directory structure."""
        mock_dir = Mock(spec=Path)
        mock_dir.name = "test_dir"
        
        mock_file1 = Mock(spec=Path)
        mock_file1.is_file.return_value = True
        mock_file1.stat.return_value.st_size = 17  # len(b"Content of file 1")
        
        mock_file2 = Mock(spec=Path)
        mock_file2.is_file.return_value = True
        mock_file2.stat.return_value.st_size = 17  # len(b"Content of file 2")
        
        mock_file3 = Mock(spec=Path)
        mock_file3.is_file.return_value = True
        mock_file3.stat.return_value.st_size = 17  # len(b"Content of file 3")
        
        mock_dir.rglob.return_value = [mock_file1, mock_file2, mock_file3]
        
        return mock_dir

    def test_compress_directory(self, compressor, mock_directory, mock_progress_reporter):
        """Test compressing a directory."""
        output = io.BytesIO()
        
        mock_tarinfo1 = Mock(spec=tarfile.TarInfo)
        mock_tarinfo1.isfile.return_value = True
        mock_tarinfo1.size = 11
        mock_tarinfo1.name = "test_dir/file1.txt"
        
        mock_tarinfo2 = Mock(spec=tarfile.TarInfo)
        mock_tarinfo2.isfile.return_value = True
        mock_tarinfo2.size = 12
        mock_tarinfo2.name = "test_dir/file2.txt"
        
        mock_tarinfo3 = Mock(spec=tarfile.TarInfo)
        mock_tarinfo3.isfile.return_value = True
        mock_tarinfo3.size = 13
        mock_tarinfo3.name = "test_dir/subdir/file3.txt"
        
        with patch('tarfile.open') as mock_tarfile:
            mock_tar = Mock()
            mock_tarfile.return_value.__enter__.return_value = mock_tar
            
            def mock_add_side_effect(path, arcname, filter):
                for tarinfo in [mock_tarinfo1, mock_tarinfo2, mock_tarinfo3]:
                    filter(tarinfo)
            
            mock_tar.add.side_effect = mock_add_side_effect
            
            compressor.compress(mock_directory, output, DEFAULT_CHUNK_SIZE, mock_progress_reporter)
            
            mock_tarfile.assert_called_once_with(fileobj=output, mode="w:gz")
            mock_tar.add.assert_called_once()
        
        mock_progress_reporter.start_compression.assert_called_once_with(51)  # 3 files * 17 bytes each
        assert mock_progress_reporter.update_compression.call_count == 3
        mock_progress_reporter.finish_compression.assert_called_once()

    def test_compress_empty_directory(self, compressor, mock_progress_reporter):
        """Test compressing an empty directory."""
        mock_dir = Mock(spec=Path)
        mock_dir.name = "empty_dir"
        mock_dir.rglob.return_value = []  # No files
        
        output = io.BytesIO()
        
        with patch('tarfile.open') as mock_tarfile:
            mock_tar = Mock()
            mock_tarfile.return_value.__enter__.return_value = mock_tar
            
            compressor.compress(mock_dir, output, DEFAULT_CHUNK_SIZE, mock_progress_reporter)
        
        mock_progress_reporter.start_compression.assert_called_once_with(0)
        mock_progress_reporter.finish_compression.assert_called_once()

    def test_compress_nonexistent_directory(self, compressor, mock_progress_reporter):
        """Test compressing a nonexistent directory raises error."""
        nonexistent_path = Path("/nonexistent/directory")
        output = io.BytesIO()
        
        with pytest.raises(CompressorInputPathError, match="Error creating tar archive"):
            compressor.compress(nonexistent_path, output, DEFAULT_CHUNK_SIZE, mock_progress_reporter)
        
        mock_progress_reporter.finish_compression.assert_called_once()

    def test_calculate_total_size(self, compressor, mock_directory):
        """Test calculating total size of directory."""
        total_size = compressor._calculate_total_size(mock_directory)
        
        expected_size = 51  # 3 files * 17 bytes each
        assert total_size == expected_size

    def test_calculate_total_size_permission_error(self, compressor):
        """Test calculating total size with permission error."""
        with patch('pathlib.Path.rglob') as mock_rglob:
            mock_rglob.side_effect = OSError("Permission denied")
            
            with pytest.raises(CompressorCalculateTotalSizeError, match="Error calculating total size"):
                compressor._calculate_total_size(Path("/some/path"))


class TestChunkWriter:
    """Test the ChunkWriter class."""

    @pytest.fixture
    def chunk_queue(self):
        """Create a queue for chunks."""
        return queue.Queue()

    def test_init(self, chunk_queue):
        """Test ChunkWriter initialization."""
        writer = ChunkWriter(chunk_queue, chunk_size=1024)
        
        assert writer._chunk_size == 1024
        assert writer.total_enqueued == 0
        assert not writer._closed
        assert writer.writable()
        assert not writer.readable()
        assert not writer.seekable()

    def test_write_small_data(self, chunk_queue):
        """Test writing data smaller than chunk size."""
        writer = ChunkWriter(chunk_queue, chunk_size=100)
        data = b"small data"
        
        bytes_written = writer.write(data)
        
        assert bytes_written == len(data)
        assert writer.total_enqueued == 0
        assert chunk_queue.empty()

    def test_write_large_data(self, chunk_queue):
        """Test writing data larger than chunk size."""
        chunk_size = 10
        writer = ChunkWriter(chunk_queue, chunk_size=chunk_size)
        data = b"this is a long piece of data"
        
        bytes_written = writer.write(data)
        
        assert bytes_written == len(data)
        assert writer.total_enqueued > 0
        assert not chunk_queue.empty()
        
        chunks = []
        while not chunk_queue.empty():
            chunk = chunk_queue.get()
            if chunk is not None:
                chunks.append(chunk)
        
        for chunk in chunks[:-1]:
            assert len(chunk) == chunk_size

    def test_flush(self, chunk_queue):
        """Test flushing remaining buffer."""
        writer = ChunkWriter(chunk_queue, chunk_size=100)
        data = b"small data"
        
        writer.write(data)
        writer.flush()
        
        assert writer.total_enqueued == len(data)
        chunks = []
        while not chunk_queue.empty():
            chunk = chunk_queue.get()
            chunks.append(chunk)
        
        assert chunks[-1] is None  # Sentinel
        assert chunks[0] == data

    def test_close(self, chunk_queue):
        """Test closing the writer."""
        writer = ChunkWriter(chunk_queue)
        data = b"test data"
        
        writer.write(data)
        writer.close()
        
        assert writer._closed
        assert not writer.writable()
        
        assert writer.total_enqueued == len(data)

    def test_context_manager(self, chunk_queue):
        """Test using ChunkWriter as context manager."""
        data = b"test data"
        
        with ChunkWriter(chunk_queue) as writer:
            writer.write(data)
        
        assert writer._closed
        assert writer.total_enqueued == len(data)

    def test_write_after_close(self, chunk_queue):
        """Test writing after close raises error."""
        writer = ChunkWriter(chunk_queue)
        writer.close()
        
        with pytest.raises(ValueError, match="I/O operation on closed file"):
            writer.write(b"data")

    def test_not_implemented_methods(self, chunk_queue):
        """Test that read methods are not implemented."""
        writer = ChunkWriter(chunk_queue)
        
        with pytest.raises(NotImplementedError, match="ChunkWriter is write-only"):
            writer.read()
        
        with pytest.raises(NotImplementedError, match="ChunkWriter is write-only"):
            writer.readline()

    def test_thread_safety(self, chunk_queue):
        """Test thread safety of ChunkWriter."""
        writer = ChunkWriter(chunk_queue, chunk_size=10)
        results = []
        
        def write_data(data):
            try:
                result = writer.write(data)
                results.append(result)
            except Exception as e:
                results.append(e)
        
        threads = []
        for i in range(5):
            data = f"data{i}".encode()
            thread = threading.Thread(target=write_data, args=(data,))
            threads.append(thread)
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        assert len(results) == 5
        assert all(isinstance(r, int) for r in results)

    def test_default_chunk_size(self, chunk_queue):
        """Test default chunk size is used."""
        writer = ChunkWriter(chunk_queue)
        assert writer._chunk_size == DEFAULT_CHUNK_SIZE
