import pytest
import threading
import time
from unittest.mock import Mock, patch, MagicMock

from aws_s3_share.progress import ProgressReporter, ClickProgressReporter


class MockProgressReporter(ProgressReporter):
    """Mock implementation of ProgressReporter for testing."""
    
    def __init__(self):
        self.compression_started = False
        self.compression_updates = []
        self.compression_finished = False
        self.upload_started = False
        self.upload_updates = []
        self.upload_finished = False
        self.start_compression_args = None
        self.start_upload_args = None
    
    def start_compression(self, total_bytes: int) -> None:
        self.compression_started = True
        self.start_compression_args = (total_bytes,)
    
    def update_compression(self, bytes_compressed: int) -> None:
        self.compression_updates.append(bytes_compressed)
    
    def finish_compression(self) -> None:
        self.compression_finished = True
    
    def start_upload(self, total_bytes: int, initial_bytes: int = 0) -> None:
        self.upload_started = True
        self.start_upload_args = (total_bytes, initial_bytes)
    
    def update_upload(self, bytes_uploaded: int) -> None:
        self.upload_updates.append(bytes_uploaded)
    
    def finish_upload(self) -> None:
        self.upload_finished = True


def test_progress_reporter_abc():
    """Test that ProgressReporter is an abstract base class."""
    with pytest.raises(TypeError):
        ProgressReporter()


def test_mock_progress_reporter():
    """Test the mock progress reporter works correctly."""
    reporter = MockProgressReporter()
    
    reporter.start_compression(1000)
    assert reporter.compression_started
    assert reporter.start_compression_args == (1000,)
    
    reporter.update_compression(100)
    reporter.update_compression(200)
    assert reporter.compression_updates == [100, 200]
    
    reporter.finish_compression()
    assert reporter.compression_finished
    
    reporter.start_upload(5000, 500)
    assert reporter.upload_started
    assert reporter.start_upload_args == (5000, 500)
    
    reporter.update_upload(300)
    reporter.update_upload(400)
    assert reporter.upload_updates == [300, 400]
    
    reporter.finish_upload()
    assert reporter.upload_finished


def test_mock_progress_reporter_upload_default_initial():
    """Test mock progress reporter with default initial bytes."""
    reporter = MockProgressReporter()
    
    reporter.start_upload(1000)
    assert reporter.upload_started
    assert reporter.start_upload_args == (1000, 0)


class TestClickProgressReporter:
    """Test cases for ClickProgressReporter."""
    
    def test_init(self):
        """Test ClickProgressReporter initialization."""
        reporter = ClickProgressReporter()
        assert reporter._compression_bar is None
        assert reporter._upload_bar is None
        assert reporter._lock is not None
        assert isinstance(reporter._lock, type(threading.RLock()))
    
    @patch('aws_s3_share.progress.click.progressbar')
    def test_start_compression(self, mock_progressbar):
        """Test starting compression progress bar."""
        mock_bar = Mock()
        mock_progressbar.return_value = mock_bar
        
        reporter = ClickProgressReporter()
        reporter.start_compression(1000)
        
        mock_progressbar.assert_called_once_with(length=1000, label="Compressing")
        assert reporter._compression_bar == mock_bar
    
    @patch('aws_s3_share.progress.click.progressbar')
    def test_start_compression_replaces_existing(self, mock_progressbar):
        """Test that starting compression finishes existing bar."""
        old_bar = Mock()
        new_bar = Mock()
        mock_progressbar.return_value = new_bar
        
        reporter = ClickProgressReporter()
        reporter._compression_bar = old_bar
        
        reporter.start_compression(2000)
        
        old_bar.finish.assert_called_once()
        mock_progressbar.assert_called_once_with(length=2000, label="Compressing")
        assert reporter._compression_bar == new_bar
    
    def test_update_compression(self):
        """Test updating compression progress."""
        mock_bar = Mock()
        reporter = ClickProgressReporter()
        reporter._compression_bar = mock_bar
        
        reporter.update_compression(100)
        
        mock_bar.update.assert_called_once_with(100)
    
    def test_update_compression_no_bar(self):
        """Test updating compression when no bar exists."""
        reporter = ClickProgressReporter()
        
        reporter.update_compression(100)  # Should not raise an exception
    
    def test_update_compression_multiple_calls(self):
        """Test multiple compression updates."""
        mock_bar = Mock()
        reporter = ClickProgressReporter()
        reporter._compression_bar = mock_bar
        
        reporter.update_compression(100)
        reporter.update_compression(200)
        reporter.update_compression(150)
        
        assert mock_bar.update.call_count == 3
        mock_bar.update.assert_any_call(100)
        mock_bar.update.assert_any_call(200)
        mock_bar.update.assert_any_call(150)
    
    def test_finish_compression(self):
        """Test finishing compression progress."""
        mock_bar = Mock()
        reporter = ClickProgressReporter()
        reporter._compression_bar = mock_bar
        
        reporter.finish_compression()
        
        mock_bar.finish.assert_called_once()
        assert reporter._compression_bar is None
    
    def test_finish_compression_no_bar(self):
        """Test finishing compression when no bar exists."""
        reporter = ClickProgressReporter()
        
        reporter.finish_compression()  # Should not raise an exception
    
    def test_finish_compression_multiple_calls(self):
        """Test that multiple finish calls are safe."""
        mock_bar = Mock()
        reporter = ClickProgressReporter()
        reporter._compression_bar = mock_bar
        
        reporter.finish_compression()
        reporter.finish_compression()  # Should not raise exception
        
        mock_bar.finish.assert_called_once()
        assert reporter._compression_bar is None
    
    @patch('aws_s3_share.progress.click.progressbar')
    def test_start_upload(self, mock_progressbar):
        """Test starting upload progress bar."""
        mock_bar = Mock()
        mock_progressbar.return_value = mock_bar
        
        reporter = ClickProgressReporter()
        reporter.start_upload(5000)
        
        mock_progressbar.assert_called_once_with(length=5000, label="Uploading  ")
        assert reporter._upload_bar == mock_bar
        mock_bar.update.assert_not_called()
    
    @patch('aws_s3_share.progress.click.progressbar')
    def test_start_upload_with_initial_bytes(self, mock_progressbar):
        """Test starting upload progress bar with initial bytes."""
        mock_bar = Mock()
        mock_progressbar.return_value = mock_bar
        
        reporter = ClickProgressReporter()
        reporter.start_upload(5000, 1000)
        
        mock_progressbar.assert_called_once_with(length=5000, label="Uploading  ")
        mock_bar.update.assert_called_once_with(1000)
        assert reporter._upload_bar == mock_bar
    
    @patch('aws_s3_share.progress.click.progressbar')
    def test_start_upload_with_zero_initial_bytes(self, mock_progressbar):
        """Test starting upload with explicitly zero initial bytes."""
        mock_bar = Mock()
        mock_progressbar.return_value = mock_bar
        
        reporter = ClickProgressReporter()
        reporter.start_upload(5000, 0)
        
        mock_progressbar.assert_called_once_with(length=5000, label="Uploading  ")
        mock_bar.update.assert_not_called()
        assert reporter._upload_bar == mock_bar
    
    @patch('aws_s3_share.progress.click.progressbar')
    def test_start_upload_replaces_existing(self, mock_progressbar):
        """Test that starting upload finishes existing bar."""
        old_bar = Mock()
        new_bar = Mock()
        mock_progressbar.return_value = new_bar
        
        reporter = ClickProgressReporter()
        reporter._upload_bar = old_bar
        
        reporter.start_upload(3000)
        
        old_bar.finish.assert_called_once()
        mock_progressbar.assert_called_once_with(length=3000, label="Uploading  ")
        assert reporter._upload_bar == new_bar
    
    def test_update_upload(self):
        """Test updating upload progress."""
        mock_bar = Mock()
        reporter = ClickProgressReporter()
        reporter._upload_bar = mock_bar
        
        reporter.update_upload(500)
        
        mock_bar.update.assert_called_once_with(500)
    
    def test_update_upload_no_bar(self):
        """Test updating upload when no bar exists."""
        reporter = ClickProgressReporter()
        
        reporter.update_upload(500)  # Should not raise an exception
    
    def test_update_upload_multiple_calls(self):
        """Test multiple upload updates."""
        mock_bar = Mock()
        reporter = ClickProgressReporter()
        reporter._upload_bar = mock_bar
        
        reporter.update_upload(300)
        reporter.update_upload(400)
        reporter.update_upload(250)
        
        assert mock_bar.update.call_count == 3
        mock_bar.update.assert_any_call(300)
        mock_bar.update.assert_any_call(400)
        mock_bar.update.assert_any_call(250)
    
    @patch('builtins.print')
    def test_finish_upload(self, mock_print):
        """Test finishing upload progress."""
        mock_bar = Mock()
        reporter = ClickProgressReporter()
        reporter._upload_bar = mock_bar
        
        reporter.finish_upload()
        
        mock_bar.finish.assert_called_once()
        assert reporter._upload_bar is None
        mock_print.assert_called_once_with("")
    
    @patch('builtins.print')
    def test_finish_upload_no_bar(self, mock_print):
        """Test finishing upload when no bar exists."""
        reporter = ClickProgressReporter()
        
        reporter.finish_upload()
        
        mock_print.assert_called_once_with("")
    
    @patch('builtins.print')
    def test_finish_upload_multiple_calls(self, mock_print):
        """Test that multiple finish upload calls are safe."""
        mock_bar = Mock()
        reporter = ClickProgressReporter()
        reporter._upload_bar = mock_bar
        
        reporter.finish_upload()
        reporter.finish_upload()  # Should not raise exception
        
        mock_bar.finish.assert_called_once()
        assert reporter._upload_bar is None
        assert mock_print.call_count == 2
    
    def test_thread_safety_compression(self):
        """Test that compression operations are thread-safe."""
        reporter = ClickProgressReporter()
        exceptions = []
        
        def update_compression():
            try:
                reporter.update_compression(100)
            except Exception as e:
                exceptions.append(e)
        
        # Create multiple threads that update compression
        threads = []
        for _ in range(10):
            thread = threading.Thread(target=update_compression)
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        assert len(exceptions) == 0  # Should not raise exception
    
    def test_thread_safety_upload(self):
        """Test that upload operations are thread-safe."""
        reporter = ClickProgressReporter()
        exceptions = []
        
        def update_upload():
            try:
                reporter.update_upload(100)
            except Exception as e:
                exceptions.append(e)
        
        # Create multiple threads that update upload
        threads = []
        for _ in range(10):
            thread = threading.Thread(target=update_upload)
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        assert len(exceptions) == 0  # Should not raise exception
    
    @patch('aws_s3_share.progress.click.progressbar')
    def test_concurrent_compression_and_upload(self, mock_progressbar):
        """Test concurrent compression and upload operations."""
        compression_bar = Mock()
        upload_bar = Mock()
        mock_progressbar.side_effect = [compression_bar, upload_bar]
        
        reporter = ClickProgressReporter()
        exceptions = []
        
        def compression_operations():
            try:
                reporter.start_compression(1000)
                for i in range(5):
                    reporter.update_compression(100)
                    time.sleep(0.001)
                reporter.finish_compression()
            except Exception as e:
                exceptions.append(e)
        
        def upload_operations():
            try:
                time.sleep(0.001)
                reporter.start_upload(2000, 200)
                for i in range(5):
                    reporter.update_upload(200)
                    time.sleep(0.001)
                reporter.finish_upload()
            except Exception as e:
                exceptions.append(e)
        
        compression_thread = threading.Thread(target=compression_operations)
        upload_thread = threading.Thread(target=upload_operations)
        
        compression_thread.start()
        upload_thread.start()
        
        compression_thread.join()
        upload_thread.join()
        
        assert len(exceptions) == 0
        
        assert mock_progressbar.call_count == 2
        assert compression_bar.update.call_count == 5
        assert compression_bar.finish.call_count == 1
        assert upload_bar.update.call_count == 6
        assert upload_bar.finish.call_count == 1
    
    def test_reentrant_lock_behavior(self):
        """Test that the RLock allows reentrant access."""
        reporter = ClickProgressReporter()
        
        def nested_operation():
            with reporter._lock:
                reporter.update_compression(50)
                with reporter._lock:
                    reporter.update_upload(75)
        
        nested_operation()
    
    @patch('aws_s3_share.progress.click.progressbar')
    def test_full_workflow(self, mock_progressbar):
        """Test a complete compression and upload workflow."""
        compression_bar = Mock()
        upload_bar = Mock()
        mock_progressbar.side_effect = [compression_bar, upload_bar]
        
        reporter = ClickProgressReporter()
        
        reporter.start_compression(1000)
        assert reporter._compression_bar == compression_bar
        
        reporter.update_compression(200)
        reporter.update_compression(300)
        reporter.update_compression(500)
        
        reporter.finish_compression()
        assert reporter._compression_bar is None
        
        reporter.start_upload(2000, 100)
        assert reporter._upload_bar == upload_bar
        
        reporter.update_upload(400)
        reporter.update_upload(600)
        reporter.update_upload(900)
        
        with patch('builtins.print') as mock_print:
            reporter.finish_upload()
            mock_print.assert_called_once_with("")
        
        assert reporter._upload_bar is None
        
        compression_bar.update.assert_any_call(200)
        compression_bar.update.assert_any_call(300)
        compression_bar.update.assert_any_call(500)
        compression_bar.finish.assert_called_once()
        
        upload_bar.update.assert_any_call(100)
        upload_bar.update.assert_any_call(400)
        upload_bar.update.assert_any_call(600)
        upload_bar.update.assert_any_call(900)
        upload_bar.finish.assert_called_once()
