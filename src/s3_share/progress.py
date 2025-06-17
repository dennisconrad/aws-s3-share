from abc import ABC, abstractmethod
import threading

import click


class ProgressReporter(ABC):
    """
    Abstract base class for progress reporting during compression and upload operations.

    This class defines the interface for reporting progress during file compression and S3 upload operations.
    Implementations should provide thread-safe progress reporting mechanisms.
    """

    @abstractmethod
    def start_compression(self, total_bytes: int) -> None:
        """
        Start compression progress reporting.

        Args:
            total_bytes (int): The total number of bytes to be compressed.
        """
        pass  # pragma: no cover

    @abstractmethod
    def update_compression(self, bytes_compressed: int) -> None:
        """
        Update compression progress reporting.

        Args:
            bytes_compressed (int): The number of bytes that have been compressed since the last update.
        """
        pass  # pragma: no cover

    @abstractmethod
    def finish_compression(self) -> None:
        """
        Finish compression progress reporting.

        This method is called when compression is complete to clean up any progress indicators and finalize the display.
        """
        pass  # pragma: no cover

    @abstractmethod
    def start_upload(self, total_bytes: int, initial_bytes: int = 0) -> None:
        """
        Start upload progress reporting.

        Args:
            total_bytes (int): The total number of bytes to be uploaded.
            initial_bytes (int): The number of bytes already uploaded before starting progress reporting.
        """
        pass  # pragma: no cover

    @abstractmethod
    def update_upload(self, bytes_uploaded: int) -> None:
        """
        Update upload progress reporting.

        Args:
            bytes_uploaded (int): The number of bytes that have been uploaded since the last update.
        """
        pass  # pragma: no cover

    @abstractmethod
    def finish_upload(self) -> None:
        """
        Finish upload progress reporting.

        This method is called when upload is complete to clean up any progress indicators and finalize the display.
        """
        pass  # pragma: no cover


class ClickProgressReporter(ProgressReporter):
    """
    Progress reporter implementation using Click progress bars.

    This class provides a concrete implementation of ProgressReporter using Click's built-in progress bar
    functionality. It supports concurrent compression and upload progress reporting with thread-safe operations.

    Attributes:
        _compression_bar: Click progress bar for compression operations.
        _upload_bar: Click progress bar for upload operations.
        _lock: Reentrant lock for thread-safe access to progress bars.
    """

    def __init__(self):
        """
        Initialize the ClickProgressReporter.

        Sets up the internal state with no active progress bars and initializes the thread lock for safe concurrent
        access.
        """
        self._compression_bar = None
        self._upload_bar = None
        self._lock = threading.RLock()

    def start_compression(self, total_size: int) -> None:
        """
        Start compression progress bar.

        Creates a new Click progress bar for compression operations. If a compression progress bar is already active,
        it will be finished before starting the new one.

        Args:
            total_size (int): The total number of bytes to be compressed.
        """
        with self._lock:
            if self._compression_bar is not None:
                self._compression_bar.finish()
            self._compression_bar = click.progressbar(length=total_size, label="Compressing")

    def update_compression(self, bytes_compressed: int) -> None:
        """
        Update compression progress bar.

        Updates the compression progress bar with the number of bytes compressed since the last update. This method is
        thread-safe and can be called from multiple threads.

        Args:
            bytes_compressed (int): The number of bytes compressed since the last update.
        """
        with self._lock:
            if self._compression_bar is not None:
                self._compression_bar.update(bytes_compressed)

    def finish_compression(self) -> None:
        """
        Finish compression progress bar.

        Completes and closes the compression progress bar, cleaning up any associated resources. This method is
        thread-safe.
        """
        with self._lock:
            if self._compression_bar is not None:
                self._compression_bar.finish()
                self._compression_bar = None

    def start_upload(self, total_size: int, initial_bytes: int = 0) -> None:
        """
        Start upload progress bar.

        Creates a new Click progress bar for upload operations.  If an upload progress bar is already active, it will
        be finished before starting the new one.  Optionally accounts for bytes already uploaded.

        Args:
            total_size (int): The total number of bytes to be uploaded.
            initial_bytes (int): The number of bytes already uploaded before starting progress reporting.
        """
        with self._lock:
            if self._upload_bar is not None:
                self._upload_bar.finish()
            self._upload_bar = click.progressbar(length=total_size, label="Uploading  ")
            if initial_bytes > 0:
                self._upload_bar.update(initial_bytes)

    def update_upload(self, bytes_uploaded: int) -> None:
        """
        Update upload progress bar.

        Updates the upload progress bar with the number of bytes uploaded since the last update.  This method is
        thread-safe and can be called from multiple threads.

        Args:
            bytes_uploaded (int): The number of bytes uploaded since the last update.
        """
        with self._lock:
            if self._upload_bar is not None:
                self._upload_bar.update(bytes_uploaded)

    def finish_upload(self) -> None:
        """
        Finish upload progress bar.

        Completes and closes the upload progress bar, cleaning up any associated resources.  Prints a newline to ensure
        proper terminal formatting after the progress bar completes. This method is thread-safe.
        """
        with self._lock:
            if self._upload_bar is not None:
                self._upload_bar.finish()
                self._upload_bar = None
        print("")  # Ensure a newline after the upload progress bar finishes
