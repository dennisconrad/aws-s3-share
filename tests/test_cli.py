from pathlib import Path
from unittest.mock import patch

from s3_share.cli import get_object_key


class TestGetObjectKey:
    """Test cases for get_object_key function."""

    def test_tar_gzip_extension(self):
        """Test object key generation for TarGzipCompressor."""
        with patch('s3_share.cli.generate_random_prefix', return_value='abc123'):
            result = get_object_key(Path("mydir"), ".tar.gz")
            assert result == "abc123/mydir.tar.gz"

    def test_gzip_extension(self):
        """Test object key generation for GzipCompressor."""
        with patch('s3_share.cli.generate_random_prefix', return_value='xyz789'):
            result = get_object_key(Path("myfile.txt"), ".gz")
            assert result == "xyz789/myfile.txt.gz"
