import os
import pytest
from pathlib import Path
from moto import mock_aws
from unittest.mock import Mock
import string
from botocore.exceptions import ClientError

from s3_share.util import (
    get_compressor_for_path, 
    get_s3_client, 
    validate_and_resolve_input_path,
    generate_random_prefix,
    generate_s3_presigned_url
)
from s3_share.compress import GzipCompressor, TarGzipCompressor
from s3_share.errors import AWSClientProfileNotFoundError, InputPathValidationError, S3PresignedURLError


class TestGetS3Client:
    """Tests for the get_s3_client function."""
    
    @mock_aws
    def test_get_s3_client_with_no_profile(self):
        """Test creating an S3 client without specifying a profile."""
        client = get_s3_client()

        assert hasattr(client, "create_multipart_upload")
        assert hasattr(client, "upload_part")
        assert hasattr(client, "complete_multipart_upload")
        assert hasattr(client, "abort_multipart_upload")
        
    @mock_aws 
    def test_get_s3_client_with_default_profile(self):
        """Test creating an S3 client with the default profile."""
        client = get_s3_client(profile="default")

        assert hasattr(client, "create_multipart_upload")
        assert hasattr(client, "upload_part")
        assert hasattr(client, "complete_multipart_upload")
        assert hasattr(client, "abort_multipart_upload")

    @mock_aws
    def test_get_s3_client_with_invalid_profile(self):
        """Test creating an S3 client with an invalid profile."""
        profile_name = "nonexistent-profile"

        with pytest.raises(AWSClientProfileNotFoundError, match=f"AWS profile '{profile_name}' not found"):
            get_s3_client(profile=profile_name)

class TestGetCompressorForPath:
    """Tests for the get_compressor_for_path function.  Please see the NOTES section in the docstring of the function"""

    def test_get_compressor_for_file(self, mocker):
        """Test that a GzipCompressor is returned for an existing file or a valid symlink to a file."""
        mock_pathlib_Path_is_dir = mocker.patch.object(Path, "is_dir", return_value=False)
        test_path = Path("/test/test_file.txt")
 
        assert isinstance(get_compressor_for_path(test_path), GzipCompressor)
        mock_pathlib_Path_is_dir.assert_called_once()

    def test_get_compressor_for_directory(self, mocker):
        """Test that a TarGzipCompressor is returned for an existing directory or a valid symlink to a directory."""
        mock_pathlib_Path_is_dir = mocker.patch.object(Path, "is_dir", return_value=True)
        test_path = Path("/test/test_directory/")

        assert isinstance(get_compressor_for_path(test_path), TarGzipCompressor)
        mock_pathlib_Path_is_dir.assert_called_once()

class TestValidateAndResolveInputPath:
    """Tests for the validate_and_resolve_input_path function."""

    def test_existing_readable_absolute_path(self, mocker):
        """Test validation of an existing readable absolute path."""
        test_path = Path("/test/existing_and_readable_test_file.txt")

        mock_pathlib_Path_resolve = mocker.patch.object(Path, "resolve", return_value=test_path)
        mock_pathlib_Path_is_symlink = mocker.patch.object(Path, "is_symlink")
        mock_os_access = mocker.patch("os.access", return_value=True)

        assert validate_and_resolve_input_path(test_path) == test_path
        mock_pathlib_Path_resolve.assert_called_once_with(strict=True)
        mock_pathlib_Path_is_symlink.assert_not_called()
        mock_os_access.assert_called_once_with(test_path, os.R_OK)

    def test_existing_readable_relative_path(self, mocker):
        """Test validation of an existing readable relative path."""
        test_path = Path("/test/../test/existing_and_readable_test_file.txt")
        test_path_resolved = Path("/test/existing_and_readable_test_file.txt")

        mock_pathlib_Path_resolve = mocker.patch.object(Path, "resolve", return_value=test_path_resolved)
        mock_pathlib_Path_is_symlink = mocker.patch.object(Path, "is_symlink")
        mock_os_access = mocker.patch("os.access", return_value=True)

        assert validate_and_resolve_input_path(test_path) == test_path_resolved
        mock_pathlib_Path_resolve.assert_called_once_with(strict=True)
        mock_pathlib_Path_is_symlink.assert_not_called()
        mock_os_access.assert_called_once_with(test_path_resolved, os.R_OK)

    def test_broken_symlink(self, mocker):
        """Test validation of a broken symlink."""
        test_path = Path("/test/broken_symlink.txt")

        mock_pathlib_Path_resolve = mocker.patch.object(Path, "resolve", side_effect=FileNotFoundError)
        mock_pathlib_Path_is_symlink = mocker.patch.object(Path, "is_symlink", return_value=True)
        mock_os_access = mocker.patch("os.access")

        with pytest.raises(InputPathValidationError, match=f"Path {test_path} is a broken symlink."):
            validate_and_resolve_input_path(test_path)
        mock_pathlib_Path_resolve.assert_called_once_with(strict=True)
        mock_pathlib_Path_is_symlink.assert_called_once()
        mock_os_access.assert_not_called()

    def test_nonexistent_path(self, mocker):
        """Test validation of a nonexistent path."""
        test_path = Path("/test/nonexistent_file.txt")

        mock_pathlib_Path_resolve = mocker.patch.object(Path, "resolve", side_effect=FileNotFoundError)
        mock_pathlib_Path_is_symlink = mocker.patch.object(Path, "is_symlink", return_value=False)
        mock_os_access = mocker.patch("os.access")

        with pytest.raises(InputPathValidationError, match=f"Path {test_path} does not exist."):
            validate_and_resolve_input_path(test_path)
        mock_pathlib_Path_resolve.assert_called_once_with(strict=True)
        mock_pathlib_Path_is_symlink.assert_called_once()
        mock_os_access.assert_not_called()

    def test_invalid_path(self, mocker):
        """Test validation of an invalid path."""
        test_path = Path("/test/invalid_file.txt")

        mock_pathlib_Path_resolve = mocker.patch.object(Path, "resolve", side_effect=OSError)
        mock_pathlib_Path_is_symlink = mocker.patch.object(Path, "is_symlink")
        mock_os_access = mocker.patch("os.access")

        with pytest.raises(InputPathValidationError, match=f"Path {test_path} is not valid."):
            validate_and_resolve_input_path(test_path)
        mock_pathlib_Path_resolve.assert_called_once_with(strict=True)
        mock_pathlib_Path_is_symlink.assert_not_called()
        mock_os_access.assert_not_called()

    def test_non_readable_path(self, mocker):
        """Test validation of an existing path that is not readable."""
        test_path = Path("/test/non_readable_file.txt")

        mock_pathlib_Path_resolve = mocker.patch.object(Path, "resolve", return_value=test_path)
        mock_pathlib_Path_is_symlink = mocker.patch.object(Path, "is_symlink")
        mock_os_access = mocker.patch("os.access", return_value=False)

        with pytest.raises(InputPathValidationError, match=f"Path {test_path} is not readable."):
            validate_and_resolve_input_path(test_path)
        mock_pathlib_Path_resolve.assert_called_once_with(strict=True)
        mock_pathlib_Path_is_symlink.assert_not_called()
        mock_os_access.assert_called_once_with(test_path, os.R_OK)

class TestGenerateRandomPrefix:
    """Tests for the generate_random_prefix function."""

    def test_default_length(self):
        """Test that the default length is 12."""
        prefix = generate_random_prefix()

        assert len(prefix) == 12

    def test_custom_length(self):
        """Test generating a prefix with custom length."""
        length = 8
        prefix = generate_random_prefix(length)

        assert len(prefix) == length

    def test_zero_length(self):
        """Test generating a prefix with zero length."""
        prefix = generate_random_prefix(0)

        assert len(prefix) == 0
        assert prefix == ""

    def test_characters_are_alphanumeric(self):
        """Test that all characters in the prefix are alphanumeric."""
        prefix = generate_random_prefix(100)
        valid_chars = string.ascii_letters + string.digits

        assert all(char in valid_chars for char in prefix)

    def test_randomness(self):
        """Test that multiple calls generate different prefixes."""
        prefixes = [generate_random_prefix(20) for _ in range(10)]

        assert len(set(prefixes)) == len(prefixes)


class TestGenerateS3PresignedUrl:
    """Tests for the generate_s3_presigned_url function."""

    def test_generate_presigned_url_default_expiry(self):
        """Test generating a presigned URL with default expiry."""
        mock_s3_client = Mock()
        mock_s3_client.generate_presigned_url.return_value = "https://example.com/presigned-url"
        
        bucket = "test-bucket"
        key = "test-key"
        
        result = generate_s3_presigned_url(mock_s3_client, bucket, key)
        
        assert result == "https://example.com/presigned-url"
        mock_s3_client.generate_presigned_url.assert_called_once_with(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=3600
        )

    def test_generate_presigned_url_custom_expiry(self):
        """Test generating a presigned URL with custom expiry."""
        mock_s3_client = Mock()
        mock_s3_client.generate_presigned_url.return_value = "https://example.com/presigned-url"
        
        bucket = "test-bucket"
        key = "test-key"
        expiry = 7200
        
        result = generate_s3_presigned_url(mock_s3_client, bucket, key, expiry)
        
        assert result == "https://example.com/presigned-url"
        mock_s3_client.generate_presigned_url.assert_called_once_with(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expiry
        )

    def test_generate_presigned_url_with_special_characters(self):
        """Test generating a presigned URL with special characters in bucket and key."""
        mock_s3_client = Mock()
        mock_s3_client.generate_presigned_url.return_value = "https://example.com/presigned-url-special"
        
        bucket = "test-bucket-with-dashes"
        key = "folder/subfolder/file-name_with.special.chars.txt"
        
        result = generate_s3_presigned_url(mock_s3_client, bucket, key)
        
        assert result == "https://example.com/presigned-url-special"
        mock_s3_client.generate_presigned_url.assert_called_once_with(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=3600
        )

    def test_generate_presigned_url_client_error(self):
        """Test handling of ClientError during presigned URL generation."""
        mock_s3_client = Mock()
        client_error = ClientError(
            error_response={'Error': {'Code': 'NoSuchBucket', 'Message': 'Bucket does not exist'}},
            operation_name='generate_presigned_url'
        )
        mock_s3_client.generate_presigned_url.side_effect = client_error
        
        bucket = "nonexistent-bucket"
        key = "test-key"
        
        with pytest.raises(S3PresignedURLError, match="Failed to generate pre-signed URL"):
            generate_s3_presigned_url(mock_s3_client, bucket, key)

    @mock_aws
    def test_generate_presigned_url_integration(self):
        """Integration test with a mocked S3 client."""
        import boto3
        from botocore.config import Config

        s3_client = boto3.client("s3", region_name="us-east-1", config=Config(signature_version="v4"))
        s3_client.create_bucket(Bucket="test-bucket")
        s3_client.put_object(Bucket="test-bucket", Key="test-key", Body=b"test-content")
        
        response = generate_s3_presigned_url(s3_client, "test-bucket", "test-key", 7200)
        
        assert "https://test-bucket.s3.amazonaws.com/test-key" in response
        assert "X-Amz-Algorithm=AWS4-HMAC-SHA256" in response
        assert "X-Amz-Expires=7200" in response
