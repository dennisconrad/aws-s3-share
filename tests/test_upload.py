import pytest
from unittest.mock import Mock, patch, MagicMock
from botocore.exceptions import ClientError, BotoCoreError

from aws_s3_share.upload import Uploader, S3Uploader, S3UploadPartResponse
from aws_s3_share.errors import S3UploadMultipartError


class MockUploader(Uploader):
    """Mock implementation of Uploader for testing."""
    
    def __init__(self):
        self.start_calls = []
        self.upload_part_calls = []
        self.complete_calls = []
        self.abort_calls = []
    
    def start_multipart_upload(self, bucket: str, key: str) -> str:
        self.start_calls.append((bucket, key))
        return f"upload-id-{len(self.start_calls)}"
    
    def upload_part(self, part: bytes, bucket: str, key: str, part_number: int, upload_id: str) -> S3UploadPartResponse:
        self.upload_part_calls.append((part, bucket, key, part_number, upload_id))
        return S3UploadPartResponse(PartNumber=part_number, ETag=f"etag-{part_number}")
    
    def complete_multipart_upload(self, bucket: str, key: str, upload_id: str, parts: list[S3UploadPartResponse]) -> None:
        self.complete_calls.append((bucket, key, upload_id, parts))
    
    def abort_multipart_upload(self, bucket: str, key: str, upload_id: str) -> None:
        self.abort_calls.append((bucket, key, upload_id))


def test_uploader_abc():
    """Test that Uploader is an abstract base class."""
    with pytest.raises(TypeError):
        Uploader()


def test_s3_upload_part_response_type():
    """Test S3UploadPartResponse TypedDict structure."""
    response = S3UploadPartResponse(PartNumber=1, ETag="test-etag")
    assert response["PartNumber"] == 1
    assert response["ETag"] == "test-etag"


def test_mock_uploader():
    """Test the mock uploader implementation."""
    uploader = MockUploader()
    
    upload_id = uploader.start_multipart_upload("test-bucket", "test-key")
    assert upload_id == "upload-id-1"
    assert uploader.start_calls == [("test-bucket", "test-key")]
    
    part_data = b"test data"
    response = uploader.upload_part(part_data, "test-bucket", "test-key", 1, upload_id)
    assert response["PartNumber"] == 1
    assert response["ETag"] == "etag-1"
    assert uploader.upload_part_calls == [(part_data, "test-bucket", "test-key", 1, upload_id)]
    
    parts = [response]
    uploader.complete_multipart_upload("test-bucket", "test-key", upload_id, parts)
    assert uploader.complete_calls == [("test-bucket", "test-key", upload_id, parts)]
    
    uploader.abort_multipart_upload("test-bucket", "test-key", upload_id)
    assert uploader.abort_calls == [("test-bucket", "test-key", upload_id)]


class TestS3Uploader:
    """Test cases for S3Uploader."""
    
    def test_init(self):
        """Test S3Uploader initialization."""
        mock_client = Mock()
        uploader = S3Uploader(mock_client)
        assert uploader._s3_client == mock_client
    
    def test_start_multipart_upload_success(self):
        """Test successful multipart upload start."""
        mock_client = Mock()
        mock_client.create_multipart_upload.return_value = {"UploadId": "test-upload-id"}
        
        uploader = S3Uploader(mock_client)
        upload_id = uploader.start_multipart_upload("test-bucket", "test-key")
        
        assert upload_id == "test-upload-id"
        mock_client.create_multipart_upload.assert_called_once_with(
            Bucket="test-bucket", 
            Key="test-key"
        )
    
    def test_start_multipart_upload_failure(self):
        """Test multipart upload start failure."""
        mock_client = Mock()
        mock_client.create_multipart_upload.side_effect = ClientError(
            {"Error": {"Code": "NoSuchBucket", "Message": "Bucket does not exist"}},
            "CreateMultipartUpload"
        )
        
        uploader = S3Uploader(mock_client)
        
        with pytest.raises(S3UploadMultipartError, match="Failed to start multipart upload"):
            uploader.start_multipart_upload("nonexistent-bucket", "test-key")
    
    def test_upload_part_success(self):
        """Test successful part upload."""
        mock_client = Mock()
        mock_client.upload_part.return_value = {"ETag": '"test-etag"'}
        
        uploader = S3Uploader(mock_client)
        part_data = b"test chunk data"
        
        response = uploader.upload_part(part_data, "test-bucket", "test-key", 1, "test-upload-id")
        
        assert response["PartNumber"] == 1
        assert response["ETag"] == '"test-etag"'
        mock_client.upload_part.assert_called_once_with(
            Body=part_data,
            Bucket="test-bucket",
            Key="test-key",
            PartNumber=1,
            UploadId="test-upload-id"
        )
    
    def test_upload_part_failure(self):
        """Test part upload failure."""
        mock_client = Mock()
        mock_client.upload_part.side_effect = ClientError(
            {"Error": {"Code": "InvalidPartNumber", "Message": "Part number invalid"}},
            "UploadPart"
        )
        
        uploader = S3Uploader(mock_client)
        part_data = b"test chunk data"
        
        with pytest.raises(S3UploadMultipartError, match="Failed to upload part 1"):
            uploader.upload_part(part_data, "test-bucket", "test-key", 1, "test-upload-id")
    
    def test_complete_multipart_upload_success(self):
        """Test successful multipart upload completion."""
        mock_client = Mock()
        uploader = S3Uploader(mock_client)
        
        parts = [
            S3UploadPartResponse(PartNumber=1, ETag='"etag1"'),
            S3UploadPartResponse(PartNumber=2, ETag='"etag2"')
        ]
        
        uploader.complete_multipart_upload("test-bucket", "test-key", "test-upload-id", parts)
        
        mock_client.complete_multipart_upload.assert_called_once_with(
            Bucket="test-bucket",
            Key="test-key",
            UploadId="test-upload-id",
            MultipartUpload={"Parts": parts}
        )
    
    def test_complete_multipart_upload_no_parts(self):
        """Test multipart upload completion with no parts."""
        mock_client = Mock()
        uploader = S3Uploader(mock_client)
        
        with pytest.raises(S3UploadMultipartError, match="No parts were provided"):
            uploader.complete_multipart_upload("test-bucket", "test-key", "test-upload-id", [])
        
        mock_client.abort_multipart_upload.assert_called_once_with(
            Bucket="test-bucket",
            Key="test-key",
            UploadId="test-upload-id"
        )
    
    def test_complete_multipart_upload_no_parts_abort_fails(self):
        """Test multipart upload completion with no parts when abort also fails."""
        mock_client = Mock()
        mock_client.abort_multipart_upload.side_effect = ClientError(
            {"Error": {"Code": "NoSuchUpload", "Message": "Upload not found"}},
            "AbortMultipartUpload"
        )
        
        uploader = S3Uploader(mock_client)
        
        with pytest.raises(S3UploadMultipartError, match="No parts were provided"):
            uploader.complete_multipart_upload("test-bucket", "test-key", "test-upload-id", [])
    
    def test_complete_multipart_upload_failure(self):
        """Test multipart upload completion failure."""
        mock_client = Mock()
        mock_client.complete_multipart_upload.side_effect = ClientError(
            {"Error": {"Code": "InvalidPart", "Message": "One or more parts invalid"}},
            "CompleteMultipartUpload"
        )
        
        uploader = S3Uploader(mock_client)
        parts = [S3UploadPartResponse(PartNumber=1, ETag='"etag1"')]
        
        with pytest.raises(S3UploadMultipartError, match="Failed to complete multipart upload"):
            uploader.complete_multipart_upload("test-bucket", "test-key", "test-upload-id", parts)
    
    def test_abort_multipart_upload_success(self):
        """Test successful multipart upload abort."""
        mock_client = Mock()
        uploader = S3Uploader(mock_client)
        
        uploader.abort_multipart_upload("test-bucket", "test-key", "test-upload-id")
        
        mock_client.abort_multipart_upload.assert_called_once_with(
            Bucket="test-bucket",
            Key="test-key",
            UploadId="test-upload-id"
        )
    
    def test_abort_multipart_upload_failure(self):
        """Test multipart upload abort failure."""
        mock_client = Mock()
        mock_client.abort_multipart_upload.side_effect = ClientError(
            {"Error": {"Code": "NoSuchUpload", "Message": "Upload not found"}},
            "AbortMultipartUpload"
        )
        
        uploader = S3Uploader(mock_client)
        
        with pytest.raises(S3UploadMultipartError, match="Failed to abort multipart upload"):
            uploader.abort_multipart_upload("test-bucket", "test-key", "test-upload-id")
    
    def test_full_upload_workflow(self):
        """Test a complete upload workflow."""
        mock_client = Mock()
        mock_client.create_multipart_upload.return_value = {"UploadId": "workflow-upload-id"}
        mock_client.upload_part.side_effect = [
            {"ETag": '"etag1"'},
            {"ETag": '"etag2"'},
            {"ETag": '"etag3"'}
        ]
        
        uploader = S3Uploader(mock_client)
        
        upload_id = uploader.start_multipart_upload("workflow-bucket", "workflow-key")
        assert upload_id == "workflow-upload-id"
        
        parts = []
        for i in range(1, 4):
            part_data = f"chunk{i}".encode()
            response = uploader.upload_part(part_data, "workflow-bucket", "workflow-key", i, upload_id)
            parts.append(response)
        
        assert len(parts) == 3
        assert all(part["PartNumber"] == i + 1 for i, part in enumerate(parts))
        assert all(part["ETag"] == f'"etag{i + 1}"' for i, part in enumerate(parts))
        
        uploader.complete_multipart_upload("workflow-bucket", "workflow-key", upload_id, parts)
        
        mock_client.create_multipart_upload.assert_called_once()
        assert mock_client.upload_part.call_count == 3
        mock_client.complete_multipart_upload.assert_called_once()
    
    def test_error_handling_with_different_exceptions(self):
        """Test error handling with various exception types."""
        mock_client = Mock()
        uploader = S3Uploader(mock_client)
        
        mock_client.create_multipart_upload.side_effect = BotoCoreError()
        with pytest.raises(S3UploadMultipartError, match="Failed to start multipart upload"):
            uploader.start_multipart_upload("test-bucket", "test-key")
        
        mock_client.upload_part.side_effect = Exception("Network error")
        with pytest.raises(S3UploadMultipartError, match="Failed to upload part 1"):
            uploader.upload_part(b"data", "test-bucket", "test-key", 1, "upload-id")
  