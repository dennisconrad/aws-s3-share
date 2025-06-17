from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, TypedDict

from aws_s3_share.errors import S3UploadMultipartError

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3 import S3Client


class S3UploadPartResponse(TypedDict):
    """
    Response from an upload_part() operation.

    Attributes:
        PartNumber (int): The sequential number of this part in the multipart
                          upload.  Must be between 1 and 10,000.
        ETag (str): The entity tag returned by S3 for this part.  Used to
                    verify the integrity of the uploaded part.
    """

    PartNumber: int
    ETag: str


class Uploader(ABC):
    """
    Abstract base class defining the interface for multipart file uploaders.
    """

    @abstractmethod
    def start_multipart_upload(self, bucket: str, key: str) -> str:
        """
        Start a multipart upload.

        Args:
            bucket (str): The name of the target bucket.
            key (str): The object key under which to store the file.

        Returns:
            str: The upload ID for tracking the multipart upload.
        """
        pass  # pragma: no cover

    @abstractmethod
    def upload_part(self, part: bytes, bucket: str, key: str, part_number: int, upload_id: str) -> S3UploadPartResponse:
        """
        Upload a single part of a multipart upload.

        Args:
            part (bytes): The data chunk to upload.
            bucket (str): The target bucket name.
            key (str): The object key.
            part_number (int): The 1-based index of this part.
            upload_id (str): The multipart upload identifier.

        Returns:
            S3UploadPartResponse: Contains PartNumber and ETag of the uploaded part.
        """
        pass  # pragma: no cover

    @abstractmethod
    def complete_multipart_upload(
        self, bucket: str, key: str, upload_id: str, parts: list[S3UploadPartResponse]
    ) -> None:
        """
        Complete the multipart upload by assembling all uploaded parts.

        Args:
            bucket (str): The target bucket name.
            key (str): The object key.
            upload_id (str): The multipart upload identifier.
            parts (list[S3UploadPartResponse]): Metadata of all uploaded parts.
        """
        pass  # pragma: no cover

    @abstractmethod
    def abort_multipart_upload(self, bucket: str, key: str, upload_id: str) -> None:
        """
        Abort an ongoing multipart upload, discarding any uploaded parts.

        Args:
            bucket (str): The target bucket name.
            key (str): The object key.
            upload_id (str): The multipart upload identifier.
        """
        pass  # pragma: no cover


class S3Uploader(Uploader):
    """S3 implementation of the Uploader interface."""

    def __init__(self, s3_client: S3Client):
        """
        Initialize S3Uploader.

        Args:
            s3_client (S3Client): A boto3 S3 client instance.
        """
        self._s3_client = s3_client

    def start_multipart_upload(self, bucket: str, key: str) -> str:
        """
        Start a multipart upload to S3.

        Args:
            bucket (str): The name of the S3 bucket.
            key (str): The object key under which to store the file.

        Returns:
            str: The upload ID for the multipart upload.

        Raises:
            S3UploadMultipartError: If the upload initiation fails.
        """
        try:
            response = self._s3_client.create_multipart_upload(Bucket=bucket, Key=key)
            return response["UploadId"]
        except Exception as e:
            raise S3UploadMultipartError(f"Failed to start multipart upload: {e}")

    def upload_part(self, part: bytes, bucket: str, key: str, part_number: int, upload_id: str) -> S3UploadPartResponse:
        """
        Upload a single part.

        Args:
            part (bytes): The bytes of the file chunk to upload.
            bucket (str): The name of the S3 bucket.
            key (str): The object key.
            part_number (int): The sequential number for this part (1–10000).
            upload_id (str): The multipart upload identifier.

        Returns:
            S3UploadPartResponse: Metadata of the uploaded part.

        Raises:
            S3UploadMultipartError: If uploading the part fails.
        """
        try:
            response = self._s3_client.upload_part(
                Body=part, Bucket=bucket, Key=key, PartNumber=part_number, UploadId=upload_id
            )
            return {"PartNumber": part_number, "ETag": response["ETag"]}
        except Exception as e:
            raise S3UploadMultipartError(f"Failed to upload part {part_number}: {e}")

    def complete_multipart_upload(
        self, bucket: str, key: str, upload_id: str, parts: list[S3UploadPartResponse]
    ) -> None:
        """
        Complete the multipart upload.

        Args:
            bucket (str): The name of the S3 bucket.
            key (str): The object key.
            upload_id (str): The multipart upload identifier.
            parts (list[S3UploadPartResponse]): Parts to assemble.

        Raises:
            S3UploadMultipartError: If no parts are provided or completion fails.
        """
        if not parts:
            try:
                self.abort_multipart_upload(bucket, key, upload_id)
            except S3UploadMultipartError:
                pass
            raise S3UploadMultipartError("No parts were provided.  Cannot complete multipart upload.")

        try:
            self._s3_client.complete_multipart_upload(
                Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={"Parts": parts}
            )
        except Exception as e:
            raise S3UploadMultipartError(f"Failed to complete multipart upload: {e}")

    def abort_multipart_upload(self, bucket: str, key: str, upload_id: str) -> None:
        """
        Abort the multipart upload.

        Args:
            bucket (str): The name of the S3 bucket.
            key (str): The object key.
            upload_id (str): The multipart upload identifier.

        Raises:
            S3UploadMultipartError: If aborting the upload fails.
        """
        try:
            self._s3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        except Exception as e:
            raise S3UploadMultipartError(f"Failed to abort multipart upload: {e}")
