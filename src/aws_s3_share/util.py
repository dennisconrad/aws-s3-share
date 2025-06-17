from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING
import boto3
from botocore.exceptions import ClientError, ProfileNotFound
from botocore.client import Config
from aws_s3_share.compress import Compressor, GzipCompressor, TarGzipCompressor
from aws_s3_share.errors import AWSClientProfileNotFoundError, InputPathValidationError, S3PresignedURLError
import random
import string

if TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3 import S3Client


def get_s3_client(profile: str = None) -> S3Client:
    """
    Creates and returns an S3 client, optionally with a specified AWS profile.

    Args:
        profile (str, optional): The AWS profile to use.

    Returns:
        S3Client: An initialized S3 client.

    Raises:
        AWSClientProfileNotFoundError: If the AWS profile is not found.
    """
    try:
        session = boto3.Session(profile_name=profile)
        return session.client("s3", config=Config(signature_version="v4"))
    except ProfileNotFound as e:
        profile = e.kwargs.get("profile", "default")
        raise AWSClientProfileNotFoundError(f"AWS profile '{profile}' not found.")


def get_compressor_for_path(input_path: Path) -> Compressor:
    """
    Get the appropriate compressor based on input path type.

    Args:
        input_path (Path): The path to the file or directory to be compressed.

    Returns:
        Compressor: Either an instance of TarGzipCompressor (for directories)
        or GzipCompressor (for files), depending on the input path type.

    Notes:
        This function does not perform any validation on the input path; it simply
        checks if the path is a directory to determine which compressor to use. Since
        pathlib.Path.is_dir() returns False for nonexistent files, directories, and
        broken symlinks, it is assumed that the input path has already been validated
        before calling this function, for example, by using
        validate_and_resolve_input_path().
    """

    if input_path.is_dir():
        return TarGzipCompressor()
    else:
        return GzipCompressor()


def validate_and_resolve_input_path(input_path: Path) -> Path:
    """
    Validates that the input path path exists, is not a broken symlink, and is
    readable, and returns the resolved absolute path.

    Args:
        input_path (Path): The path to validate.

    Returns:
        Path: The resolved and validated absolute path.

    Raises:
        InputPathValidationError: If the input path cannot be validated.
    """

    try:
        resolved_path = input_path.resolve(strict=True)
    except FileNotFoundError:
        if input_path.is_symlink():
            raise InputPathValidationError(f"Path {input_path} is a broken symlink.")
        else:
            raise InputPathValidationError(f"Path {input_path} does not exist.")
    except OSError:
        raise InputPathValidationError(f"Path {input_path} is not valid.")

    if not os.access(resolved_path, os.R_OK):
        raise InputPathValidationError(f"Path {input_path} is not readable.")

    return resolved_path


def generate_random_prefix(length: int = 12) -> str:
    """
    Generates a random prefix of the specified length using alphanumeric characters.

    Args:
        length (int): The length of the random prefix to generate. Default is 8.

    Returns:
        str: A random alphanumeric prefix.
    """

    characters = string.ascii_letters + string.digits
    return "".join(random.choice(characters) for _ in range(length))


def generate_s3_presigned_url(s3_client: S3Client, bucket: str, key: str, expiry: int = 3600) -> str:
    """
    Generates a pre-signed URL for an S3 object.

    Args:
        s3_client (S3Client): The S3 client to use.
        bucket (str): The name of the S3 bucket.
        key (str): The key of the S3 object.
        expiry (int): The expiry of the pre-signed URL in seconds.

    Returns:
        str: The pre-signed URL.
    """
    params = {"Bucket": bucket, "Key": key}
    try:
        return s3_client.generate_presigned_url("get_object", Params=params, ExpiresIn=expiry)
    except ClientError as e:
        raise S3PresignedURLError(f"Failed to generate pre-signed URL: {e}")
