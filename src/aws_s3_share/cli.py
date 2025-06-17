import sys
from pathlib import Path

import click

from aws_s3_share.coordinator import Coordinator
from aws_s3_share.progress import ClickProgressReporter
from aws_s3_share.upload import S3Uploader
from aws_s3_share.config import verify_and_build_config, DEFAULT_EXPIRY_SECONDS
from aws_s3_share.errors import (
    ConfigFileNotFoundError,
    ConfigFormatError,
    ConfigPermissionError,
    AWSClientProfileNotFoundError,
)
from aws_s3_share.util import generate_s3_presigned_url, generate_random_prefix, get_compressor_for_path, get_s3_client


def get_object_key(input_path: Path, file_extension: str) -> str:
    """
    Generate S3 object key with random prefix and appropriate file extension based on the compressor type.

    Args:
        input_path (Path): The local file or directory path to generate a key for.
        file_extension (str): The file extension for the compressed files, e.g., '.tar.gz' or '.gz'.

    Returns:
        str: A unique S3 object key in the format "{prefix}/{filename}.{file_extension}".
    """
    prefix = generate_random_prefix()
    return f"{prefix}/{input_path.name}{file_extension}"


@click.command()
@click.argument("path", type=click.Path(exists=True, readable=True, path_type=Path))
@click.option("-b", "--bucket", type=str, help="S3 bucket to upload to")
@click.option(
    "-e",
    "--expiry",
    type=int,
    default=DEFAULT_EXPIRY_SECONDS,
    help=f"Pre-signed URL expiry time in seconds (default: {DEFAULT_EXPIRY_SECONDS})",
)
@click.option("-p", "--profile", type=str, help="AWS profile name to use for authentication")
@click.help_option("-h", "--help", help="Show this message and exit")
@click.version_option(package_name="aws-s3-share", prog_name="aws-s3-share")
def main(path: Path, bucket: str | None, expiry: int, profile: str | None) -> None:
    try:
        # Build configuration
        config = verify_and_build_config(path, bucket, expiry, profile)

        # Set up dependencies
        s3_client = get_s3_client(profile)
        compressor = get_compressor_for_path(config["path"])
        uploader = S3Uploader(s3_client)
        progress_reporter = ClickProgressReporter()

        # Generate object key with appropriate extension
        object_key = get_object_key(config["path"], compressor.file_extension)

        # Coordinate compression and upload
        coordinator = Coordinator(compressor=compressor, uploader=uploader, progress_reporter=progress_reporter)
        coordinator.archive_and_upload(input_path=config["path"], bucket=config["bucket"], key=object_key)

        # Generate and display pre-signed URL
        presigned_url = generate_s3_presigned_url(
            s3_client=s3_client, bucket=config["bucket"], key=object_key, expiry=config["expiry"]
        )

        click.echo(f"\nPre-signed URL: {presigned_url}")

    except (ConfigFileNotFoundError, ConfigFormatError, ConfigPermissionError, AWSClientProfileNotFoundError) as e:
        click.echo(f"Configuration error: {str(e)}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()  # pragma: no cover
