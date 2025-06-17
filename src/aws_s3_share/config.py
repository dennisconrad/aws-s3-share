try:
    from tomllib import load as toml_load, TOMLDecodeError  # Use built-in tomllib for Python 3.11+
except ImportError:
    from tomli import load as toml_load, TOMLDecodeError  # Fallback to external tomli for earlier Python versions

from typing import TypedDict
from aws_s3_share.errors import ConfigFileNotFoundError, ConfigFormatError, ConfigPermissionError
from pathlib import Path
import os
from aws_s3_share.util import validate_and_resolve_input_path

CONFIG_FILE_NAME = "aws-s3-share.toml"
MIN_EXPIRY_SECONDS = 1
MAX_EXPIRY_SECONDS = 604_800  # 7 days
DEFAULT_EXPIRY_SECONDS = 3_600  # 1 hour
POSIX_CONFIG_SUBDIR = ".config"


class S3ShareConfig(TypedDict):
    """Configuration for S3 operations."""

    path: Path
    bucket: str
    expiry: int
    profile: str | None


def get_config_path() -> Path:
    """
    Returns the platform-specific path for the aws-s3-share configuration file.

    On Windows, it tries to use the %APPDATA% environment variable.  If not set
    or empty, it defaults to %USERPROFILE%\\AppData\\Roaming\\aws-s3-share.toml.
    On POSIX systems (Linux, macOS, etc.), it uses $HOME/.config/aws-s3-share.toml.

    Returns:
        Path: The absolute path to the configuration file.
    """
    if os.name == "nt":  # Windows
        appdata = os.environ.get("APPDATA")
        if appdata:
            base_dir = Path(appdata)
        else:
            # Fallback if %APPDATA% is not set or is an empty string
            base_dir = Path.home() / "AppData" / "Roaming"
    else:  # POSIX system (macOS, Linux, etc.)
        base_dir = Path.home() / POSIX_CONFIG_SUBDIR

    return base_dir / CONFIG_FILE_NAME


def validate_config(config: S3ShareConfig) -> None:
    """
    Validate the merged configuration.

    Ensures that required configuration values are present and within acceptable ranges.

    Args:
        config (S3ShareConfig): The configuration dictionary to validate.

    Raises:
        ConfigFormatError: If the bucket is missing or if expiry is not within valid range.
    """
    if not config.get("bucket"):
        raise ConfigFormatError("Please provide the 'bucket' option.")

    expiry = config.get("expiry")
    if not isinstance(expiry, int) or not MIN_EXPIRY_SECONDS <= expiry <= MAX_EXPIRY_SECONDS:
        raise ConfigFormatError(
            f"'expiry' must be an integer between {MIN_EXPIRY_SECONDS} and {MAX_EXPIRY_SECONDS}, got {expiry}"
        )


def read_config_file(path: Path) -> dict:
    """
    Reads and parses the TOML configuration file.

    Args:
        path (pathlib.Path): The path to the configuration file.

    Returns:
        dict: The parsed configuration.

    Raises:
        ConfigFileNotFoundError: If the configuration file does not exist.
        ConfigFormatError: If the file content is not valid TOML.
        ConfigPermissionError: If reading the file is denied due to permissions.
    """
    if not path.exists():
        raise ConfigFileNotFoundError(f"Configuration file {path} does not exist")

    try:
        with open(path, "rb") as config_file:
            config = toml_load(config_file)
    except PermissionError as e:
        raise ConfigPermissionError(f"Permission denied while trying to read {path}") from e
    except TOMLDecodeError as e:
        raise ConfigFormatError(f"Error decoding TOML file {path}: {e}") from e

    return config


def verify_and_build_config(path: Path, bucket: str | None, expiry: int | None, profile: str | None) -> S3ShareConfig:
    """
    Merges the provided configuration with the configuration from the file.

    Args:
        path (Path): The path to the file or directory to be uploaded.
        bucket (str | None): The S3 bucket name.
        expiry (int | None): The expiry time for the pre-signed URL in seconds.
        profile (str | None): The AWS profile to use.

    Returns:
        S3ShareConfig: The merged configuration.
    """
    config_from_file = {}
    try:
        config_from_file = read_config_file(get_config_path())
    except ConfigFileNotFoundError:
        pass  # If the config file does not exist, we'll use the provided options or defaults
    except (ConfigPermissionError, ConfigFormatError) as e:
        raise e

    config = {
        "path": validate_and_resolve_input_path(path),
        "bucket": bucket or config_from_file.get("bucket"),
        "expiry": expiry
        if expiry != DEFAULT_EXPIRY_SECONDS
        else config_from_file.get("expiry", DEFAULT_EXPIRY_SECONDS),
        "profile": profile or config_from_file.get("profile"),
    }

    validate_config(config)

    return config
