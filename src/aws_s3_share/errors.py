class S3ShareError(Exception):
    """Base exception for all aws-s3-share errors."""

    pass


class ConfigError(S3ShareError):
    """Base class for configuration errors."""

    pass


class ConfigFileNotFoundError(ConfigError):
    """Raised when the config file cannot be found."""

    pass


class ConfigFormatError(ConfigError):
    """Raised when the config file format is invalid."""

    pass


class ConfigPermissionError(ConfigError):
    """Raised when there's a permission issue reading the config file."""

    pass


class S3PresignedURLError(S3ShareError):
    """Raised when generating a pre-signed URL fails."""

    pass


class S3UploadError(S3ShareError):
    """Base class for S3 upload errors."""

    pass


class S3UploadProfileNotFoundError(S3UploadError):
    """Raised when the specified AWS profile is not found."""

    pass


class S3UploadMultipartError(S3UploadError):
    """Raised when a multipart upload error occurs."""

    pass


class S3UploadTimeoutError(S3UploadError):
    """Raised when an upload operation times out."""

    pass


class CompressorError(S3ShareError):
    """Base class for compressor errors."""

    pass


class CompressorInputPathError(CompressorError):
    """Raised when the input path for a compressor is not found, not readable, or of the wrong type."""

    pass


class CompressorCalculateTotalSizeError(CompressorError):
    """Raised when calculating the size of a directory fails."""

    pass


class CompressorCompressionError(CompressorError):
    """Raised when compression operation fails."""

    pass


class AWSClientError(Exception):
    """Base class for AWS-related errors."""

    pass


class AWSClientProfileNotFoundError(AWSClientError):
    """Raised when the specified AWS profile is not found."""

    pass


class ValidationError(Exception):
    """Base class for validation errors."""

    pass


class InputPathValidationError(ValidationError):
    """Raised when the input path is invalid or does not exist."""

    pass
