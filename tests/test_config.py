import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from aws_s3_share.config import (
    S3ShareConfig,
    get_config_path,
    validate_config,
    read_config_file,
    verify_and_build_config,
    CONFIG_FILE_NAME,
    DEFAULT_EXPIRY_SECONDS,
    MAX_EXPIRY_SECONDS,
    MIN_EXPIRY_SECONDS,
    POSIX_CONFIG_SUBDIR,
)
from aws_s3_share.errors import (
    ConfigFileNotFoundError,
    ConfigFormatError,
    ConfigPermissionError,
    InputPathValidationError,
)


class TestGetConfigPath:
    """Tests for the get_config_path function."""

    @patch("os.name", "nt")  # Windows
    @patch("aws_s3_share.config.Path")
    def test_windows_with_appdata(self, mock_config_path):
        """Test Windows path with APPDATA environment variable."""
        mock_config_path_windows = Mock()
        mock_config_path_windows.__truediv__ = Mock(return_value=mock_config_path)
        mock_config_path.return_value = mock_config_path_windows

        with patch.dict(os.environ, {"APPDATA": "C:\\Users\\Test\\AppData\\Roaming"}):
            path = get_config_path()
            
        mock_config_path.assert_called_once_with("C:\\Users\\Test\\AppData\\Roaming")
        assert path == mock_config_path

    @patch("os.name", "nt")  # Windows
    @patch("aws_s3_share.config.Path")
    def test_windows_without_appdata(self, mock_config_path):
        """Test Windows path without APPDATA environment variable."""
        mock_home_path = Mock()
        mock_config_path.home.return_value = mock_home_path
        mock_appdata_path = Mock()
        mock_roaming_path = Mock()
        mock_final_path = Mock()
        
        mock_home_path.__truediv__ = Mock(return_value=mock_appdata_path)
        mock_appdata_path.__truediv__ = Mock(return_value=mock_roaming_path)
        mock_roaming_path.__truediv__ = Mock(return_value=mock_final_path)
        
        with patch.dict(os.environ, {}, clear=True):
            path = get_config_path()
            
        mock_config_path.home.assert_called_once()
        mock_home_path.__truediv__.assert_called_once_with("AppData")
        mock_appdata_path.__truediv__.assert_called_once_with("Roaming")
        mock_roaming_path.__truediv__.assert_called_once_with(CONFIG_FILE_NAME)
        assert path == mock_final_path

    @patch("os.name", "nt")  # Windows
    @patch("aws_s3_share.config.Path")
    def test_windows_with_empty_appdata(self, mock_config_path):
        """Test Windows path with empty APPDATA environment variable."""
        mock_home_path = Mock()
        mock_config_path.home.return_value = mock_home_path
        
        mock_appdata_path = Mock()
        mock_roaming_path = Mock()
        mock_final_path = Mock()
        mock_home_path.__truediv__ = Mock(return_value=mock_appdata_path)
        mock_appdata_path.__truediv__ = Mock(return_value=mock_roaming_path)
        mock_roaming_path.__truediv__ = Mock(return_value=mock_final_path)
        
        with patch.dict(os.environ, {"APPDATA": ""}):
            path = get_config_path()
            
        mock_config_path.home.assert_called_once()
        mock_home_path.__truediv__.assert_called_once_with("AppData")
        mock_appdata_path.__truediv__.assert_called_once_with("Roaming")
        mock_roaming_path.__truediv__.assert_called_once_with(CONFIG_FILE_NAME)
        assert path == mock_final_path

    @patch("os.name", "posix")  # POSIX (Linux/macOS)
    @patch("aws_s3_share.config.Path")
    def test_posix_path(self, mock_path_class):
        """Test POSIX path configuration."""
        mock_home_path = Mock()
        mock_path_class.home.return_value = mock_home_path
        mock_config_path = Mock()
        mock_final_path = Mock()
        mock_home_path.__truediv__ = Mock(return_value=mock_config_path)
        mock_config_path.__truediv__ = Mock(return_value=mock_final_path)
        
        path = get_config_path()
        
        mock_path_class.home.assert_called_once()
        mock_home_path.__truediv__.assert_called_once_with(POSIX_CONFIG_SUBDIR)
        mock_config_path.__truediv__.assert_called_once_with(CONFIG_FILE_NAME)
        assert path == mock_final_path


class TestValidateConfig:
    """Tests for the validate_config function."""

    def test_valid_config(self):
        """Test validation of a valid configuration."""
        config = S3ShareConfig(
            path=Path("/test/path"),
            bucket="test-bucket",
            expiry=3600,
            profile="default"
        )

        validate_config(config)  # Should not raise any exceptions

    def test_empty_bucket(self):
        """Test validation fails when bucket is an empty string."""
        config = S3ShareConfig(
            path=Path("/test/path"),
            bucket="",
            expiry=3600,
            profile="default"
        )
        with pytest.raises(ConfigFormatError, match="Please provide the 'bucket' option"):
            validate_config(config)

    def test_none_bucket(self):
        """Test validation fails when bucket is None."""
        config = S3ShareConfig(
            path=Path("/test/path"),
            bucket=None,
            expiry=3600,
            profile="default"
        )
        with pytest.raises(ConfigFormatError, match="Please provide the 'bucket' option"):
            validate_config(config)

    def test_expiry_too_low(self):
        """Test validation fails when expiry is too low."""
        config = S3ShareConfig(
            path=Path("/test/path"),
            bucket="test-bucket",
            expiry=MIN_EXPIRY_SECONDS - 1,
            profile="default"
        )
        with pytest.raises(ConfigFormatError, match="'expiry' must be an integer between"):
            validate_config(config)

    def test_expiry_too_high(self):
        """Test validation fails when expiry is too high."""
        config = S3ShareConfig(
            path=Path("/test/path"),
            bucket="test-bucket",
            expiry=MAX_EXPIRY_SECONDS + 1,
            profile="default"
        )
        with pytest.raises(ConfigFormatError, match="'expiry' must be an integer between"):
            validate_config(config)

    def test_expiry_not_integer(self):
        """Test validation fails when expiry is not an integer."""
        config = S3ShareConfig(
            path=Path("/test/path"),
            bucket="test-bucket",
            expiry="3600",  # str instead of int
            profile="default"
        )
        with pytest.raises(ConfigFormatError, match="'expiry' must be an integer between"):
            validate_config(config)

    def test_expiry_boundary_values(self):
        """Test validation passes for boundary expiry values."""
        config_min = S3ShareConfig(
            path=Path("/test/path"),
            bucket="test-bucket",
            expiry=MIN_EXPIRY_SECONDS,
            profile="default"
        )
        validate_config(config_min)

        config_max = S3ShareConfig(
            path=Path("/test/path"),
            bucket="test-bucket",
            expiry=MAX_EXPIRY_SECONDS,
            profile="default"
        )
        validate_config(config_max)


class TestReadConfigFile:
    """Tests for the read_config_file function."""

    def test_read_valid_config_file(self):
        """Test reading a valid TOML configuration file."""
        toml_content = """
        bucket = "test-bucket"
        expiry = 7200
        profile = "production"
        """
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            f.write(toml_content)
            f.flush()
            config_path = Path(f.name)
        
        try:
            config = read_config_file(config_path)
            assert config["bucket"] == "test-bucket"
            assert config["expiry"] == 7200
            assert config["profile"] == "production"
        finally:
            config_path.unlink()

    def test_read_nonexistent_file(self):
        """Test reading a nonexistent configuration file."""
        nonexistent_path = Path("/nonexistent/config.toml")
        
        with pytest.raises(ConfigFileNotFoundError, match="Configuration file .* does not exist"):
            read_config_file(nonexistent_path)

    def test_read_invalid_toml_file(self):
        """Test reading an invalid TOML file."""
        invalid_toml = """
        bucket = "test-bucket
        expiry = 7200
        """  # Missing closing quote
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            f.write(invalid_toml)
            f.flush()
            config_path = Path(f.name)
        
        try:
            with pytest.raises(ConfigFormatError, match="Error decoding TOML file"):
                read_config_file(config_path)
        finally:
            config_path.unlink()

    @patch("builtins.open")
    def test_read_permission_error(self, mock_open_func):
        """Test handling permission error when reading config file."""
        mock_path = Mock(spec=Path)
        mock_path.exists.return_value = True
        mock_open_func.side_effect = PermissionError("Permission denied")
        
        with pytest.raises(ConfigPermissionError, match="Permission denied while trying to read"):
            read_config_file(mock_path)

    def test_read_empty_config_file(self):
        """Test reading an empty configuration file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            f.write("")  # Empty file
            f.flush()
            config_path = Path(f.name)
        
        try:
            config = read_config_file(config_path)
            assert config == {}
        finally:
            config_path.unlink()


class TestVerifyAndBuildConfig:
    """Tests for the verify_and_build_config function."""

    @pytest.fixture
    def mock_valid_path(self):
        """Create a mock valid path."""
        mock_path = Mock(spec=Path)
        return mock_path

    @patch("aws_s3_share.config.validate_and_resolve_input_path")
    @patch("aws_s3_share.config.read_config_file")
    @patch("aws_s3_share.config.get_config_path")
    def test_build_config_with_all_args(self, mock_get_config_path, mock_read_config, mock_validate_path):
        """Test building config with all command line arguments provided."""
        mock_resolved_path = Path("/resolved/test/path")
        mock_validate_path.return_value = mock_resolved_path
        mock_read_config.return_value = {}
        mock_get_config_path.return_value = Path("/config/path")
        
        input_path = Path("/test/path")
        bucket = "cli-bucket"
        expiry = 7200
        profile = "cli-profile"
        
        config = verify_and_build_config(input_path, bucket, expiry, profile)
        
        assert config["path"] == mock_resolved_path
        assert config["bucket"] == bucket
        assert config["expiry"] == expiry
        assert config["profile"] == profile

    @patch("aws_s3_share.config.validate_and_resolve_input_path")
    @patch("aws_s3_share.config.read_config_file")
    @patch("aws_s3_share.config.get_config_path")
    def test_build_config_with_file_fallback(self, mock_get_config_path, mock_read_config, mock_validate_path):
        """Test building config with fallback to config file values."""
        mock_resolved_path = Path("/resolved/test/path")
        mock_validate_path.return_value = mock_resolved_path
        mock_read_config.return_value = {
            "bucket": "file-bucket",
            "expiry": 5400,
            "profile": "file-profile"
        }
        mock_get_config_path.return_value = Path("/config/path")
        
        input_path = Path("/test/path")
        
        config = verify_and_build_config(input_path, None, DEFAULT_EXPIRY_SECONDS, None)
        
        assert config["path"] == mock_resolved_path
        assert config["bucket"] == "file-bucket"
        assert config["expiry"] == 5400
        assert config["profile"] == "file-profile"

    @patch("aws_s3_share.config.validate_and_resolve_input_path")
    @patch("aws_s3_share.config.read_config_file")
    @patch("aws_s3_share.config.get_config_path")
    def test_build_config_with_defaults(self, mock_get_config_path, mock_read_config, mock_validate_path):
        """Test building config with default values when file doesn't exist."""
        mock_resolved_path = Path("/resolved/test/path")
        mock_validate_path.return_value = mock_resolved_path
        mock_read_config.side_effect = ConfigFileNotFoundError("File not found")
        mock_get_config_path.return_value = Path("/config/path")
        
        input_path = Path("/test/path")
        bucket = "test-bucket"
        
        config = verify_and_build_config(input_path, bucket, DEFAULT_EXPIRY_SECONDS, None)
        
        assert config["path"] == mock_resolved_path
        assert config["bucket"] == bucket
        assert config["expiry"] == DEFAULT_EXPIRY_SECONDS
        assert config["profile"] is None

    @patch("aws_s3_share.config.validate_and_resolve_input_path")
    @patch("aws_s3_share.config.read_config_file")
    @patch("aws_s3_share.config.get_config_path")
    def test_build_config_cli_overrides_file(self, mock_get_config_path, mock_read_config, mock_validate_path):
        """Test that CLI arguments override config file values."""
        mock_resolved_path = Path("/resolved/test/path")
        mock_validate_path.return_value = mock_resolved_path
        mock_read_config.return_value = {
            "bucket": "file-bucket",
            "expiry": 5400,
            "profile": "file-profile"
        }
        mock_get_config_path.return_value = Path("/config/path")
        
        input_path = Path("/test/path")
        cli_bucket = "cli-bucket"
        cli_expiry = 7200
        cli_profile = "cli-profile"
        
        config = verify_and_build_config(input_path, cli_bucket, cli_expiry, cli_profile)
        
        assert config["bucket"] == cli_bucket
        assert config["expiry"] == cli_expiry
        assert config["profile"] == cli_profile

    @patch("aws_s3_share.config.validate_and_resolve_input_path")
    @patch("aws_s3_share.config.read_config_file")
    @patch("aws_s3_share.config.get_config_path")
    def test_build_config_permission_error_propagated(self, mock_get_config_path, mock_read_config, mock_validate_path):
        """Test that config file permission errors are propagated."""
        mock_validate_path.return_value = Path("/resolved/path")
        mock_read_config.side_effect = ConfigPermissionError("Permission denied")
        mock_get_config_path.return_value = Path("/config/path")
        
        with pytest.raises(ConfigPermissionError, match="Permission denied"):
            verify_and_build_config(Path("/test/path"), "bucket", 3600, None)

    @patch("aws_s3_share.config.validate_and_resolve_input_path")
    @patch("aws_s3_share.config.read_config_file")
    @patch("aws_s3_share.config.get_config_path")
    def test_build_config_format_error_propagated(self, mock_get_config_path, mock_read_config, mock_validate_path):
        """Test that config file format errors are propagated."""
        mock_validate_path.return_value = Path("/resolved/path")
        mock_read_config.side_effect = ConfigFormatError("Invalid TOML")
        mock_get_config_path.return_value = Path("/config/path")
        
        with pytest.raises(ConfigFormatError, match="Invalid TOML"):
            verify_and_build_config(Path("/test/path"), "bucket", 3600, None)

    @patch("aws_s3_share.config.validate_and_resolve_input_path")
    @patch("aws_s3_share.config.read_config_file")
    @patch("aws_s3_share.config.get_config_path")
    def test_build_config_validation_error(self, mock_get_config_path, mock_read_config, mock_validate_path):
        """Test that config validation errors are raised."""
        mock_validate_path.return_value = Path("/resolved/path")
        mock_read_config.return_value = {}
        mock_get_config_path.return_value = Path("/config/path")
        
        with pytest.raises(ConfigFormatError, match="Please provide the 'bucket' option"):
            verify_and_build_config(Path("/test/path"), None, 3600, None)

    @patch("aws_s3_share.config.validate_and_resolve_input_path")
    @patch("aws_s3_share.config.read_config_file")
    @patch("aws_s3_share.config.get_config_path")
    def test_build_config_input_path_validation_error(self, mock_get_config_path, mock_read_config, mock_validate_path):
        """Test that input path validation errors are propagated."""
        mock_validate_path.side_effect = InputPathValidationError("Invalid path")
        mock_read_config.return_value = {}
        mock_get_config_path.return_value = Path("/config/path")
        
        with pytest.raises(InputPathValidationError, match="Invalid path"):
            verify_and_build_config(Path("/invalid/path"), "bucket", 3600, None)

    @patch("aws_s3_share.config.validate_and_resolve_input_path")
    @patch("aws_s3_share.config.read_config_file")
    @patch("aws_s3_share.config.get_config_path")
    def test_expiry_precedence_logic(self, mock_get_config_path, mock_read_config, mock_validate_path):
        """Test the complex expiry precedence logic."""
        mock_validate_path.return_value = Path("/resolved/path")
        mock_read_config.return_value = {"bucket": "test-bucket", "expiry": 5400}
        mock_get_config_path.return_value = Path("/config/path")
        
        config1 = verify_and_build_config(Path("/test/path"), "bucket", DEFAULT_EXPIRY_SECONDS, None)
        assert config1["expiry"] == 5400 
        
        config2 = verify_and_build_config(Path("/test/path"), "bucket", 7200, None)
        assert config2["expiry"] == 7200 
        
        mock_read_config.return_value = {"bucket": "test-bucket"}
        config3 = verify_and_build_config(Path("/test/path"), "bucket", DEFAULT_EXPIRY_SECONDS, None)
        assert config3["expiry"] == DEFAULT_EXPIRY_SECONDS
