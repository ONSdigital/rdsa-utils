"""Tests for the input.py module."""

import pytest

from rdsa_utils.io.input import *


class TestParseJson:
    """Tests for parse_json function."""

    def test_expected(
        self,
        json_config_string,
        expected_standard_config,
    ):
        """Test expected functionality."""
        actual = parse_json(json_config_string)

        assert actual == expected_standard_config


class TestParseToml:
    """Tests for parse_toml function."""

    def test_expected(
        self,
        toml_config_string,
        expected_standard_config,
    ):
        """Test expected functionality."""
        actual = parse_toml(toml_config_string)

        assert actual == expected_standard_config


class TestParseYaml:
    """Tests for parse_yaml function."""

    def test_expected(
        self,
        yaml_config_string,
        expected_standard_config,
    ):
        """Test expected functionality."""
        actual = parse_yaml(yaml_config_string)

        assert actual == expected_standard_config


@pytest.mark.skip(reason="test shell")
class TestReadFile:
    """Tests for read_file function."""

    def test_expected(self):
        """Test expected functionality."""
        pass


class TestFileSize:
    """Tests for file_size function."""

    def test_expected(self, tmp_path):
        """Test expected functionality."""
        # Create a temporary file
        temp_file = tmp_path / "test_file.txt"
        content = "This is a test file."
        temp_file.write_text(content)

        # Get the file size
        actual = file_size(str(temp_file))

        # Assert the file size matches the content length
        assert actual == len(content)

    def test_file_not_found(self):
        """Test behavior when file does not exist."""
        with pytest.raises(FileNotFoundError):
            file_size("non_existent_file.txt")
