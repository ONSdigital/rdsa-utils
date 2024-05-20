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
