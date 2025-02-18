"""Tests for pyspark_log_parser.py module."""

import json
from typing import Callable

import pytest

from rdsa_utils.helpers.pyspark_log_parser import load_json_log


class TestLoadJsonLog:
    """Tests for load_json_log function."""

    def test_load_valid_json_log(self, tmp_path: Callable) -> None:
        """Test loading a valid JSON log file."""
        # Create a temporary JSON log file
        log_file = tmp_path / "test_log.json"
        log_file.write_text('{"event": "start", "timestamp": "2025-02-18T12:00:00Z"}\n')

        # Load the JSON log data
        log_data = load_json_log(str(log_file))

        # Define the expected result
        expected_data = [{"event": "start", "timestamp": "2025-02-18T12:00:00Z"}]

        # Assert the loaded data matches the expected data
        assert log_data == expected_data

    def test_file_not_found(self) -> None:
        """Test loading a non-existent JSON log file."""
        with pytest.raises(FileNotFoundError):
            load_json_log("./non_existent_file.json")

    def test_invalid_json(self, tmp_path: Callable) -> None:
        """Test loading an invalid JSON log file."""
        # Create a temporary invalid JSON log file
        log_file = tmp_path / "invalid_log.json"
        log_file.write_text('{"event": "start", "timestamp": "2025-02-18T12:00:00Z"\n')

        # Assert that a JSONDecodeError is raised
        with pytest.raises(json.JSONDecodeError):
            load_json_log(str(log_file))
