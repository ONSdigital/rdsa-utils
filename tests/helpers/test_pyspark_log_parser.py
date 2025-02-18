"""Tests for pyspark_log_parser.py module."""

import json
from typing import Callable

import pytest

from rdsa_utils.helpers.pyspark_log_parser import (
    convert_value,
    load_json_log,
    parse_pyspark_logs,
)


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


class TestConvertValue:
    """Tests for convert_value function."""

    def test_convert_milliseconds_to_minutes(self) -> None:
        """Test conversion of milliseconds to minutes."""
        assert convert_value(60000, "ms") == 1  # 60 sec -> 1 min
        assert convert_value(300000, "ms") == 5  # 300 sec -> 5 min

    def test_convert_nanoseconds_to_minutes(self) -> None:
        """Test conversion of nanoseconds to minutes."""
        assert convert_value(6e10, "ns") == 1  # 60 billion ns -> 1 min
        assert convert_value(3e11, "ns") == 5  # 300 billion ns -> 5 min

    def test_convert_bytes_to_megabytes(self) -> None:
        """Test conversion of bytes to megabytes."""
        assert convert_value(1048576, "bytes") == 1  # 1MB in bytes
        assert convert_value(5242880, "bytes") == 5  # 5MB in bytes

    def test_invalid_unit(self) -> None:
        """Test conversion with an invalid unit (should return input as float)."""
        assert convert_value(500, "unknown") == 500.0

    def test_non_numeric_input(self) -> None:
        """Test conversion with non-numeric input (should return 0.0)."""
        assert convert_value("not a number", "ms") == 0.0


class TestParsePySparkLogs:
    """Tests for parse_pyspark_logs function."""

    def test_empty_log_data(self) -> None:
        """Test with empty log data (should return empty summary)."""
        assert parse_pyspark_logs([]) == {}

    def test_single_event(self) -> None:
        """Test with a single event log entry."""
        log_data = [
            {
                "Event": "SparkListenerTaskEnd",
                "Task Metrics": {
                    "Executor Deserialize Time": 60000,  # 1 min
                    "Executor Run Time": 300000,  # 5 min
                    "Peak Execution Memory": 1048576,  # 1 MB
                    "Shuffle Write Metrics": {"Shuffle Bytes Written": 5242880},  # 5 MB
                },
            },
        ]
        actual_output = parse_pyspark_logs(log_data)

        # Validate only expected keys
        assert actual_output["Executor Deserialize Time"] == 1.0
        assert actual_output["Executor Run Time"] == 5.0
        assert actual_output["Peak Execution Memory"] == 1.0
        assert actual_output["Shuffle Bytes Written"] == 5.0

    def test_multiple_events(self) -> None:
        """Test with multiple event log entries."""
        log_data = [
            {
                "Event": "SparkListenerTaskEnd",
                "Task Metrics": {
                    "Executor Deserialize Time": 60000,
                    "Executor Run Time": 300000,
                    "Peak Execution Memory": 1048576,
                    "Shuffle Write Metrics": {"Shuffle Bytes Written": 5242880},
                },
            },
            {
                "Event": "SparkListenerTaskEnd",
                "Task Metrics": {
                    "Executor Deserialize Time": 120000,  # 2 min
                    "Executor Run Time": 600000,  # 10 min
                    "Peak Execution Memory": 2097152,  # 2 MB
                    "Shuffle Write Metrics": {
                        "Shuffle Bytes Written": 10485760,  # 10 MB
                    },
                },
            },
        ]
        actual_output = parse_pyspark_logs(log_data)

        # Validate only expected keys
        assert actual_output["Executor Deserialize Time"] == 3.0
        assert actual_output["Executor Run Time"] == 15.0
        assert actual_output["Peak Execution Memory"] == 2.0  # Max value across tasks
        assert actual_output["Shuffle Bytes Written"] == 15.0
