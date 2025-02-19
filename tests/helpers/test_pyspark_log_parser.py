"""Tests for pyspark_log_parser.py module."""

from unittest.mock import patch

import boto3
import pytest
from moto import mock_s3

from rdsa_utils.helpers.pyspark_log_parser import (
    convert_value,
    find_pyspark_log_files,
    parse_pyspark_logs,
)


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


class TestFindPysparkLogFiles:
    """Tests for find_pyspark_log_files function."""

    @pytest.fixture
    def s3_client_for_find_pyspark_log_files(self, _aws_credentials):
        """
        Provide a mocked AWS S3 client with temporary
        credentials for testing find_pyspark_log_files function.

        Creates a temporary S3 bucket and sets up some
        objects within it for testing.

        Yields the S3 client for use in the test functions.
        """
        with mock_s3():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="test-bucket")
            # Set up some objects in S3 for testing
            objects = [
                "user/dominic.bean/eventlog_v2_spark-1234/events_1_spark-1234",
                "user/dominic.bean/eventlog_v2_spark-5678/events_1_spark-5678",
                "user/dominic.bean/eventlog_v2_spark-1234/other_file.txt",
                "user/dominic.bean/other_folder/file.txt",
            ]
            for obj in objects:
                client.put_object(
                    Bucket="test-bucket",
                    Key=obj,
                    Body=b"Test content",
                )
            yield client

    @patch("rdsa_utils.helpers.pyspark_log_parser.list_files")
    def test_find_pyspark_log_files(
        self,
        mock_list_files,
        s3_client_for_find_pyspark_log_files,
    ):
        """Test finding PySpark log files in the folder."""
        mock_list_files.return_value = [
            "user/dominic.bean/eventlog_v2_spark-1234/events_1_spark-1234",
            "user/dominic.bean/eventlog_v2_spark-5678/events_1_spark-5678",
            "user/dominic.bean/eventlog_v2_spark-1234/other_file.txt",
            "user/dominic.bean/other_folder/file.txt",
        ]
        client = s3_client_for_find_pyspark_log_files
        bucket_name = "test-bucket"
        folder = "user/dominic.bean"
        log_files = find_pyspark_log_files(client, bucket_name, folder)
        assert len(log_files) == 2
        assert (
            "user/dominic.bean/eventlog_v2_spark-1234/events_1_spark-1234" in log_files
        )
        assert (
            "user/dominic.bean/eventlog_v2_spark-5678/events_1_spark-5678" in log_files
        )

    @patch("rdsa_utils.helpers.pyspark_log_parser.list_files")
    def test_find_pyspark_log_files_no_match(
        self,
        mock_list_files,
        s3_client_for_find_pyspark_log_files,
    ):
        """Test finding PySpark log files when no files match."""
        mock_list_files.return_value = [
            "user/dominic.bean/other_folder/file.txt",
            "user/dominic.bean/eventlog_v2_spark-1234/other_file.txt",
        ]
        client = s3_client_for_find_pyspark_log_files
        bucket_name = "test-bucket"
        folder = "user/dominic.bean"
        log_files = find_pyspark_log_files(client, bucket_name, folder)
        assert len(log_files) == 0

    @patch("rdsa_utils.helpers.pyspark_log_parser.list_files")
    def test_find_pyspark_log_files_with_prefix(
        self,
        mock_list_files,
        s3_client_for_find_pyspark_log_files,
    ):
        """Test finding PySpark log files with a specific prefix."""
        mock_list_files.return_value = [
            "user/dominic.bean/eventlog_v2_spark-1234/events_1_spark-1234",
            "user/dominic.bean/eventlog_v2_spark-1234/other_file.txt",
            "user/dominic.bean/eventlog_v2_spark-5678/events_1_spark-5678",
        ]
        client = s3_client_for_find_pyspark_log_files
        bucket_name = "test-bucket"
        folder = "user/dominic.bean"
        log_files = find_pyspark_log_files(client, bucket_name, folder)
        assert len(log_files) == 2
        assert (
            "user/dominic.bean/eventlog_v2_spark-1234/events_1_spark-1234" in log_files
        )
        assert (
            "user/dominic.bean/eventlog_v2_spark-5678/events_1_spark-5678" in log_files
        )

    @patch("rdsa_utils.helpers.pyspark_log_parser.list_files")
    def test_find_pyspark_log_files_empty_folder(
        self,
        mock_list_files,
        s3_client_for_find_pyspark_log_files,
    ):
        """Test finding PySpark log files in an empty folder."""
        mock_list_files.return_value = []
        client = s3_client_for_find_pyspark_log_files
        bucket_name = "test-bucket"
        folder = "user/dominic.bean"
        log_files = find_pyspark_log_files(client, bucket_name, folder)
        assert len(log_files) == 0
