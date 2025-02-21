"""Tests for parser.py module."""

from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

from rdsa_utils.helpers.pyspark_log_parser.parser import (
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
        assert parse_pyspark_logs([]) == {
            "Timestamp": None,
            "Pipeline Name": None,
            "Start Time": None,
            "End Time": None,
            "Total Time": 0,
            "Total Cores": 0,
            "Total Memory": 0,
        }

    def test_single_event(self) -> None:
        """Test with a single event log entry."""
        log_data = [
            {
                "Event": "SparkListenerApplicationStart",
                "Timestamp": 1739793526775,
            },
            {
                "Event": "SparkListenerExecutorAdded",
                "Executor Info": {"Total Cores": 4},
            },
            {
                "Event": "SparkListenerStageSubmitted",
                "Properties": {
                    "spark.executor.memory": "4g",
                    "spark.yarn.executor.memoryOverhead": "2g",
                    "spark.executor.cores": "4",
                },
            },
            {
                "Event": "SparkListenerApplicationEnd",
                "Timestamp": 1739793626775,
            },
        ]
        actual_output = parse_pyspark_logs(log_data)

        # Validate expected keys
        assert actual_output["Timestamp"] == 1739793526775
        assert actual_output["Pipeline Name"] is None
        assert actual_output["Start Time"] == 1739793526775
        assert actual_output["End Time"] == 1739793626775
        assert actual_output["Total Time"] == 100000  # 10 minutes in milliseconds
        assert actual_output["Total Cores"] == 4
        assert actual_output["Total Memory"] == 6  # 4GB + 2GB

    def test_multiple_events(self) -> None:
        """Test with multiple event log entries."""
        log_data = [
            {
                "Event": "SparkListenerApplicationStart",
                "Timestamp": 1739793526775,
            },
            {
                "Event": "SparkListenerExecutorAdded",
                "Executor Info": {"Total Cores": 4},
            },
            {
                "Event": "SparkListenerExecutorAdded",
                "Executor Info": {"Total Cores": 4},
            },
            {
                "Event": "SparkListenerStageSubmitted",
                "Properties": {
                    "spark.executor.memory": "4g",
                    "spark.yarn.executor.memoryOverhead": "2g",
                    "spark.executor.cores": "4",
                },
            },
            {
                "Event": "SparkListenerApplicationEnd",
                "Timestamp": 1739793626775,
            },
        ]
        actual_output = parse_pyspark_logs(log_data)

        # Validate expected keys
        assert actual_output["Timestamp"] == 1739793526775
        assert actual_output["Pipeline Name"] is None
        assert actual_output["Start Time"] == 1739793526775
        assert actual_output["End Time"] == 1739793626775
        assert actual_output["Total Time"] == 100000  # 10 minutes in milliseconds
        assert actual_output["Total Cores"] == 8  # 4 cores per executor * 2 executors
        assert actual_output["Total Memory"] == 12  # 6GB * 2 executors


class TestFindPysparkLogFiles:
    """Tests for find_pyspark_log_files function."""

    @pytest.fixture(scope="class")
    def _aws_credentials(self):
        """Mock AWS Credentials for moto."""
        boto3.setup_default_session(
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            aws_session_token="testing",
        )

    @pytest.fixture
    def s3_client_for_find_pyspark_log_files(self, _aws_credentials):
        """
        Provide a mocked AWS S3 client with temporary
        credentials for testing find_pyspark_log_files function.

        Creates a temporary S3 bucket and sets up some
        objects within it for testing.

        Yields the S3 client for use in the test functions.
        """
        with mock_aws():
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

    @patch("rdsa_utils.helpers.pyspark_log_parser.parser.list_files")
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

    @patch("rdsa_utils.helpers.pyspark_log_parser.parser.list_files")
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

    @patch("rdsa_utils.helpers.pyspark_log_parser.parser.list_files")
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

    @patch("rdsa_utils.helpers.pyspark_log_parser.parser.list_files")
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
