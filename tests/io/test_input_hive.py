"""Tests for the input_hive.py module."""
from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame as SparkDF

from rdsa_utils.io.input_hive import *
from rdsa_utils.exceptions import DataframeEmptyError

class TestLoadAndValidateTable:
    """Tests for load_and_validate_table function."""

    def test_load_and_validate_table_with_empty_table(self) -> None:
        """Test that load_and_validate_table raises a ValueError when the table
        is empty and skip_validation is False.
        """
        table_name = 'empty_table'
        # Mock SparkSession and DataFrame
        spark_session = MagicMock(spec=SparkSession)
        df = MagicMock(spec=SparkDF)
        df.rdd.isEmpty.return_value = True
        spark_session.read.table.return_value = df
        with pytest.raises(DataframeEmptyError):
            load_and_validate_table(spark_session, table_name)

    def test_load_and_validate_table_with_non_existing_table(self) -> None:
        """Test that load_and_validate_table raises a PermissionError when the
        table doesn't exist.
        """
        table_name = 'non_existing_table'
        # Mock SparkSession
        spark_session = MagicMock(spec=SparkSession)
        spark_session.read.table.side_effect = Exception('Table not found.')
        with pytest.raises(PermissionError):
            load_and_validate_table(spark_session, table_name)

    def test_load_and_validate_table_with_filter(self) -> None:
        """Test that load_and_validate_table applies the filter condition and
        raises a ValueError when the DataFrame is empty after filtering.
        """
        table_name = 'test_table'
        filter_cond = 'age > 30'
        # Mock SparkSession and DataFrame
        spark_session = MagicMock(spec=SparkSession)
        df = MagicMock(spec=SparkDF)
        df.rdd.isEmpty.side_effect = [False, True]
        spark_session.read.table.return_value = df
        with pytest.raises(DataframeEmptyError):
            load_and_validate_table(
                spark_session, table_name, filter_cond=filter_cond,
            )

    def test_load_and_validate_table_with_skip_validation(self) -> None:
        """Test that load_and_validate_table doesn't raise any exceptions when
        skip_validation is True even if the table is empty.
        """
        table_name = 'empty_table'
        # Mock SparkSession and DataFrame
        spark_session = MagicMock(spec=SparkSession)
        df = MagicMock(spec=SparkDF)
        df.rdd.isEmpty.return_value = True
        spark_session.read.table.return_value = df
        # No exception is expected to be raised here
        load_and_validate_table(spark_session, table_name, skip_validation=True)

    def test_load_and_validate_table_with_normal_table(self) -> None:
        """Test that load_and_validate_table works correctly when the table
        exists, is not empty, and doesn't need a filter.
        """
        table_name = 'normal_table'
        # Mock SparkSession and DataFrame
        spark_session = MagicMock(spec=SparkSession)
        df = MagicMock(spec=SparkDF)
        df.rdd.isEmpty.return_value = False
        spark_session.read.table.return_value = df
        # No exception is expected to be raised here
        result = load_and_validate_table(spark_session, table_name)
        # Check that the returned DataFrame is our mock DataFrame
        assert result == df
