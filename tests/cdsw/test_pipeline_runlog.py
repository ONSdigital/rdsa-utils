"""
This module contains pytest functions to test the behaviour of the functions
contained within the rdsa_utils/cdsw/pipeline_runlog.py Module.
"""
import pytest
from pyspark.sql import DataFrame, Row

from rdsa_utils.cdsw.pipeline_runlog import (
    _get_run_ids,
    _write_entry,
    create_runlog_table,
    get_last_run_id,
    get_penultimate_run_id,
    reserve_id,
)


class TestWriteEntry:
    def test_write_entry_non_empty(self, mocker):
        """
        Test that the function successfully writes data to the specified table.
        """
        # Mock DataFrame and write method
        mock_df = mocker.Mock(spec=DataFrame)
        mock_df.write.insertInto.return_value = None

        # Call function with mock DataFrame and table name
        _write_entry(mock_df, "test_table")

        # Assert that DataFrame write method was called with correct arguments
        mock_df.write.insertInto.assert_called_once_with("test_table")

    def test_write_entry_edge_cases(self, mocker):
        """
        Test that the function does not raise any exceptions when entry_df is empty or
        log_table is an empty string.
        """
        # Mock empty DataFrame and write method
        mock_df = mocker.Mock(spec=DataFrame)
        mock_df.write.insertInto.return_value = None

        # Call function with empty DataFrame and empty table name
        _write_entry(mock_df, "")

        # Assert that DataFrame write method was called once with empty string as argument
        mock_df.write.insertInto.assert_called_once_with("")


class TestCreateRunlogTable:
    def test_create_runlog_table_default_tablename(self, mocker):
        """
        Test that the function creates a runlog table with default tablename.
        """
        # Mock SparkSession
        mock_spark = mocker.Mock()

        # Call function with default tablename
        create_runlog_table(mock_spark, "test_db")

        # Assert that the main table and _reserved_ids table were created with correct names
        mock_spark.sql.assert_any_call(
            """
        CREATE TABLE IF NOT EXISTS test_db.pipeline_runlog (
            run_id int,
            desc string,
            user string,
            datetime timestamp,
            pipeline_name string,
            pipeline_version string,
            config string
        )
        STORED AS parquet
    """
        )
        mock_spark.sql.assert_any_call(
            """
        CREATE TABLE IF NOT EXISTS test_db.pipeline_runlog_reserved_ids (
            run_id int,
            reserved_date timestamp
        )
        STORED AS parquet
    """
        )

    def test_create_runlog_table_custom_tablename(self, mocker):
        """
        Test that the function creates a runlog table with a custom tablename.
        """
        # Mock SparkSession
        mock_spark = mocker.Mock()

        # Call function with custom tablename
        create_runlog_table(mock_spark, "test_db", "custom_table")

        # Assert that the main table and _reserved_ids table were created with correct names
        mock_spark.sql.assert_any_call(
            """
        CREATE TABLE IF NOT EXISTS test_db.custom_table (
            run_id int,
            desc string,
            user string,
            datetime timestamp,
            pipeline_name string,
            pipeline_version string,
            config string
        )
        STORED AS parquet
    """
        )
        mock_spark.sql.assert_any_call(
            """
        CREATE TABLE IF NOT EXISTS test_db.custom_table_reserved_ids (
            run_id int,
            reserved_date timestamp
        )
        STORED AS parquet
    """
        )


class TestReserveId:
    def test_reserve_id_non_empty(self, mocker):
        """
        Tests that the function successfully reads the last run id from the reserved ids
        table and increments it to create a new id, and writes the new id with the
        current timestamp to the reserved ids table.
        """
        # Mock SparkSession
        spark_mock = mocker.Mock()
        spark_mock.read.table.return_value.select.return_value.first.return_value = (1,)
        spark_mock.createDataFrame.return_value = mocker.Mock()

        # Mock _write_entry function
        mock_write_entry = mocker.patch("rdsa_utils.cdsw.pipeline_runlog._write_entry")

        # Mock pyspark.sql.functions.max
        mocker.patch("pyspark.sql.functions.max", return_value=mocker.Mock())

        # Call function
        result = reserve_id(spark_mock)

        # Assert result
        assert result == 2

        # Assert SparkSession methods were called correctly
        spark_mock.read.table.assert_called_once_with("pipeline_runlog_reserved_ids")
        spark_mock.createDataFrame.assert_called_once_with(
            [(2, mocker.ANY)], "run_id INT, reserved_date TIMESTAMP"
        )
        mock_write_entry.assert_called_once_with(
            mocker.ANY, "pipeline_runlog_reserved_ids"
        )

    def test_reserve_id_edge_case(self, mocker):
        """
        Tests that the function handles the case where the reserved ids table is empty,
        does not exist, has no "run_id" column, or has no "reserved_date" column.
        """
        # Mock SparkSession
        spark_mock = mocker.Mock()
        spark_mock.read.table.return_value.select.return_value.first.return_value = [
            None
        ]
        spark_mock.createDataFrame.return_value = mocker.Mock()

        # Mock _write_entry function
        mock_write_entry = mocker.patch("rdsa_utils.cdsw.pipeline_runlog._write_entry")

        # Mock pyspark.sql.functions.max
        mocker.patch("pyspark.sql.functions.max", return_value=mocker.Mock())

        # Call function
        result = reserve_id(spark_mock)

        # Assert result
        assert result == 1

        # Assert SparkSession methods were called correctly
        spark_mock.read.table.assert_called_once_with("pipeline_runlog_reserved_ids")
        spark_mock.createDataFrame.assert_called_once_with(
            [(1, mocker.ANY)], "run_id INT, reserved_date TIMESTAMP"
        )
        mock_write_entry.assert_called_once_with(
            mocker.ANY, "pipeline_runlog_reserved_ids"
        )


class TestGetRunIds:
    def test_get_run_ids_returns_non_empty(self, mocker):
        """
        Tests that the function returns the correct list of most recent run ids for a
        given pipeline and for all pipelines.
        """
        # Mock SparkSession and log table
        spark_mock = mocker.Mock()
        log_table = "test_log_table"

        # Create test data
        test_data = [
            (1, "pipeline1", "2022-01-01 00:00:00"),
            (2, "pipeline1", "2022-01-06 00:00:00"),
            (3, "pipeline2", "2022-01-03 00:00:00"),
            (4, "pipeline2", "2022-01-04 00:00:00"),
            (5, "pipeline2", "2022-01-05 00:00:00"),
        ]

        # Sort the test data by datetime in descending order
        test_data_sorted = sorted(test_data, key=lambda x: x[2], reverse=True)

        # Mock DataFrame methods
        test_df = mocker.Mock()
        test_df.filter.return_value = test_df
        test_df.orderBy.return_value = test_df
        test_df.select.return_value = test_df
        test_df.limit.return_value = test_df

        # Mock SparkSession methods
        spark_mock.read.table.return_value = test_df

        # Call function for all pipelines
        test_df.collect.return_value = [(x[0],) for x in test_data_sorted[:3]]
        result_all = _get_run_ids(spark_mock, 3, log_table=log_table)

        # Assert results; the top 3 run_ids based on datetime
        assert result_all == [2, 5, 4]

        # Call function for specific pipeline
        test_df.collect.return_value = [
            (x[0],) for x in test_data_sorted if x[1] == "pipeline2"
        ][:2]
        result_pipeline = _get_run_ids(
            spark_mock, 2, pipeline="pipeline2", log_table=log_table
        )

        # Assert results; The top 2 run_ids for "pipeline2" based on datetime
        assert result_pipeline == [
            5,
            4,
        ]

        # Assert DataFrame methods were called correctly
        test_df.orderBy.assert_called_with("datetime", ascending=False)
        test_df.select.assert_called_with("run_id")
        test_df.filter.assert_called_with(test_df.pipeline_name == "pipeline2")
        test_df.limit.assert_called_with(2)

    def test_get_run_ids_empty_table(self, mocker):
        """
        Tests that the function returns an empty list if the log table is empty.
        """
        # Mock SparkSession and log table
        spark_mock = mocker.Mock()
        log_table = "test_log_table"

        # Mock DataFrame methods for an empty DataFrame
        test_df = mocker.Mock()
        test_df.filter.return_value = test_df
        test_df.orderBy.return_value = test_df
        test_df.select.return_value = test_df
        test_df.limit.return_value = test_df

        # Mock SparkSession methods
        spark_mock.read.table.return_value = test_df

        # Set collect() method to return an empty list
        test_df.collect.return_value = []

        # Call function for empty log table
        result_empty = _get_run_ids(spark_mock, 3, log_table=log_table)

        # Assert results
        assert result_empty == []

        # Assert DataFrame methods were called correctly
        test_df.orderBy.assert_called_with("datetime", ascending=False)
        test_df.select.assert_called_with("run_id")
        test_df.limit.assert_called_with(3)


class TestGetLastRunId:
    def test_get_last_run_id_general_pipeline_non_empty(self, mocker):
        """
        Test retrieving the last run ID for a general pipeline with at least one entry
        in the log table.
        """
        # Mock SparkSession and _get_run_ids function
        spark_mock = mocker.Mock()

        # Patch _get_run_ids function and return a Mock object
        get_run_ids_mock = mocker.patch(
            "rdsa_utils.cdsw.pipeline_runlog._get_run_ids", return_value=[3, 2, 1]
        )

        # Call function
        result = get_last_run_id(spark_mock)

        # Assert result
        assert result == 3

        # Assert _get_run_ids was called correctly
        get_run_ids_mock.assert_called_once_with(spark_mock, 1, None, "pipeline_runlog")

    def test_get_last_run_id_specific_pipeline_empty(self, mocker):
        """
        Test retrieving the last run ID for a specific pipeline with no entries in the log table.
        """
        # Mock SparkSession and _get_run_ids function
        spark_mock = mocker.Mock()

        # Patch _get_run_ids function and return a Mock object
        get_run_ids_mock = mocker.patch(
            "rdsa_utils.cdsw.pipeline_runlog._get_run_ids", return_value=[]
        )

        # Call function with specific pipeline and empty log table
        result = get_last_run_id(spark_mock, pipeline="test_pipeline")

        # Assert result is None
        assert result is None

        # Assert _get_run_ids was called correctly
        get_run_ids_mock.assert_called_once_with(
            spark_mock, 1, "test_pipeline", "pipeline_runlog"
        )


class TestGetPenultimateRunId:
    def test_penultimate_run_id_non_empty(self, mocker):
        """
        Test retrieving the penultimate run ID for a pipeline with at least two entries
        in the log table.
        """
        # Mock SparkSession and _get_run_ids function
        spark_mock = mocker.Mock()

        # Patch _get_run_ids function and return a Mock object
        get_run_ids_mock = mocker.patch(
            "rdsa_utils.cdsw.pipeline_runlog._get_run_ids", return_value=[3, 2, 1]
        )

        # Call function
        result = get_penultimate_run_id(spark_mock, pipeline="test_pipeline")

        # Assert result
        assert result == 2

        # Assert _get_run_ids was called correctly
        get_run_ids_mock.assert_called_once_with(
            spark_mock, 2, "test_pipeline", "pipeline_runlog"
        )

    def test_penultimate_run_id_edge_cases(self, mocker):
        """
        Test retrieving the penultimate run ID for a pipeline with only one entry in the log table,
        for a pipeline with no entries in the log table, for the general log table with only one entry,
        and for the general log table with no entries.
        """
        # Mock SparkSession and _get_run_ids function
        spark_mock = mocker.Mock()

        ## Test Case 1

        # Patch _get_run_ids function and return a Mock object
        get_run_ids_mock = mocker.patch(
            "rdsa_utils.cdsw.pipeline_runlog._get_run_ids", return_value=[1]
        )

        # Call function with specific pipeline and non-empty log table
        result = get_penultimate_run_id(spark_mock, pipeline="test_pipeline")

        # Assert result is None
        assert result is None

        # Assert _get_run_ids was called correctly
        get_run_ids_mock.assert_called_once_with(
            spark_mock, 2, "test_pipeline", "pipeline_runlog"
        )

        ## Test Case 2

        # Patch _get_run_ids function and return a Mock object
        get_run_ids_mock = mocker.patch(
            "rdsa_utils.cdsw.pipeline_runlog._get_run_ids", return_value=[]
        )

        # Call function with specific pipeline and empty log table
        result = get_penultimate_run_id(spark_mock, pipeline="test_pipeline")

        # Assert result is None
        assert result is None

        # Assert _get_run_ids was called correctly
        get_run_ids_mock.assert_called_once_with(
            spark_mock, 2, "test_pipeline", "pipeline_runlog"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
