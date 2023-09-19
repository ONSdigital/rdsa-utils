"""Tests for pipeline_runlog.py module."""

import pytest
from pyspark.sql import DataFrame

from rdsa_utils.cdsw.pipeline_runlog import (
    _get_run_ids,
    _write_entry,
    add_runlog_entry,
    create_runlog_entry,
    create_runlog_table,
    get_last_run_id,
    get_penultimate_run_id,
    reserve_id,
    write_runlog_file,
)


class TestWriteEntry:
    """Tests for _write_entry function."""

    def test_write_entry_non_empty(self, mocker):
        """Test that the function successfully writes data to the specified table."""
        # Mock DataFrame and write method
        mock_df = mocker.Mock(spec=DataFrame)
        mock_df.write.insertInto.return_value = None

        # Call function with mock DataFrame and table name
        _write_entry(mock_df, 'test_table')

        # Assert that DataFrame write method was called with correct arguments
        mock_df.write.insertInto.assert_called_once_with('test_table')

    def test_write_entry_edge_cases(self, mocker):
        """Test for no exceptions with empty entry_df or log_table as empty string."""
        # Mock empty DataFrame and write method
        mock_df = mocker.Mock(spec=DataFrame)
        mock_df.write.insertInto.return_value = None

        # Call function with empty DataFrame and empty table name
        _write_entry(mock_df, '')

        # Assert that DataFrame write method was called once with
        # empty string as argument
        mock_df.write.insertInto.assert_called_once_with('')


class TestCreateRunlogTable:
    """Tests for create_runlog_entry function."""

    def test_create_runlog_table_default_tablename(self, mocker):
        """Test that the function creates a runlog table with default tablename."""
        # Mock SparkSession
        mock_spark = mocker.Mock()

        # Call function with default tablename
        create_runlog_table(mock_spark, 'test_db')

        # Assert that the main table and _reserved_ids table were created
        # with correct names
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
    """,
        )
        mock_spark.sql.assert_any_call(
            """
        CREATE TABLE IF NOT EXISTS test_db.pipeline_runlog_reserved_ids (
            run_id int,
            reserved_date timestamp
        )
        STORED AS parquet
    """,
        )

    def test_create_runlog_table_custom_tablename(self, mocker):
        """Test that the function creates a runlog table with a custom tablename."""
        # Mock SparkSession
        mock_spark = mocker.Mock()

        # Call function with custom tablename
        create_runlog_table(mock_spark, 'test_db', 'custom_table')

        # Assert that the main table and _reserved_ids table were created
        # with correct names
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
    """,
        )
        mock_spark.sql.assert_any_call(
            """
        CREATE TABLE IF NOT EXISTS test_db.custom_table_reserved_ids (
            run_id int,
            reserved_date timestamp
        )
        STORED AS parquet
    """,
        )


class TestReserveId:
    """Tests for reserve_id function."""

    def test_reserve_id_non_empty(self, mocker):
        """Tests successful creation and storage of a new ID based on the last run ID.

        Checks if the function can correctly retrieve the last run ID from
        the reserved IDs table, increment it to generate a new ID, and then
        record the new ID along with the current timestamp in the reserved IDs table.
        """
        # Mock SparkSession
        spark_mock = mocker.Mock()
        spark_mock.read.table.return_value.select.return_value.first.return_value = (1,)
        spark_mock.createDataFrame.return_value = mocker.Mock()

        # Mock _write_entry function
        mock_write_entry = mocker.patch('rdsa_utils.cdsw.pipeline_runlog._write_entry')

        # Mock pyspark.sql.functions.max
        mocker.patch('pyspark.sql.functions.max', return_value=mocker.Mock())

        # Call function
        result = reserve_id(spark_mock)

        # Assert result
        assert result == 2

        # Assert SparkSession methods were called correctly
        spark_mock.read.table.assert_called_once_with('pipeline_runlog_reserved_ids')
        spark_mock.createDataFrame.assert_called_once_with(
            [(2, mocker.ANY)],
            'run_id INT, reserved_date TIMESTAMP',
        )
        mock_write_entry.assert_called_once_with(
            mocker.ANY,
            'pipeline_runlog_reserved_ids',
        )

    def test_reserve_id_edge_case(self, mocker):
        """Tests handling of edge cases with the reserved IDs table.

        Ensures the function gracefully manages scenarios where the
        reserved IDs table is empty, nonexistent, lacks a "run_id" column,
        or lacks a "reserved_date" column.
        """
        # Mock SparkSession
        spark_mock = mocker.Mock()
        spark_mock.read.table.return_value.select.return_value.first.return_value = [
            None,
        ]
        spark_mock.createDataFrame.return_value = mocker.Mock()

        # Mock _write_entry function
        mock_write_entry = mocker.patch('rdsa_utils.cdsw.pipeline_runlog._write_entry')

        # Mock pyspark.sql.functions.max
        mocker.patch('pyspark.sql.functions.max', return_value=mocker.Mock())

        # Call function
        result = reserve_id(spark_mock)

        # Assert result
        assert result == 1

        # Assert SparkSession methods were called correctly
        spark_mock.read.table.assert_called_once_with('pipeline_runlog_reserved_ids')
        spark_mock.createDataFrame.assert_called_once_with(
            [(1, mocker.ANY)],
            'run_id INT, reserved_date TIMESTAMP',
        )
        mock_write_entry.assert_called_once_with(
            mocker.ANY,
            'pipeline_runlog_reserved_ids',
        )


class TestGetRunIds:
    """Tests for _get_run_ids function."""

    def test_get_run_ids_returns_non_empty(self, mocker):
        """Tests correct retrieval of recent run IDs for specific and all pipelines."""
        # Mock SparkSession and log table
        spark_mock = mocker.Mock()
        log_table = 'test_log_table'

        # Create test data
        test_data = [
            (1, 'pipeline1', '2022-01-01 00:00:00'),
            (2, 'pipeline1', '2022-01-06 00:00:00'),
            (3, 'pipeline2', '2022-01-03 00:00:00'),
            (4, 'pipeline2', '2022-01-04 00:00:00'),
            (5, 'pipeline2', '2022-01-05 00:00:00'),
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
            (x[0],) for x in test_data_sorted if x[1] == 'pipeline2'
        ][:2]
        result_pipeline = _get_run_ids(
            spark_mock,
            2,
            pipeline='pipeline2',
            log_table=log_table,
        )

        # Assert results; The top 2 run_ids for "pipeline2" based on datetime
        assert result_pipeline == [
            5,
            4,
        ]

        # Assert DataFrame methods were called correctly
        test_df.orderBy.assert_called_with('datetime', ascending=False)
        test_df.select.assert_called_with('run_id')
        test_df.filter.assert_called_with(test_df.pipeline_name == 'pipeline2')
        test_df.limit.assert_called_with(2)

    def test_get_run_ids_empty_table(self, mocker):
        """Test the function for returning an empty list when the log table is empty."""
        # Mock SparkSession and log table
        spark_mock = mocker.Mock()
        log_table = 'test_log_table'

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
        test_df.orderBy.assert_called_with('datetime', ascending=False)
        test_df.select.assert_called_with('run_id')
        test_df.limit.assert_called_with(3)


class TestGetLastRunId:
    """Tests for get_last_run_id function."""

    def test_get_last_run_id_general_pipeline_non_empty(self, mocker):
        """Test retrieving the last run ID for a general pipeline with at least one log entry."""
        # Mock SparkSession and _get_run_ids function
        spark_mock = mocker.Mock()

        # Patch _get_run_ids function and return a Mock object
        get_run_ids_mock = mocker.patch(
            'rdsa_utils.cdsw.pipeline_runlog._get_run_ids',
            return_value=[3, 2, 1],
        )

        # Call function
        result = get_last_run_id(spark_mock)

        # Assert result
        assert result == 3

        # Assert _get_run_ids was called correctly
        get_run_ids_mock.assert_called_once_with(spark_mock, 1, None, 'pipeline_runlog')

    def test_get_last_run_id_specific_pipeline_empty(self, mocker):
        """Test retrieving the last run ID for a specific pipeline with no log entries."""
        # Mock SparkSession and _get_run_ids function
        spark_mock = mocker.Mock()

        # Patch _get_run_ids function and return a Mock object
        get_run_ids_mock = mocker.patch(
            'rdsa_utils.cdsw.pipeline_runlog._get_run_ids',
            return_value=[],
        )

        # Call function with specific pipeline and empty log table
        result = get_last_run_id(spark_mock, pipeline='test_pipeline')

        # Assert result is None
        assert result is None

        # Assert _get_run_ids was called correctly
        get_run_ids_mock.assert_called_once_with(
            spark_mock,
            1,
            'test_pipeline',
            'pipeline_runlog',
        )


class TestGetPenultimateRunId:
    """Tests for get_penultimate_run_id function."""

    def test_penultimate_run_id_non_empty(self, mocker):
        """Test retrieving the penultimate run ID.

        Tests the functionality for retrieving the penultimate run ID for a
        pipeline with at least two entries in the log table.
        """
        # Mock SparkSession and _get_run_ids function
        spark_mock = mocker.Mock()

        # Patch _get_run_ids function and return a Mock object
        get_run_ids_mock = mocker.patch(
            'rdsa_utils.cdsw.pipeline_runlog._get_run_ids',
            return_value=[3, 2, 1],
        )

        # Call function
        result = get_penultimate_run_id(spark_mock, pipeline='test_pipeline')

        # Assert result
        assert result == 2

        # Assert _get_run_ids was called correctly
        get_run_ids_mock.assert_called_once_with(
            spark_mock,
            2,
            'test_pipeline',
            'pipeline_runlog',
        )

    def test_penultimate_run_id_edge_cases(self, mocker):
        """Test retrieving the penultimate run ID in various edge cases.

        This test covers the functionality for retrieving the penultimate run ID in
        the following scenarios:
        - For a pipeline with only one entry in the log table
        - For a pipeline with no entries in the log table
        - For the general log table with only one entry
        - For the general log table with no entries.
        """
        # Mock SparkSession and _get_run_ids function
        spark_mock = mocker.Mock()

        ## Test Case 1

        # Patch _get_run_ids function and return a Mock object
        get_run_ids_mock = mocker.patch(
            'rdsa_utils.cdsw.pipeline_runlog._get_run_ids',
            return_value=[1],
        )

        # Call function with specific pipeline and non-empty log table
        result = get_penultimate_run_id(spark_mock, pipeline='test_pipeline')

        # Assert result is None
        assert result is None

        # Assert _get_run_ids was called correctly
        get_run_ids_mock.assert_called_once_with(
            spark_mock,
            2,
            'test_pipeline',
            'pipeline_runlog',
        )

        ## Test Case 2

        # Patch _get_run_ids function and return a Mock object
        get_run_ids_mock = mocker.patch(
            'rdsa_utils.cdsw.pipeline_runlog._get_run_ids',
            return_value=[],
        )

        # Call function with specific pipeline and empty log table
        result = get_penultimate_run_id(spark_mock, pipeline='test_pipeline')

        # Assert result is None
        assert result is None

        # Assert _get_run_ids was called correctly
        get_run_ids_mock.assert_called_once_with(
            spark_mock,
            2,
            'test_pipeline',
            'pipeline_runlog',
        )


class TestCreateRunlogEntry:
    """Tests for create_runlog_entry function."""

    def test_create_runlog_entry(self, mocker):
        """Tests function for returning a DataFrame with log entry for valid inputs."""
        # Mock SparkSession
        spark_mock = mocker.Mock()

        # Set up test data
        run_id = 1
        desc = 'test description'
        version = '1.0'
        config = {'param1': 'value1', 'param2': 'value2'}
        pipeline = 'test_pipeline'

        # Mock createDataFrame function
        mock_df = mocker.Mock()
        mock_df.columns = [
            'run_id',
            'desc',
            'user',
            'datetime',
            'pipeline_name',
            'pipeline_version',
            'config',
        ]
        spark_mock.createDataFrame.return_value = mock_df

        # Call function
        result = create_runlog_entry(
            spark_mock,
            run_id,
            desc,
            version,
            config,
            pipeline,
        )

        # Assert result is a DataFrame with the correct columns and values
        assert result == mock_df
        assert result.columns == [
            'run_id',
            'desc',
            'user',
            'datetime',
            'pipeline_name',
            'pipeline_version',
            'config',
        ]

    def test_create_runlog_entry_edge_cases(self, mocker):
        """Tests the function for raising an error with invalid inputs."""
        # Mock SparkSession
        spark_mock = mocker.Mock()

        # Set up test data with invalid config object
        run_id = 1
        desc = 'test description'
        version = '1.0'
        config = object()
        pipeline = 'test_pipeline'

        # Call function and assert it raises a ValueError
        with pytest.raises(ValueError):
            create_runlog_entry(spark_mock, run_id, desc, version, config, pipeline)


class TestAddRunlogEntry:
    """Tests for add_runlog_entry function."""

    def test_add_runlog_entry(self, mocker):
        """Test adding an entry to the runlog with a newly reserved run_id."""
        # Mock SparkSession
        spark_mock = mocker.Mock()

        # Set up test data
        desc = 'test description'
        version = '1.0'
        config = {'param1': 'value1', 'param2': 'value2'}
        pipeline = 'test_pipeline'
        log_table = 'test_log_table'

        # Mock reserve_id, create_runlog_entry, _write_entry
        reserve_id_mock = mocker.patch(
            'rdsa_utils.cdsw.pipeline_runlog.reserve_id',
            return_value=1,
        )
        entry_mock = mocker.Mock()
        create_runlog_entry_mock = mocker.patch(
            'rdsa_utils.cdsw.pipeline_runlog.create_runlog_entry',
            return_value=entry_mock,
        )
        _write_entry_mock = mocker.patch('rdsa_utils.cdsw.pipeline_runlog._write_entry')

        # Call function
        result = add_runlog_entry(
            spark_mock,
            desc,
            version,
            config,
            pipeline,
            log_table,
        )

        # Assert functions were called correctly
        reserve_id_mock.assert_called_once_with(spark_mock, log_table)
        create_runlog_entry_mock.assert_called_once_with(
            spark_mock,
            1,
            desc,
            version,
            config,
            pipeline,
        )
        _write_entry_mock.assert_called_once_with(entry_mock, log_table)

        # Assert function returned correct value
        assert result == entry_mock

    def test_add_runlog_entry_specified_id(self, mocker):
        """Tests adding an entry to the runlog with a specified run_id."""
        # Mock SparkSession
        spark_mock = mocker.Mock()

        # Set up test data
        run_id = 2
        desc = 'test description'
        version = '1.0'
        config = {'param1': 'value1', 'param2': 'value2'}
        pipeline = 'test_pipeline'
        log_table = 'test_log_table'

        # Mock create_runlog_entry, _write_entry
        entry_mock = mocker.Mock()
        create_runlog_entry_mock = mocker.patch(
            'rdsa_utils.cdsw.pipeline_runlog.create_runlog_entry',
            return_value=entry_mock,
        )
        _write_entry_mock = mocker.patch('rdsa_utils.cdsw.pipeline_runlog._write_entry')

        # Call function
        result = add_runlog_entry(
            spark_mock,
            desc,
            version,
            config,
            pipeline,
            log_table,
            run_id,
        )

        # Assert functions were called correctly
        create_runlog_entry_mock.assert_called_once_with(
            spark_mock,
            run_id,
            desc,
            version,
            config,
            pipeline,
        )
        _write_entry_mock.assert_called_once_with(entry_mock, log_table)

        # Assert function returned correct value
        assert result == entry_mock


class TestWriteRunlogFile:
    """Tests for write_runlog_file function."""

    def test_write_runlog_file(self, mocker):
        """Tests successful creation of a text file in HDFS with runlog metadata."""
        # Mock SparkSession
        spark_mock = mocker.Mock()

        # Set up test data
        runlog_table = 'test_log_table'
        runlog_id = 1
        path = '/test/path'

        # Mock _parse_runlog_as_string and create_txt_from_string
        parse_mock = mocker.patch(
            'rdsa_utils.cdsw.pipeline_runlog._parse_runlog_as_string',
        )
        parse_mock.return_value = 'test metadata'
        create_mock = mocker.patch(
            'rdsa_utils.cdsw.pipeline_runlog.create_txt_from_string',
        )

        # Call function
        write_runlog_file(spark_mock, runlog_table, runlog_id, path)

        # Assert functions were called correctly
        parse_mock.assert_called_once_with(spark_mock, runlog_table, runlog_id)
        create_mock.assert_called_once_with(path, 'test metadata')

    def test_write_runlog_file_edge_case(self, mocker):
        """Tests FileNotFoundError when the specified path is not found."""
        # Mock SparkSession
        spark_mock = mocker.Mock()

        # Set up test data
        runlog_table = 'test_log_table'
        runlog_id = 1
        path = '/nonexistent/path'

        # Mock _parse_runlog_as_string
        parse_mock = mocker.patch(
            'rdsa_utils.cdsw.pipeline_runlog._parse_runlog_as_string',
        )
        parse_mock.return_value = 'test metadata'

        # Call function and assert FileNotFoundError is raised
        with pytest.raises(FileNotFoundError):
            write_runlog_file(spark_mock, runlog_table, runlog_id, path)
