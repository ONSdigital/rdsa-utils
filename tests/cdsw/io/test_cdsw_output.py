"""Tests for the cdsw/io/output.py module."""
from typing import Callable
from unittest.mock import Mock, patch

import pytest
from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import types as T

from rdsa_utils.cdsw.io.output import *


class TestInsertDataFrameToHiveTable:
    """Tests for insert_df_to_hive_table function."""

    @pytest.fixture()
    def test_df(self, spark_session: SparkSession, create_spark_df: Callable):
        """Fixture to create a test DataFrame with the help of `create_spark_df`
        callable.

        This fixture uses the `create_spark_df` callable to generate
        a DataFrame for testing.

        The created DataFrame has three columns: 'id' (an integer),
        'name' (a string), and 'age' (an integer).

        It has two rows of data with values (1, 'Alice', 25)
        and (2, 'Bob', 30).

        Parameters
        ----------
        spark_session
            Active SparkSession to use for creating the DataFrame.
        create_spark_df
            A callable function to create a DataFrame.

        Returns
        -------
        SparkDF
            A DataFrame with specified schema and data.
        """
        input_schema = T.StructType(
            [
                T.StructField('id', T.IntegerType(), True),
                T.StructField('name', T.StringType(), True),
                T.StructField('age', T.IntegerType(), True),
            ],
        )
        df = create_spark_df(
            [
                (input_schema),
                (1, 'Alice', 25),
                (2, 'Bob', 30),
            ],
        )

        return df

    @patch('pyspark.sql.DataFrameWriter.insertInto')
    @patch('pyspark.sql.DataFrameReader.table')
    def test_insert_df_to_hive_table_with_missing_columns(
        self,
        mock_table,
        mock_insert_into,
        spark_session: SparkSession,
        test_df: SparkDF,
    ) -> None:
        """Test that insert_df_to_hive_table correctly inserts data into a Hive
        table when 'fill_missing_cols' is True.
        """
        table_name = 'test_table'
        # Mock the table columns
        mock_table.return_value.columns = ['id', 'name', 'age', 'address']
        # Mock the DataFrameWriter insertInto
        mock_insert_into.return_value = None
        insert_df_to_hive_table(
            spark_session,
            test_df,
            table_name,
            overwrite=True,
            fill_missing_cols=True,
        )
        # Assert that insertInto was called with correct arguments
        mock_insert_into.assert_called_with(table_name, True)

    @patch('pyspark.sql.DataFrameReader.table')
    def test_insert_df_to_hive_table_without_missing_columns(
        self,
        mock_table,
        spark_session: SparkSession,
        test_df: SparkDF,
    ) -> None:
        """Test that insert_df_to_hive_table raises a ValueError when
        'fill_missing_cols' is False and DataFrame schema doesn't match with the
        table schema.
        """
        table_name = 'test_table'
        # Mock the table columns
        mock_table.return_value.columns = ['id', 'name', 'age', 'address']
        with pytest.raises(ValueError):
            insert_df_to_hive_table(
                spark_session,
                test_df,
                table_name,
                fill_missing_cols=False,
            )

    @patch('pyspark.sql.DataFrameReader.table')
    def test_insert_df_to_hive_table_with_non_existing_table(
        self,
        mock_table,
        spark_session: SparkSession,
        test_df: SparkDF,
    ) -> None:
        """Test that insert_df_to_hive_table raises an AnalysisException when
        the table doesn't exist.
        """
        table_name = 'non_existing_table'
        # Create an AnalysisException with a stack trace
        exc = AnalysisException(f'Table {table_name} not found.')
        mock_table.side_effect = exc
        with pytest.raises(AnalysisException):
            insert_df_to_hive_table(spark_session, test_df, table_name)


class TestWriteAndReadHiveTable:
    """Tests for write_and_read_hive_table function."""

    @pytest.fixture()
    def mock_spark(self):
        """Fixture for mocked SparkSession."""
        return Mock(spec=SparkSession)

    @pytest.fixture()
    def mock_df(self):
        """Fixture for mocked DataFrame with 'run_id' and 'data' columns."""
        mock_df = Mock(spec=SparkDF)
        mock_df.columns = ['run_id', 'data']
        return mock_df

    @patch('rdsa_utils.cdsw.io.output.load_and_validate_table')
    @patch('rdsa_utils.cdsw.io.output.insert_df_to_hive_table')
    def test_write_and_read_hive_table_success(
        self,
        mock_insert,
        mock_load_and_validate,
        mock_spark,
        mock_df,
    ):
        """Test that write_and_read_hive_table function successfully writes
        SparkDF to the Hive table and reads it back when all arguments are
        valid.
        """
        # Mock the functions
        mock_insert.return_value = None
        mock_load_and_validate.return_value = mock_df

        # Call the function
        result_df = write_and_read_hive_table(
            mock_spark,
            mock_df,
            'test_table',
            'test_database',
            'test_run',
        )

        # Verify the calls
        mock_insert.assert_called_once_with(
            mock_spark,
            mock_df,
            'test_database.test_table',
            fill_missing_cols=False,
        )
        mock_load_and_validate.assert_called_once_with(
            mock_spark,
            'test_database.test_table',
            skip_validation=False,
            err_msg=None,
            filter_cond="run_id = 'test_run'",
        )

        # Check the result
        assert result_df == mock_df

    def test_hive_table_does_not_exist(self, mock_spark, mock_df):
        """Check exception handling when the Hive table does not exist."""
        mock_spark.catalog.tableExists.return_value = False
        with pytest.raises(
            TableNotFoundError,
            match='The specified Hive table test_database.test_table does not exist.',
        ):
            write_and_read_hive_table(
                mock_spark,
                mock_df,
                'test_table',
                'test_database',
                'test_run',
            )

    def test_df_missing_filter_column(self, mock_spark, mock_df):
        """Check exception handling when the DataFrame is missing the filter
        column.
        """
        mock_df.columns = ['col1', 'col2']
        with pytest.raises(
            ColumnNotInDataframeError,
            match=(
                "The provided DataFrame doesn't contain the specified "
                "filter column: run_id"
            ),
        ):
            write_and_read_hive_table(
                mock_spark,
                mock_df,
                'test_table',
                'test_database',
                'test_run',
            )


class TestSaveCSVToHDFS:
    """Tests for save_csv_to_hdfs function."""

    @pytest.fixture()
    def mock_df(self) -> Mock:
        """Fixture for mocked Spark DataFrame."""
        return Mock(spec=SparkDF)

    @patch('rdsa_utils.cdsw.io.output.logger')
    @patch('rdsa_utils.cdsw.io.output.delete_path')
    @patch('rdsa_utils.cdsw.io.output.rename')
    @patch('rdsa_utils.cdsw.io.output.file_exists')
    def test_save_csv_to_hdfs_success(
        self,
        mock_file_exists,
        mock_rename,
        mock_delete_path,
        mock_logger,
        mock_df,
    ):
        """Test successful saving of DataFrame to HDFS as a single CSV file."""
        mock_file_exists.return_value = False
        mock_rename.return_value = True

        file_name = 'test_output.csv'
        file_path = '/test/hdfs/path'

        save_csv_to_hdfs(mock_df, file_name, file_path)

        mock_df.coalesce.assert_called_once_with(1)
        mock_rename.assert_called_once()
        mock_delete_path.assert_called_once()
        assert mock_logger.info.call_count > 0

    @patch('rdsa_utils.cdsw.io.output.file_exists')
    @patch('rdsa_utils.cdsw.io.output.rename')
    @patch('rdsa_utils.cdsw.io.output.delete_path')
    @patch('rdsa_utils.cdsw.io.output.logger')
    def test_overwriting_existing_file(
        self,
        mock_logger,
        mock_delete_path,
        mock_rename,
        mock_file_exists,
        mock_df,
    ):
        """Ensure the function correctly overwrites an existing file when overwrite=True."""
        file_name = 'should_overwrite.csv'
        file_path = '/test/overwrite/path'

        # Attempt to save, expecting no errors
        try:
            save_csv_to_hdfs(mock_df, file_name, file_path, overwrite=True)
        except Exception as e:
            pytest.fail(f'Function raised an unexpected exception: {e}')

        mock_rename.assert_called_once()

    @patch('rdsa_utils.cdsw.io.output.file_exists')
    def test_save_csv_to_hdfs_file_exists_error(
        self,
        mock_file_exists,
        mock_df,
    ):
        """Test error raised when the target file exists and overwrite is False."""
        mock_file_exists.return_value = True

        file_name = 'test_output.csv'
        file_path = '/test/hdfs/path'

        with pytest.raises(IOError):
            save_csv_to_hdfs(mock_df, file_name, file_path, overwrite=False)

        mock_file_exists.assert_called_once_with(f"{file_path.rstrip('/')}/{file_name}")

    def test_save_csv_to_hdfs_invalid_file_name(self, mock_df):
        """Test error raised when file name does not end with '.csv'."""
        file_name = 'invalid_file_name'
        file_path = '/test/hdfs/path'

        with pytest.raises(ValueError):
            save_csv_to_hdfs(mock_df, file_name, file_path)

    @pytest.mark.parametrize(
        ('file_path', 'expected_call'),
        [
            ('s3a://bucket/path/', 's3a://bucket/path/should_write.csv'),
            ('/user/hdfs/test/path', '/user/hdfs/test/path/should_write.csv'),
        ],
    )
    @patch('rdsa_utils.cdsw.io.output.file_exists', return_value=False, create=True)
    @patch('rdsa_utils.cdsw.io.output.rename', create=True)
    @patch('rdsa_utils.cdsw.io.output.delete_path', create=True)
    @patch('rdsa_utils.cdsw.io.output.logger', create=True)
    def test_file_path_schemes(
        self,
        mock_logger,
        mock_delete_path,
        mock_rename,
        mock_file_exists,
        mock_df,
        file_path,
        expected_call,
    ):
        """Test the function with different file path schemes, including S3 and HDFS."""
        file_name = 'should_write.csv'

        try:
            save_csv_to_hdfs(mock_df, file_name, file_path)
        except Exception as e:
            pytest.fail(
                f"Function raised an unexpected exception with file path '{file_path}': {e}",
            )

        # We're focusing on path handling, so just ensure it gets to the rename call
        mock_rename.assert_called_once()
