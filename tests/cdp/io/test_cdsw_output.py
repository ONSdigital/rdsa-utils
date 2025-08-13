"""Tests for the cdp/io/output.py module."""

import re
from typing import Callable
from unittest.mock import MagicMock, Mock, patch

import pytest
from moto import mock_aws
from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import types as T

from rdsa_utils.cdp.io.output import *


class TestInsertDataFrameToHiveTable:
    """Tests for insert_df_to_hive_table function."""

    @pytest.fixture
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
                T.StructField("id", T.IntegerType(), True),
                T.StructField("name", T.StringType(), True),
                T.StructField("age", T.IntegerType(), True),
            ],
        )
        df = create_spark_df(
            [
                (input_schema),
                (1, "Alice", 25),
                (2, "Bob", 30),
            ],
        )

        return df

    @patch("pyspark.sql.DataFrame.withColumn")
    @patch("pyspark.sql.DataFrameReader.table")
    @patch("pyspark.sql.functions.lit")
    def test_insert_df_to_hive_table_with_missing_columns(
        self,
        mock_lit,
        mock_table,
        mock_with_column,
        spark_session: SparkSession,
        test_df: SparkDF,
    ) -> None:
        """Test that insert_df_to_hive_table correctly inserts data into a Hive
        table when 'fill_missing_cols' is True.
        """
        table_name = "test_table"

        # Mock the table's columns to include 'address'
        mock_table.return_value.columns = ["id", "name", "age", "address"]

        # Mock the Hive table schema
        mock_table_schema = T.StructType(
            [
                T.StructField("id", T.IntegerType()),
                T.StructField("name", T.StringType()),
                T.StructField("age", T.IntegerType()),
                T.StructField(
                    "address",
                    T.StringType(),
                ),  # Include 'address' with StringType
            ],
        )
        mock_table.return_value.schema = mock_table_schema

        # Create a mock of the DataFrame (test_df) that does not contain 'address'
        test_df_mock = MagicMock()
        test_df_mock.columns = [
            "id",
            "name",
            "age",
        ]  # Mock that it doesn't have 'address'

        # Mock `withColumn` behavior - ensure it's being called
        # with correct column expression
        mock_with_column.return_value = (
            test_df_mock  # Simulate that `withColumn` returns `test_df_mock`
        )

        # Mock the return of the `lit` function to simulate the expression
        mock_lit.return_value = F.lit(None).cast(
            T.StringType(),
        )  # We expect the `lit` to return the null expression

        # Call the function to insert data into the table
        insert_df_to_hive_table(
            spark_session,
            test_df,
            table_name,
            overwrite=True,
            fill_missing_cols=True,
        )

        # Assert that `lit` was called with `None` to create the null expression
        mock_lit.assert_called_with(None)

        # Since `lit().cast()` returns the same object,
        # directly assert the final return value
        expected_column = F.lit(None).cast(T.StringType())

        # Check if withColumn was called with 'address' and the expected expression
        # Compare the exact column expression (the expected one, not the mock)
        mock_with_column.assert_any_call("address", expected_column)

    @patch("rdsa_utils.cdp.io.output.is_df_empty", return_value=False)
    @patch("pyspark.sql.DataFrameReader.table")
    def test_insert_df_to_hive_table_schema_mismatch(
        self,
        mock_table,
        mock_is_empty,
        spark_session: SparkSession,
        caplog,
        test_df,
    ) -> None:
        """Test ValueError for schema mismatch with 'fill_missing_cols=False'.

        This test verifies that the function correctly identifies and reports a
        schema mismatch between the source DataFrame and the target Hive table
        when automatic column filling is disabled.

        The specific mismatch scenario is configured as follows:
        - DataFrame columns: ['id', 'name', 'age']
        - Mock Hive table columns: ['id', 'name', 'address', 'status']

        This creates a difference where:
        - Columns 'address' and 'status' are missing from the DataFrame.
        - Column 'age' is an extra column in the DataFrame that is not
          present in the target table.

        The test asserts that a `ValueError` is raised and that its message
        accurately details these specific column differences.
        """
        table_name = "test_db.mismatched_table"

        mock_table_schema = T.StructType(
            [
                T.StructField("id", T.IntegerType()),
                T.StructField("name", T.StringType()),
                T.StructField("address", T.StringType()),
                T.StructField("status", T.StringType()),
            ],
        )
        mock_hive_df = MagicMock()
        mock_hive_df.schema = mock_table_schema
        mock_hive_df.columns = ["id", "name", "address", "status"]
        mock_table.return_value = mock_hive_df

        expected_error_pattern = re.escape(
            f"Schema mismatch for table '{table_name}' with 'fill_missing_cols=False'.\n"
            f"  - Columns missing from DataFrame: ['address', 'status']\n"
            f"  - Extra columns in DataFrame: ['age']",
        )

        caplog.set_level(logging.INFO)

        with pytest.raises(ValueError, match=expected_error_pattern):
            insert_df_to_hive_table(
                spark=spark_session,
                df=test_df,
                table_name=table_name,
                fill_missing_cols=False,
            )

    @patch("pyspark.sql.DataFrameWriter.insertInto")
    @patch("pyspark.sql.DataFrameReader.table")
    def test_insert_df_to_hive_table_insert_into_existing_table(
        self,
        mock_table,
        mock_insert_into,
        spark_session: SparkSession,
        test_df: SparkDF,
    ) -> None:
        """Test that insertInto is called when the table already exists in Hive."""
        table_name = "existing_table"

        # Mock the table columns to simulate the table already exists
        mock_table.return_value.columns = ["id", "name", "age"]

        # Simulate a successful call to `insertInto`
        mock_insert_into.return_value = None

        # Call the function that triggers insertInto when the table exists
        insert_df_to_hive_table(
            spark_session,
            test_df,
            table_name,
        )

        # Assert that insertInto was called with the correct table name
        mock_insert_into.assert_called_once_with(table_name)

    @patch("pyspark.sql.DataFrameWriter.saveAsTable")
    @patch("pyspark.sql.DataFrameReader.table")
    def test_insert_df_to_hive_table_save_as_table_when_table_does_not_exist(
        self,
        mock_table,
        mock_save_as_table,
        spark_session: SparkSession,
        test_df: SparkDF,
    ) -> None:
        """Test that saveAsTable is called when the Hive table does not exist."""
        table_name = "new_table"

        # Simulate the table not existing by raising an AnalysisException
        mock_table.side_effect = AnalysisException(f"Table {table_name} not found.")

        # Simulate a successful call to `saveAsTable`
        mock_save_as_table.return_value = None

        # Call the function that triggers saveAsTable when the table does not exist
        insert_df_to_hive_table(
            spark_session,
            test_df,
            table_name,
        )

        # Assert that saveAsTable was called with the correct table name
        mock_save_as_table.assert_called_once_with(table_name)

    @patch("pyspark.sql.DataFrame.repartition")
    @patch("pyspark.sql.DataFrameReader.table")
    def test_insert_df_to_hive_table_with_repartition_data_by(
        self,
        mock_table,
        mock_repartition,
        spark_session: SparkSession,
        test_df: SparkDF,
    ) -> None:
        """Test that the DataFrame is repartitioned by a specified column."""
        table_name = "test_table"
        mock_table.return_value.columns = ["id", "name", "age"]

        # Ensure that mock_repartition is set to return the same test_df
        mock_repartition.return_value = test_df

        # Call the function that triggers repartition
        insert_df_to_hive_table(
            spark_session,
            test_df,
            table_name,
            repartition_data_by="id",  # We expect "id" column to be used for repartitioning
            overwrite=True,
        )

        # Assert that repartition was called with the correct argument (the column name)
        mock_repartition.assert_called_once_with("id")

    @patch("pyspark.sql.DataFrame.repartition")
    @patch("pyspark.sql.DataFrameReader.table")
    def test_insert_df_to_hive_table_with_repartition_num_partitions(
        self,
        mock_table,
        mock_repartition,
        spark_session: SparkSession,
        test_df: SparkDF,
    ) -> None:
        """Test that the DataFrame is repartitioned into a specific number of partitions."""
        table_name = "test_table"
        mock_table.return_value.columns = ["id", "name", "age"]

        # Ensure that mock_repartition is set to return the same test_df
        mock_repartition.return_value = test_df

        # Call the function that triggers repartition
        insert_df_to_hive_table(
            spark_session,
            test_df,
            table_name,
            repartition_data_by=5,  # Expecting 5 partitions
            overwrite=True,
        )

        # Assert that repartition was called with the number of partitions (5)
        mock_repartition.assert_called_once_with(5)

    def test_insert_df_to_hive_table_with_empty_dataframe(
        self,
        spark_session: SparkSession,
    ) -> None:
        """Test that an empty DataFrame raises DataframeEmptyError."""
        from rdsa_utils.exceptions import DataframeEmptyError

        table_name = "test_table"
        empty_df = spark_session.createDataFrame(
            [],
            schema="id INT, name STRING, age INT",
        )
        with pytest.raises(DataframeEmptyError):
            insert_df_to_hive_table(
                spark_session,
                empty_df,
                table_name,
            )


class TestWriteAndReadHiveTable:
    """Tests for write_and_read_hive_table function."""

    @pytest.fixture
    def mock_spark(self):
        """Fixture for mocked SparkSession."""
        return Mock(spec=SparkSession)

    @pytest.fixture
    def mock_df(self):
        """Fixture for mocked DataFrame with 'run_id' and 'data' columns."""
        mock_df = Mock(spec=SparkDF)
        mock_df.columns = ["run_id", "data"]
        return mock_df

    @patch("rdsa_utils.cdp.io.output.load_and_validate_table")
    @patch("rdsa_utils.cdp.io.output.insert_df_to_hive_table")
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
            "test_table",
            "test_database",
            "test_run",
        )

        # Verify the calls
        mock_insert.assert_called_once_with(
            mock_spark,
            mock_df,
            "test_database.test_table",
            fill_missing_cols=False,
        )
        mock_load_and_validate.assert_called_once_with(
            mock_spark,
            "test_database.test_table",
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
            match="The specified Hive table test_database.test_table does not exist.",
        ):
            write_and_read_hive_table(
                mock_spark,
                mock_df,
                "test_table",
                "test_database",
                "test_run",
            )

    def test_df_missing_filter_column(self, mock_spark, mock_df):
        """Check exception handling when the DataFrame is missing the filter
        column.
        """
        mock_df.columns = ["col1", "col2"]
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
                "test_table",
                "test_database",
                "test_run",
            )


class TestSaveCSVToHDFS:
    """Tests for save_csv_to_hdfs function."""

    @pytest.fixture
    def mock_df(self) -> Mock:
        """Fixture for mocked Spark DataFrame."""
        return Mock(spec=SparkDF)

    @patch("rdsa_utils.cdp.io.output.logger")
    @patch("rdsa_utils.cdp.io.output.delete_path")
    @patch("rdsa_utils.cdp.io.output.rename")
    @patch("rdsa_utils.cdp.io.output.file_exists")
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

        file_name = "test_output.csv"
        file_path = "/test/hdfs/path"

        save_csv_to_hdfs(mock_df, file_name, file_path)

        mock_df.coalesce.assert_called_once_with(1)
        mock_rename.assert_called_once()
        mock_delete_path.assert_called_once()
        assert mock_logger.info.call_count > 0

    @patch("rdsa_utils.cdp.io.output.file_exists")
    @patch("rdsa_utils.cdp.io.output.rename")
    @patch("rdsa_utils.cdp.io.output.delete_path")
    @patch("rdsa_utils.cdp.io.output.logger")
    def test_overwriting_existing_file(
        self,
        mock_logger,
        mock_delete_path,
        mock_rename,
        mock_file_exists,
        mock_df,
    ):
        """Ensure the function correctly overwrites an existing file when overwrite=True."""
        file_name = "should_overwrite.csv"
        file_path = "/test/overwrite/path"

        # Attempt to save, expecting no errors
        try:
            save_csv_to_hdfs(mock_df, file_name, file_path, overwrite=True)
        except Exception as e:
            pytest.fail(f"Function raised an unexpected exception: {e}")

        mock_rename.assert_called_once()

    @patch("rdsa_utils.cdp.io.output.file_exists")
    def test_save_csv_to_hdfs_file_exists_error(
        self,
        mock_file_exists,
        mock_df,
    ):
        """Test error raised when the target file exists and overwrite is False."""
        mock_file_exists.return_value = True

        file_name = "test_output.csv"
        file_path = "/test/hdfs/path"

        with pytest.raises(IOError):
            save_csv_to_hdfs(mock_df, file_name, file_path, overwrite=False)

        mock_file_exists.assert_called_once_with(
            f"{file_path.rstrip('/')}/{file_name}",
        )

    def test_save_csv_to_hdfs_invalid_file_name(self, mock_df):
        """Test error raised when file name does not end with '.csv'."""
        file_name = "invalid_file_name"
        file_path = "/test/hdfs/path"

        with pytest.raises(ValueError):
            save_csv_to_hdfs(mock_df, file_name, file_path)

    @pytest.mark.parametrize(
        ("file_path", "expected_call"),
        [
            ("s3a://bucket/path/", "s3a://bucket/path/should_write.csv"),
            ("/user/hdfs/test/path", "/user/hdfs/test/path/should_write.csv"),
        ],
    )
    @patch("rdsa_utils.cdp.io.output.file_exists")
    @patch("rdsa_utils.cdp.io.output.rename")
    @patch("rdsa_utils.cdp.io.output.delete_path")
    @patch("rdsa_utils.cdp.io.output.logger")
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
        file_name = "should_write.csv"

        try:
            save_csv_to_hdfs(mock_df, file_name, file_path)
        except Exception as e:
            pytest.fail(
                f"Function raised an unexpected exception with file path '{file_path}': {e}",
            )

        # We're focusing on path handling, so just ensure it gets to the rename call
        mock_rename.assert_called_once()


class TestSaveCSVToS3:
    """Tests for save_csv_to_s3 function."""

    @pytest.fixture(scope="class")
    def _aws_credentials(self):
        """Mock AWS Credentials for moto."""
        boto3.setup_default_session(
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            aws_session_token="testing",
        )

    @pytest.fixture(scope="class")
    def s3_client(self, _aws_credentials):
        """Provide a mocked AWS S3 client for testing
        using moto with temporary credentials.
        """
        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="test-bucket")
            yield client

    @pytest.fixture
    def dummy_dataframe(self, spark_session):
        """Create a dummy PySpark DataFrame for testing."""
        data = [("John", 1), ("Jane", 2), ("Bob", 3)]
        columns = ["name", "id"]
        return spark_session.createDataFrame(data, columns)

    @patch("rdsa_utils.cdp.io.output.SparkDF.write")
    @patch("rdsa_utils.cdp.io.output.list_files")
    @patch("rdsa_utils.cdp.io.output.copy_file")
    @patch("rdsa_utils.cdp.io.output.delete_folder")
    @patch("rdsa_utils.cdp.io.output.file_exists")
    @patch("uuid.uuid4")
    def test_save_csv_to_s3(
        self,
        mock_uuid,
        mock_file_exists,
        mock_delete_folder,
        mock_copy_file,
        mock_list_files,
        mock_write,
        dummy_dataframe,
        s3_client,
    ):
        """Test saving a PySpark DataFrame to S3 as a CSV file."""
        bucket_name = "test-bucket"
        file_name = "data_output.csv"
        file_path = "data_folder/"

        # Mock UUID to return a fixed value
        mock_uuid.return_value.hex = "1234"

        # Mock the relevant methods
        mock_write.return_value.csv.return_value = None
        mock_list_files.return_value = [
            f'{file_path.rstrip("/")}/temp_1234_data_output.csv/part-00000.csv',
        ]
        mock_delete_folder.return_value = None
        mock_file_exists.side_effect = lambda client, bucket, key: (
            False if key == f"{file_path.rstrip('/')}/{file_name}" else False
        )

        def copy_file_side_effect(
            s3_client,
            src_bucket_name,
            src_key,
            dest_bucket_name,
            dest_key,
            overwrite,
        ):
            # Simulate copying by creating an object in the destination
            copy_source = {"Bucket": src_bucket_name, "Key": src_key}
            s3_client.copy_object(
                CopySource=copy_source,
                Bucket=dest_bucket_name,
                Key=dest_key,
            )
            return True

        mock_copy_file.side_effect = copy_file_side_effect

        # Create the temporary file in the bucket to simulate the write operation
        temp_key = f'{file_path.rstrip("/")}/temp_1234_data_output.csv/part-00000.csv'
        s3_client.put_object(Bucket=bucket_name, Key=temp_key, Body="data")

        # Call the function
        save_csv_to_s3(
            df=dummy_dataframe,
            bucket_name=bucket_name,
            file_name=file_name,
            file_path=file_path,
            s3_client=s3_client,
            overwrite=True,
        )

        # Check if the file is saved using boto3
        destination_path = f"{file_path.rstrip('/')}/{file_name}"
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=file_path,
        )
        keys = [obj["Key"] for obj in response.get("Contents", [])]
        assert destination_path in keys

    @patch("rdsa_utils.cdp.io.output.SparkDF.write")
    @patch("rdsa_utils.cdp.io.output.list_files")
    @patch("rdsa_utils.cdp.io.output.copy_file")
    @patch("rdsa_utils.cdp.io.output.delete_folder")
    @patch("rdsa_utils.cdp.io.output.file_exists")
    @patch("uuid.uuid4")
    def test_save_csv_to_s3_overwrite_false(
        self,
        mock_uuid,
        mock_file_exists,
        mock_delete_folder,
        mock_copy_file,
        mock_list_files,
        mock_write,
        dummy_dataframe,
        s3_client,
    ):
        """Test saving a PySpark DataFrame to S3 with overwrite set to False."""
        bucket_name = "test-bucket"
        file_name = "data_output.csv"
        file_path = "data_folder/"

        # Mock UUID to return a fixed value
        mock_uuid.return_value.hex = "1234"

        # Mock the relevant methods
        mock_write.return_value.csv.return_value = None
        mock_list_files.return_value = [
            f'{file_path.rstrip("/")}/temp_1234_data_output.csv/part-00000.csv',
        ]
        mock_delete_folder.return_value = None

        def copy_file_side_effect(
            s3_client,
            src_bucket_name,
            src_key,
            dest_bucket_name,
            dest_key,
            overwrite,
        ):
            # Simulate copying by creating an object in the destination
            copy_source = {"Bucket": src_bucket_name, "Key": src_key}
            s3_client.copy_object(
                CopySource=copy_source,
                Bucket=dest_bucket_name,
                Key=dest_key,
            )
            return True

        mock_copy_file.side_effect = copy_file_side_effect

        # Create the temporary file in the bucket to simulate the write operation
        temp_key = f'{file_path.rstrip("/")}/temp_1234_data_output.csv/part-00000.csv'
        s3_client.put_object(Bucket=bucket_name, Key=temp_key, Body="data")

        # Set up file_exists to return False initially, then True
        mock_file_exists.side_effect = lambda client, bucket, key: (
            True if key == f"{file_path.rstrip('/')}/{file_name}" else False
        )

        # Save the DataFrame once
        save_csv_to_s3(
            df=dummy_dataframe,
            bucket_name=bucket_name,
            file_name=file_name,
            file_path=file_path,
            s3_client=s3_client,
            overwrite=True,
        )

        # Try to save the DataFrame again with overwrite set to False,
        # which should raise IOError
        with pytest.raises(IOError):
            save_csv_to_s3(
                df=dummy_dataframe,
                bucket_name=bucket_name,
                file_name=file_name,
                file_path=file_path,
                s3_client=s3_client,
                overwrite=False,
            )

    @patch("rdsa_utils.cdp.io.output.SparkDF.write")
    def test_save_csv_to_s3_invalid_extension(
        self,
        mock_write,
        dummy_dataframe,
        s3_client,
    ):
        """Test saving a PySpark DataFrame to S3 with an invalid file extension."""
        bucket_name = "test-bucket"
        file_name = "data_output.txt"
        file_path = "data_folder/"

        mock_write.return_value.csv.return_value = None

        with pytest.raises(ValueError):
            save_csv_to_s3(
                df=dummy_dataframe,
                bucket_name=bucket_name,
                file_name=file_name,
                file_path=file_path,
                s3_client=s3_client,
                overwrite=True,
            )
