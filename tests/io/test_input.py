"""Tests for the input.py module."""
from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame as SparkDF

from rdsa_utils.exceptions import DataframeEmptyError
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


@pytest.mark.skip(reason='test shell')
class TestReadFile:
    """Tests for read_file function."""

    def test_expected(self):
        """Test expected functionality."""
        pass


class TestGetCurrentDatabase:
    """Tests for get_current_database function."""

    @pytest.fixture()
    def setup_and_teardown_database(self, spark_session: SparkSession) -> None: # noqa: PT004
        """
        Fixture that sets up a dummy Spark database for testing.

        This fixture creates a test database named 'temp_test_db'. After the
        tests using this fixture are completed, it cleans up by dropping the
        test database.

        Parameters
        ----------
        spark_session : SparkSession
            Active SparkSession to use for creating and deleting the test
            database.

        Yields
        ------
        None
        """
        spark_session.sql('CREATE DATABASE IF NOT EXISTS temp_test_db')
        yield
        spark_session.sql('DROP DATABASE IF EXISTS temp_test_db')

    def test_get_current_database_default(
        self, spark_session: SparkSession,
    ) -> None:
        """Test that get_current_database returns the default database when no
        database is explicitly set.
        """
        current_db = get_current_database(spark_session)
        default_db = spark_session.sql('SELECT current_database()').collect()[
            0
        ]['current_database()']
        assert current_db == default_db

    def test_get_current_database_after_setting(
        self, spark_session: SparkSession, setup_and_teardown_database: None,
    ) -> None:
        """Test that get_current_database returns the correct database after
        explicitly setting a different active database.
        """
        spark_session.sql('USE temp_test_db')
        current_db = get_current_database(spark_session)
        assert current_db == 'temp_test_db'


class TestExtractDatabaseName:
    """Tests for extract_database_name function."""

    @pytest.fixture()
    def dummy_database_and_table(self, spark_session: SparkSession) -> str:
        """Fixture that creates a dummy Spark database and table for testing.

        This fixture creates a test database named 'test_db' and a test table
        named 'test_table' in that database. The table is simple and contains
        two columns: 'name' (a string) and 'age' (an integer).

        The name of the table in the form 'database.table' is then
        yielded for use in the tests.

        After the tests using this fixture are completed, it cleans up by
        dropping the test table and the test database.

        Parameters
        ----------
        spark_session
            Active SparkSession to use for creating and deleting the test
            database and table.

        Yields
        ------
        str
            The name of the test table in the form 'database.table'.
        """
        spark_session.sql('CREATE DATABASE IF NOT EXISTS test_db')
        spark_session.sql(
            'CREATE TABLE IF NOT EXISTS test_db.test_table (name STRING, age INT)',
        )
        yield 'test_db.test_table'
        spark_session.sql('DROP TABLE IF EXISTS test_db.test_table')
        spark_session.sql('DROP DATABASE IF EXISTS test_db')

    def test_extract_database_name_correct_format(
        self,
        spark_session: SparkSession,
        dummy_database_and_table: str,
    ) -> None:
        """Test that extract_database_name correctly identifies the database and
        table name from a correctly formatted input.
        """
        long_table_name = dummy_database_and_table
        db_name, table_name = extract_database_name(
            spark_session, long_table_name,
        )
        assert db_name == 'test_db'
        assert table_name == 'test_table'

    def test_extract_database_name_incorrect_format(
        self,
        spark_session: SparkSession,
    ) -> None:
        """Test that extract_database_name raises a ValueError when the input is
        incorrectly formatted.
        """
        long_table_name = 'part1.part2.part3.part4'
        with pytest.raises(ValueError):
            db_name, table_name = extract_database_name(
                spark_session, long_table_name,
            )

    def test_extract_database_name_gcp_format(
        self,
        spark_session: SparkSession,
    ) -> None:
        """Test that extract_database_name correctly identifies the database and
        table name from the GCP format input.
        """
        long_table_name = 'project_name.test_db.test_table'
        db_name, table_name = extract_database_name(
            spark_session, long_table_name,
        )
        assert db_name == 'test_db'
        assert table_name == 'test_table'

    def test_extract_database_name_no_specified_database(
        self,
        spark_session: SparkSession,
    ) -> None:
        """Test that extract_database_name correctly identifies the current
        database when no database is specified in the input.
        """
        long_table_name = 'test_table'
        db_name, table_name = extract_database_name(
            spark_session, long_table_name,
        )
        current_db = spark_session.sql('SELECT current_database()').collect()[
            0
        ]['current_database()']
        assert db_name == current_db
        assert table_name == 'test_table'


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
                spark_session,
                table_name,
                filter_cond=filter_cond,
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
