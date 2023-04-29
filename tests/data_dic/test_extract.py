"""
This module contains pytest functions to test the behaviour of the functions
contained within the rdsa_utils/data_dic/extract.py Module.

This module contains Pytest unit tests for the following helper functions:
    _extract_column_info: Extracts column information such as column name, data type, constraints, and description from a column line in a Hive DDL script.
    _extract_columns_part: Extracts the columns part of a Hive DDL 'CREATE TABLE' statement.
    _extract_database_and_table_name: Extracts the database and table names from a Hive DDL 'CREATE TABLE' statement.
    _extract_partition_columns: Extracts partition column names from a Hive DDL 'CREATE TABLE' statement.
    _extract_storage_type: Extracts the storage type from a Hive DDL 'CREATE TABLE' statement.

As well as the main function:
    extract_table_information_hive: Extracts table information (table name, column name, data type, constraints, description, storage type, partition columns) from Hive DDL scripts.

Each of the functions is tested using various test cases to ensure they correctly extract the required information from Hive DDL scripts.
"""
import re

import pytest

from rdsa_utils.data_dic.extract import (
    RegexPatterns,
    TableInformation,
    _extract_column_info,
    _extract_columns_part,
    _extract_database_and_table_name,
    _extract_partition_columns,
    _extract_storage_type,
    extract_table_information_hive,
)

patterns = RegexPatterns()


@pytest.mark.parametrize(
    "script, expected_output",
    [
        (
            "CREATE TABLE test_db.sample_table (id INT, name STRING);",
            ("test_db", "sample_table"),
        ),
        ("CREATE TABLE sample_table (id INT, name STRING);", (None, "sample_table")),
    ],
)
def test_extract_database_and_table_name(script, expected_output):
    """
    Test the extract_database_and_table_name function with various input scripts.

    Test cases cover scenarios where:
    1. Both the database and table names are present in the script.
    2. Only the table name is present in the script.
    """
    database_name, table_name = _extract_database_and_table_name(script, patterns)
    assert (database_name, table_name) == expected_output


@pytest.mark.parametrize(
    "script, expected_output",
    [
        (
            "CREATE TABLE test_db.sample_table (id INT, name STRING);",
            "id INT, name STRING",
        ),
        (
            "CREATE TABLE test_db.sample_table (id INT, name STRING, age INT);",
            "id INT, name STRING, age INT",
        ),
    ],
)
def test_extract_columns_part(script, expected_output):
    """
    Test the extract_columns_part function with various input scripts.

    Test cases cover scenarios where:
    1. The table has two columns.
    2. The table has three columns.
    """
    columns_part = _extract_columns_part(script, patterns)
    assert columns_part == expected_output


@pytest.mark.parametrize(
    "script, expected_output",
    [
        (
            "CREATE TABLE test_db.sample_table (id INT, name STRING) STORED AS ORC;",
            "orc",
        ),
        ("CREATE TABLE test_db.sample_table (id INT, name STRING);", None),
    ],
)
def test_extract_storage_type(script, expected_output):
    """
    Test the extract_storage_type function with various input scripts.

    Test cases cover scenarios where:
    1. The table has a specified storage type.
    2. The table does not have a specified storage type.
    """
    storage_type = _extract_storage_type(script, patterns)
    assert storage_type == expected_output


@pytest.mark.parametrize(
    "script, expected_output",
    [
        (
            "CREATE TABLE test_db.sample_table (id INT, name STRING) PARTITIONED BY (date STRING);",
            "date",
        ),
        (
            "CREATE TABLE test_db.sample_table (id INT, name STRING) PARTITIONED BY (date STRING, country STRING);",
            "date, country",
        ),
        ("CREATE TABLE test_db.sample_table (id INT, name STRING);", ""),
    ],
)
def test_extract_partition_columns(script, expected_output):
    """
    Test the extract_partition_columns function with various input scripts.

    Test cases cover scenarios where:
    1. The table has one partition column.
    2. The table has multiple partition columns.
    3. The table does not have any partition columns.
    """
    partition_columns = _extract_partition_columns(script, patterns)
    assert partition_columns == expected_output


@pytest.mark.parametrize(
    "column_line, expected_output",
    [
        (
            "id INT NOT NULL -- The ID of the item",
            ("id", "INT", "NOT NULL", "The ID of the item"),
        ),
        ("id INT", ("id", "INT", "", "")),
        ("id INT -- The ID of the item", ("id", "INT", "", "The ID of the item")),
    ],
)
def test_extract_column_info(column_line, expected_output):
    """
    Test the extract_column_info function with various input column lines.

    Test cases cover scenarios where:
    1. The column has a data type, constraints, and a description.
    2. The column has only a data type.
    3. The column has a data type and a description.
    """
    column_match = re.search(patterns.column_info, column_line.strip(), re.IGNORECASE)
    column_name, data_type, constraints, description = _extract_column_info(
        column_match
    )
    assert (column_name, data_type, constraints, description) == expected_output


def test_extract_table_information_hive():
    """
    Test the `extract_table_information_hive()` function to ensure that it correctly
    extracts table information from Hive DDL scripts.

    This test covers the following scenarios:
    - A basic Hive DDL script with a single table, multiple columns, and no partitioning.
    - A Hive DDL script with multiple tables, each having different storage types and partition columns.
    """
    # Test case 1: Basic Hive DDL script with a single table and multiple columns
    ddl_script1 = """
    CREATE TABLE test_db.test_table1 (
        id INT -- Unique identifier,
        name STRING -- Name of the person,
        age INT -- Age of the person
    ) STORED AS TEXTFILE;
    """

    table_info1 = extract_table_information_hive(ddl_script1)
    expected_table_info1 = [
        TableInformation(
            database_name="test_db",
            table_name="test_table1",
            column_name="id",
            data_type="INT",
            constraints="",
            description="Unique identifier,",
            storage_type="textfile",
            partition_columns="",
        ),
        TableInformation(
            database_name="test_db",
            table_name="test_table1",
            column_name="name",
            data_type="STRING",
            constraints="",
            description="Name of the person,",
            storage_type="textfile",
            partition_columns="",
        ),
        TableInformation(
            database_name="test_db",
            table_name="test_table1",
            column_name="age",
            data_type="INT",
            constraints="",
            description="Age of the person",
            storage_type="textfile",
            partition_columns="",
        ),
    ]

    assert table_info1 == expected_table_info1

    # Test case 2: Hive DDL script with multiple tables, storage types, and partition columns
    ddl_script2 = """
    CREATE TABLE test_db.test_table2 (
        id INT -- Unique identifier,
        name STRING -- Name of the person,
        age INT -- Age of the person,
    ) STORED AS ORC
    PARTITIONED BY (country STRING);

    CREATE TABLE test_db.test_table3 (
        id INT -- Unique identifier,
        value DOUBLE -- Value of the record
    ) STORED AS PARQUET;
    """

    table_info2 = extract_table_information_hive(ddl_script2)
    expected_table_info2 = [
        TableInformation(
            database_name="test_db",
            table_name="test_table2",
            column_name="id",
            data_type="INT",
            constraints="",
            description="Unique identifier,",
            storage_type="orc",
            partition_columns="country",
        ),
        TableInformation(
            database_name="test_db",
            table_name="test_table2",
            column_name="name",
            data_type="STRING",
            constraints="",
            description="Name of the person,",
            storage_type="orc",
            partition_columns="country",
        ),
        TableInformation(
            database_name="test_db",
            table_name="test_table2",
            column_name="age",
            data_type="INT",
            constraints="",
            description="Age of the person,",
            storage_type="orc",
            partition_columns="country",
        ),
        TableInformation(
            database_name="test_db",
            table_name="test_table3",
            column_name="id",
            data_type="INT",
            constraints="",
            description="Unique identifier,",
            storage_type="parquet",
            partition_columns="",
        ),
        TableInformation(
            database_name="test_db",
            table_name="test_table3",
            column_name="value",
            data_type="DOUBLE",
            constraints="",
            description="Value of the record",
            storage_type="parquet",
            partition_columns="",
        ),
    ]

    assert table_info2 == expected_table_info2
