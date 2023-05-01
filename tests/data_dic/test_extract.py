"""
This module contains pytest functions to test the behaviour of the functions
contained within the rdsa_utils/data_dic/extract.py Module.

This module contains pytest unit tests for the following helper functions:
    remove_multiline_comments: Remove multiline comments from a DDL script.
    extract_contents_inside_parentheses: Extract the content inside the outermost parentheses in a DDL script.
    extract_column_description: Extract the column name and its description from a line in a DDL script. 
    extract_column_descriptions: Extract column descriptions from a DDL script.

As well as the main function:
    read_ddl_scripts: Read the DDL scripts from a given file path.
    replace_variables: Replace the f-string expressions in the DDL script with their values.
    extract_data_dictionary: Extracts table information from a DDL script and returns a list of TableInformation objects. 
    extract_data_dictionaries_from_multiple_tables: Process multiple table definitions in a DDL script and return a list of TableInformation objects.

And finally Data Class:
    TableInformation: A data class representing table information extracted from Hive & Non-Hive DDL scripts.

Each of the functions is tested using various test cases to ensure they correctly extract the required information from Hive & Non-Hive DDL scripts.
"""
import pytest

from rdsa_utils.data_dic.extract import (
    TableInformation,
    extract_column_description,
    extract_column_descriptions,
    extract_content_inside_parentheses,
    extract_data_dictionaries_from_multiple_tables,
    extract_data_dictionary,
    read_ddl_scripts,
    remove_multiline_comments,
    replace_variables,
)


@pytest.mark.parametrize(
    "database_name, table_name, column_name, data_type, constraints, description, storage_type, partition_columns",
    [
        (
            None,
            "table1",
            "column1",
            "int",
            "NOT NULL",
            "A sample column",
            "textfile",
            "part_col1,part_col2",
        ),
        (
            "db1",
            "table2",
            "column2",
            "varchar(255)",
            "NULL",
            "Another column",
            "orc",
            "",
        ),
    ],
)
def test_table_information_creation(
    database_name,
    table_name,
    column_name,
    data_type,
    constraints,
    description,
    storage_type,
    partition_columns,
):
    """
    Test the creation of TableInformation instances.

    Parameters
    ----------
    database_name : Optional[str]
        The name of the database, or None if not specified.
    table_name : str
        The name of the table.
    column_name : str
        The name of the column.
    data_type : str
        The data type of the column.
    constraints : str
        Column constraints, e.g., 'NOT NULL'.
    description : str
        The description of the column, if provided.
    storage_type : Optional[str]
        The storage type of the table, e.g., 'textfile', 'orc', or None if not specified.
    partition_columns : str
        A comma-separated string of partition column names, if any.

    Returns
    -------
    None
    """
    table_info = TableInformation(
        database_name=database_name,
        table_name=table_name,
        column_name=column_name,
        data_type=data_type,
        constraints=constraints,
        description=description,
        storage_type=storage_type,
        partition_columns=partition_columns,
    )

    assert table_info.database_name == database_name
    assert table_info.table_name == table_name
    assert table_info.column_name == column_name
    assert table_info.data_type == data_type
    assert table_info.constraints == constraints
    assert table_info.description == description
    assert table_info.storage_type == storage_type
    assert table_info.partition_columns == partition_columns


def test_read_ddl_scripts(tmp_path):
    """
    Test the read_ddl_scripts function.

    This test case creates a temporary file with sample DDL scripts and
    checks if the read_ddl_scripts function reads the content correctly.

    Parameters
    ----------
    tmp_path : Path
        A pytest fixture that provides a temporary path for file operations.

    Returns
    -------
    None
    """
    # Create a temporary DDL file with sample content
    ddl_content = "CREATE TABLE test_table (id INT, name STRING);"
    ddl_file = tmp_path / "sample_ddl.py"
    ddl_file.write_text(ddl_content)

    # Read the DDL file content using the function
    result = read_ddl_scripts(str(ddl_file))

    # Check if the content is read correctly
    assert result == ddl_content


def test_replace_variables():
    """
    Test the replace_variables function.

    This test case checks if the replace_variables function correctly
    replaces f-string expressions in a given DDL script with their values.

    Returns
    -------
    None
    """
    # Define a sample DDL script with f-string expressions
    ddl_script = "CREATE TABLE test_table (id INT, name {data_type});"

    # Define the script_globals dictionary
    script_globals = {"data_type": "STRING"}

    # Replace the f-string expressions using the function
    result = replace_variables(ddl_script, script_globals)

    # Check if the f-string expressions are replaced correctly
    assert result == "CREATE TABLE test_table (id INT, name STRING);"


def test_remove_multiline_comments():
    """
    Test the remove_multiline_comments function.

    This test checks whether the function correctly removes multiline comments from the input DDL script.
    """
    ddl_script = (
        "/*This is a multiline\n"
        "comment*/\n"
        "    CREATE TABLE IF NOT EXISTS my_database.my_table (\n"
        "        id INT,\n"
        "        name STRING\n"
        "    );\n"
        "    "
    )

    expected_output = (
        "CREATE TABLE IF NOT EXISTS my_database.my_table ("
        "    id INT,"
        "    name STRING );"
    )

    output = remove_multiline_comments(ddl_script)

    # Remove newline characters and extra spaces from both output and expected_output
    output_no_newline = " ".join(output.split()).strip()
    expected_output_no_newline = " ".join(expected_output.split()).strip()

    assert output_no_newline == expected_output_no_newline


def test_extract_content_inside_parentheses():
    """
    Test the extract_content_inside_parentheses function.

    This test checks whether the function correctly extracts the content inside the parentheses from the input DDL script.
    """
    input_ddl = "CREATE TABLE my_table (id INT, name STRING);"
    expected_output = "id INT, name STRING"

    assert extract_content_inside_parentheses(input_ddl) == expected_output


def test_extract_content_inside_parentheses_edge_case():
    """
    Test the extract_content_inside_parentheses function with an edge case.

    This test checks whether the function correctly extracts the content inside the outermost parentheses from the input DDL script,
    including nested parentheses within comments.
    """
    input_ddl = """
    CREATE TABLE IF NOT EXISTS my_database.my_table(
        id INT AUTO_INCREMENT PRIMARY KEY, -- Unique identifier for the record (integer with auto-increment) and primary key constraint
        name VARCHAR(255) NOT NULL, -- Full name of the person, with not null constraint
        age INT CHECK (age >= 0), -- Age of the person, with check constraint to ensure age is non-negative
        city VARCHAR(255), -- City where the person lives
        UNIQUE (name, city), -- Unique constraint on the combination of name and city
        FOREIGN KEY (city) REFERENCES another_table(city) -- Foreign key constraint referencing another table (assuming another_table exists with a city column)
    ) ENGINE=InnoDB;
    """
    expected_output = """
        id INT AUTO_INCREMENT PRIMARY KEY, -- Unique identifier for the record (integer with auto-increment) and primary key constraint
        name VARCHAR(255) NOT NULL, -- Full name of the person, with not null constraint
        age INT CHECK (age >= 0), -- Age of the person, with check constraint to ensure age is non-negative
        city VARCHAR(255), -- City where the person lives
        UNIQUE (name, city), -- Unique constraint on the combination of name and city
        FOREIGN KEY (city) REFERENCES another_table(city) -- Foreign key constraint referencing another table (assuming another_table exists with a city column)
    """

    output = extract_content_inside_parentheses(input_ddl)
    assert output == expected_output


def test_extract_column_description():
    """
    Test the extract_column_description function.

    This test checks whether the function correctly extracts the column description from the input line of the DDL script.
    """
    line = "        id INT, -- Unique identifier for the record"
    expected_output = {
        "column_name": "id",
        "column_description": "Unique identifier for the record",
    }

    assert extract_column_description(line) == expected_output


def test_extract_column_descriptions():
    """
    Test the extract_column_descriptions function.

    This test checks whether the function correctly extracts the column descriptions from the input DDL script.
    """
    ddl_script = """CREATE TABLE IF NOT EXISTS my_database.my_table (
        id INT, -- Unique identifier for the record
        name STRING, -- Full name of the person
        age INT, -- Age of the person
        city STRING -- City where the person lives
    );
    """

    expected_output = [
        {"column_name": "id", "column_description": "Unique identifier for the record"},
        {"column_name": "name", "column_description": "Full name of the person"},
        {"column_name": "age", "column_description": "Age of the person"},
        {"column_name": "city", "column_description": "City where the person lives"},
    ]

    assert extract_column_descriptions(ddl_script) == expected_output


def test_extract_data_dictionaries_from_multiple_tables_hive():
    """
    Test the extract_data_dictionaries_from_multiple_tables() function with Hive table definitions.
    This test verifies that the function can process multiple table definitions in a DDL script and return a list
    of TableInformation objects containing the correct data.
    """
    ddl_scripts = """
        CREATE TABLE IF NOT EXISTS my_database.my_table1 (
            id INT, -- Unique identifier for the record
            name STRING, -- Full name of the person
            age INT -- Age of the person
        ) ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE;

        CREATE TABLE IF NOT EXISTS my_database.my_table2 (
            id INT, -- Unique identifier for the record
            city STRING, -- City where the person lives
            country STRING -- Country where the person lives
        ) ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE;
    """

    result = extract_data_dictionaries_from_multiple_tables(ddl_scripts, is_hive=True)

    expected_result = [
        TableInformation(
            database_name="my_database",
            table_name="my_table1",
            column_name="id",
            data_type="INT",
            constraints="",
            description="Unique identifier for the record",
            storage_type="TEXTFILE",
            partition_columns="",
        ),
        TableInformation(
            database_name="my_database",
            table_name="my_table1",
            column_name="name",
            data_type="STRING",
            constraints="",
            description="Full name of the person",
            storage_type="TEXTFILE",
            partition_columns="",
        ),
        TableInformation(
            database_name="my_database",
            table_name="my_table1",
            column_name="age",
            data_type="INT",
            constraints="",
            description="Age of the person",
            storage_type="TEXTFILE",
            partition_columns="",
        ),
        TableInformation(
            database_name="my_database",
            table_name="my_table2",
            column_name="id",
            data_type="INT",
            constraints="",
            description="Unique identifier for the record",
            storage_type="TEXTFILE",
            partition_columns="",
        ),
        TableInformation(
            database_name="my_database",
            table_name="my_table2",
            column_name="city",
            data_type="STRING",
            constraints="",
            description="City where the person lives",
            storage_type="TEXTFILE",
            partition_columns="",
        ),
        TableInformation(
            database_name="my_database",
            table_name="my_table2",
            column_name="country",
            data_type="STRING",
            constraints="",
            description="Country where the person lives",
            storage_type="TEXTFILE",
            partition_columns="",
        ),
    ]

    assert result == expected_result


def test_extract_data_dictionaries_from_multiple_tables_non_hive():
    """
    Test the extract_data_dictionaries_from_multiple_tables() function with non-Hive table definitions.

    This test verifies that the function can process multiple table definitions in a non-Hive DDL script and return a list
    of TableInformation objects containing the correct data.
    """
    ddl_scripts = """
        CREATE TABLE my_database.my_table1 (
            id INT, -- Unique identifier for the record
            name VARCHAR(255), -- Full name of the person
            age INT -- Age of the person
        );

        CREATE TABLE my_database.my_table2 (
            id INT, -- Unique identifier for the record
            city VARCHAR(255), -- City where the person lives
            country VARCHAR(255) -- Country where the person lives
        );
    """

    result = extract_data_dictionaries_from_multiple_tables(ddl_scripts, is_hive=False)
    result

    expected_result = [
        TableInformation(
            database_name="my_database",
            table_name="my_table1",
            column_name="id",
            data_type="INT",
            constraints="",
            description="Unique identifier for the record",
            storage_type=None,
            partition_columns="",
        ),
        TableInformation(
            database_name="my_database",
            table_name="my_table1",
            column_name="name",
            data_type="VARCHAR",
            constraints="",
            description="Full name of the person",
            storage_type=None,
            partition_columns="",
        ),
        TableInformation(
            database_name="my_database",
            table_name="my_table1",
            column_name="age",
            data_type="INT",
            constraints="",
            description="Age of the person",
            storage_type=None,
            partition_columns="",
        ),
        TableInformation(
            database_name="my_database",
            table_name="my_table2",
            column_name="id",
            data_type="INT",
            constraints="",
            description="Unique identifier for the record",
            storage_type=None,
            partition_columns="",
        ),
        TableInformation(
            database_name="my_database",
            table_name="my_table2",
            column_name="city",
            data_type="VARCHAR",
            constraints="",
            description="City where the person lives",
            storage_type=None,
            partition_columns="",
        ),
        TableInformation(
            database_name="my_database",
            table_name="my_table2",
            column_name="country",
            data_type="VARCHAR",
            constraints="",
            description="Country where the person lives",
            storage_type=None,
            partition_columns="",
        ),
    ]

    assert result == expected_result


def test_extract_data_dictionary_hive():
    """
    Test the extract_data_dictionary() function for a single table definition with Hive table definitions.

    This test verifies that the function can process a single table definition in a DDL script and return a list
    of TableInformation objects containing the correct data.
    """
    ddl_script = """
        CREATE TABLE my_database.my_table (
            id INT, -- Unique identifier for the record
            name STRING, -- Full name of the person
            age INT -- Age of the person 
        );
    """

    description_dict = [
        {"column_name": "id", "column_description": "Unique identifier for the record"},
        {"column_name": "name", "column_description": "Full name of the person"},
        {"column_name": "age", "column_description": "Age of the person"},
    ]

    result = extract_data_dictionary(
        ddl_script, is_hive=True, description_dict=description_dict
    )

    expected_result = [
        TableInformation(
            database_name="my_database",
            table_name="my_table",
            column_name="id",
            data_type="INT",
            constraints="",
            description="Unique identifier for the record",
            storage_type=None,
            partition_columns="",
        ),
        TableInformation(
            database_name="my_database",
            table_name="my_table",
            column_name="name",
            data_type="STRING",
            constraints="",
            description="Full name of the person",
            storage_type=None,
            partition_columns="",
        ),
        TableInformation(
            database_name="my_database",
            table_name="my_table",
            column_name="age",
            data_type="INT",
            constraints="",
            description="Age of the person",
            storage_type=None,
            partition_columns="",
        ),
    ]

    assert result == expected_result


def test_extract_data_dictionary_non_hive():
    """
    Test the extract_data_dictionary() function for a single table definition with non-Hive table definitions.

    This test verifies that the function can process a single table definition in a DDL script and return a list
    of TableInformation objects containing the correct data.
    """
    ddl_script = """
        CREATE TABLE my_database.my_table (
            id INT, -- Unique identifier for the record
            name VARCHAR(255), -- Full name of the person
            age INT -- Age of the person
        );
    """

    description_dict = [
        {"column_name": "id", "column_description": "Unique identifier for the record"},
        {"column_name": "name", "column_description": "Full name of the person"},
        {"column_name": "age", "column_description": "Age of the person"},
    ]

    result = extract_data_dictionary(
        ddl_script, is_hive=False, description_dict=description_dict
    )

    expected_result = [
        TableInformation(
            database_name="my_database",
            table_name="my_table",
            column_name="id",
            data_type="INT",
            constraints="",
            description="Unique identifier for the record",
            storage_type=None,
            partition_columns="",
        ),
        TableInformation(
            database_name="my_database",
            table_name="my_table",
            column_name="name",
            data_type="VARCHAR",
            constraints="",
            description="Full name of the person",
            storage_type=None,
            partition_columns="",
        ),
        TableInformation(
            database_name="my_database",
            table_name="my_table",
            column_name="age",
            data_type="INT",
            constraints="",
            description="Age of the person",
            storage_type=None,
            partition_columns="",
        ),
    ]

    assert result == expected_result
