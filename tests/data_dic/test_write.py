"""
This module contains pytest functions to test the behaviour of the functions
contained within the rdsa_utils/data_dic/write.py Module.

This module contains pytest unit tests for the following functions:
    create_data_dictionary_markdown: Create a data dictionary as a Markdown file using the extracted table information.
    markdown_file_to_html_with_theme: Convert a markdown file to themed HTML using a Jinja2 template.
    create_data_dictionary_excel: Create a data dictionary Excel file from the extracted table information.
"""
import pytest

from rdsa_utils.data_dic.extract import TableInformation
from rdsa_utils.data_dic.write import (
    create_data_dictionary_excel,
    create_data_dictionary_markdown,
    markdown_file_to_html_with_theme,
)


def test_create_data_dictionary_markdown_hive(tmp_path):
    """
    Test the create_data_dictionary_markdown function with Hive table definitions.

    This test verifies if the function creates a Markdown file with the expected contents
    based on the provided table information.

    Parameters
    ----------
    tmp_path : pytest fixture
        A temporary directory provided by the pytest framework.
    """
    table_info = [
        TableInformation(
            database_name="test_db",
            table_name="test_table",
            column_name="id",
            data_type="int",
            constraints="NOT NULL",
            description="Unique identifier",
            storage_type="managed",
            partition_columns=None,
        )
    ]

    output_file = tmp_path / "data_dictionary.md"
    create_data_dictionary_markdown(table_info, str(output_file), is_hive=True)

    assert output_file.exists()

    with open(output_file, "r") as f:
        content = f.read()

    assert "Database: test_db | Table: test_table" in content
    assert "**Storage Type:** managed" in content
    assert "**Partition Columns:** None" in content
    assert "| Column Name | Data Type | Constraints | Description |" in content
    assert "| id | int | NOT NULL | Unique identifier |" in content


def test_create_data_dictionary_markdown_non_hive(tmp_path):
    """
    Test the create_data_dictionary_markdown function with Non-Hive table definitions.

    This test verifies if the function creates a Markdown file with the expected contents
    based on the provided table information.

    Parameters
    ----------
    tmp_path : pytest fixture
        A temporary directory provided by the pytest framework.
    """
    table_info = [
        TableInformation(
            database_name="test_db",
            table_name="test_table",
            column_name="id",
            data_type="int",
            constraints="NOT NULL",
            description="Unique identifier",
            storage_type=None,
            partition_columns=None,
        )
    ]

    output_file = tmp_path / "data_dictionary.md"
    create_data_dictionary_markdown(table_info, str(output_file), is_hive=False)

    assert output_file.exists()

    with open(output_file, "r") as f:
        content = f.read()

    assert "Database: test_db | Table: test_table" in content
    assert "**Storage Type:** managed" not in content
    assert "**Partition Columns:** None" not in content
    assert "| Column Name | Data Type | Constraints | Description |" in content
    assert "| id | int | NOT NULL | Unique identifier |" in content


def test_markdown_file_to_html_with_theme(tmp_path):
    """
    Test the markdown_file_to_html_with_theme function.

    This test verifies if the function converts a Markdown file to a themed HTML file
    using a Jinja2 template.

    Parameters
    ----------
    tmp_path : pytest fixture
        A temporary directory provided by the pytest framework.
    """
    markdown_file = tmp_path / "test.md"
    html_output_file = tmp_path / "test.html"

    with open(markdown_file, "w") as f:
        f.write("# Test Title\n\nThis is a test paragraph.")

    markdown_file_to_html_with_theme(str(markdown_file), str(html_output_file))

    assert html_output_file.exists()

    with open(html_output_file, "r") as f:
        content = f.read()

    assert "<h1>Test Title</h1>" in content
    assert "<p>This is a test paragraph.</p>" in content


def test_create_data_dictionary_excel_hive(tmp_path):
    """
    Test the create_data_dictionary_excel function with Hive table definitions.

    This test verifies if the function creates an Excel file with the expected contents
    based on the provided table information.

    Parameters
    ----------
    tmp_path : pytest fixture
        A temporary directory provided by the pytest framework.
    """
    table_info = [
        TableInformation(
            database_name="test_db",
            table_name="test_table",
            column_name="id",
            data_type="int",
            constraints="NOT NULL",
            description="Unique identifier",
            storage_type="managed",
            partition_columns=None,
        )
    ]

    output_file = tmp_path / "data_dictionary.xlsx"
    create_data_dictionary_excel(table_info, str(output_file), is_hive=True)

    assert output_file.exists()

    # Read the Excel file and verify the contents
    import pandas as pd

    df = pd.read_excel(output_file, sheet_name="Data Dictionary", engine="openpyxl")

    assert df.at[0, "database_name"] == "test_db"
    assert df.at[0, "table_name"] == "test_table"
    assert df.at[0, "column_name"] == "id"
    assert df.at[0, "data_type"] == "int"
    assert df.at[0, "constraints"] == "NOT NULL"
    assert df.at[0, "description"] == "Unique identifier"
    assert df.at[0, "storage_type"] == "managed"
    assert pd.isna(df.at[0, "partition_columns"])


def test_create_data_dictionary_excel_non_hive(tmp_path):
    """
    Test the create_data_dictionary_excel function with Non-Hive table definitions.

    This test verifies if the function creates an Excel file with the expected contents
    based on the provided table information.

    Parameters
    ----------
    tmp_path : pytest fixture
        A temporary directory provided by the pytest framework.
    """
    table_info = [
        TableInformation(
            database_name="test_db",
            table_name="test_table",
            column_name="id",
            data_type="int",
            constraints="NOT NULL",
            description="Unique identifier",
            storage_type=None,
            partition_columns=None,
        )
    ]

    output_file = tmp_path / "data_dictionary.xlsx"
    create_data_dictionary_excel(table_info, str(output_file), is_hive=False)

    assert output_file.exists()

    # Read the Excel file and verify the contents
    import pandas as pd

    df = pd.read_excel(output_file, sheet_name="Data Dictionary", engine="openpyxl")

    assert df.at[0, "database_name"] == "test_db"
    assert df.at[0, "table_name"] == "test_table"
    assert df.at[0, "column_name"] == "id"
    assert df.at[0, "data_type"] == "int"
    assert df.at[0, "constraints"] == "NOT NULL"
    assert df.at[0, "description"] == "Unique identifier"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
