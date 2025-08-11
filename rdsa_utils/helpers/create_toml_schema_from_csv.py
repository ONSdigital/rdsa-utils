"""Script to read in a CSV file, deduce metadata and output a toml schema."""

# Import necessary libraries here
import re
from datetime import datetime
from pathlib import Path
from typing import Tuple

import pandas as pd
import toml


# Define functions and classes here
def read_csv_file(file_path: str) -> pd.DataFrame:
    """Read a CSV file and return the contents as a DataFrame.

    Args:
        file_path (str): The path to the CSV file.

    Returns
    -------
        pd.DataFrame: The contents of the CSV file as a DataFrame.

    Raises
    ------
        FileNotFoundError: If the file does not exist.
    """
    # Check if the file exists
    if not Path(file_path).exists():
        msg = f"File not found: {Path(file_path).name}"
        raise FileNotFoundError(msg)

    return pd.read_csv(file_path, low_memory=False)


def create_integer_columns(col: pd.Series) -> pd.Series:
    """Create integer columns from float columns if possible.

    If all the values in a float column are integers, convert the column to an
    integer column. If there are nulls, the column should be a float.

    Args:
        col (pd.Series): The column to be converted.

    Returns
    -------
        pd.Series: The column converted to type integer if possible.
    """
    # check whether the colum contains nulls
    if col.isna().any():
        # if there are nulls, the column should be a float
        return col.astype("float64")
    elif (col % 1 == 0).all():
        # if all values are integers, convert to integer type
        return col.astype("int64")
    else:
        return col


def decide_numeric_or_non_numeric(
        column_data: pd.Series,
        type_dict: dict,
    ) -> Tuple[pd.Series, dict, str]:
    """Decide whether a column is numeric or non-numeric.

    Numeric columns are those which contain continuous values that can be used in
    mathematical operations, whether they are integers or floats. Non-numeric columns
    are those which contain categorical or text data or dates, including reference
    identifiers that look like numbers.

    The process is as follows:
    1. If the column contains a string starting with a zero but not 0.,
        it should be treated as non-numeric.
    2. Attempt to convert the column to type float.
        If this fails, the column is non-numeric.
    3. Attempt to convert a column of type float to type integer.
        If this fails, the column is still numeric.
    4. For columns that could be of type integer, apply the following rules:
        a. If the column contains zero entries, it is probably numeric.
        b. If all the values are of the same length and this length is > 3,
            it is probably non-numeric.
        c. If the integers are normally distributed, it is probably numeric.

    Args:
        column_data (pd.Series): The column data.

    Returns
    -------
        Tuple[pd.Series, dict, str]: A tuple containing the column data,
        a dictionary with type information,
        and a string indicating whether the column is numeric or non-numeric.
    """
    # Check if the column contains a string starting with a zero but not '0.'
    # This is a common indicator that the column is non-numeric.
    if column_data.astype(str).str.contains(r"^0(?!\.)").any():
        # convert the column to type string
        column_data = column_data.astype(str)
        type_dict["Deduced_Data_Type"] = "str"
        return column_data, type_dict, "non-numeric"

    try:
        # Attempt to convert the column to type float
        column_data = column_data.astype(float)
        # If successful, check whether the column could be integer
        column_data = create_integer_columns(column_data)
        if (column_data.dtype == "int64"):
            # check whether the column could be datetime and return dictionary
            date_format = check_format_datetime_column(column_data)
            if date_format != "Inconclusive":
                type_dict["Deduced_Data_Type"] = "DateTime"
                type_dict["Date_Format"] = date_format
        return column_data, type_dict, "numeric"

    except ValueError:
        return column_data, type_dict, "non-numeric"


def return_datetime_format(test_strings: list[str]) -> list[str]:
    """Determine whether strings in a list match an item in a list of datetime formats.

    Use a list of possible formats to see whether a string fits this.
    If it does, return the format. If not, return an empty list.

    Args:
        test_string (str): The string to test for datetime format.

    Returns
    -------
        list[str]: A list of possible datetime formats that match the string.
        If no formats match, return an empty list.
    """
    mnth_year_formats = ["%Y%m", "%m%Y", "%Y", "%m", "%Y-%m", "%m-%Y", "%Y/%m", "%m/%Y"]
    date_formats = [
        "%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y", "%Y/%m/%d", "%m/%d/%Y","%m/%d/%y",
        "%Y%m%d", "%d%m%Y", "%m%d%Y",
    ]
    time_formats = ["%H:%M:%S", "%H:%M"]
    date_time_formats = [d + " " + t for d in date_formats for t in time_formats]
    trail_list = mnth_year_formats + date_formats + time_formats + date_time_formats

    possible_formats = []
    for string in test_strings:
        test_string = string.strip()
        for format_item in trail_list:
            try:
                datetime.strptime(test_string, format_item)  # noqa: DTZ007 (timezone)
                possible_formats.append(format_item)
            except ValueError:
                pass
    return possible_formats


def _matches_format(val: str, date_format: str) -> bool:
    try:
        datetime.strptime(val, date_format)  # noqa: DTZ007 (timezone info not needed)
        return True
    except Exception:
        return False


def proportion_matching_format(
        column_data:
        pd.Series,
        date_format: str,
        threshold: float = 0.5,
    ) -> bool:
    """Check if proportion of non-null values match the given datetime format.

    Args:
        column_data (pd.Series): The column data.
        date_format (str): The datetime format to check.
        threshold (float): Proportion threshold (default 0.5).

    Returns
    -------
        bool: True if more than `threshold` proportion match, else False.
    """
    non_null = column_data.dropna().astype(str)
    match_count = sum(
        1 for val in non_null
        if _matches_format(val, date_format)
    )
    proportion = match_count / len(non_null) if len(non_null) > 0 else 0
    return proportion > threshold


def check_format_datetime_column(column_data: pd.Series) -> Tuple[pd.Series, str]:
    """Determine whether a column is of type datetime and return its format.

    Use list of possible formats to see whether the first item of the column fits this.
    If it does, return the format. If not, return "Inconclusive".


    Args:
        column_data (pd.Series[str]): The column data as a Series of strings.

    Returns
    -------
        pd.Series: The column data which may have been converted to type datetime.
        str: The format of the datetime column.
    """
    # Check whether the column could be of type datetime by coercing a copy to datetime
    col_copy = column_data.copy()
    try:
        pd.to_datetime(col_copy, errors="coerce").notna().all()
    except Exception:
        # If coercion fails, return "Inconclusive"
        return column_data, "Inconclusive"
    # Extract the first five unique non-null value from the column
    first_values = column_data.dropna().unique()[:5]
    # Estimate the format of this string
    first_string_format = return_datetime_format(first_values)
    # Check whether more than 50% of the column matches this format
    if first_string_format != "Inconclusive":
        if proportion_matching_format(column_data, first_string_format, threshold=0.5):
            # If more than 50% of the column matches this format,
            # return the col as datetime
            date_format = first_string_format
            column_data = pd.to_datetime(
                column_data, format=date_format, errors="coerce",
            )
        else:
            # If less than 50% of the column matches this format, return "Inconclusive"
            date_format = "Inconclusive"
    return date_format


def process_numeric_column(column_data: pd.Series) -> dict:
    """Process a numeric column and return its properties.

    Convert the column to an integer type if possible.
    Return the minimum and maximum values.

    Args:
        column_data (pd.Series): The column data.

    Returns
    -------
        dict: A dictionary representing the properties of the numeric column.
    """
    max_min_dict = {
        "Min_value": column_data.min(),
        "Max_value": column_data.max(),
    }
    return max_min_dict


def proportions_for_categorical(column_data: pd.Series) -> dict:
    """Generate proportions for a categorical column.

    Args:
        column_data (pd.Series): The column data.

    Returns
    -------
        dict: A dictionary representing the random proportions for the column.
    """
    unique_list = column_data.dropna().unique()
    prop_dict = {}
    for item in unique_list:
        prop_dict[item] = round(column_data.value_counts(normalize=True)[item], 2)

    return prop_dict


def process_categorical_column(column_data: pd.Series) -> dict:
    """Process a categorical column and return its properties.

    Args:
        column_data (pd.Series): The column data.

    Returns
    -------
        dict: A dictionary representing the properties of the categorical column.
    """
    unique_list = column_data.dropna().unique()
    if len(unique_list) < 11:
        prop_dict = proportions_for_categorical(column_data)
        cat_dict = {
            "Deduced_Data_Type": "categorical",
            "categories": unique_list,
            "proportions": prop_dict,
        }
    else:
        cat_dict = {"Deduced_Data_Type": "text", "count": column_data.count()}
    return cat_dict


def deduce_data_type(column_data: pd.Series) -> dict:
    """Deduce the data type of a column based on its contents.

    Args:
        column_data (pd.Series): The column data.

    Returns
    -------
        dict: A dictionary representing the deduced data type.
    """
    type_dict = {}
    # determine whether a column is numeric or non-numeric
    column_data, type_dict, is_numeric = decide_numeric_or_non_numeric(
        column_data,
        type_dict,
    )
    type_dict["Is_numeric"] = is_numeric

    column_type = str(column_data.dtype)

    if is_numeric == "numeric":
         type_dict["Deduced_Data_Type"] = column_type
         # add further properties for numeric columns and return dictionary
         num_info_dic = process_numeric_column(column_data)
         type_dict.update(num_info_dic)
         return type_dict

    elif column_type == "object":
        # check whether the column could be datetime and return dictionary
        date_format = check_format_datetime_column(column_data)
        if date_format != "Inconclusive":
            type_dict["Deduced_Data_Type"] = "DateTime"
            type_dict["Date_Format"] = date_format
            return type_dict

        # check if the column is categorical and return dictionary
        data_type_dict = process_categorical_column(column_data)
    else:
        data_type_dict = {"Deduced_Data_Type": column_type}
    return data_type_dict


def convert_csv_to_toml_schema(data: pd.DataFrame) -> dict:
    """Convert a DataFrame into a TOML schema.

    Args:
        data (pd.DataFrame): The input DataFrame.

    Returns
    -------
        dict: A dictionary representing the TOML schema.
    """
    # Initialize an empty dictionary to store the schema
    schema = {}
    # Extract column names and data types from the DataFrame
    for column in data.columns:
        column_data = data[column]
        # Deduce the data type of the column
        schema[column] = deduce_data_type(column_data)

        # Add column description if available
        # State whether column is nullable
        schema[column]["Nullable"] = column_data.isna().any()
        # Add additional properties based on the data type
    return schema


def create_yaml_output(schema: dict) -> str:
    """Create a text output from a TOML schema.

    Args:
        schema (dict): The TOML schema as a dictionary.

    Returns
    -------
        str: The text output.
    """
    text_output = ""
    for column in schema:
        # check if the column starts with 1, 2, 3, 4, 5, 6, 7, 8, 9
        if re.match(r"^[1-9]", column):
            text_output += f'"{column}"\n'
        else:
            text_output += f"{column}\n"
        for key, value in schema[column].items():
            # if the value is a dictionary then iterate over the keys and values
            if isinstance(value, dict):
                text_output += f"  {key}\n"
                for k, v in value.items():
                    text_output += f"    {k} : {v}\n"
            else:
                text_output += f"  {key} : {value}\n"
        text_output += "\n"
    return text_output


# Main script logic here
def convert_csv_to_toml(csv_path: str, toml_path: str, yaml_path: str)-> None:
    """Convert a CSV file into a TOML schema and save it to a file.

    Args:
        csv_path (str): The path to the input CSV file.
        toml_path (str): The path to the output TOML schema.

    Returns
    -------
        None
    """
    input_df = read_csv_file(csv_path)
    # Convert the DataFrame to a TOML schema
    schema = convert_csv_to_toml_schema(input_df)

    # save the schema to a toml file
    with open(toml_path, "w") as file:
        toml.dump(schema, file)

    # save the schema to a yaml file
    text_output = create_yaml_output(schema)
    with open(yaml_path, "w") as file:
        file.write(text_output)


# Script entry point
if __name__ == "__main__":
    in_root = "R:/BERD Results System Development 2023/DAP_emulation/"
    staged_path = in_root + "2023_FROZEN_staged_full_responses_25-05-01_v117.csv"
    backdata_path = in_root + "2022_surveys/BERD/06_imputation/backdata_output/"
    backdata_path += "2022_backdata_published_v347.csv"

    out_root = "D:/coding_projects/github_repos/data/synth_data/"
    yaml_path_out = out_root + "frozen_schema_out.yaml"
    toml_path_out = out_root + "frozen_schema_out.toml"
    yaml_backdata_path_out = out_root + "backdata_schema_out.yaml"
    toml_backdata_path_out = out_root + "backdata_schema_out.toml"

    convert_csv_to_toml(staged_path, toml_path_out, yaml_path_out)
    convert_csv_to_toml(backdata_path, toml_backdata_path_out, yaml_backdata_path_out)
