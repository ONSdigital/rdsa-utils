"""Script to read in a CSV file, deduce metadata and output a toml schema."""

# Import necessary libraries here
import pandas as pd
import os
import re

from datetime import datetime
from typing import Tuple

# Define functions and classes here
def read_csv_file(file_path: str) -> pd.DataFrame:
    """Read a CSV file and return the contents as a DataFrame.
    
    Args:
        file_path (str): The path to the CSV file.
    Returns:
        pd.DataFrame: The contents of the CSV file as a DataFrame.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    return pd.read_csv(file_path)
    

def create_integer_columns(col: pd.Series) -> pd.Series:
    """Create integer columns from float columns if possible.
    
    If all the values in a float column are integers, convert the column to an 
    integer column. This should be type int64 if there are no nulls, or type Int64
    if there are nulls.

    Args:
        data (pd.DataFrame): The input DataFrame.
    Returns:
        pd.Series: The column converted to type integer if possible.     
    """
    # first select the non-null entries
    non_null_col = col.dropna()
    if (non_null_col % 1 == 0).all():
        if col.isnull().any():
            col = col.astype("Int64")
        else:
            col = col.astype("int64")
    return col


def decide_numeric_or_non_numeric(column_data: pd.Series, type_dict) -> Tuple[pd.Series, str]:
    """Decide whether a column is numeric or non-numeric.

    Numeric columns are those which contain continuous values that can be used in 
    mathematical operations, whether they are integers or floats. Non-numeric columns
    are those which contain categorical or text data or dates, including reference 
    identifiers that look like numbers.

    The process is as follows:
    1. If the column contains a string starting with a zero but not 0., it should be treated as non-numeric.
    2. Attempt to convert the column to type float. If this fails, the column is non-numeric.
    3. Attempt to convert a column of type float to type integer. If this fails, the column is still numeric.
    4. For columns that could be of type integer, apply the following rules:
        a. If the column contains zero entries, it is probably numeric.
        b. If all the values are of the same length and this length is > 3, it is probably non-numeric.
        c. If the integers are normally distributed, it is probably numeric.
    
    Args:
        column_data (pd.Series): The column data.
    Returns:
        pd.Series: The column data which may have been converted to type float or int.
        str: "numeric" or "non-numeric".
    """
    # Check if the column contains a string starting with a zero but not 0. 
    # This is a common indicator that the column is non-numeric.
    if column_data.astype(str).str.contains(r'^0(?!\.)').any():
        # convert the column to type string
        column_data = column_data.astype(str)
        return column_data, "non-numeric"

    # Attempt to convert the column to type float
    #TODO: extra conditions to check whether columns with numerical values should be
    # considered as categorical.
    try:
        column_data = column_data.astype(float)
        # Attempt to convert a column of type float to type integer
        column_data = create_integer_columns(column_data)
        if (column_data.dtype == "int64") or (column_data.dtype == "Int64"):
            # check whether the column could be datetime and return dictionary
            date_format = check_format_datetime_column(column_data)
            if date_format != "Inconclusive":
                type_dict["Deduced_Data_Type"] = "DateTime"
                type_dict["Date_Format"] = date_format
                return column_data, type_dict, "numeric"
        return column_data, type_dict, "numeric"
    
    except ValueError:
        return column_data, type_dict, "non-numeric"


def return_datetime_format(test_string: str) -> str:
    """Determine whether a string matches an item in a list of datetime formats.

    Use a list of possible formats to see whether the first item of the column fits this.
    If it does, return the format. If not, return "Inconclusive".

    Args:
        test_string (str): The string to test for datetime format.
    Returns:
        str: The format of the datetime column.
    """
    month_year_formats = ["%Y%m", "%m%Y", "%Y", "%m", "%Y-%m", "%m-%Y", "%Y/%m", "%m/%Y"]
    date_formats = [
        "%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y", "%Y/%m/%d", '%m/%d/%Y','%m/%d/%y',
        "%Y%m%d", "%d%m%Y", "%m%d%Y"
    ]
    time_formats = ["%H:%M:%S", "%H:%M"]
    date_time_formats = [d + " " + t for d in date_formats for t in time_formats]
    possible_formats = month_year_formats + date_formats + time_formats + date_time_formats 

    for format in possible_formats:
        try:
            datetime.strptime(test_string, format)
            return format
        except ValueError:
            pass
    return "Inconclusive"


def check_format_datetime_column(column_data: pd.Series) -> Tuple[pd.Series, str]:
    """Determine whether a column is of type datetime and return its format.

    Use a list of possible formats to see whether the first item of the column fits this.
    If it does, return the format. If not, return "Inconclusive".

    TODO: see if the whole column can successfully be converted to a datetime object.

    Args:
        column_data (pd.Series[str]): The column data as a Series of strings.
    Returns:
        pd.Series: The column data which may have been converted to type datetime.
        str: The format of the datetime column.
    """
    # Extract the first non-null value from the column
    first_value = column_data.dropna().iloc[0]
    # check first_value is a non-null string
    if not isinstance(first_value, str):
        return "Inconclusive"
    # Format of the date
    date_format = return_datetime_format(first_value)
    #TODO: see whether the whole column (exc nulls) an be converted
    return date_format


def process_numeric_column(column_data: pd.Series) -> dict:
    """Process a numeric column and return its properties.

    Convert the column to an integer type if possible. 
    Return the minimum and maximum values.
    
    Args:
        column_data (pd.Series): The column data.
    Returns:
        dict: A dictionary representing the properties of the numeric column.
    """
    max_min_dict = {
        "Min_value": column_data.min(),
        "Max_value": column_data.max()
    }
    return max_min_dict


def proportions_for_categorical(column_data: pd.Series) -> dict:
    """Generate proportions for a categorical column.
    
    Args:
        column_data (pd.Series): The column data.
    Returns:
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
    Returns:
        dict: A dictionary representing the properties of the categorical column.
    """
    unique_list = column_data.dropna().unique()
    if len(unique_list) < 30:
        prop_dict = proportions_for_categorical(column_data)
        cat_dict = {"Deduced_Data_Type": "categorical", "categories": unique_list, "proportions": prop_dict}
    else:
        cat_dict = {"Deduced_Data_Type": "text", "count": column_data.count()}    
    return cat_dict
    

def deduce_data_type(column_data: pd.Series) -> dict:
    """Deduce the data type of a column based on its contents.
    
    Args:
        column_data (pd.Series): The column data.
    Returns:
        dict: A dictionary representing the deduced data type.
    """
    type_dict = {}
    # determine whether a column is numeric or non-numeric
    column_data, type_dict, is_numeric = decide_numeric_or_non_numeric(column_data, type_dict)
    type_dict["Is_numeric"] = is_numeric

    column_type = str(column_data.dtype)

    if is_numeric == "numeric":
         type_dict["Deduced_Data_Type"] = column_type
         # add further properties for numeric columns and return dictionary
         num_info_dic = process_numeric_column(column_data)
         type_dict.update(num_info_dic)
         return type_dict

    elif column_type == "object":
        # # check whether the column could be datetime and return dictionary
        # date_format = check_format_datetime_column(column_data)
        # if date_format != "Inconclusive":
        #     type_dict["Deduced_Data_Type"] = "DateTime"
        #     type_dict["Date_Format"] = date_format
        #     return type_dict
        
        # check if the column is categorical and return dictionary
        data_type_dict = process_categorical_column(column_data)
    else:
        data_type_dict = {"Deduced_Data_Type": column_type}
    return data_type_dict


def convert_csv_to_toml_schema(data: pd.DataFrame) -> dict:
    """Convert a DataFrame into a TOML schema.
    
    Args:
        data (pd.DataFrame): The input DataFrame.
    Returns:
        dict: A dictionary representing the TOML schema.
    """
    # Initialize an empty dictionary to store the schema
    schema = {}
    # Extract column names and data types from the DataFrame
    for column in data.columns:
        column_data = data[column]
        # Deduce the data type of the column
        try:
            schema[column] = deduce_data_type(column_data)
        except:
            schema[column] = {"Deduced_Data_Type": "error"}
        # Add column description if available
        # State whether column is nullable
        schema[column]["Nullable"] = column_data.isnull().any()
        # Add additional properties based on the data type
    return schema


def create_txt_output(schema: dict) -> str:
    """Create a text output from a TOML schema.
    
    Args:
        schema (dict): The TOML schema as a dictionary.
    Returns:
        str: The text output.
    """
    text_output = ""
    for column in schema:
        text_output += f"[{column}]\n"
        for key, value in schema[column].items():
            text_output += f"{key} = {value}\n"
        text_output += "\n"
    return text_output

# Main script logic here
def convert_csv_to_toml(csv_path: str, toml_path: str)-> None:
    """Convert a CSV file into a TOML schema and save it to a file.
    
    Args:
        csv_path (str): The path to the input CSV file.
        toml_path (str): The path to the output TOML schema.
    Returns:
        None
    """
    input_df = read_csv_file(csv_path)
    # Convert the DataFrame to a TOML schema
    schema = convert_csv_to_toml_schema(input_df)

    # save the schema to a txt file
    text_output = create_txt_output(schema)
    with open(toml_path, "w") as file:
        file.write(text_output)


# Script entry point
if __name__ == "__main__":
    staged_path = "R:/BERD Results System Development 2023/DAP_emulation/2023_surveys/BERD/01_staging/staging_qa/full_responses_qa/2023_staged_BERD_full_responses_24-08-05_v822.csv"
    toml_path_orig = "D:/coding_projects/github_repos/research-and-development/config/output_schemas/staged_BERD_full_responses_schema.toml"
    toml_path_out = "D:/coding_projects/copilot_project/data/synth_data/full_responses_schema_out.toml"
    convert_csv_to_toml(staged_path, toml_path_out)
