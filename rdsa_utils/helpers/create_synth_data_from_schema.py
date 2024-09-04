"""
create_synth_data_from_schema.py

This script generates synthetic data based on a schema from a toml file.

Author: [Your Name]
Date: 
"""
import random
import pandas as pd
import os
import toml

def read_toml_file(file_path: str) -> dict:
    """Read a toml file and return the contents as a dictionary."""
    # check file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    with open(file_path, "r") as file:
        schema = toml.load(file)
        return schema
    

def random_proportion_catagorical_data(proportion_dict: dict, num_rows: int) -> pd.Series:
    """Generate a random series of catagorical data based on a proportion dictionary."""
    # Get the categories and proportions from the dictionary
    categories = list(proportion_dict.keys())
    proportions = list(proportion_dict.values())
    proportions = [float(p) for p in proportions]
    # Generate a random sample of categories based on the proportions
    data = random.choices(categories, weights=proportions, k=num_rows)
    return pd.Series(data)


def random_uniform_data(min_value: float, max_value: float, num_rows: int) -> pd.Series:
    """Generate a random series of data from a uniform distribution."""
    # check that min and max values are floats
    try:
        min_value = float(min_value)
        max_value = float(max_value)
    except ValueError:
        raise ValueError("min_value and max_value must be integers or floats")
    if min_value > max_value:
        raise ValueError("min_value must be less than max_value")
    
    data = [random.uniform(min_value, max_value) for _ in range(num_rows)]
    return pd.Series(data)
    

def generate_synthetic_data(schema: dict, num_rows: int) -> pd.DataFrame:
    """Generate synthetic data based on the schema."""
    # Initialize an empty dictionary to store the generated data
    data = {}
    # Generate data for each field in the schema
    for field, properties in schema.items():
        # Generate data based on the field type
        if properties["Deduced_Data_Type"] == "categorical":
            # check there is a proportions key in the properties
            if "proportions" not in properties:
                # generate categories using uniform distribution
                data[field] = random_proportion_catagorical_data({cat: 1 for cat in properties["categories"]}, num_rows)
            else:
                data[field] = random_proportion_catagorical_data(properties["proportions"], num_rows)
        elif properties["Deduced_Data_Type"] in ["int", "float", "numeric", "Int64", "int64", "float64"]:
            data[field] = random_uniform_data(properties["Min_value"], properties["Max_value"], num_rows)
            # data[field] = [random.uniform(100, 999) for _ in range(num_rows)]
        # elif properties["Deduced_Data_Type"] == "text":
        #     data[field] = [random.choice(properties["texts"]) for _ in range(num_rows)]
        elif properties["Deduced_Data_Type"] == "date":
            data[field] = [random.choice(properties["dates"]) for _ in range(num_rows)]
        else: 
            data[field] = "No value"
        # else:
        #     raise ValueError(f"Invalid field type: {properties['Deduced_Data_Type']}")
    # Create a DataFrame from the generated data
    return pd.DataFrame(data)


def write_data_to_csv(data: pd.DataFrame, output_path: str):
    """Write the generated data to a CSV file.
    
    Args:
        data (pd.DataFrame): The generated data.
        output_path (str): The path to write the data to.
    
    Returns:
        None
    """
    data.to_csv(output_path, index=False)


def generate_and_write_data(schema_path: str, num_rows: int, output_path: str):
    """Generate synthetic data based on the schema and write it to a CSV file."""
    schema = read_toml_file(schema_path)
    data = generate_synthetic_data(schema, num_rows)
    write_data_to_csv(data, output_path)    


if __name__ == "__main__":

    num_rows = 100
    schema_path = "D:/coding_projects/copilot_project/data/synth_data/full_responses_schema_out.toml"
    output_filename = "synthetic_staged_berd_data.csv"
    output_path = f"D:/coding_projects/copilot_project/data/synth_data/{output_filename}"
    generate_and_write_data(schema_path, num_rows, output_path)
    print(f"Synthetic data generated and saved to: {output_path}")