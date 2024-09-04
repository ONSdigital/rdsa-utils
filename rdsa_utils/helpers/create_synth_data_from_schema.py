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
    

def generate_synthetic_data(schema: dict, num_rows: int) -> pd.DataFrame:
    """Generate synthetic data based on the schema."""
    # Initialize an empty dictionary to store the generated data
    data = {}
    # Generate data for each field in the schema
    for field, properties in schema.items():
        # Generate data based on the field type
        if properties["Deduced_Data_Type"] == "categorical":
            data[field] = [random.choice(properties["categories"]) for _ in range(num_rows)]
        elif properties["Deduced_Data_Type"] in ["int", "float", "numeric", "Int64", "int64", "float64"]:
            # data[field] = [random.uniform(properties["min"], properties["max"]) for _ in range(num_rows)]
            data[field] = [random.uniform(100, 999) for _ in range(num_rows)]
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