"""CSV Datagen."""

import random
import string
import uuid
from datetime import datetime, timedelta
from tkinter import filedialog, messagebox

import numpy as np
import pandas as pd
from faker import Faker


def create_csv(app):
    """Create a CSV file with the specified data."""
    try:
        num_columns = len(app.column_entries)
        num_rows = int(app.num_rows_entry.get())

        if num_columns <= 0 or num_rows <= 0:
            error_message = "Number of columns and rows must be positive"
            raise ValueError(error_message)

        data = {}
        rng = np.random.default_rng()

        # Dictionary mapping data types to their corresponding generators
        data_type_mapping = {
            "Numbers": generate_numbers,
            "Dates and Times": generate_dates_times,
            "Booleans": generate_booleans,
            "Currency": generate_currencies,
            "Percentages": generate_percentages,
            "String": generate_strings,
            "Geolocation": generate_geolocations,
            "Unique ID": generate_unique_ids,
            "Phone Number": generate_phone_numbers,
            "Postcode": generate_postcodes,
        }

        # Iterate through each column entry and generate data based on the type
        for column_entry in app.column_entries:
            col_name = column_entry["col_name_entry"].get()
            data_type = column_entry["data_type_var"].get()
            data[col_name] = data_type_mapping[data_type](column_entry, num_rows, rng)

            # Handle missing values
            data[col_name] = handle_missing_values(
                data[col_name],
                column_entry,
                num_rows,
                rng,
            )

        # Create DataFrame and save as CSV
        df = pd.DataFrame(data)
        df = df.reset_index()  # Convert index to column

        # Ask where to save the CSV file
        save_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
        )
        if save_path:
            df.to_csv(save_path, index=False)
            messagebox.showinfo(
                "Success",
                f"CSV file created successfully at {save_path}",
            )

    except ValueError as e:
        messagebox.showerror("Invalid Input", str(e))


def generate_numbers(column_entry, num_rows, rng):
    """Generate numeric data based on type and distribution."""
    min_val = (
        float(column_entry["min_val_entry"].get())
        if column_entry["min_val_entry"].get()
        else 0
    )
    max_val = (
        float(column_entry["max_val_entry"].get())
        if column_entry["max_val_entry"].get()
        else 1
    )
    dist = column_entry["dist_var"].get()

    dist_mapping = {
        "Uniform": lambda: rng.integers(min_val, max_val + 1, num_rows).astype(int),
        "Normal": lambda: rng.normal(
            (min_val + max_val) / 2,
            (max_val - min_val) / 6,
            num_rows,
        ).astype(int),
        "Exponential": lambda: rng.exponential(
            (max_val - min_val) / 2,
            num_rows,
        ).astype(int),
        "Log Normal": lambda: rng.lognormal(
            mean=(min_val + max_val) / 2,
            sigma=(max_val - min_val) / 6,
            size=num_rows,
        ).astype(int),
        "Random Walk": lambda: np.cumsum(
            rng.uniform(min_val, max_val, num_rows),
        ).astype(int),
    }

    if dist in dist_mapping:
        return dist_mapping[dist]()
    else:
        error_message = f"Unsupported distribution: {dist}"
        raise ValueError(error_message)


def generate_dates_times(column_entry, num_rows):
    """Generate date data."""
    start_date_str = column_entry["start_date_entry"].get()
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").astimezone()
    interval = column_entry["interval_var"].get()

    interval_mapping = {
        "Daily": timedelta(days=1),
        "Weekly": timedelta(weeks=1),
        "Monthly": timedelta(days=30),
        "Quarterly": timedelta(days=90),
        "Yearly": timedelta(days=365),
    }

    delta = interval_mapping.get(interval, timedelta(days=1))
    date_format = column_entry["date_format_var"].get()

    return [(start_date + i * delta).strftime(date_format) for i in range(num_rows)]


def generate_booleans(column_entry, num_rows, rng):
    """Generate boolean data."""
    boolean_rep = column_entry["boolean_rep_var"].get().split(", ")
    true_ratio = column_entry["bool_slider"].get() / 100.0
    num_true = int(true_ratio * num_rows)
    num_false = num_rows - num_true
    data_col = np.array(boolean_rep * [num_true, num_false], dtype=object)

    # Shuffle to randomize true/false distribution
    rng.shuffle(data_col)

    return data_col


def generate_currencies(column_entry, num_rows, rng):
    """Generate currency data."""
    min_val = (
        float(column_entry["min_val_entry"].get())
        if column_entry["min_val_entry"].get()
        else 0
    )
    max_val = (
        float(column_entry["max_val_entry"].get())
        if column_entry["max_val_entry"].get()
        else 1
    )
    currency_symbol = column_entry["currency_symbol_var"].get()
    decimal_places = (
        int(column_entry["decimal_places_entry"].get())
        if column_entry["decimal_places_entry"].get()
        else 2
    )
    thousands_sep = column_entry["thousands_sep_var"].get()

    data_col = rng.uniform(min_val, max_val, num_rows)

    if thousands_sep == "(no sep)":
        return np.array([f"{currency_symbol}{x:.{decimal_places}f}" for x in data_col])
    else:
        return np.array(
            [
                f"{currency_symbol}{x:,.{decimal_places}f}".replace(",", thousands_sep)
                for x in data_col
            ],
        )


def generate_percentages(column_entry, num_rows, rng):
    """Generate percentage data."""
    percentage_format = column_entry["percentage_symbol_var"].get()
    data_col = rng.uniform(0, 100, num_rows)

    if percentage_format == "0.75":
        return data_col / 100
    elif percentage_format == "75%":
        return np.array([f"{int(x)}%" for x in data_col])
    else:
        error_message = f"Unsupported percentage format: {percentage_format}"
        raise ValueError(error_message)


def faker_instance(countries):
    """Initialize a Faker instance based on the specified list of country codes."""
    # Mapper dictionary for country codes to Faker locales
    locale_mapper = {
        "US": "en_US",
        "UK": "en_GB",
        "Canada": "en_CA",
        "China": "zh_CN",
        "Spain": "es_ES",
        "Germany": "de_DE",
        "France": "fr_FR",
    }

    # Convert country codes to locales
    locales = [locale_mapper[country] for country in countries]
    faker_instance = Faker(locales)
    return faker_instance


def generate_strings(column_entry, num_rows, _):
    """Generate string data based on specified type and countries."""
    countries = column_entry["country_var"].get().split(", ")
    string_type = column_entry["string_type_var"].get()
    fk = faker_instance(countries)

    def generate_names():
        """Generate names based on selected countries and type."""
        name_type = column_entry["names_type_var"].get()
        name_type_mapping = {
            "First": lambda: fk.first_name(),
            "Last": lambda: fk.last_name(),
            "Full Name": lambda: fk.name(),
        }
        return name_type_mapping[name_type]()

    # Dictionary mapping string types to their corresponding generation functions
    string_type_mapping = {
        "Email": lambda: [fk.email() for _ in range(num_rows)],
        "URL": lambda: [fk.url() for _ in range(num_rows)],
        "Names": lambda: [generate_names() for _ in range(num_rows)],
        "Short text": lambda: [fk.sentence() for _ in range(num_rows)],
        "Long text": lambda: [fk.paragraph() for _ in range(num_rows)],
    }

    # Generate the data based on the string type
    return string_type_mapping[string_type]()


def generate_geolocations(column_entry, num_rows, _):
    """Generate geolocation data based on the specified format."""
    format_type = column_entry["geo_type_var"].get()

    # Mapping dictionary for different geo_type formats
    geo_type_mapping = {
        "Lat,Long": lambda: f"{random.uniform(-90, 90):.6f},{random.uniform(-180, 180):.6f}",  # noqa: E501
        "Latitude": lambda: f"{random.uniform(-90, 90):.6f}",
        "Longitude": lambda: f"{random.uniform(-180, 180):.6f}",
    }

    # Generate data based on the specified format
    return [geo_type_mapping[format_type]() for _ in range(num_rows)]


def generate_unique_ids(column_entry, num_rows, _):
    """Generate unique IDs."""
    format_type = column_entry["unique_id_format_var"].get()
    format_type_mapping = {
        "uuid": lambda: str(uuid.uuid4()),
        "numeric": lambda: str(random.randint(10000, 99999)),
        "alphanumeric": lambda: "".join(
            random.choices(string.ascii_uppercase + string.digits, k=10),
        ),
        "structured_id": lambda: f"{random.randint(1, 999):03d}-{random.randint(2000, 2100)}-{random.randint(1, 999999):06d}",  # noqa: E501
        "hex_short_id": lambda: f"{random.randint(0, 65535):04x}-{random.randint(0, 65535):04x}-{random.randint(0, 65535):04x}",  # noqa: E501
        "custom_alphanumeric_uuid": lambda: f"uuid-{random.randint(10000, 99999)}-{random.choice(string.ascii_lowercase)}{''.join(random.choices(string.ascii_lowercase + string.digits, k=5))}",  # noqa: E501
        "timestamp_numeric": lambda: pd.Timestamp.now().strftime("%Y%m%d%H%M%S"),
        "invoice_custom_id": lambda: f"INVOICE-{random.randint(2000, 2100)}-{random.randint(1, 999999):06d}",  # noqa: E501
    }
    return [format_type_mapping[format_type]() for _ in range(num_rows)]


def generate_phone_numbers(column_entry, num_rows, _):
    """Generate phone numbers based on selected countries."""
    countries = column_entry["country_var"].get().split(", ")
    fk = faker_instance(countries)
    return [fk.phone_number() for _ in range(num_rows)]


def generate_postcodes(column_entry, num_rows, _):
    """Generate postcodes based on selected countries."""
    countries = column_entry["country_var"].get().split(", ")
    fk = faker_instance(countries)
    return [fk.postcode() for _ in range(num_rows)]


def handle_missing_values(data_col, column_entry, num_rows, rng):
    """Handle missing values in a column using a dictionary mapping."""
    missing_val_type = column_entry["missing_values_var"].get()
    missing_percentage = (
        float(column_entry["missing_values_entry"].get()) / 100.0
        if column_entry["missing_values_entry"].get()
        else 0
    )

    missing_value_mapping = {
        "N/A": "N/A",
        "Null": None,
        "Nan": np.nan,
        "(blank_cells)": "",
    }

    if missing_percentage > 0:
        mask = rng.random(num_rows) < missing_percentage
        missing_value = missing_value_mapping.get(missing_val_type, None)
        data_col[mask] = missing_value
    return data_col
