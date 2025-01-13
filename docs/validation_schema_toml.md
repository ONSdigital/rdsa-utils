# Data Validation Schema (TOML)

This document explains how to define data validation rules using a TOML configuration file. This schema is designed to be used with a data validation tool or framework. Initially this has been written to work with [Great Expectations](https://greatexpectations.io/), but could also work with other data validation libraries such as Pandera. 

The utility is designed to validate Pandas or PySpark DataFrames.

The schema used for validation is taken from a toml file which should be easy to configure for the average develop, or even end user. 

The validation scripts do two main things:
1) validate the schema itself, making sure it's written in valid toml language and contains no logical or input errors. 
2) creates a batch of "expectations" from the toml to validate the data against
3) validates the data against the schema

![Image]([https://github.com/ONSdigital/rdsa-utils/blob/8f56a02a60fb0432aaee66924f118041e3ee705a/Data%20Validator%20Diagram.png](https://github.com/ONSdigital/rdsa-utils/blob/5fb2c9b2062ef396bdc99655d9561c549c4db640/docs/img/Data_Validator_Diagram.png))

## Structure

The TOML file is structured as a series of tables, where each table represents a column in your DataFrame and its associated validation rules. The `[data_asset]` table is used to name the data asset these rules apply to within Great Expectations.


## All Possible Data Validation Fields

Here's a breakdown of the available fields within each column's table:

* **`description`:** A human-readable description of the column's purpose or meaning.  This is for documentation and doesn't affect validation. *Required*.

* **`nullable`:**  Specifies whether null or missing values are allowed in the column.  Use `true` to allow nulls, `false` to disallow them. *Required*.

* **`data_type`:** The expected data type of the column. Supported types include:

    * **Python:**
        * `"int"`: Integer
        * `"float"`: Floating-point number
        * `"str"`: String
        * `"bool"`: Boolean (true/false)
        * `"list"`: List
        * `"tuple"`: Tuple
        * `"dict"`: Dictionary
        * `"set"`: Set
        * `"datetime.datetime"`: Date and time

    * **Pandas/NumPy:**
        * `"int64"`: 64-bit Integer  (and other integer types like `"int32"`, `"int16"`, `"int8"`)
        * `"float64"`: 64-bit Floating-point number (and other float types like `"float32"`)
        * `"object"`:  For mixed or arbitrary data types.
        * `"bool_"`: Boolean
        * `"datetime64[ns]`": Date and time (nanosecond precision)
        * `"timedelta64[ns]`": Timedelta (nanosecond precision)
        * `"category"`:  For categorical or enumerated types.

    * **PySpark:**
        * `"StringType"`: String
        * `"IntegerType"`: Integer
        * `"FloatType"`: Floating-point number
        * `"DoubleType"`: Double-precision floating point
        * `"BooleanType"`: Boolean
        * `"TimestampType"`: Timestamp
        * `"DateType"`: Date
        * `"ArrayType"`: Array
        * `"MapType"`: Map
        * `"StructType"`: Struct

* **`length`:** For string columns, specifies the *minimum* required length. You can also implement a separate `max_length` field if needed (not shown in the example, but easy to add). Use a string value like `">=1"` for minimum length, or an integer for a fixed length (e.g., `6` implies a length of exactly 6).

* **`min_value`, `max_value`:**  For numeric and string types, specifies the minimum and maximum allowed values, respectively.  For string types, the comparison is alphabetical. Provide numerical values or strings enclosed in double quotes.  Use `"nan"` if there is no limit.

* **`possible_values`:** A list of allowed values for the column. This is most useful for categorical or enumerated type columns, where the valid values are known in advance.  Use `["nan"]` if the field isn't being used (or preferably remove the field entirely).

* **`regex_pattern`:**  For string columns, a regular expression (regex) pattern that values must match.  Use raw strings (e.g., `r"^\d+$"`) for regex to avoid issues with escaping special characters.

* **`unique`:** For any data type, set to `true` if all values in the column must be unique.

* **`date_format`:** For `datetime64[ns]` columns, specifies the expected date/time format using Python's `strftime` syntax.  Examples: `"%Y-%m-%d"`, `"%Y-%m-%dT%H:%M:%S%z"`. Use `"nan"` if no specific date format needs to be validated.

* **`number_str_format`:**: A string formatter for specifying a desired number format. Numeric types and string types can be checked against this. 

1. Integer Formatting:

If your data_type is int (or any of the integer types like int64, IntegerType, etc.), you might want formats like:

"{}": Just the integer itself (1234).
"{:d}": Same as above.
"{:,d}": Integer with thousands separators (1,234).
"{:05d}": Padded with zeros to a width of 5 (01234). Useful for things like zip codes or other fixed width numerical codes.
"{:x}": Hexadecimal format (4d2).
"{:o}": Octal format (2322).
"{:b}": Binary format (10011010010).

2. Floating-Point Formatting:

If your data_type is float (or float64, float32, DoubleType):

"{}" : Default float representation.
"{:.2f}": Two decimal places (1234.56).
"{:,.2f}": Two decimal places with thousands separator (1,234.56).
"{:e}": Scientific notation (1.234560e+03).
"{:.2e}": Scientific notation with two decimal places (1.23e+03).
"{:%}": Percentage format (123456.000000%).
"{:.2%}": Percentage with two decimal places (123456.00%).

* **`custom_check`:**  Allows you to define custom validation logic using Python code. The value of this field should be a string containing a valid Python function definition.  Your validation code needs to be able to parse and execute this Python code.


## Example

```toml
[data_asset]
name = "my_data_asset"  # Data asset name (A helpful reference and will be used in Great Expectations).

[reference]
description = "Unique identifier for each record."
nullable = false
data_type = "str"
unique = true
regex_pattern = r"^\d+$" # Example: Must be all numbers.


[period]
description = "Data reporting period (YYYYMM)."
nullable = false
data_type = "str"
length = 6
regex_pattern = r"^\d{6}$"
```

### Credits

The code for the RDSA Data Validator was originally created by James Westwood.
