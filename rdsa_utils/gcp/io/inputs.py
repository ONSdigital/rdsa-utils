"""Read from BigQuery."""

import logging
import textwrap
from typing import Dict, Optional, Sequence, Tuple, Union

from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import SparkSession

from rdsa_utils.exceptions import ColumnNotInDataframeError, TableNotFoundError
from rdsa_utils.gcp.helpers.gcp_utils import get_table_columns, table_exists
from rdsa_utils.helpers.pyspark import convert_struc_col_to_columns, is_df_empty
from rdsa_utils.helpers.python import (
    convert_date_strings_to_datetimes,
    list_convert,
    tuple_convert,
)
from rdsa_utils.logging import log_spark_df_schema
from rdsa_utils.typing import BigQueryTimePartitions, TablePath

logger = logging.getLogger(__name__)


@log_spark_df_schema(log_schema_on_input=False)
def read_table(
    spark: SparkSession,
    table_path: TablePath,
    columns: Optional[Sequence[str]] = None,
    date_column: Optional[str] = None,
    date_range: Optional[Sequence[str]] = None,
    column_filter_dict: Optional[Dict[str, Sequence[str]]] = None,
    run_id_column: Optional[str] = "run_id",
    run_id: Optional[str] = None,
    flatten_struct_cols: bool = False,
    partition_column: Optional[str] = None,
    partition_type: Optional[BigQueryTimePartitions] = None,
    partition_value: Optional[Union[Tuple[str, str], str]] = None,
) -> SparkDF:
    """Read BigQuery table given table path and column selection.

    Parameters
    ----------
    spark
        Spark session.
    table_path
        The target BigQuery table name of form:
        <project_id>.<database>.<table_name>
    columns
        The column selection. Selects all columns if None passed.
    date_column
        The name of the column to be used to filter the date range on.
    date_range
        Sequence with two values, a lower and upper value for dates to load in.
    column_filter_dict
        A dictionary containing column: [values] where the values correspond to
        terms in the column that are to be filtered by.
    run_id_column
        The name of the column to be used to filter to the specified run_id.
    run_id
        The unique identifier for a run within the table that the read data is
        filtered to.
    partition_column
        The name of the column that the table is partitioned by.
    partition_type
        The unit of time the table is partitioned by, must be one of:
            * `hour`
            * `day`
            * `month`
            * `year`
    partition_value
        The value or pair of values for filtering the partition column to.
    flatten_struct_cols
        When true, any struct type columns in the loaded dataframe are replaced
        with individual columns for each of the fields in the structs.

    Returns
    -------
    SparkDF
    """
    if not table_exists(table_path=table_path):
        msg = f"{table_path=} cannot be found."
        logger.error(msg)
        raise TableNotFoundError(msg)

    if not column_filter_dict:
        column_filter_dict = {}

    # If columns are specified, ensure they exist in the table to be read.
    if columns:
        table_cols = get_table_columns(table_path)
        list_cols_not_in_table_cols = [col for col in columns if col not in table_cols]
        if list_cols_not_in_table_cols:
            message = f"""
            Columns: {list_cols_not_in_table_cols} are not in dateset.
            Choose columns from: {', '.join(table_cols)}.
            """
            logger.error(message)
            raise ColumnNotInDataframeError(message)

    # If a run_id is passed, it is added to the column filter dictionary for
    # use in reading and filtering the table.
    if run_id:
        column_filter_dict[run_id_column] = run_id

    query = build_sql_query(
        table_path=table_path,
        columns=columns,
        date_column=date_column,
        date_range=date_range,
        column_filter_dict=column_filter_dict,
        partition_column=partition_column,
        partition_type=partition_type,
        partition_value=partition_value,
    )

    logger.info(f"Reading table using query: \n{query}")

    df = spark.read.load(query, format="bigquery")

    if is_df_empty(df):
        logger.warning(f"No data has been read from {table_path}")

    if flatten_struct_cols:
        df = convert_struc_col_to_columns(df=df)

    return df


def build_sql_query(  # noqa: C901
    table_path: TablePath,
    columns: Optional[Sequence[str]] = None,
    date_column: Optional[str] = None,
    date_range: Optional[Sequence[str]] = None,
    column_filter_dict: Optional[Dict[str, Sequence[str]]] = None,
    partition_column: Optional[str] = None,
    partition_type: Optional[str] = None,
    partition_value: Optional[Union[Tuple[str, str], str]] = None,
) -> str:
    """Create SQL query to load data with the specified filter conditions.

    Parameters
    ----------
    spark
        Spark session.
    table_path
        BigQuery table path in format "database_name.table_name".
    columns
        The column selection. Selects all columns if None passed.
    date_column
        The name of the column to be used to filter the date range on.
    date_range
        Sequence with two values, a lower and upper value for dates to load in.
    column_filter_dict
        A dictionary containing column: [values] where the values correspond to
        terms in the column that are to be filtered by.
    partition_column
        The name of the column that the table is partitioned by.
    partition_type
        The unit of time the table is partitioned by, must be one of:
            * `hour`
            * `day`
            * `month`
            * `year`
    partition_value
        The value or pair of values for filtering the partition column to.

    Returns
    -------
    str
        The string containing the SQL query.
    """
    # Create empty list to store all parts of query - combined at end.
    sql_query = []

    # Flag to check whether or not to use a WHERE or AND statement as only one
    # instance of WHERE is allowed in a query.
    first_filter_applied = False

    filter_start = {
        False: "WHERE",
        True: "AND",
    }

    # Join columns to comma-separated string for the SQL query.
    selection = ", ".join(columns) if columns else "*"

    sql_query.append(f"SELECT {selection}\nFROM {table_path}")

    if partition_column and partition_value and partition_type:
        sql_query.append(f"{filter_start[first_filter_applied]} (")

        # If a single partition value is being used we use an "=" for
        # comparison, otherwise we use the "BETWEEN" SQL function.
        partition_value = tuple_convert(partition_value)
        if len(partition_value) == 1:
            sql_query.append(
                f"TIMESTAMP_TRUNC({partition_column}, {partition_type}) "
                f"= TIMESTAMP_TRUNC(TIMESTAMP('{partition_value[0]}'), {partition_type})",  # noqa: E501
            )
        elif len(partition_value) == 2:
            partition_value = convert_date_strings_to_datetimes(
                *partition_value,
            )

            sql_query.append(f"{partition_column}")
            sql_query.append(f"BETWEEN '{partition_value[0]}'")
            sql_query.append(f"AND '{partition_value[1]}'")

        else:
            msg = f"{partition_value=} must have either 1 or 2 values only."
            logger.error(msg)
            raise ValueError(msg)

        sql_query.append(")")

        first_filter_applied = True

    if date_column and date_range:
        sql_query.append(f"{filter_start[first_filter_applied]} (")
        sql_query.append(f"{date_column} >= '{date_range[0]}'")
        sql_query.append(f"AND {date_column} < '{date_range[1]}'")
        sql_query.append(")")

        first_filter_applied = True

    # Add any column-value specific filters onto the query. Addtional queries
    # are of the form:
    # AND (column_A = 'value1' OR column_A = 'value2' OR ...)
    if column_filter_dict:
        for column in column_filter_dict.keys():
            # Ensure values are in a list if not already.
            column_filter_dict[column] = list_convert(
                column_filter_dict[column],
            )

            sql_query.append(f"{filter_start[first_filter_applied]} (\n")

            # If the value is a string we wrap it in quotes, otherwise we don't
            # which avoids turning e.g. an integer into a string (1 -> '1').
            if isinstance(column_filter_dict[column][0], str):
                sql_query.append(
                    f"{column} = '{column_filter_dict[column][0]}'",
                )
            else:
                sql_query.append(
                    f"{column} = {column_filter_dict[column][0]}",
                )

            first_filter_applied = True

            # Subsequent queries on column are the same form but use OR.
            if len(column_filter_dict[column]) > 1:
                for item in column_filter_dict[column][1:]:
                    if isinstance(item, str):
                        sql_query.append(f"OR {column} = '{item}'\n")
                    else:
                        sql_query.append(f"OR {column} = {item}\n")

            # close off the column filter query
            sql_query.append(")\n")

    # Join entries in list into one nicely formatted string for easier unit
    # testing. Use textwrap.dedent to remove leading whitespace from multiline
    # strings.
    return "\n".join([textwrap.dedent(line.strip()) for line in sql_query])
