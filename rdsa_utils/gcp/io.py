"""Useful I/O functions."""
import json
import logging
import textwrap
from typing import (
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
)

from google.cloud import (
    bigquery,
    storage,
)
from google.cloud.exceptions import NotFound
from pandas import DataFrame as PandasDF
from pyspark.sql import (
    DataFrame as SparkDF,
    SparkSession,
)
import yaml

from rdsa_utils.helpers._typing import TablePath
from rdsa_utils.helpers.helpers_python import (
    list_convert,
)
from rdsa_utils.helpers.helpers_spark import (
    is_df_empty,
    convert_struc_col_to_columns,
)
from rdsa_utils.helpers.logging import (
    log_spark_df_schema,
)


logger = logging.getLogger(__name__)


@log_spark_df_schema(log_schema_on_input=False)
def read_table(
    spark: SparkSession,
    table_path: TablePath,
    columns: Optional[Sequence[str]] = None,
    date_column: Optional[str] = None,
    date_range: Optional[Sequence[str]] = None,
    column_filter_dict: Optional[Dict[str, Sequence[str]]] = {},
    run_id_column: Optional[str] = 'run_id',
    run_id: Optional[str] = None,
    flatten_struct_cols: bool = False,
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
    flatten_struct_cols
        When true, any struct type columns in the loaded dataframe are replaced
        with individual columns for each of the fields in the structs.

    Returns
    -------
    SparkDF
    """
    # If columns are specified, ensure they exist in the table to be read.
    if columns:
        table_cols = get_table_columns(table_path)
        list_cols_not_in_table_cols = [
            col for col in columns if col not in table_cols
        ]
        if list_cols_not_in_table_cols:
            message = f"""
            Columns: {list_cols_not_in_table_cols} are not in dateset.
            Choose columns from: {', '.join(table_cols)}.
            """
            logger.error(message)
            raise ValueError(message)

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
    )

    logger.info(f'Reading table using query: \n{query}')

    df = spark.read.load(query, format='bigquery')

    if is_df_empty(df):
        logger.warning(f'No data has been read from {table_path}')

    if flatten_struct_cols:
        df = convert_struc_col_to_columns(df=df)

    return df


def build_sql_query(
    table_path: TablePath,
    columns: Optional[Sequence[str]] = None,
    date_column: Optional[str] = None,
    date_range: Optional[Sequence[str]] = None,
    column_filter_dict: Optional[Dict[str, Sequence[str]]] = None,
) -> str:
    """Create the SQL query to load the data with specified filter conditions.

    Parameters
    ----------
    spark
        Spark session.
    table_path
        Hive table path in format "database_name.table_name".
    columns
        The column selection. Selects all columns if None passed.
    date_column
        The name of the column to be used to filter the date range on.
    date_range
        Sequence with two values, a lower and upper value for dates to load in.
    column_filter_dict
        A dictionary containing column: [values] where the values correspond to
        terms in the column that are to be filtered by.

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

    # Join columns to comma-separated string for the SQL query.
    selection = ', '.join(columns) if columns else '*'

    sql_query.append(f'SELECT {selection}\nFROM {table_path}')

    if date_column and date_range:
        sql_query.append('WHERE (')
        sql_query.append(f"{date_column} >= '{date_range[0]}'")
        sql_query.append(f"AND {date_column} < '{date_range[1]}'")
        sql_query.append(')')

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
            # First query for column is different to subsequent ones. If date
            # has been filtered we use AND, if not we use WHERE for the first
            # instance.
            if first_filter_applied:
                if isinstance(column_filter_dict[column][0], str):
                    sql_query.append(f"""
                        AND (\n{column} = '{column_filter_dict[column][0]}'
                    """)
                else:
                    sql_query.append(f"""
                        AND (\n{column} = {column_filter_dict[column][0]}
                    """)
            else:
                if isinstance(column_filter_dict[column][0], str):
                    sql_query.append(f"""
                        WHERE (\n{column} = '{column_filter_dict[column][0]}'
                    """)
                else:
                    sql_query.append(f"""
                        WHERE (\n{column} = {column_filter_dict[column][0]}
                    """)

                first_filter_applied = True

            # Subsequent queries on column are the same form but use OR.
            if len(column_filter_dict[column]) > 1:
                for item in column_filter_dict[column][1:]:
                    if isinstance(item, str):
                        sql_query.append(f"OR {column} = '{item}'\n")
                    else:
                        sql_query.append(f'OR {column} = {item}\n')

            # close off the column filter query
            sql_query.append(')\n')

    # Join entries in list into one nicely formatted string for easier unit
    # testing. Use textwrap.dedent to remove leading whitespace from multiline
    # strings.
    return '\n'.join([textwrap.dedent(line.strip()) for line in sql_query])


def write_table(
    df: SparkDF,
    table_name: TablePath,
    mode: str = 'error',
 ) -> None:
    """Write Spark DataFrame out to a BigQuery table.

    In the case the table already exists, behavior of this function depends on
    the save mode, specified by the mode function (default to throwing an
    exception). When mode is Overwrite, the schema of the DataFrame does not
    need to be the same as that of the existing table (the column order
    doesn't need be the same).

    If you use the `df.printSchema()` method directly in a print/log statement
    the code is processed and printed regardless of logging level. Instead you
    need to capture the output and pass this to the logger. See explanation
    here - https://stackoverflow.com/a/59935109

    Parameters
    ----------
    df
        The dataframe to write to BigQuery.
    table_name
        The target BigQuery table name of form:
        <project_id>.<database>.<table_name>
    mode : {'overwrite', 'append', 'error', 'ignore'}
        Whether to overwrite or append to the Hive table.
        * `append`: Append contents of this :class:`DataFrame` to table.
        * `overwrite`: Overwrite existing data.
        * `error` or `errorifexists`: Throw exception if data already exists.
        * `ignore`: Silently ignore this operation if data already exists.

    Returns
    -------
    None
    """
    if is_df_empty(df):
        logger.warning(
            f"""The output contains no records. No data will be appended
            to the {table_name} table.""",
        )

    logger.info(f'Writing to table {table_name} with mode {mode.upper()}')

    # Pandas df should always be small enough to be saved as a single
    # file/partition.
    if isinstance(df, PandasDF):
        df = df.sql_ctx.createDataFrame(df).coalesce(1)

    logger.info(
        f'Output dataframe has schema\n{df._jdf.schema().treeString()}',
    )

    df.write.save(table_name, format='bigquery', mode=mode)


def get_table_columns(table_path) -> List[str]:
    """Return the column names for given bigquery table."""
    client = bigquery.Client()

    table = client.get_table(table_path)
    return [column.name for column in table.schema]


def table_exists(table_path: TablePath) -> bool:
    """Check the big query catalogue to see if a table exists.

    Returns True if a table exists.
    See code sample explanation here:
    https://cloud.google.com/bigquery/docs/samples/bigquery-table-exists#bigquery_table_exists-python
    Parameters
    ----------
    table_path
        The target BigQuery table name of form:
        <project_id>.<database>.<table_name>

    Returns
    -------
    bool
        Returns True if table exists and False if table does not exist.
    """
    try:
        bigquery.Client().get_table(table_path)
        table_exists = True
        logger.debug(f'Table {table_path} exists.')

    except NotFound:
        table_exists = False
        logger.warning(f'Table {table_path} not found.')

    return table_exists


def load_config_gcp(config_path: str) -> Tuple[Dict, Dict]:
    """Load the config and dev_config files to dictionaries.

    Parameters
    ----------
    config_path
        The path of the config file in a yaml format.

    Returns
    -------
    Tuple[Dict, Dict]
        The contents of the config files.
    """
    logger.info(f"""loading config from file: {config_path}""")

    storage_client = storage.Client()

    bucket_name = config_path.split('//')[1].split('/')[0]
    blob_name = '/'.join(config_path.split('//')[1].split('/')[1:])

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    contents = blob.download_as_string()

    config_file = yaml.safe_load(contents)

    logger.info(json.dumps(config_file, indent=4))
    return config_file
