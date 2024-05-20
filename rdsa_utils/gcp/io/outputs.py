"""Write outputs to GCP."""

import logging
from typing import List, Literal, Optional, Union

from pandas import DataFrame as PandasDF
from pyspark.sql import DataFrame as SparkDF

from rdsa_utils.gcp.helpers.gcp_utils import run_bq_query
from rdsa_utils.helpers.pyspark import is_df_empty
from rdsa_utils.helpers.python import list_convert
from rdsa_utils.typing import BigQueryTimePartitions, TablePath

logger = logging.getLogger(__name__)


def write_table(
    df: Union[PandasDF, SparkDF],
    table_name: TablePath,
    mode: Literal["append", "error", "ignore", "overwrite"] = "error",
    partition_col: Optional[str] = None,
    partition_type: Optional[BigQueryTimePartitions] = None,
    partition_expiry_days: Optional[float] = None,
    clustered_fields: Optional[Union[str, List[str]]] = None,
) -> None:
    """Write dataframe out to a Google BigQuery table.

    In the case the table already exists, behavior of this function depends on
    the save mode, specified by the mode function (default to throwing an
    exception). When mode is Overwrite, the schema of the DataFrame does not
    need to be the same as that of the existing table (the column order
    doesn't need be the same).

    If you use the `df.printSchema()` method directly in a print/log statement
    the code is processed and printed regardless of logging level. Instead you
    need to capture the output and pass this to the logger. See explanation
    here - https://stackoverflow.com/a/59935109

    To learn more about the partitioning of tables and how to use them in
    BigQuery: https://cloud.google.com/bigquery/docs/partitioned-tables

    To learn more about the clustering of tables and how to use them in
    BigQuery: https://cloud.google.com/bigquery/docs/clustered-tables

    To learn more about how spark dataframes are saved to BigQuery:
    https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/master/README.md

    Parameters
    ----------
    df
        The dataframe to be saved.
    table_name
        The target BigQuery table name of form:
        <project_id>.<database>.<table_name>
    mode
        Whether to overwrite or append to the BigQuery table.
            * `append`: Append contents of this :class:`DataFrame` to table.
            * `overwrite`: Overwrite existing data.
            * `error`: Throw exception if data already exists.
            * `ignore`: Silently ignore this operation if data already exists.
    partition_col
        A date or timestamp type column in the dataframe to use for the table
        partitioning.
    partition_type
        The unit of time to partition the table by, must be one of:
            * `hour`
            * `day`
            * `month`
            * `year`

        If `partition_col` is specified and `partition_type = None` then
        BigQuery will default to using `day` partition type.

        If 'partition_type` is specified and `partition_col = None` then the
        table will be partitioned by the ingestion time pseudo column, and can
        be referenced in BigQuery via either  `_PARTITIONTIME as pt` or
        `_PARTITIONDATE' as pd`.

        See https://cloud.google.com/bigquery/docs/querying-partitioned-tables
        for more information on querying partitioned tables.
    partition_expiry_days
        If specified, this is the number of days (any decimal values are
        converted to that proportion of a day) that BigQuery keeps the data
        in each partition.
    clustered_fields
        If specified, the columns (up to four) in the dataframe to cluster the
        data by when outputting. The order the columns are specified is
        important as will be the ordering of the clustering on the BigQuery
        table.

        See: https://cloud.google.com/bigquery/docs/querying-clustered-tables
        for more information on querying clustered tables.

    Returns
    -------
    None
    """  # noqa: E501
    logger.info(f"Writing to table {table_name} with mode {mode.upper()}")

    # Pandas df should always be small enough to be saved as a single
    # file/partition.
    if isinstance(df, PandasDF):
        logger.debug("Converting pandas dataframe to spark")
        df = df.sql_ctx.createDataFrame(df).coalesce(1)

    logger.info(
        f"Output dataframe has schema\n{df._jdf.schema().treeString()}",
    )

    if is_df_empty(df):
        logger.warning(
            "The output contains no records. No data will be appended to the "
            f"{table_name} table.",
        )

    write = df.write.format("bigquery").mode(mode)

    if partition_col:
        logger.info(f"Data in BigQuery will be partitioned on {partition_col}")
        write = write.option("partitionField", partition_col)

    if partition_type:
        logger.info(
            f"Data in BigQuery will be partitioned by {partition_type}",
        )
        write = write.option("partitionType", partition_type.upper())

    if clustered_fields:
        clustered_fields = list_convert(clustered_fields)

        if len(clustered_fields) > 4:
            msg = (
                f"Cannot save {table_name=} with clustered columns"
                f"Number of columns specified = {len(clustered_fields)} > 4"
            )
            logger.error(msg)
            raise ValueError(msg)

        cluster_string = ",".join(clustered_fields)

        logger.info(f"Data in BigQuery will be clustered on {cluster_string}")
        write = write.option("clusteredFields", cluster_string)

    write.save(table_name)

    # For any partitioned table it is best practice to require partition
    # filtering for any SQL queries in BigQuery.
    if partition_col or partition_type:
        logger.info("Setting BigQuery require_partition_filter to True")
        run_bq_query(
            f"""
            ALTER TABLE {table_name}
            SET OPTIONS (
                require_partition_filter = true
            );
            """,
        )

    if partition_expiry_days:
        logger.info(f"Setting BigQuery {partition_expiry_days=}")
        run_bq_query(
            f"""
            ALTER TABLE {table_name}
            SET OPTIONS (
                partition_expiration_days = {partition_expiry_days}
            );
            """,
        )
