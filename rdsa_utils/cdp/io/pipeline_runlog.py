"""Utilities for managing a Pipeline Runlog using Hive Tables."""

import json
import logging
import os
from ast import literal_eval
from configparser import ConfigParser
from datetime import datetime
from typing import Dict, List, Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from rdsa_utils.cdp.helpers.hdfs_utils import create_txt_from_string

logger = logging.getLogger(__name__)


def _write_entry(entry_df: DataFrame, log_table: str) -> None:
    """Write a DataFrame entry into a specified table.

    Parameters
    ----------
    entry_df
        The DataFrame containing the data to be written to the table.
    log_table
        The name of the table into which the data should be written.

    Returns
    -------
    None
    """
    try:
        entry_df.write.insertInto(log_table)
    except AnalysisException as e:
        logger.error(f"Error writing entry to table {log_table}: {e}")


def create_runlog_table(
    spark: SparkSession,
    database: str,
    tablename: Optional[str] = "pipeline_runlog",
) -> None:
    """Create runlog and _reserved_ids tables in the target database if needed.

    This function executes two SQL queries to create two tables, if they do
    not already exist in the target database. The first table's structure
    includes columns for run_id, desc, user, datetime, pipeline_name,
    pipeline_version, and config, while the second table includes run_id and
    reserved_date.

    Parameters
    ----------
    spark
        A running spark session which will be used to execute SQL queries.
    database
        The name of the target database where tables will be created.
    tablename
        The name of the main table to be created (default is "pipeline_runlog").
        The associated _reserved_ids table will be suffixed with this name.

    Returns
    -------
    None

    Examples
    --------
    >>> spark = SparkSession.builder.appName("test_session").getOrCreate()
    >>> create_runlog_table(spark, "test_db", "test_table")
    """
    # Create the main table if it does not exist
    runlog_sql_str = f"""
        CREATE TABLE IF NOT EXISTS {database}.{tablename} (
            run_id int,
            desc string,
            user string,
            datetime timestamp,
            pipeline_name string,
            pipeline_version string,
            config string
        )
        STORED AS parquet
    """
    spark.sql(runlog_sql_str)

    # Create the _reserved_ids table if it does not exist
    reserved_ids_sql_str = f"""
        CREATE TABLE IF NOT EXISTS {database}.{tablename}_reserved_ids (
            run_id int,
            reserved_date timestamp
        )
        STORED AS parquet
    """
    spark.sql(reserved_ids_sql_str)


def reserve_id(
    spark: SparkSession,
    log_table: Optional[str] = "pipeline_runlog",
) -> int:
    """Reserve a run id in the reserved ids table linked to the runlog table.

    The function reads the last run id from the reserved ids table,
    increments it to create a new id,and writes the new id with the
    current timestamp to the reserved ids table.

    Parameters
    ----------
    spark
        A running SparkSession instance.
    log_table
        The name of the main pipeline runlog table associated with
        this reserved id table, by default "pipeline_runlog".

    Returns
    -------
    int
        The new run id.
    """
    current_time = datetime.now()  # noqa: DTZ005

    last_id = (
        spark.read.table(f"{log_table}_reserved_ids").select(F.max("run_id")).first()[0]
    )

    new_id = last_id + 1 if last_id else 1

    new_entry = [(new_id, current_time)]
    df = spark.createDataFrame(new_entry, "run_id INT, reserved_date TIMESTAMP")

    _write_entry(df, f"{log_table}_reserved_ids")

    return new_id


def _get_run_ids(
    spark: SparkSession,
    limit: int,
    pipeline: Optional[str] = None,
    log_table: str = "pipeline_runlog",
) -> List[int]:
    """Retrieve the most recent run ids.

    Parameters
    ----------
    spark
        A running Spark session.
    limit
        The number of recent run ids to retrieve.
    pipeline
        If specified, the result will be for the listed pipeline only.
    log_table
        The target runlog table. If the database is not set, this should
        include the database.

    Returns
    -------
    list
        List of the most recent run ids. Returns an empty list
        if the log table is empty.
    """
    log = spark.read.table(log_table)

    if pipeline:
        log = log.filter(log.pipeline_name == pipeline)

    result = (
        log.orderBy("datetime", ascending=False).select("run_id").limit(limit).collect()
    )

    return [row[0] for row in result]


def get_last_run_id(
    spark: SparkSession,
    pipeline: Optional[str] = None,
    log_table: str = "pipeline_runlog",
) -> Optional[int]:
    """Retrieve the last run_id, either in general or for a specific pipeline.

    Parameters
    ----------
    spark
        A running Spark session.
    pipeline
        If specified, the result will be for the listed pipeline only.
    log_table
        The target runlog table. If the database is not set, this should
        include the database.

    Returns
    -------
    int or None
        The id of the last run. Returns None if the log table is empty.
    """
    result = _get_run_ids(spark, 1, pipeline, log_table)

    if result:
        return result[0]

    return None


def get_penultimate_run_id(
    spark: SparkSession,
    pipeline: Optional[str] = None,
    log_table: str = "pipeline_runlog",
) -> Optional[int]:
    """Retrieve penultimate run_id in general or a specific pipeline.

    Parameters
    ----------
    spark
        A running Spark session.
    pipeline
        If specified, the result will be for the listed pipeline only.
    log_table
        The target runlog table. If the database is not set, this should
        include the database.

    Returns
    -------
    int or None
        The id of the penultimate run. Returns None if the log table is empty
        or has less than two entries.
    """
    result = _get_run_ids(spark, 2, pipeline, log_table)

    if len(result) > 1:
        return result[1]

    return None


def create_runlog_entry(
    spark: SparkSession,
    run_id: int,
    desc: str,
    version: str,
    config: Union[ConfigParser, Dict[str, str]],
    pipeline: Optional[str] = None,
) -> DataFrame:
    """Create an entry for the runlog.

    Parameters
    ----------
    spark
        A running spark session.
    run_id
        Entry run id.
    desc
        Description to attach to the log entry.
    version
        Version of the pipeline.
    config
        Configuration object for the run.
    pipeline
        Pipeline name. If None, derives from spark app name.

    Returns
    -------
    DataFrame
        The log entry returned as a spark dataframe.
    """
    cols = [
        "run_id",
        "desc",
        "user",
        "datetime",
        "pipeline_name",
        "pipeline_version",
        "config",
    ]
    user = os.getenv("HADOOP_USER_NAME", "unknown")

    if not pipeline:
        pipeline = spark.sparkContext.appName

    timestamp = datetime.now()  # noqa: DTZ005

    try:
        conf = json.dumps(
            config._sections if isinstance(config, ConfigParser) else config,
        )
    except Exception as err:
        msg = (
            "Problem converting config object. "
            "Either use a ConfigParser object or "
            "something that can be encoded with json.dumps"
        )
        raise ValueError(msg) from err

    entry = [(run_id, desc, user, timestamp, pipeline, version, conf)]
    return spark.createDataFrame(entry, cols)


def add_runlog_entry(
    spark: SparkSession,
    desc: str,
    version: str,
    config: Union[ConfigParser, Dict[str, str]],
    pipeline: Optional[str] = None,
    log_table: str = "pipeline_runlog",
    run_id: Optional[int] = None,
) -> DataFrame:
    """Add an entry to a target runlog.

    Parameters
    ----------
    spark
        A running spark session.
    desc
        Description to attach to the log entry.
    version
        Version of the pipeline.
    config
        Configuration object for the run.
    pipeline
        Pipeline name. If None, uses the spark application name.
    log_table
        Target runlog table. If database not set, this should
        include the database.
    run_id
        Run id to use if already reserved. If not specified,
        a new one is generated.

    Returns
    -------
    DataFrame
        The log entry returned as a spark dataframe.
    """
    if not run_id:
        run_id = reserve_id(spark, log_table)

    entry = create_runlog_entry(spark, run_id, desc, version, config, pipeline)
    _write_entry(entry, log_table)
    return entry


def _parse_runlog_as_string(
    spark: SparkSession,
    runlog_table: str,
    runlog_id: int,
) -> str:
    """Parse a single runlog entry as a string.

    Parameters
    ----------
    spark
        A running spark session.
    runlog_table
        Table containing the runlog entries.
    runlog_id
        ID of the desired entry.

    Returns
    -------
    str
        Parsed runlog entry.
    """
    query = f"SELECT * FROM {runlog_table} WHERE run_id = {runlog_id}"
    df = spark.sql(query)

    config = literal_eval(df.select("config").first()[0])

    meta = "\n".join(
        f"{col}: {df.select(col).first()[0]}" for col in df.drop("config").columns
    )
    config_str = "\n\n".join(
        f"{k.replace('_', ' ').title()}:\n\n"
        + "\n".join(f"{key}: {value}" for key, value in v.items())
        for k, v in config.items()
    )

    return f"Metadata:\n\n{meta}\n\n{config_str}\n".replace("$", "")


def write_runlog_file(
    spark: SparkSession,
    runlog_table: str,
    runlog_id: int,
    path: str,
) -> None:
    """Write metadata from runlog entry to a text file.

    Parameters
    ----------
    spark
        A running SparkSession instance.
    runlog_table
        The name of the table containing the runlog entries.
    runlog_id
        The id of the desired entry.
    path
        The HDFS path where the file will be written.

    Returns
    -------
    None
        This function doesn't return anything; it's used for its
        side effect of creating a text file.
    """
    string_to_write = _parse_runlog_as_string(spark, runlog_table, runlog_id)
    create_txt_from_string(path, string_to_write)
