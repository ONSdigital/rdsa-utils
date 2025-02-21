"""Module to parse and load JSON logs in PySpark."""

import logging
from collections import defaultdict
from datetime import timedelta
from typing import Any, Dict, List

import boto3

from rdsa_utils.cdp.helpers.s3_utils import list_files, load_json
from rdsa_utils.helpers.pyspark_log_parser.ec2_pricing import calculate_pipeline_cost

logger = logging.getLogger(__name__)


def convert_value(value: int, unit: str) -> float:
    """Convert values based on unit type using built-in libraries.

    Parameters
    ----------
    value
        Raw value from logs.
    unit
        Type of conversion: 'ms' -> minutes, 'ns' -> minutes, 'bytes' -> megabytes.

    Returns
    -------
    float
        Converted value.

    Examples
    --------
    >>> convert_value(60000, 'ms')
    1.0
    >>> convert_value(60000000000, 'ns')
    1.0
    >>> convert_value(1048576, 'bytes')
    1.0
    """
    if not isinstance(value, (int, float)):  # Ensure value is numeric
        return 0.0

    if unit == "ms":
        return timedelta(milliseconds=value).total_seconds() / 60  # Convert to minutes
    elif unit == "ns":
        return value / 6e10  # Convert nanoseconds to minutes
    elif unit == "bytes":
        return value / (1024 * 1024)  # Convert bytes to MB
    return float(value)


def parse_pyspark_logs(
    log_data: Dict[str, Any],
    log_summary: bool = True,
) -> Dict[str, Any]:
    r"""Parse PySpark event log data and return a summary of key execution metrics.

    This function processes a Python dictionary containing parsed JSON log data.
    It expects the data structure to match the event log format generated by Spark
    when `spark.eventLog.enabled = true` is set.

    The function works on `events_1_spark-xxxx` JSON log file found in the folder
    created inside `spark.eventLog.dir`, which should be called something like
    `eventlog_v2_spark-xxxx`.

    To enable Spark event logging, configure your SparkSession as follows:

    ```python
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("YourApp")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "s3a://your-bucket/path")
        .getOrCreate()
    )
    ```

    This generates a folder inside `"s3a://your-bucket/path"`, where JSON logs for Spark
    jobs are stored.

    For a detailed explanation of available Spark task metrics, refer to:
    [Spark Monitoring Documentation](https://spark.apache.org/docs/latest/monitoring.html#executor-task-metrics)

    Parameters
    ----------
    log_data
        Parsed JSON log data from Spark event logs.
    log_summary
        Whether to log the summary in a human-readable format (default is True).

    Returns
    -------
    Dict[str, Any]
        A dictionary containing aggregated summary metrics.

    Examples
    --------
    >>> log_data = [
    ...     {
    ...         "Event": "SparkListenerApplicationStart",
    ...         "Timestamp": 1739793526775,
    ...         "App Name": "ExamplePipeline"
    ...     },
    ...     {
    ...         "Event": "SparkListenerExecutorAdded",
    ...         "Executor Info": {"Total Cores": 4},
    ...     },
    ...     {
    ...         "Event": "SparkListenerStageSubmitted",
    ...         "Properties": {
    ...             "spark.executor.memory": "4g",
    ...             "spark.yarn.executor.memoryOverhead": "2g",
    ...             "spark.executor.cores": "4",
    ...         },
    ...     },
    ...     {
    ...         "Event": "SparkListenerApplicationEnd",
    ...         "Timestamp": 1739793626775,
    ...     },
    ... ]
    >>> summary = parse_pyspark_logs(log_data)
    >>> print(summary)
    >>> {
    ...     'Timestamp': 1739793526775,
    ...     'Pipeline Name': 'ExamplePipeline',
    ...     'Start Time': 1739793526775,
    ...     'End Time': 1739793626775,
    ...     'Total Time': 100000,  # 10 minutes in milliseconds
    ...     'Total Cores': 4,
    ...     'Total Memory': 6  # 4GB + 2GB
    ... }
    """
    summary_metrics = defaultdict(
        int,
        {
            "Timestamp": None,
            "Pipeline Name": None,
            "Start Time": None,
            "End Time": None,
            "Total Time": 0,
            "Total Cores": 0,
            "Total Memory": 0,
        },
    )

    for event in log_data:
        event_type = event.get("Event")

        if event_type == "SparkListenerApplicationStart":
            summary_metrics["Timestamp"] = event.get("Timestamp")
            summary_metrics["Pipeline Name"] = event.get("App Name")

        elif event_type == "SparkListenerApplicationEnd":
            summary_metrics["End Time"] = event.get("Timestamp")

        elif event_type == "SparkListenerExecutorAdded":
            summary_metrics["Total Executors"] += 1
            summary_metrics["Total Cores"] += event["Executor Info"]["Total Cores"]

        elif event_type == "SparkListenerStageSubmitted":
            props = event.get("Properties", {})
            mem, overhead = props.get("spark.executor.memory", "0g"), props.get(
                "spark.yarn.executor.memoryOverhead",
                "0g",
            )

            # Keep memory values in gigabytes
            memory_value = int(mem[:-1])  # Remove 'g' and convert to int
            overhead_value = int(overhead[:-1])  # Remove 'g' and convert to int

            summary_metrics["Memory Per Executor"] = memory_value + overhead_value
            summary_metrics["Total Memory"] = (
                summary_metrics["Memory Per Executor"]
                * summary_metrics["Total Executors"]
            )
            summary_metrics["Total Cores"] = (
                int(props.get("spark.executor.cores", 0))
                * summary_metrics["Total Executors"]
            )

    summary_metrics["Start Time"] = summary_metrics["Timestamp"]
    summary_metrics["Total Time"] = (
        summary_metrics["End Time"] - summary_metrics["Start Time"]
    )

    if log_summary:
        logger.info("Summary of Task Metrics: %s", summary_metrics)

    return dict(summary_metrics)


def find_pyspark_log_files(
    client: boto3.client,
    bucket_name: str,
    folder: str,
) -> List[str]:
    """Find all PySpark log files in the specified folder.

    Parameters
    ----------
    client
        The boto3 S3 client instance.
    bucket_name
        The name of the S3 bucket.
    folder
        The folder to search for PySpark log files.

    Returns
    -------
    List[str]
        A list of S3 object keys for the PySpark log files.

    Examples
    --------
    >>> client = boto3.client('s3')
    >>> bucket_name = 'your-bucket-name'
    >>> folder = 'user/dominic.bean'
    >>> log_files = find_pyspark_log_files(client, bucket_name, folder)
    >>> print(log_files)
    ['user/dominic.bean/eventlog_v2_spark-1234/events_1_spark-1234', ...]
    """
    prefix = f"{folder}/"
    all_files = list_files(client, bucket_name, prefix)
    log_files = [
        file
        for file in all_files
        if file.startswith(f"{folder}/eventlog_v2_spark-") and "events_1_spark" in file
    ]
    return log_files


def process_pyspark_logs(
    client: boto3.client,
    s3_bucket: str,
    user_folder: str,
) -> Dict[str, List[Any]]:
    """Find all PySpark log files in specified S3 folder & calculate pipeline costs.

    Parameters
    ----------
    client
        The boto3 S3 client instance.
    s3_bucket
        The name of the S3 bucket.
    user_folder
        The folder to search for PySpark log files.

    Returns
    -------
    Dict[str, List[Any]]
        A dictionary containing aggregated log metrics and cost metrics.

    Examples
    --------
    >>> client = boto3.client('s3')
    >>> s3_bucket = "your-bucket-name"
    >>> user_folder = 'user/dominic.bean'
    >>> result = process_pyspark_logs(client, s3_bucket, user_folder)
    >>> print(result)
    {
        'agg_log_metrics': [...],
        'agg_cost_metrics': [...]
    }
    """
    log_files = find_pyspark_log_files(client, s3_bucket, user_folder)

    agg_log_metrics = []
    agg_cost_metrics = []
    for json_object_file_path in log_files:
        log_data = load_json(
            client,
            s3_bucket,
            json_object_file_path,
            multi_line=True,
        )

        metrics_dict = parse_pyspark_logs(log_data)
        agg_log_metrics.append(metrics_dict)

        agg_cost_metrics.append(calculate_pipeline_cost(metrics_dict, fetch_data=False))

    return {"agg_log_metrics": agg_log_metrics, "agg_cost_metrics": agg_cost_metrics}
