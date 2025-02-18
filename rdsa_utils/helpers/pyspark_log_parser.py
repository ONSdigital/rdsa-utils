"""Module to parse and load JSON logs in PySpark."""

import json
import logging
from collections import defaultdict
from datetime import timedelta
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def load_json_log(file_path: str) -> List[Dict[str, Any]]:
    """Load JSON log data from a file.

    Parameters
    ----------
    file_path : str
        Path to the JSON log file.

    Returns
    -------
    List[Dict[str, Any]]
        A list of dictionaries containing the parsed JSON log data.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    json.JSONDecodeError
        If there is an error decoding the JSON data.

    Examples
    --------
    >>> log_data = load_json_log("./test_log.json")
    >>> print(log_data)
    [{'event': 'start', 'timestamp': '2025-02-18T12:00:00Z'}, ...]

    >>> try:
    ...     log_data = load_json_log("./non_existent_file.json")
    ... except FileNotFoundError as e:
    ...     print(e)
    Error: The file ./non_existent_file.json was not found.
    """
    try:
        with open(file_path, "r") as file:
            data = [json.loads(line) for line in file]
        logger.info(f"Successfully loaded JSON log data from {file_path}")
        return data
    except FileNotFoundError as e:
        logger.error(f"Error: The file {file_path} was not found.")
        raise e
    except json.JSONDecodeError as e:
        logger.error(f"Error: Failed to decode JSON from the file {file_path}.")
        raise e


def convert_value(value: int, unit: str) -> float:
    """Convert values based on unit type using built-in libraries.

    Parameters
    ----------
    value : int
        Raw value from logs.
    unit : str
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
    log_data : Dict[str, Any]
        Parsed JSON log data from Spark event logs.
    log_summary : bool, optional
        Whether to log the summary in a human-readable format (default is True).

    Returns
    -------
    Dict[str, Any]
        A dictionary containing aggregated summary metrics.

    Examples
    --------
    >>> log_data = [
    ...     {
    ...         "Event": "SparkListenerTaskEnd",
    ...         "Task Metrics": {
    ...             "Executor Run Time": 60000,
    ...             "Executor CPU Time": 60000000000,
    ...             "Peak Execution Memory": 1048576
    ...         }
    ...     },
    ...     {
    ...         "Event": "SparkListenerTaskEnd",
    ...         "Task Metrics": {
    ...             "Executor Run Time": 120000,
    ...             "Executor CPU Time": 120000000000,
    ...             "Peak Execution Memory": 2097152
    ...         }
    ...     }
    ... ]
    >>> summary = parse_pyspark_logs(log_data)
    >>> print(summary)
    >>> {
    ...     'Executor Deserialize Time': 0.0,
    ...     'Executor Deserialize CPU Time': 0.0,
    ...     'Executor Run Time': 3.0,
    ...     'Executor CPU Time': 3.0,
    ...     'Peak Execution Memory': 2.0,
    ...     'Result Size': 0.0,
    ...     'JVM GC Time': 0.0,
    ...     'Result Serialization Time': 0.0,
    ...     'Memory Bytes Spilled': 0.0,
    ...     'Disk Bytes Spilled': 0.0,
    ...     'Shuffle Bytes Written': 0.0,
    ...     'Shuffle Write Time': 0.0,
    ...     'Shuffle Records Written': 0,
    ...     'Bytes Read': 0.0,
    ...     'Records Read': 0,
    ...     'Bytes Written': 0.0,
    ...     'Records Written': 0
    ... }
    """
    # Initialise summary metrics with default values
    summary_metrics = defaultdict(int)

    # Aggregate metrics from each SparkListenerTaskEnd event
    for event in log_data:
        if event.get("Event") == "SparkListenerTaskEnd":
            task_metrics = event.get("Task Metrics", {})

            summary_metrics["Executor Deserialize Time"] += convert_value(
                task_metrics.get("Executor Deserialize Time", 0),
                "ms",
            )
            summary_metrics["Executor Deserialize CPU Time"] += convert_value(
                task_metrics.get("Executor Deserialize CPU Time", 0),
                "ns",
            )
            summary_metrics["Executor Run Time"] += convert_value(
                task_metrics.get("Executor Run Time", 0),
                "ms",
            )
            summary_metrics["Executor CPU Time"] += convert_value(
                task_metrics.get("Executor CPU Time", 0),
                "ns",
            )
            summary_metrics["Peak Execution Memory"] = max(
                summary_metrics["Peak Execution Memory"],
                convert_value(task_metrics.get("Peak Execution Memory", 0), "bytes"),
            )
            summary_metrics["Result Size"] += convert_value(
                task_metrics.get("Result Size", 0),
                "bytes",
            )
            summary_metrics["JVM GC Time"] += convert_value(
                task_metrics.get("JVM GC Time", 0),
                "ms",
            )
            summary_metrics["Result Serialization Time"] += convert_value(
                task_metrics.get("Result Serialization Time", 0),
                "ms",
            )
            summary_metrics["Memory Bytes Spilled"] += convert_value(
                task_metrics.get("Memory Bytes Spilled", 0),
                "bytes",
            )
            summary_metrics["Disk Bytes Spilled"] += convert_value(
                task_metrics.get("Disk Bytes Spilled", 0),
                "bytes",
            )

            # Shuffle Write Metrics
            shuffle_write_metrics = task_metrics.get("Shuffle Write Metrics", {})
            summary_metrics["Shuffle Bytes Written"] += convert_value(
                shuffle_write_metrics.get("Shuffle Bytes Written", 0),
                "bytes",
            )
            summary_metrics["Shuffle Write Time"] += convert_value(
                shuffle_write_metrics.get("Shuffle Write Time", 0),
                "ns",
            )
            summary_metrics["Shuffle Records Written"] += shuffle_write_metrics.get(
                "Shuffle Records Written",
                0,
            )

            # Input Metrics
            input_metrics = task_metrics.get("Input Metrics", {})
            summary_metrics["Bytes Read"] += convert_value(
                input_metrics.get("Bytes Read", 0),
                "bytes",
            )
            summary_metrics["Records Read"] += input_metrics.get("Records Read", 0)

            # Output Metrics
            output_metrics = task_metrics.get("Output Metrics", {})
            summary_metrics["Bytes Written"] += convert_value(
                output_metrics.get("Bytes Written", 0),
                "bytes",
            )
            summary_metrics["Records Written"] += output_metrics.get(
                "Records Written",
                0,
            )

    if log_summary:
        logger.info("Summary of Task Metrics: %s", summary_metrics)

    return dict(summary_metrics)
