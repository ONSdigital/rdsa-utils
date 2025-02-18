"""Module to parse and load JSON logs in PySpark."""

import json
import logging
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
