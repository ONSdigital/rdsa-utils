"""Module containing generic input functionality code."""

import json
import logging
from pathlib import Path
from typing import Union

import tomli
import yaml
from cloudpathlib import CloudPath

from rdsa_utils.typing import Config

logger = logging.getLogger(__name__)


def parse_json(data: str) -> Config:
    """Parse JSON formatted string into a dictionary.

    Parameters
    ----------
    data
        String containing standard JSON-formatted data.

    Returns
    -------
    Config
        A dictionary containing the parsed data.

    Raises
    ------
    json.decoder.JSONDecodeError
        If the string format of config_overrides cannot be decoded by
        json.loads (i.e. converted to a dictionary).
    """
    # Attempt to convert string to dictionary using json module. If this cannot
    # be done, capture error and log a useful description on what needs to be
    # changed before raising the error
    try:
        return json.loads(data)

    except json.decoder.JSONDecodeError:
        msg = """
        Cannot convert config_overrides parameter to a dictionary.

        Ensure that argument input is of form:

        '{"var1": "value1", "var2": {"var3": 1.1}, "var4": [1, 2, 3], ... }'

        where single quote is used around entire entry and double quotes are
        used for any string values within the argument.
        """
        logger.error(msg)
        raise json.decoder.JSONDecodeError(msg)  # noqa: B904


def parse_toml(data: str) -> Config:
    """Parse TOML formatted string into a dictionary.

    Parameters
    ----------
    data
        String containing standard TOML-formatted data.

    Returns
    -------
    Config
        A dictionary containing the parsed data.
    """
    return tomli.loads(data)


def parse_yaml(data: str) -> Config:
    """Parse YAML formatted string into a dictionary.

    Parameters
    ----------
    data
        String containing standard YAML-formatted data.

    Returns
    -------
    Config
        A dictionary containing the parsed data.
    """
    return yaml.safe_load(data)


def read_file(file: Union[CloudPath, Path]) -> str:
    """Load contents of specified file.

    Parameters
    ----------
    file
        The absolute file path of the file to be read.

    Returns
    -------
    str
        The contents of the provided file.

    Raises
    ------
    FileNotFoundError
        If the provided file does not exist.
    """
    if file.exists():
        return file.read_text()
    else:
        msg = f"{file=} cannot be found."
        logger.error(msg)
        raise FileNotFoundError(msg)
