"""
This module provides utility functions for working with Impala, a SQL query engine for 
data stored in a computer cluster running Apache Hadoop.

Specifically, the module focuses on metadata invalidation when working with Impala through 
the intermediate Hive tables. It contains the following functions:

- invalidate_impala_metadata

The invalidate_impala_metadata function is particularly useful during the execution of a 
data pipeline after writing to an intermediate Hive table. When using the Impala Query Editor in Hue, 
it is common for end-users and analysts to run the "INVALIDATE METADATA" command on a given table 
to ensure that the table's metadata is up-to-date. However, this step can be easily forgotten, 
leading to the use of outdated metadata and potentially incorrect results.

The invalidate_impala_metadata function automates this step by running the "INVALIDATE METADATA" 
command on the specified table, ensuring that the table's metadata is up-to-date for subsequent queries. 

This saves time for end-users and analysts by eliminating the need to manually execute 
the command in Hue's Impala Query Editor, making it less likely for them to encounter issues 
related to outdated metadata.
"""
import subprocess
from typing import Optional


def invalidate_impala_metadata(
    table: str,
    impalad_address_port: str,
    impalad_ca_cert: str,
    keep_stderr: Optional[bool] = False,
):
    """
    Invalidate the metadata of a specified table using the impala-shell command.

    This function runs the impala-shell command with the provided impalad_address_port and impalad_ca_cert, and
    invalidates the metadata of the given table.

    Parameters
    ----------
    table : str
        Name of the table whose metadata needs to be invalidated.
    impalad_address_port : str
        The address and port of the impalad instance in the format 'address:port'.
    impalad_ca_cert : str
        Path to the impalad Certificate Authority (CA) certificate file.
    keep_stderr : bool, optional, default: False
        If True, the function will print the standard error output of the impala-shell command.

    Returns
    -------
    None

    Examples
    --------
    >>> invalidate_impala_metadata('my_table', 'localhost:21050', '/path/to/ca_cert.pem')
    >>> invalidate_impala_metadata('my_table', 'localhost:21050', '/path/to/ca_cert.pem', keep_stderr=True)
    """

    result = subprocess.run(
        [
            "impala-shell",
            "-k",
            "--ssl",
            "-i",
            impalad_address_port,
            "--ca_cert",
            impalad_ca_cert,
            "-q",
            f"invalidate metadata {table};",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if keep_stderr:
        print(result.stderr.decode())
