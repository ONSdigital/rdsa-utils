"""Utilities for working with Impala."""

import logging
import subprocess
from typing import Optional

logger = logging.getLogger(__name__)


def invalidate_impala_metadata(
    table: str,
    impalad_address_port: str,
    impalad_ca_cert: str,
    keep_stderr: Optional[bool] = False,
):
    """Automate the invalidation of a table's metadata using impala-shell.

    This function uses the impala-shell command with the given
    impalad_address_port and impalad_ca_cert, to invalidate a specified
    table's metadata.

    It proves useful during a data pipeline's execution after writing to an
    intermediate Hive table. Using Impala Query Editor in Hue, end-users often
    need to run "INVALIDATE METADATA" command to refresh a table's metadata.
    However, this manual step can be missed, leading to potential use of
    outdated metadata.

    The function automates the "INVALIDATE METADATA" command for a given table,
    ensuring up-to-date metadata for future queries. This reduces manual
    intervention, making outdated metadata issues less likely to occur.

    Parameters
    ----------
    table
        Name of the table for metadata invalidation.
    impalad_address_port
        'address:port' of the impalad instance.
    impalad_ca_cert
        Path to impalad's CA certificate file.
    keep_stderr
        If True, will print impala-shell command's stderr output.

    Returns
    -------
    None

    Examples
    --------
    >>> invalidate_impala_metadata(
    ...     'my_table',
    ...     'localhost:21050',
    ...     '/path/to/ca_cert.pem'
    ... )
    >>> invalidate_impala_metadata(
    ...     'my_table',
    ...     'localhost:21050',
    ...     '/path/to/ca_cert.pem',
    ...     keep_stderr=True
    ... )
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
        logger.info(result.stderr.decode())
