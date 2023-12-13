"""General helper functions for GCP."""
import json
import logging
from typing import (
    Dict,
    List,
    Tuple,
)

from google.cloud import (
    bigquery,
    storage,
)
from google.cloud.exceptions import NotFound
import yaml

from rdsa_utils.typing import TablePath


logger = logging.getLogger(__name__)


def run_bq_query(query: str) -> bigquery.QueryJob:
    """Run an SQL query in BigQuery."""
    return bigquery.Client().query(query)


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
