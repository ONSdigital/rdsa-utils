"""Utility functions for interacting with Google Cloud Storage.

To initialise a client for GCS and configure it with a service account
JSON key file, you can use the following code snippet:

```python
from google.cloud import storage

# Create a GCS client
client = storage.Client.from_service_account_json('path/to/keyfile.json')
```
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound

from rdsa_utils.exceptions import InvalidBucketNameError
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
        logger.debug(f"Table {table_path} exists.")

    except NotFound:
        table_exists = False
        logger.warning(f"Table {table_path} not found.")

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

    bucket_name = config_path.split("//")[1].split("/")[0]
    blob_name = "/".join(config_path.split("//")[1].split("/")[1:])

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    contents = blob.download_as_string()

    config_file = yaml.safe_load(contents)

    logger.info(json.dumps(config_file, indent=4))
    return config_file


def remove_leading_slash(text: str) -> str:
    """Remove the leading forward slash from a string if present.

    Parameters
    ----------
    text
        The text from which the leading slash will be removed.

    Returns
    -------
    str
        The text stripped of its leading slash.

    Examples
    --------
    >>> remove_leading_slash('/example/path')
    'example/path'
    """
    return text.lstrip("/")


def validate_bucket_name(bucket_name: str) -> str:
    """Validate the format of a GCS bucket name according to GCS rules.

    Parameters
    ----------
    bucket_name
        The name of the bucket to validate.

    Returns
    -------
    str
        The validated bucket name if valid.

    Raises
    ------
    InvalidBucketNameError
        If the bucket name does not meet GCS specifications.

    Examples
    --------
    >>> validate_bucket_name('valid-bucket-name')
    'valid-bucket-name'

    >>> validate_bucket_name('Invalid_Bucket_Name')
    InvalidBucketNameError: Bucket name must not contain underscores.
    """
    # Bucket name must be between 3 and 63 characters long
    if len(bucket_name) < 3 or len(bucket_name) > 63:
        error_msg = "Bucket name must be between 3 and 63 characters long."
        raise InvalidBucketNameError(error_msg)

    # Bucket name must not contain uppercase letters
    if bucket_name != bucket_name.lower():
        error_msg = "Bucket name must not contain uppercase letters."
        raise InvalidBucketNameError(error_msg)

    # Bucket name must not contain underscores
    if "_" in bucket_name:
        error_msg = "Bucket name must not contain underscores."
        raise InvalidBucketNameError(error_msg)

    # Bucket name must start and end with a lowercase letter or number
    if not bucket_name[0].isalnum() or not bucket_name[-1].isalnum():
        error_msg = "Bucket name must start and end with a lowercase letter or number."
        raise InvalidBucketNameError(error_msg)

    # Bucket name must not contain forward slashes
    if "/" in bucket_name:
        error_msg = "Bucket name must not contain forward slashes."
        raise InvalidBucketNameError(error_msg)

    return bucket_name


def is_gcs_directory(
    client: storage.Client,
    bucket_name: str,
    object_name: str,
) -> bool:
    """Check if a GCS key is a directory by listing its contents.

    Parameters
    ----------
    client
        The GCS client instance.
    bucket_name
        The name of the GCS bucket.
    object_name
        The GCS object name to check.

    Returns
    -------
    bool
        True if the key represents a directory, False otherwise.
    """
    bucket = client.bucket(bucket_name)
    if not object_name.endswith("/"):
        object_name += "/"
    blobs = list(client.list_blobs(bucket, prefix=object_name, max_results=1))
    return len(blobs) > 0


def file_exists(
    client: storage.Client,
    bucket_name: str,
    object_name: str,
) -> bool:
    """Check if a specific file exists in a GCS bucket.

    Parameters
    ----------
    client
        The GCS client.
    bucket_name
        The name of the bucket.
    object_name
        The GCS object name to check for existence.

    Returns
    -------
    bool
        True if the file exists, otherwise False.

    Examples
    --------
    >>> client = storage.Client()
    >>> file_exists(client, 'mybucket', 'folder/file.txt')
    True
    """
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    return blob.exists()


def upload_file(
    client: storage.Client,
    bucket_name: str,
    local_path: str,
    object_name: Optional[str] = None,
    overwrite: bool = False,
) -> bool:
    """Upload a file to a GCS bucket from local directory.

    Parameters
    ----------
    client
        The GCS client instance.
    bucket_name
        The name of the target GCS bucket.
    local_path
        The file path on the local system to upload.
    object_name
        The target GCS object name. If None, uses the base name of
        the local file path.
    overwrite
        If True, the existing file on GCS will be overwritten.

    Returns
    -------
    bool
        True if the file was uploaded successfully, False otherwise.

    Examples
    --------
    >>> client = storage.Client()
    >>> upload_file(
    ...     client,
    ...     'mybucket',
    ...     '/path/to/file.txt',
    ...     'folder/gcs_file.txt'
    ... )
    True
    """
    bucket_name = validate_bucket_name(bucket_name)

    local_path = Path(local_path)
    if not local_path.exists():
        logger.error("Local file does not exist.")
        return False

    if object_name is None:
        object_name = local_path.name

    object_name = remove_leading_slash(object_name)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    if not overwrite and blob.exists():
        logger.error("File already exists in the bucket.")
        return False

    try:
        blob.upload_from_filename(str(local_path))
        logger.info(
            f"Uploaded {local_path} to {bucket_name}/{object_name}",
        )
        return True
    except Exception as e:
        logger.error(f"Failed to upload file: {str(e)}")
        return False


def download_file(
    client: storage.Client,
    bucket_name: str,
    object_name: str,
    local_path: str,
    overwrite: bool = False,
) -> bool:
    """Download a file from a GCS bucket to a local directory.

    Parameters
    ----------
    client
        The GCS client instance.
    bucket_name
        The name of the GCS bucket from which to download the file.
    object_name
        The GCS object name of the file to download.
    local_path
        The local file path where the downloaded file will be saved.
    overwrite
        If True, overwrite the local file if it exists.

    Returns
    -------
    bool
        True if the file was downloaded successfully, False otherwise.

    Examples
    --------
    >>> client = storage.Client()
    >>> download_file(
    ...     client,
    ...     'mybucket',
    ...     'folder/gcs_file.txt',
    ...     '/path/to/download.txt'
    ... )
    True
    """
    bucket_name = validate_bucket_name(bucket_name)

    local_path = Path(local_path)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    if not blob.exists():
        logger.error("File does not exist in the bucket.")
        return False

    if not overwrite and local_path.exists():
        logger.error("Local file already exists.")
        return False

    try:
        blob.download_to_filename(str(local_path))
        logger.info(
            f"Downloaded {bucket_name}/{object_name} to {local_path}",
        )
        return True
    except Exception as e:
        logger.error(f"Failed to download file: {str(e)}")
        return False


def delete_file(
    client: storage.Client,
    bucket_name: str,
    object_name: str,
) -> bool:
    """Delete a file from a GCS bucket.

    Parameters
    ----------
    client
        The GCS client instance.
    bucket_name
        The name of the bucket from which the file will be deleted.
    object_name
        The GCS object name of the file to delete.

    Returns
    -------
    bool
        True if the file was deleted successfully, otherwise False.

    Examples
    --------
    >>> client = storage.Client()
    >>> delete_file(client, 'mybucket', 'folder/gcs_file.txt')
    True
    """
    bucket_name = validate_bucket_name(bucket_name)
    object_name = remove_leading_slash(object_name)

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    if not blob.exists():
        logger.error("File does not exist in the bucket.")
        return False

    try:
        blob.delete()
        logger.info(f"Deleted {bucket_name}/{object_name}")
        return True
    except Exception as e:
        logger.error(f"Failed to delete file: {str(e)}")
        return False


def copy_file(
    client: storage.Client,
    source_bucket_name: str,
    source_object_name: str,
    destination_bucket_name: str,
    destination_object_name: str,
    overwrite: bool = False,
) -> bool:
    """Copy a file from one GCS bucket to another.

    Parameters
    ----------
    client
        The GCS client instance.
    source_bucket_name
        The name of the source bucket.
    source_object_name
        The GCS object name of the source file.
    destination_bucket_name
        The name of the destination bucket.
    destination_object_name
        The GCS object name of the destination file.
    overwrite
        If True, overwrite the destination file if it already exists.

    Returns
    -------
    bool
        True if the file was copied successfully, otherwise False.

    Examples
    --------
    >>> client = storage.Client()
    >>> copy_file(
    ...     client,
    ...     'source-bucket',
    ...     'source_file.txt',
    ...     'destination-bucket',
    ...     'destination_file.txt'
    ... )
    True
    """
    source_bucket_name = validate_bucket_name(source_bucket_name)
    destination_bucket_name = validate_bucket_name(destination_bucket_name)

    source_object_name = remove_leading_slash(source_object_name)
    destination_object_name = remove_leading_slash(destination_object_name)

    if is_gcs_directory(client, source_bucket_name, source_object_name):
        logger.error("Source object is a directory.")
        return False

    source_bucket = client.bucket(source_bucket_name)
    destination_bucket = client.bucket(destination_bucket_name)
    source_blob = source_bucket.blob(source_object_name)
    destination_blob = destination_bucket.blob(destination_object_name)

    if not overwrite and destination_blob.exists():
        logger.error(
            "Destination file already exists in the destination bucket.",
        )
        return False

    if not source_blob.exists():
        logger.error("Source file does not exist in the bucket.")
        return False

    try:
        source_bucket.copy_blob(
            source_blob,
            destination_bucket,
            destination_object_name,
        )
        logger.info(
            f"Copied {source_bucket_name}/{source_object_name} to "
            f"{destination_bucket_name}/{destination_object_name}",
        )
        return True
    except Exception as e:
        logger.error(f"Failed to copy file: {str(e)}")
        return False


def create_folder_on_gcs(
    client: storage.Client,
    bucket_name: str,
    folder_path: str,
) -> bool:
    """Create a folder in a GCS bucket if it doesn't already exist.

    Parameters
    ----------
    client
        The GCS client instance.
    bucket_name
        The name of the bucket where the folder will be created.
    folder_path
        The name of the folder to create.

    Returns
    -------
    bool
        True if the folder was created successfully or
        already exists, otherwise False.

    Examples
    --------
    >>> client = storage.Client()
    >>> create_folder_on_gcs(client, 'mybucket', 'new_folder/')
    True
    """
    bucket_name = validate_bucket_name(bucket_name)
    folder_path = remove_leading_slash(folder_path)

    if not folder_path.endswith("/"):
        folder_path += "/"

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(folder_path)

    if blob.exists():
        logger.info(f"Folder '{folder_path}' already exists on GCS.")
        return True
    else:
        try:
            blob.upload_from_string("")
            logger.info(f"Created folder '{folder_path}' on GCS.")
            return True
        except Exception as e:
            logger.error(f"Failed to create folder on GCS: {str(e)}")
            return False


def upload_folder(
    client: storage.Client,
    bucket_name: str,
    local_path: str,
    prefix: str = "",
    overwrite: bool = False,
) -> bool:
    """Upload an entire folder from the local file system to a GCS bucket.

    Parameters
    ----------
    client
        The GCS client instance.
    bucket_name
        The name of the bucket to which the folder will be uploaded.
    local_path
        The path to the local folder to upload.
    prefix
        The prefix to prepend to each object name when uploading to GCS.
    overwrite
        If True, overwrite existing files in the bucket.

    Returns
    -------
    bool
        True if the folder was uploaded successfully, otherwise False.

    Examples
    --------
    >>> client = storage.Client()
    >>> upload_folder(
    ...     client,
    ...     'mybucket',
    ...     '/path/to/local/folder',
    ...     'folder_prefix',
    ...     True
    ... )
    True
    """
    bucket_name = validate_bucket_name(bucket_name)
    local_path = Path(local_path)

    # Check if the local folder exists
    if not local_path.is_dir():
        logger.error("Local folder does not exist.")
        return False

    prefix = remove_leading_slash(prefix)

    if prefix and not prefix.endswith("/"):
        prefix += "/"

    # Iterate over files in the local folder and its subdirectories
    for file_path in local_path.rglob("*"):
        if file_path.is_file():
            # Determine the GCS object key
            object_name = f"{prefix}{file_path.relative_to(local_path)}"
            # Check if the file already exists in the bucket
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            if not overwrite and blob.exists():
                logger.error(
                    f"File '{object_name}' already exists in the bucket.",
                )
                return False
            # Upload the file to GCS
            try:
                blob.upload_from_filename(str(file_path))
                logger.info(f"Uploaded '{file_path}' to '{object_name}'.")
            except Exception as e:
                logger.error(f"Failed to upload file: {str(e)}")
                return False

    return True


def list_files(
    client: storage.Client,
    bucket_name: str,
    prefix: str = "",
) -> List[str]:
    """List files in a GCS bucket that match a specific prefix.

    Parameters
    ----------
    client
        The GCS client.
    bucket_name
        The name of the bucket.
    prefix
        The prefix to filter files, by default "".

    Returns
    -------
    List[str]
        A list of GCS object keys matching the prefix.

    Examples
    --------
    >>> client = storage.Client()
    >>> list_files(client, 'mybucket', 'folder_prefix/')
    ['folder_prefix/file1.txt', 'folder_prefix/file2.txt']
    """
    bucket_name = validate_bucket_name(bucket_name)
    prefix = remove_leading_slash(prefix)

    bucket = client.bucket(bucket_name)
    blobs = client.list_blobs(bucket, prefix=prefix)

    return [blob.name for blob in blobs]


def download_folder(
    client: storage.Client,
    bucket_name: str,
    prefix: str,
    local_path: str,
    overwrite: bool = False,
) -> bool:
    """Download a folder from a GCS bucket to a local directory.

    Parameters
    ----------
    client
        The GCS client instance.
    bucket_name
        The name of the GCS bucket from which to download the folder.
    prefix
        The GCS prefix of the folder to download.
    local_path
        The local directory path where the downloaded folder will be saved.
    overwrite
        If True, overwrite existing local files if they exist.

    Returns
    -------
    bool
        True if the folder was downloaded successfully, False otherwise.

    Examples
    --------
    >>> client = storage.Client()
    >>> download_folder(
    ...     client,
    ...     'mybucket',
    ...     'folder/subfolder/',
    ...     '/path/to/local_folder',
    ...     overwrite=False
    ... )
    True
    """
    bucket_name = validate_bucket_name(bucket_name)
    local_path = Path(local_path)

    prefix = remove_leading_slash(prefix)
    if not is_gcs_directory(client, bucket_name, prefix):
        logger.error(f"The provided GCS prefix {prefix} is not a directory.")
        return False

    if not local_path.exists():
        local_path.mkdir(parents=True)

    try:
        blobs = client.list_blobs(bucket_name, prefix=prefix)
        for blob in blobs:
            # Ensure the blob is strictly within the given directory
            if not blob.name.startswith(f"{prefix}/") and blob.name != prefix:
                continue

            target = local_path / Path(blob.name).relative_to(prefix)
            if not overwrite and target.exists():
                logger.info(f"Skipping {target} as it already exists.")
                continue
            if not target.parent.exists():
                target.parent.mkdir(parents=True)
            blob.download_to_filename(str(target))
            logger.info(f"Downloaded {blob.name} to {target}")
        return True
    except Exception as e:
        logger.error(f"Failed to download folder: {str(e)}")
        return False


def move_file(
    client: storage.Client,
    source_bucket_name: str,
    source_object_name: str,
    destination_bucket_name: str,
    destination_object_name: str,
) -> bool:
    """Move a file within or between GCS buckets.

    Parameters
    ----------
    client
        The GCS client instance.
    source_bucket_name
        The name of the source GCS bucket.
    source_object_name
        The GCS object name of the source file.
    destination_bucket_name
        The name of the destination GCS bucket.
    destination_object_name
        The GCS object name of the destination file.

    Returns
    -------
    bool
        True if the file was moved successfully, False otherwise.

    Examples
    --------
    >>> client = storage.Client()
    >>> move_file(
    ...     client,
    ...     'sourcebucket',
    ...     'source_folder/file.txt',
    ...     'destbucket',
    ...     'dest_folder/file.txt'
    ... )
    True
    """
    source_bucket_name = validate_bucket_name(source_bucket_name)
    destination_bucket_name = validate_bucket_name(destination_bucket_name)

    source_object_name = remove_leading_slash(source_object_name)
    destination_object_name = remove_leading_slash(destination_object_name)

    if is_gcs_directory(client, source_bucket_name, source_object_name):
        logger.error("Source object is a directory.")
        return False

    source_bucket = client.bucket(source_bucket_name)
    destination_bucket = client.bucket(destination_bucket_name)
    source_blob = source_bucket.blob(source_object_name)

    if not source_blob.exists():
        logger.error("Source file does not exist in the bucket.")
        return False

    try:
        source_bucket.copy_blob(
            source_blob,
            destination_bucket,
            destination_object_name,
        )
        source_blob.delete()
        logger.info(
            f"Moved {source_bucket_name}/{source_object_name} to "
            f"{destination_bucket_name}/{destination_object_name}",
        )
        return True
    except Exception as e:
        logger.error(f"Failed to move file: {str(e)}")
        return False


def delete_folder(
    client: storage.Client,
    bucket_name: str,
    folder_path: str,
) -> bool:
    """Delete a folder in a GCS bucket.

    Parameters
    ----------
    client
        The GCS client instance.
    bucket_name
        The name of the GCS bucket.
    folder_path
        The path of the folder to delete.

    Returns
    -------
    bool
        True if the folder was deleted successfully, otherwise False.

    Examples
    --------
    >>> client = storage.Client()
    >>> delete_folder(client, 'mybucket', 'path/to/folder/')
    True
    """
    bucket_name = validate_bucket_name(bucket_name)
    folder_path = remove_leading_slash(folder_path)

    if not is_gcs_directory(client, bucket_name, folder_path):
        logger.error(f"The provided path {folder_path} is not a directory.")
        return False

    try:
        blobs = client.list_blobs(bucket_name, prefix=folder_path)
        for blob in blobs:
            blob.delete()
        logger.info(f"Deleted folder {folder_path} in bucket {bucket_name}")
        return True
    except Exception as e:
        logger.error(
            f"Failed to delete folder {folder_path} "
            f"in bucket {bucket_name}: {str(e)}",
        )
        return False
