"""Utility functions for interacting with AWS S3.

To initialise a boto3 client for S3 and configure it with Ranger RAZ
and SSL certificate, you can use the following code snippet:

```python
import boto3
import raz_client

ssl_file_path = "/path/to/your/ssl_certificate.crt"

# Create a boto3 client for S3
client = boto3.client("s3")

# Configure the client with RAZ and SSL certificate
raz_client.configure_ranger_raz(client, ssl_file=ssl_file_path)

Note:
- The `raz-client` library is required only when running in a
  managed Cloudera environment.
- You can install it using `pip install raz-client` when needed.
```
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

import boto3
import pandas as pd

from rdsa_utils.exceptions import InvalidBucketNameError, InvalidS3FilePathError

logger = logging.getLogger(__name__)


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
    """Validate the format of an AWS S3 bucket name according to AWS rules.

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
        If the bucket name does not meet AWS specifications.

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


def validate_s3_file_path(file_path: str, allow_s3_scheme: bool) -> str:
    """Validate the file path based on the S3 URI scheme.

    If `allow_s3_scheme` is True, the file path must contain an S3 URI scheme
    (either 's3://' or 's3a://').

    If `allow_s3_scheme` is False, the file path should not contain an S3 URI scheme.

    Parameters
    ----------
    file_path
        The file path to validate.
    allow_s3_scheme
        Whether or not to allow an S3 URI scheme in the file path.

    Returns
    -------
    str
        The validated file path if valid.

    Raises
    ------
    InvalidS3FilePathError
        If the validation fails based on the value of `allow_s3_scheme`.

    Examples
    --------
    >>> validate_s3_file_path('data_folder/data.csv', allow_s3_scheme=False)
    'data_folder/data.csv'

    >>> validate_s3_file_path('s3a://bucket-name/data.csv', allow_s3_scheme=True)
    's3a://bucket-name/data.csv'

    >>> validate_s3_file_path('s3a://bucket-name/data.csv', allow_s3_scheme=False)
    InvalidS3FilePathError: The file_path should not contain an S3 URI scheme
    like 's3://' or 's3a://'.
    """
    # Check if the file path is empty
    if not file_path:
        error_msg = "The file path cannot be empty."
        raise InvalidS3FilePathError(error_msg)

    has_s3_scheme = file_path.startswith("s3://") or file_path.startswith("s3a://")

    if allow_s3_scheme and not has_s3_scheme:
        error_msg = (
            "The file_path must contain an S3 URI scheme like 's3://' or 's3a://'."
        )
        raise InvalidS3FilePathError(error_msg)

    if not allow_s3_scheme and has_s3_scheme:
        error_msg = (
            "The file_path should not contain an S3 URI scheme "
            "like 's3://' or 's3a://'."
        )
        raise InvalidS3FilePathError(error_msg)

    return file_path


def is_s3_directory(
    client: boto3.client,
    bucket_name: str,
    object_name: str,
) -> bool:
    """Check if an AWS S3 key is a directory by listing its contents.

    Parameters
    ----------
    client
        The boto3 S3 client instance.
    bucket_name
        The name of the S3 bucket.
    object_name
        The S3 object name to check.

    Returns
    -------
    bool
        True if the key represents a directory, False otherwise.
    """
    if not object_name.endswith("/"):
        object_name += "/"
    try:
        response = client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=object_name,
            Delimiter="/",
            MaxKeys=1,
        )
        if "Contents" in response or "CommonPrefixes" in response:
            return True
        else:
            return False
    except client.exceptions.ClientError as e:
        logger.error(f"Failed to check if key is a directory: {str(e)}")
        return False


def file_exists(
    client: boto3.client,
    bucket_name: str,
    object_name: str,
) -> bool:
    """Check if a specific file exists in an AWS S3 bucket.

    Parameters
    ----------
    client
        The boto3 S3 client.
    bucket_name
        The name of the bucket.
    object_name
        The S3 object name to check for existence.

    Returns
    -------
    bool
        True if the file exists, otherwise False.

    Examples
    --------
    >>> client = boto3.client('s3')
    >>> file_exists(client, 'mybucket', 'folder/file.txt')
    True
    """
    bucket_name = validate_bucket_name(bucket_name)
    object_name = remove_leading_slash(object_name)

    try:
        client.head_object(Bucket=bucket_name, Key=object_name)
        return True
    except client.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            logger.error(f"Failed to check file existence: {str(e)}")
            return False


def upload_file(
    client: boto3.client,
    bucket_name: str,
    local_path: str,
    object_name: Optional[str] = None,
    overwrite: bool = False,
) -> bool:
    """Upload a file to an Amazon S3 bucket from local directory.

    Parameters
    ----------
    client
        The boto3 S3 client instance.
    bucket_name
        The name of the target S3 bucket.
    local_path
        The file path on the local system to upload.
    object_name
        The target S3 object name. If None, uses the base name of
        the local file path.
    overwrite
        If True, the existing file on S3 will be overwritten.

    Returns
    -------
    bool
        True if the file was uploaded successfully, False otherwise.

    Examples
    --------
    >>> client = boto3.client('s3')
    >>> upload_file(
    ...     client,
    ...     'mybucket',
    ...     '/path/to/file.txt',
    ...     'folder/s3_file.txt'
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

    if not overwrite and file_exists(client, bucket_name, object_name):
        logger.error("File already exists in the bucket.")
        return False

    try:
        client.upload_file(str(local_path), bucket_name, object_name)
        logger.info(
            f"Uploaded {local_path} to {bucket_name}/{object_name}",
        )
        return True
    except FileNotFoundError:
        logger.error("The local file was not found.")
        return False
    except client.exceptions.NoCredentialsError:
        logger.error("Credentials not available.")
        return False


def download_file(
    client: boto3.client,
    bucket_name: str,
    object_name: str,
    local_path: str,
    overwrite: bool = False,
) -> bool:
    """Download a file from an AWS S3 bucket to a local directory.

    Parameters
    ----------
    client
        The boto3 S3 client instance.
    bucket_name
        The name of the S3 bucket from which to download the file.
    object_name
        The S3 object name of the file to download.
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
    >>> client = boto3.client('s3')
    >>> download_file(
    ...     client,
    ...     'mybucket',
    ...     'folder/s3_file.txt',
    ...     '/path/to/download.txt'
    ... )
    True
    """
    bucket_name = validate_bucket_name(bucket_name)

    local_path = Path(local_path)

    if not overwrite and local_path.exists():
        logger.error("Local file already exists.")
        return False

    object_name = remove_leading_slash(object_name)

    if file_exists(client, bucket_name, object_name):
        try:
            client.download_file(
                bucket_name,
                object_name,
                str(local_path),
            )
            logger.info(
                f"Downloaded {bucket_name}/{object_name} to {local_path}",
            )
            return True
        except client.exceptions.ClientError as e:
            logger.error(f"Failed to download file: {str(e)}")
            return False
    else:
        logger.error("File does not exist in the bucket.")
        return False


def delete_file(
    client: boto3.client,
    bucket_name: str,
    object_name: str,
    overwrite: bool = False,
) -> bool:
    """Delete a file from an AWS S3 bucket.

    Parameters
    ----------
    client
        The boto3 S3 client instance.
    bucket_name
        The name of the bucket from which the file will be deleted.
    object_name
        The S3 object name of the file to delete.
    overwrite
        If False, the function will not delete the file if it does not exist;
        set to True to ignore non-existence on delete.

    Returns
    -------
    bool
        True if the file was deleted successfully, otherwise False.

    Examples
    --------
    >>> client = boto3.client('s3')
    >>> delete_file(client, 'mybucket', 'folder/s3_file.txt')
    True
    """
    bucket_name = validate_bucket_name(bucket_name)
    object_name = remove_leading_slash(object_name)

    if not overwrite and not file_exists(client, bucket_name, object_name):
        logger.error("File does not exist in the bucket.")
        return False

    try:
        client.delete_object(Bucket=bucket_name, Key=object_name)
        logger.info(f"Deleted {bucket_name}/{object_name}")
        return True
    except client.exceptions.ClientError as e:
        logger.error(f"Failed to delete file: {str(e)}")
        return False


def copy_file(
    client: boto3.client,
    source_bucket_name: str,
    source_object_name: str,
    destination_bucket_name: str,
    destination_object_name: str,
    overwrite: bool = False,
) -> bool:
    """Copy a file from one aWS S3 bucket to another.

    Parameters
    ----------
    client
        The boto3 S3 client instance.
    source_bucket_name
        The name of the source bucket.
    source_object_name
        The S3 object name of the source file.
    destination_bucket_name
        The name of the destination bucket.
    destination_object_name
        The S3 object name of the destination file.
    overwrite
        If True, overwrite the destination file if it already exists.

    Returns
    -------
    bool
        True if the file was copied successfully, otherwise False.

    Examples
    --------
    >>> client = boto3.client('s3')
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

    if not overwrite and file_exists(
        client,
        destination_bucket_name,
        destination_object_name,
    ):
        logger.error(
            "Destination file already exists in the destination bucket.",
        )
        return False

    copy_source = {"Bucket": source_bucket_name, "Key": source_object_name}
    try:
        client.copy_object(
            CopySource=copy_source,
            Bucket=destination_bucket_name,
            Key=destination_object_name,
        )
        logger.info(
            f"Copied {source_bucket_name}/{source_object_name} to "
            f"{destination_bucket_name}/{destination_object_name}",
        )
        return True
    except client.exceptions.ClientError as e:
        logger.error(f"Failed to copy file: {str(e)}")
        return False


def create_folder_on_s3(
    client: boto3.client,
    bucket_name: str,
    folder_path: str,
) -> bool:
    """Create a folder in an AWS S3 bucket if it doesn't already exist.

    Parameters
    ----------
    client
        The boto3 S3 client instance.
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
    >>> client = boto3.client('s3')
    >>> create_folder_on_s3(client, 'mybucket', 'new_folder/')
    True
    """
    bucket_name = validate_bucket_name(bucket_name)
    folder_path = remove_leading_slash(folder_path)

    if not folder_path.endswith("/"):
        folder_path += "/"

    try:
        client.head_object(Bucket=bucket_name, Key=folder_path)
        logger.info(f"Folder '{folder_path}' already exists on S3.")
        return True
    except client.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            # Folder does not exist, create it
            try:
                client.put_object(Bucket=bucket_name, Key=folder_path)
                logger.info(f"Created folder '{folder_path}' on S3.")
                return True
            except client.exceptions.ClientError as e:
                logger.error(f"Failed to create folder on S3: {str(e)}")
                return False
        else:
            logger.error(f"Failed to check folder existence on S3: {str(e)}")
            return False


def upload_folder(
    client: boto3.client,
    bucket_name: str,
    local_path: str,
    prefix: str = "",
    overwrite: bool = False,
) -> bool:
    """Upload an entire folder from the local file system to an AWS S3 bucket.

    Parameters
    ----------
    client
        The boto3 S3 client instance.
    bucket_name
        The name of the bucket to which the folder will be uploaded.
    local_path
        The path to the local folder to upload.
    prefix
        The prefix to prepend to each object name when uploading to S3.
    overwrite
        If True, overwrite existing files in the bucket.

    Returns
    -------
    bool
        True if the folder was uploaded successfully, otherwise False.

    Examples
    --------
    >>> client = boto3.client('s3')
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

    # Ensure the folder exists on S3
    if not create_folder_on_s3(client, bucket_name, prefix):
        logger.error("Failed to create folder on S3.")
        return False

    # Iterate over files in the local folder and its subdirectories
    for file_path in local_path.rglob("*"):
        if file_path.is_file():
            # Determine the S3 object key
            object_name = prefix + "/" + str(file_path.relative_to(local_path))
            # Check if the file already exists in the bucket
            if not overwrite and file_exists(
                client,
                bucket_name,
                object_name,
            ):
                logger.error(
                    f"File '{object_name}' already exists in the bucket.",
                )
                return False
            # Upload the file to S3
            try:
                client.upload_file(str(file_path), bucket_name, object_name)
                logger.info(f"Uploaded '{file_path}' to '{object_name}'.")
            except FileNotFoundError:
                logger.error(f"The local file '{file_path}' was not found.")
                return False
            except client.exceptions.NoCredentialsError:
                logger.error("Credentials not available.")
                return False

    return True


def list_files(
    client: boto3.client,
    bucket_name: str,
    prefix: str = "",
) -> List[str]:
    """List files in an AWS S3 bucket that match a specific prefix.

    Parameters
    ----------
    client
        The boto3 S3 client.
    bucket_name
        The name of the bucket.
    prefix
        The prefix to filter files, by default "".

    Returns
    -------
    List[str]
        A list of S3 object keys matching the prefix.

    Examples
    --------
    >>> client = boto3.client('s3')
    >>> list_files(client, 'mybucket', 'folder_prefix/')
    ['folder_prefix/file1.txt', 'folder_prefix/file2.txt']
    """
    bucket_name = validate_bucket_name(bucket_name)
    prefix = remove_leading_slash(prefix)

    try:
        response = client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        files = []
        if "Contents" in response:
            for obj in response["Contents"]:
                files.append(obj["Key"])
        return files
    except client.exceptions.ClientError as e:
        logger.error(f"Failed to list files in bucket: {str(e)}")
        return []


def download_folder(
    client: boto3.client,
    bucket_name: str,
    prefix: str,
    local_path: str,
    overwrite: bool = False,
) -> bool:
    """Download a folder from an AWS S3 bucket to a local directory.

    Parameters
    ----------
    client
        The boto3 S3 client instance.
    bucket_name
        The name of the S3 bucket from which to download the folder.
    prefix
        The S3 prefix of the folder to download.
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
    >>> client = boto3.client('s3')
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
    if not is_s3_directory(client, bucket_name, prefix):
        logger.error(f"The provided S3 prefix {prefix} is not a directory.")
        return False

    if not local_path.exists():
        local_path.mkdir(parents=True)

    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                if is_s3_directory(client, bucket_name, obj["Key"]):
                    continue
                target = local_path / Path(obj["Key"]).relative_to(prefix)
                if not overwrite and target.exists():
                    logger.info(f"Skipping {target} as it already exists.")
                    continue
                if not target.parent.exists():
                    target.parent.mkdir(parents=True)
                client.download_file(bucket_name, obj["Key"], str(target))
                logger.info(f'Downloaded {obj["Key"]} to {target}')
        return True
    except client.exceptions.ClientError as e:
        logger.error(f"Failed to download folder: {str(e)}")
        return False


def move_file(
    client: boto3.client,
    source_bucket_name: str,
    source_object_name: str,
    destination_bucket_name: str,
    destination_object_name: str,
) -> bool:
    """Move a file within or between AWS S3 buckets.

    Parameters
    ----------
    client
        The boto3 S3 client instance.
    source_bucket_name
        The name of the source S3 bucket.
    source_object_name
        The S3 object name of the source file.
    destination_bucket_name
        The name of the destination S3 bucket.
    destination_object_name
        The S3 object name of the destination file.

    Returns
    -------
    bool
        True if the file was moved successfully, False otherwise.

    Examples
    --------
    >>> client = boto3.client('s3')
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

    if file_exists(client, source_bucket_name, source_object_name):
        try:
            copy_source = {
                "Bucket": source_bucket_name,
                "Key": source_object_name,
            }
            client.copy(
                copy_source,
                destination_bucket_name,
                destination_object_name,
            )
            client.delete_object(
                Bucket=source_bucket_name,
                Key=source_object_name,
            )
            logger.info(
                f"Moved {source_bucket_name}/{source_object_name} to "
                f"{destination_bucket_name}/{destination_object_name}",
            )
            return True
        except client.exceptions.ClientError as e:
            logger.error(f"Failed to move file: {str(e)}")
            return False
    else:
        logger.error("Source file does not exist.")
        return False


def delete_folder(
    client: boto3.client,
    bucket_name: str,
    folder_path: str,
) -> bool:
    """Delete a folder in an AWS S3 bucket.

    Parameters
    ----------
    client
        The boto3 S3 client instance.
    bucket_name
        The name of the S3 bucket.
    folder_path
        The path of the folder to delete.

    Returns
    -------
    bool
        True if the folder was deleted successfully, otherwise False.

    Examples
    --------
    >>> client = boto3.client('s3')
    >>> delete_folder(client, 'mybucket', 'path/to/folder/')
    True
    """
    bucket_name = validate_bucket_name(bucket_name)
    folder_path = remove_leading_slash(folder_path)

    if not is_s3_directory(client, bucket_name, folder_path):
        logger.error(f"The provided path {folder_path} is not a directory.")
        return False

    paginator = client.get_paginator("list_objects_v2")
    try:
        for page in paginator.paginate(Bucket=bucket_name, Prefix=folder_path):
            if "Contents" in page:
                for obj in page["Contents"]:
                    client.delete_object(Bucket=bucket_name, Key=obj["Key"])
        logger.info(f"Deleted folder {folder_path} in bucket {bucket_name}")
        return True
    except client.exceptions.ClientError as e:
        logger.error(
            f"Failed to delete folder {folder_path} "
            f"in bucket {bucket_name}: {str(e)}",
        )
        return False


def load_csv(
    client: boto3.client,
    bucket_name: str,
    filepath: str,
    keep_columns: Optional[List[str]] = None,
    rename_columns: Optional[Dict[str, str]] = None,
    drop_columns: Optional[List[str]] = None,
    **kwargs,
) -> pd.DataFrame:
    """Load a CSV file from an S3 bucket into a Pandas DataFrame.

    Parameters
    ----------
    client
        The boto3 S3 client instance.
    bucket_name
        The name of the S3 bucket.
    filepath
        The key (full path and filename) of the CSV file in the S3 bucket.
    keep_columns
        A list of column names to keep in the DataFrame, dropping all others.
        Default value is None.
    rename_columns
        A dictionary to rename columns where keys are existing column
        names and values are new column names.
        Default value is None.
    drop_columns
        A list of column names to drop from the DataFrame.
        Default value is None.
    kwargs
        Additional keyword arguments to pass to the `pd.read_csv` method.

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame containing the data from the CSV file.

    Raises
    ------
    InvalidBucketNameError
        If the bucket name does not meet AWS specifications.
    InvalidS3FilePathError
        If the file_path contains an S3 URI scheme like 's3://' or 's3a://'.
    Exception
        If there is an error loading the file.
    ValueError
        If a column specified in rename_columns, drop_columns, or
        keep_columns is not found in the DataFrame.

    Notes
    -----
    Transformation order:
    1. Columns are kept according to `keep_columns`.
    2. Columns are dropped according to `drop_columns`.
    3. Columns are renamed according to `rename_columns`.

    Examples
    --------
    Load a CSV file and rename columns:

    >>> df = load_csv(
            client,
            "my-bucket",
            "path/to/file.csv",
            rename_columns={"old_name": "new_name"}
        )

    Load a CSV file and keep only specific columns:

    >>> df = load_csv(
            client,
            "my-bucket",
            "path/to/file.csv",
            keep_columns=["col1", "col2"]
        )

    Load a CSV file and drop specific columns:

    >>> df = load_csv(
            client,
            "my-bucket",
            "path/to/file.csv",
            drop_columns=["col1", "col2"]
        )

    Load a CSV file with custom delimiter:

    >>> df = load_csv(
            client,
            "my-bucket",
            "path/to/file.csv",
            sep=";"
        )
    """
    bucket_name = validate_bucket_name(bucket_name)
    filepath = validate_s3_file_path(filepath, allow_s3_scheme=False)

    try:
        # Get the CSV file from S3
        response = client.get_object(Bucket=bucket_name, Key=filepath)
        logger.info(
            f"Loaded CSV file from S3 bucket {bucket_name}, filepath {filepath}",
        )

        # Read the CSV file into a Pandas DataFrame
        df = pd.read_csv(response["Body"], **kwargs)

    except Exception as e:
        error_message = (
            f"Error loading file from bucket {bucket_name}, filepath {filepath}: {e}"
        )
        logger.error(error_message)
        raise Exception(error_message) from e

    columns = df.columns.tolist()

    # Apply column transformations: keep, drop, rename
    if keep_columns:
        missing_columns = [col for col in keep_columns if col not in columns]
        if missing_columns:
            error_message = (
                f"Columns {missing_columns} not found in DataFrame and cannot be kept"
            )
            logger.error(error_message)
            raise ValueError(error_message)
        df = df[keep_columns]

    if drop_columns:
        for col in drop_columns:
            if col in columns:
                df = df.drop(columns=[col])
            else:
                error_message = (
                    f"Column '{col}' not found in DataFrame and cannot be dropped"
                )
                logger.error(error_message)
                raise ValueError(error_message)

    if rename_columns:
        for old_name, new_name in rename_columns.items():
            if old_name in columns:
                df = df.rename(columns={old_name: new_name})
            else:
                error_message = (
                    f"Column '{old_name}' not found in DataFrame and "
                    f"cannot be renamed to '{new_name}'"
                )
                logger.error(error_message)
                raise ValueError(error_message)

    return df


def load_json(
    client: boto3.client,
    bucket_name: str,
    filepath: str,
    encoding: Optional[str] = "utf-8",
) -> Dict:
    """Load a JSON file from an S3 bucket.

    Parameters
    ----------
    client : boto3.client
        The boto3 S3 client instance.
    bucket_name : str
        The name of the S3 bucket.
    filepath : str
        The key (full path and filename) of the JSON file in the S3 bucket.
    encoding : str, optional
        The encoding of the JSON file. Default is 'utf-8'.

    Returns
    -------
    Dict
        Dictionary containing the data from the JSON file.

    Raises
    ------
    InvalidBucketNameError
        If the bucket name is invalid according to AWS rules.
    Exception
        If there is an error loading the file from S3 or parsing the JSON.

    Examples
    --------
    >>> client = boto3.client('s3')
    >>> data = load_json(client, 'my-bucket', 'path/to/file.json')
    >>> print(data)
    {
        "name": "John",
        "age": 30,
        "city": "Manchester"
    }
    """
    # Validate bucket name and clean the filepath
    bucket_name = validate_bucket_name(bucket_name)
    filepath = remove_leading_slash(filepath)

    try:
        # Get the JSON file from S3
        response = client.get_object(Bucket=bucket_name, Key=filepath)
        logger.info(
            f"Loaded JSON file from S3 bucket {bucket_name}, filepath {filepath}",
        )

        # Read the JSON content
        json_data = response["Body"].read().decode(encoding)
        data = json.loads(json_data)

    except Exception as e:
        error_message = (
            f"Error loading or parsing JSON file from bucket {bucket_name}, "
            "filepath {filepath}: {e}"
        )
        logger.error(error_message)
        raise Exception(error_message) from e

    return data
