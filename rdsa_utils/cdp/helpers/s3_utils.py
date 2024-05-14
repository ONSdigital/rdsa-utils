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

import logging
from pathlib import Path
from typing import List, Optional

import boto3

from rdsa_utils.exceptions import InvalidBucketNameError

logger = logging.getLogger(__name__)


def remove_leading_slash(text: str) -> str:
    """Remove the leading forward slash from a string if present.

    Parameters
    ----------
    text : str
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
    return text.lstrip('/')


def validate_bucket_name(bucket_name: str) -> str:
    """Validate the format of an AWS S3 bucket name according to AWS rules.

    Parameters
    ----------
    bucket_name : str
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
        error_msg = 'Bucket name must be between 3 and 63 characters long.'
        raise InvalidBucketNameError(error_msg)

    # Bucket name must not contain uppercase letters
    if bucket_name != bucket_name.lower():
        error_msg = 'Bucket name must not contain uppercase letters.'
        raise InvalidBucketNameError(error_msg)

    # Bucket name must not contain underscores
    if '_' in bucket_name:
        error_msg = 'Bucket name must not contain underscores.'
        raise InvalidBucketNameError(error_msg)

    # Bucket name must start and end with a lowercase letter or number
    if not bucket_name[0].isalnum() or not bucket_name[-1].isalnum():
        error_msg = (
            'Bucket name must start and end with a lowercase letter or number.'
        )
        raise InvalidBucketNameError(error_msg)

    # Bucket name must not contain forward slashes
    if '/' in bucket_name:
        error_msg = 'Bucket name must not contain forward slashes.'
        raise InvalidBucketNameError(error_msg)

    return bucket_name


def is_s3_directory(client: boto3.client, bucket_name: str, key: str) -> bool:
    """Check if an AWS S3 key is a directory by listing its contents.

    Parameters
    ----------
    client : boto3.client
        The boto3 S3 client instance.
    bucket_name : str
        The name of the S3 bucket.
    key : str
        The S3 object key to check.

    Returns
    -------
    bool
        True if the key represents a directory, False otherwise.
    """
    if not key.endswith('/'):
        key += '/'
    try:
        response = client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=key,
            Delimiter='/',
            MaxKeys=1,
        )
        if 'Contents' in response or 'CommonPrefixes' in response:
            return True
        else:
            return False
    except client.exceptions.ClientError as e:
        logger.error(f'Failed to check if key is a directory: {str(e)}')
        return False


def file_exists(
    client: boto3.client,
    bucket_name: str,
    object_name: str,
) -> bool:
    """Check if a specific file exists in an AWS S3 bucket.

    Parameters
    ----------
    client : boto3.client
        The boto3 S3 client.
    bucket_name : str
        The name of the bucket.
    object_name : str
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
        if e.response['Error']['Code'] == '404':
            return False
        else:
            logger.error(f'Failed to check file existence: {str(e)}')
            return False


def upload_file(
    client: boto3.client,
    bucket_name: str,
    local_file_path: str,
    s3_object_name: Optional[str] = None,
    overwrite: bool = False,
) -> bool:
    """Upload a file to an Amazon S3 bucket from local directory.

    Parameters
    ----------
    client : boto3.client
        The boto3 S3 client instance.
    bucket_name : str
        The name of the target S3 bucket.
    local_file_path : str
        The file path on the local system to upload.
    s3_object_name : Optional[str]
        The target S3 object name. If None, uses the base name of
        the local file path.
    overwrite : bool, optional
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

    local_file_path = Path(local_file_path)
    if not local_file_path.exists():
        logger.error('Local file does not exist.')
        return False

    if s3_object_name is None:
        s3_object_name = local_file_path.name

    s3_object_name = remove_leading_slash(s3_object_name)

    if not overwrite and file_exists(client, bucket_name, s3_object_name):
        logger.error('File already exists in the bucket.')
        return False

    try:
        client.upload_file(str(local_file_path), bucket_name, s3_object_name)
        logger.info(
            f'Uploaded {local_file_path} to {bucket_name}/{s3_object_name}',
        )
        return True
    except FileNotFoundError:
        logger.error('The local file was not found.')
        return False
    except client.exceptions.NoCredentialsError:
        logger.error('Credentials not available.')
        return False


def download_file(
    client: boto3.client,
    bucket_name: str,
    s3_object_name: str,
    local_file_path: str,
    overwrite: bool = False,
) -> bool:
    """Download a file from an AWS S3 bucket to a local directory.

    Parameters
    ----------
    client : boto3.client
        The boto3 S3 client instance.
    bucket_name : str
        The name of the S3 bucket from which to download the file.
    s3_object_name : str
        The S3 object name of the file to download.
    local_file_path : str
        The local file path where the downloaded file will be saved.
    overwrite : bool, optional
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

    local_file_path = Path(local_file_path)

    if not overwrite and local_file_path.exists():
        logger.error('Local file already exists.')
        return False

    s3_object_name = remove_leading_slash(s3_object_name)

    if file_exists(client, bucket_name, s3_object_name):
        try:
            client.download_file(
                bucket_name,
                s3_object_name,
                str(local_file_path),
            )
            logger.info(
                f'Downloaded {bucket_name}/{s3_object_name} '
                f'to {local_file_path}',
            )
            return True
        except client.exceptions.ClientError as e:
            logger.error(f'Failed to download file: {str(e)}')
            return False
    else:
        logger.error('File does not exist in the bucket.')
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
    client : boto3.client
        The boto3 S3 client instance.
    bucket_name : str
        The name of the bucket from which the file will be deleted.
    object_name : str
        The S3 object name of the file to delete.
    overwrite : bool, optional
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
        logger.error('File does not exist in the bucket.')
        return False

    try:
        client.delete_object(Bucket=bucket_name, Key=object_name)
        logger.info(f'Deleted {bucket_name}/{object_name}')
        return True
    except client.exceptions.ClientError as e:
        logger.error(f'Failed to delete file: {str(e)}')
        return False


def copy_file(
    client: boto3.client,
    source_bucket: str,
    source_key: str,
    dest_bucket: str,
    dest_key: str,
    overwrite: bool = False,
) -> bool:
    """Copy a file from one aWS S3 bucket to another.

    Parameters
    ----------
    client : boto3.client
        The boto3 S3 client instance.
    source_bucket : str
        The name of the source bucket.
    source_key : str
        The S3 object name of the source file.
    dest_bucket : str
        The name of the destination bucket.
    dest_key : str
        The S3 object name of the destination file.
    overwrite : bool, optional
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
    ...     'dest_file.txt'
    ... )
    True
    """
    source_bucket = validate_bucket_name(source_bucket)
    dest_bucket = validate_bucket_name(dest_bucket)

    source_key = remove_leading_slash(source_key)
    dest_key = remove_leading_slash(dest_key)

    if not overwrite and file_exists(client, dest_bucket, dest_key):
        logger.error(
            'Destination file already exists in the destination bucket.',
        )
        return False

    copy_source = {'Bucket': source_bucket, 'Key': source_key}
    try:
        client.copy_object(
            CopySource=copy_source,
            Bucket=dest_bucket,
            Key=dest_key,
        )
        logger.info(
            f'Copied {source_bucket}/{source_key} to {dest_bucket}/{dest_key}',
        )
        return True
    except client.exceptions.ClientError as e:
        logger.error(f'Failed to copy file: {str(e)}')
        return False


def create_folder_on_s3(
    client: boto3.client,
    bucket_name: str,
    folder_name: str,
) -> bool:
    """Create a folder in an AWS S3 bucket if it doesn't already exist.

    Parameters
    ----------
    client : boto3.client
        The boto3 S3 client instance.
    bucket_name : str
        The name of the bucket where the folder will be created.
    folder_name : str
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
    folder_name = remove_leading_slash(folder_name)

    if not folder_name.endswith('/'):
        folder_name += '/'

    try:
        client.head_object(Bucket=bucket_name, Key=folder_name)
        logger.info(f"Folder '{folder_name}' already exists on S3.")
        return True
    except client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            # Folder does not exist, create it
            try:
                client.put_object(Bucket=bucket_name, Key=folder_name)
                logger.info(f"Created folder '{folder_name}' on S3.")
                return True
            except client.exceptions.ClientError as e:
                logger.error(f'Failed to create folder on S3: {str(e)}')
                return False
        else:
            logger.error(f'Failed to check folder existence on S3: {str(e)}')
            return False


def upload_folder(
    client: boto3.client,
    bucket_name: str,
    local_folder_path: str,
    s3_prefix: str = '',
    overwrite: bool = False,
) -> bool:
    """Upload an entire folder from the local file system to an AWS S3 bucket.

    Parameters
    ----------
    client : boto3.client
        The boto3 S3 client instance.
    bucket_name : str
        The name of the bucket to which the folder will be uploaded.
    local_folder_path : str
        The path to the local folder to upload.
    s3_prefix : str, optional
        The prefix to prepend to each object name when uploading to S3.
    overwrite : bool, optional
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
    local_folder_path = Path(local_folder_path)

    # Check if the local folder exists
    if not local_folder_path.is_dir():
        logger.error('Local folder does not exist.')
        return False

    s3_prefix = remove_leading_slash(s3_prefix)

    # Ensure the folder exists on S3
    if not create_folder_on_s3(client, bucket_name, s3_prefix):
        logger.error('Failed to create folder on S3.')
        return False

    # Iterate over files in the local folder and its subdirectories
    for file_path in local_folder_path.rglob('*'):
        if file_path.is_file():
            # Determine the S3 object key
            s3_object_key = (
                s3_prefix + '/' + str(file_path.relative_to(local_folder_path))
            )
            # Check if the file already exists in the bucket
            if not overwrite and file_exists(
                client,
                bucket_name,
                s3_object_key,
            ):
                logger.error(
                    f"File '{s3_object_key}' already exists in the bucket.",
                )
                return False
            # Upload the file to S3
            try:
                client.upload_file(str(file_path), bucket_name, s3_object_key)
                logger.info(f"Uploaded '{file_path}' to '{s3_object_key}'.")
            except FileNotFoundError:
                logger.error(f"The local file '{file_path}' was not found.")
                return False
            except client.exceptions.NoCredentialsError:
                logger.error('Credentials not available.')
                return False

    return True


def list_files(
    client: boto3.client,
    bucket_name: str,
    prefix: str = '',
) -> List[str]:
    """List files in an AWS S3 bucket that match a specific prefix.

    Parameters
    ----------
    client : boto3.client
        The boto3 S3 client.
    bucket_name : str
        The name of the bucket.
    prefix : str, optional
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
        if 'Contents' in response:
            for obj in response['Contents']:
                files.append(obj['Key'])
        return files
    except client.exceptions.ClientError as e:
        logger.error(f'Failed to list files in bucket: {str(e)}')
        return []


def download_folder(
    client: boto3.client,
    bucket_name: str,
    s3_prefix: str,
    local_path: str,
    overwrite: bool = False,
) -> bool:
    """Download a folder from an AWS S3 bucket to a local directory.

    Parameters
    ----------
    client : boto3.client
        The boto3 S3 client instance.
    bucket_name : str
        The name of the S3 bucket from which to download the folder.
    s3_prefix : str
        The S3 prefix of the folder to download.
    local_path : str
        The local directory path where the downloaded folder will be saved.
    overwrite : bool, optional
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

    s3_prefix = remove_leading_slash(s3_prefix)
    if not is_s3_directory(client, bucket_name, s3_prefix):
        logger.error(f'The provided S3 prefix {s3_prefix} is not a directory.')
        return False

    if not local_path.exists():
        local_path.mkdir(parents=True)

    try:
        paginator = client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix):
            for obj in page.get('Contents', []):
                if is_s3_directory(client, bucket_name, obj['Key']):
                    continue
                target = local_path / Path(obj['Key']).relative_to(s3_prefix)
                if not overwrite and target.exists():
                    logger.info(f'Skipping {target} as it already exists.')
                    continue
                if not target.parent.exists():
                    target.parent.mkdir(parents=True)
                client.download_file(bucket_name, obj['Key'], str(target))
                logger.info(f'Downloaded {obj["Key"]} to {target}')
        return True
    except client.exceptions.ClientError as e:
        logger.error(f'Failed to download folder: {str(e)}')
        return False


def move_file(
    client: boto3.client,
    src_bucket: str,
    src_key: str,
    dest_bucket: str,
    dest_key: str,
) -> bool:
    """Move a file within or between AWS S3 buckets.

    Parameters
    ----------
    client : boto3.client
        The boto3 S3 client instance.
    src_bucket : str
        The name of the source S3 bucket.
    src_key : str
        The S3 object key of the source file.
    dest_bucket : str
        The name of the destination S3 bucket.
    dest_key : str
        The S3 object key of the destination file.

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
    src_bucket = validate_bucket_name(src_bucket)
    dest_bucket = validate_bucket_name(dest_bucket)

    src_key = remove_leading_slash(src_key)
    dest_key = remove_leading_slash(dest_key)

    if file_exists(client, src_bucket, src_key):
        try:
            copy_source = {'Bucket': src_bucket, 'Key': src_key}
            client.copy(copy_source, dest_bucket, dest_key)
            client.delete_object(Bucket=src_bucket, Key=src_key)
            logger.info(
                f'Moved {src_bucket}/{src_key} to {dest_bucket}/{dest_key}',
            )
            return True
        except client.exceptions.ClientError as e:
            logger.error(f'Failed to move file: {str(e)}')
            return False
    else:
        logger.error('Source file does not exist.')
        return False
