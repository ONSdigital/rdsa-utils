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
import zipfile
from datetime import datetime, timedelta, timezone
from io import BytesIO, StringIO, TextIOWrapper
from pathlib import Path
from typing import Dict, List, Optional, Union

import boto3
import pandas as pd
from dateutil.relativedelta import relativedelta

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


def s3_walk(
    client: boto3.client,
    bucket_name: str,
    prefix: str,
) -> Dict:
    """Traverse an S3 bucket and return its structure in a dictionary format.

    Mimics the functionality of os.walk in s3 bucket using long filenames with slashes.
    Recursively goes through the long filenames and splits it into subdirectories, and
    "files" - short file names.

    Parameters
    ----------
    client
        The boto3 S3 client.
    bucket_name
        The name of the bucket.
    prefix
        The prefix of the object to start the walk from.

    Returns
    -------
    Dict
        A dictionary representing the bucket structure where:
        - Keys are directory paths ending with '/'
        - Values are tuples of (set(subdirectories), set(files)) where:
          - subdirectories: a set of directory names ending with '/'
          - files: a set of file paths

    Examples
    --------
    >>> client = boto3.client('s3')
    >>> # For a bucket with files: file5.txt, folder1/file1.txt, folder1/file2.txt,
    >>> # folder1/subfolder1/file3.txt, and folder2/file4.txt
    >>> s3_walk(client, 'test-bucket', '')
    {
        '': ({"folder1/", "folder2/"}, {"file5.txt"}),
        'folder1/': (set(), {"folder1/"}),
        'folder2/': (set(), {"folder2/"})
    }

    >>> # When using a specific prefix
    >>> s3_walk(client, 'test-bucket', 'folder1/')
    {
        'folder1/': ({"subfolder1/"}, {"folder1/file1.txt", "folder1/file2.txt"}),
        'folder1/subfolder1/': (set(), {"folder1/subfolder1/"})
    }

    >>> # Empty bucket or nonexistent prefix
    >>> s3_walk(client, 'test-bucket', 'nonexistent/')
    {}
    """

    def process_location(root, prefix_local, location):
        # Add new root location if not available
        if prefix_local not in root:
            root[prefix_local] = (set(), set())
        # Check how many folders are available after prefix
        remainder = location[len(prefix_local) :]
        structure = remainder.split("/")

        # If we are not yet in the folder of the file we need to continue with
        # a larger prefix
        if len(structure) > 1:
            # Add folder dir
            root[prefix_local][0].add(structure[0] + "/")
            # Make sure file is added along the way
            process_location(
                root,
                prefix_local + structure[0] + "/",
                location,
            )
        else:
            # Add to file
            root[prefix_local][1].add(location)

    root = {}
    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=bucket_name,
            Prefix=prefix,
            Delimiter="/",
        ):
            if "CommonPrefixes" in page:
                for common_prefix in page["CommonPrefixes"]:
                    process_location(root, prefix, common_prefix["Prefix"])
            if "Contents" in page:
                for content in page["Contents"]:
                    if content["Key"] != prefix:
                        process_location(root, prefix, content["Key"])
    except client.exceptions.ClientError as e:
        logger.error(f"Failed to list directories: {str(e)}")

    return root


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


def file_size(
    client: boto3.client,
    bucket_name: str,
    object_name: str,
) -> int:
    """Check the size of a file in an AWS S3 bucket.

    Parameters
    ----------
    client
        The boto3 S3 client.
    bucket_name
        The name of the bucket.
    object_name
        The S3 object name to check for size.

    Returns
    -------
    int
        An integer value indicating the size of the file in bytes.

    Examples
    --------
    >>> client = boto3.client('s3')
    >>> file_size(client, 'mybucket', 'folder/file.txt')
    8
    """
    response = client.head_object(Bucket=bucket_name, Key=object_name)
    file_size = response["ContentLength"]

    return file_size


def md5_sum(
    client: boto3.client,
    bucket_name: str,
    object_name: str,
) -> str:
    """Get md5 hash of a specific object on s3.

    Parameters
    ----------
    client
        The boto3 S3 client.
    bucket_name
        The name of the bucket.
    object_name
        The S3 object name to create md5 hash from.

    Returns
    -------
    str
        A string value with the MD5 hash of the object data.

    Examples
    --------
    >>> client = boto3.client('s3')
    >>> md5_sum(client, 'mybucket', 'folder/file.txt')
    "d41d8cd98f00b204e9800998ecf8427e"
    """
    try:
        md5result = client.head_object(Bucket=bucket_name, Key=object_name)["ETag"][
            1:-1
        ]
    except client.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            # This is specifically for it to raise a ClientError exception
            # when a file is not found.
            raise client.exceptions.ClientError(
                {
                    "Error": {
                        "Code": "404",
                        "Message": f"The file {object_name} not in {bucket_name}.",
                    },
                },
                operation_name="HeadObject",
            ) from e
        else:
            logger.error(
                f"Failed to get md5 from file: {str(e)}",
            )

        md5result = None

    return md5result


def check_file(
    client: boto3.client,
    bucket_name: str,
    object_name: str,
) -> bool:
    """Check if a file exists in an S3 bucket and meets specific criteria.

    Verifies that the given path corresponds to a file in an S3 bucket,
    ensuring it exists, is not a directory, and has a size greater than 0.

    Parameters
    ----------
    client
        The boto3 S3 client.
    bucket_name
        The name of the bucket.
    object_name
        The path to a file in s3 bucket.

    Returns
    -------
    bool
        True if the file exists, is not a directory, and size > 0,
        otherwise False.

    Examples
    --------
    >>> client = boto3.client('s3')
    >>> check_file(client, 'mybucket', 'folder/file.txt')
    True

    >>> check_file(client, 'mybucket', 'folder/nonexistent_file.txt')
    False

    >>> check_file(client, 'mybucket', 'folder/')
    False
    """
    if object_name is None:
        response = False

    if file_exists(client, bucket_name, object_name):
        isdir = is_s3_directory(client, bucket_name, object_name)
        size = file_size(client, bucket_name, object_name)
        response = (not isdir) and (size > 0)
    else:
        response = False
    return response


def read_header(
    client: boto3.client,
    bucket_name: str,
    object_name: str,
) -> str:
    """Read the first line of a file on s3.

    Gets the entire file using boto3 get_objects, converts its body into
    an input stream, reads the first line and remove the carriage return
    character (backslash-n) from the end.

    Parameters
    ----------
    client
        The boto3 S3 client.
    bucket_name
        The name of the bucket.
    object_name
        The S3 object name to read header from.

    Returns
    -------
    str
        Returns the first line of the file.

    Examples
    --------
    >>> client = boto3.client('s3')
    >>> read_header(client, 'mybucket', 'folder/file.txt')
    "First line"
    """
    # Create an input/output stream pointer, same as open
    stream = TextIOWrapper(
        client.get_object(
            Bucket=bucket_name,
            Key=object_name,
        )["Body"],
    )

    # Read the first line from the stream
    response = stream.readline()

    # Remove the last character (carriage return, or new line)
    response = response.rstrip("\n\r")

    return response


def write_string_to_file(
    client: boto3.client,
    bucket_name: str,
    object_name: str,
    object_content: bytes,
) -> None:
    """Write a string into the specified object in the s3 bucket.

    Parameters
    ----------
    client
        The boto3 S3 client.
    bucket_name
        The name of the bucket.
    object_name
        The S3 object name to write into.
    object_content
        The content (str) to be written to "object_name".

    Returns
    -------
    None
        The outcome of this operation is the string written
        into the object in the s3 bucket. It will overwrite
        anything in the object.

    Examples
    --------
    >>> client = boto3.client('s3')
    >>> write_string_to_file(client, 'mybucket', 'folder/file.txt', b'example content')
    """
    # Put context to a new Input-Output buffer
    str_buffer = StringIO(object_content.decode("utf-8"))

    # "Rewind" the stream to the start of the buffer
    str_buffer.seek(0)

    # Write the buffer into the s3 bucket
    client.put_object(
        Bucket=bucket_name,
        Body=str_buffer.getvalue(),
        Key=object_name,
    )

    return None


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


def create_folder(
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
    >>> create_folder(client, 'mybucket', 'new_folder/')
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
    if not create_folder(client, bucket_name, prefix):
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
        files = []
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
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
    multi_line: bool = False,
) -> Union[Dict, List[Dict]]:
    """Load a JSON file from an S3 bucket, with optional line-by-line parsing.

    Parameters
    ----------
    client
        The boto3 S3 client instance.
    bucket_name
        The name of the S3 bucket.
    filepath
        The key (full path and filename) of the JSON file in the S3 bucket.
    encoding
        The encoding of the JSON file.
    multi_line
        If True, reads the JSON file line by line, treating each line as a
        separate JSON object.

    Returns
    -------
    Union[Dict, List[Dict]]
        - If `multi_line=False`: A dictionary containing the parsed JSON data.
        - If `multi_line=True`: A list of dictionaries, each corresponding to
          a JSON object per line.

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

    >>> log_data = load_json(client, 'my-bucket', 'path/to/log.json', multi_line=True)
    >>> print(log_data)
    [{'event': 'start', 'timestamp': '2025-02-18T12:00:00Z'}, ...]
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

        # Read and parse JSON data
        json_data = response["Body"].read().decode(encoding)

        if multi_line:
            data = [json.loads(line) for line in json_data.strip().split("\n")]
        else:
            data = json.loads(json_data)

    except Exception as e:
        error_message = (
            f"Error loading or parsing JSON file from bucket {bucket_name}, "
            f"filepath {filepath}: {str(e)}"
        )
        logger.error(error_message)
        raise Exception(error_message) from e

    return data


def write_csv(
    client: boto3.client,
    bucket_name: str,
    data: pd.DataFrame,
    filepath: str,
    **kwargs,
) -> bool:
    """Write a Pandas Dataframe to csv in an S3 bucket.

    Uses StringIO library as a RAM buffer, so at first Pandas writes data to the
    buffer, then the buffer returns to the beginning, and then it is sent to
    the S3 bucket using the boto3.put_object method.

    Parameters
    ----------
    client
        The boto3 S3 client instance.
    bucket_name
        The name of the S3 bucket.
    data
        The dataframe to write to the specified path.
    filepath
        The filepath to save the dataframe to.
    kwargs
        Optional dictionary of Pandas to_csv arguments.

    Returns
    -------
    bool
        True if the dataframe is written successfully.
        False if it was not possible to serialise or write the file.

    Raises
    ------
    Exception
        If there is an error writing the file to S3.

    Examples
    --------
    >>> client = boto3.client('s3')
    >>> data = pd.DataFrame({
    >>>     'column1': [1, 2, 3],
    >>>     'column2': ['a', 'b', 'c']
    >>> })
    >>> write_csv(client, 'my_bucket', data, 'path/to/file.csv')
    True
    """
    try:
        # Create an Input-Output buffer
        csv_buffer = StringIO()

        # Write the dataframe to the buffer in the CSV format
        data.to_csv(csv_buffer, **kwargs)

        # "Rewind" the stream to the start of the buffer
        csv_buffer.seek(0)

        # Write the buffer into the s3 bucket. Assign the output to a mute
        # variable, so the output is not displayed in the console or log.
        _ = client.put_object(
            Bucket=bucket_name,
            Body=csv_buffer.getvalue(),
            Key=filepath,
        )
        logger.info(f"Successfully wrote dataframe to {bucket_name}/{filepath}")
        return True

    except Exception as e:
        error_message = (
            f"Error writing to csv or saving to bucket {bucket_name}, "
            f"filepath {filepath}: {e}"
        )
        logger.error(error_message)
        return False


def write_excel(
    client: boto3.client,
    bucket_name: str,
    data: pd.DataFrame,
    filepath: str,
    **kwargs,
) -> bool:
    """Write a Pandas DataFrame to an Excel file in an S3 bucket.

    Uses BytesIO as a RAM buffer. Pandas writes data to the buffer,
    the buffer rewinds to the beginning, and then it is sent to S3
    using the boto3.put_object method.

    Parameters
    ----------
    client : boto3.client
        The boto3 S3 client instance.
    bucket_name : str
        The name of the S3 bucket.
    data : pd.DataFrame
        The dataframe to write to the specified path.
    filepath : str
        The filepath to save the dataframe to in the S3 bucket.
    kwargs : dict
        Optional dictionary of Pandas `to_excel` arguments.

    Returns
    -------
    bool
        True if the dataframe is written successfully, False otherwise.

    Raises
    ------
    Exception
        If there is an error writing the file to S3.

    Examples
    --------
    >>> client = boto3.client('s3')
    >>> data = pd.DataFrame({
    >>>     'column1': [1, 2, 3],
    >>>     'column2': ['a', 'b', 'c']
    >>> })
    >>> write_excel(client, 'my_bucket', data, 'path/to/file.xlsx')
    True
    """
    try:
        # Create an in-memory bytes buffer
        excel_buffer = BytesIO()

        # Write DataFrame to the buffer in Excel format
        with pd.ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:
            data.to_excel(writer, index=False, **kwargs)

        # Ensure the buffer is at the beginning
        excel_buffer.seek(0)

        # Upload the buffer to S3
        client.put_object(
            Bucket=bucket_name,
            Body=excel_buffer.getvalue(),
            Key=filepath,
        )

        logger.info(f"Successfully wrote dataframe to {bucket_name}/{filepath}")
        return True

    except Exception as e:
        logger.error(
            f"Error writing to Excel or saving to bucket {bucket_name}, "
            f"filepath {filepath}: {e}",
        )
        return False


def delete_old_objects_and_folders(
    client: boto3.client,
    bucket_name: str,
    prefix: str,
    age: str,
    dry_run: bool = False,
) -> bool:
    """Delete objects and folders in an S3 bucket that are older than a specified age.

    Parameters
    ----------
    client
        The boto3 S3 client instance.
    bucket_name
        The name of the S3 bucket.
    prefix
        The prefix to filter objects.
    age
        The age threshold for deleting objects. Supported formats:
        - "1 day", "2 days", etc.
        - "1 week", "2 weeks", etc.
        - "1 month", "2 months", etc.
    dry_run
        If True, the function will only log the objects and folders
        that would be deleted, without actually performing the deletion.
        Default is False.

    Returns
    -------
    bool
        True if the objects and folders were (or would be)
        deleted successfully, otherwise False.

    Examples
    --------
    >>> client = boto3.client('s3')
    >>> # This will actually delete objects:
    >>> delete_old_objects_and_folders(client, 'mybucket', 'folder/', '1 week')
    True
    >>> # This will only log the objects/folders to be deleted:
    >>> delete_old_objects_and_folders(
    ...     client,
    ...     'mybucket',
    ...     'folder/',
    ...     '1 week',
    ...     dry_run=True
    ... )
    True
    """
    if not prefix:
        logger.error(
            "Prefix must be specified to avoid accidental deletion of all objects.",
        )
        return False
    bucket_name = validate_bucket_name(bucket_name)
    prefix = remove_leading_slash(prefix)

    # Parse the age parameter
    try:
        number, unit = age.split()
        number = int(number)
    except ValueError:
        logger.error("Invalid age format. Use formats like '1 day', '2 weeks', etc.")
        return False

    if unit in ["day", "days"]:
        delta = timedelta(days=number)
    elif unit in ["week", "weeks"]:
        delta = timedelta(weeks=number)
    elif unit in ["month", "months"]:
        delta = relativedelta(months=number)
    else:
        logger.error("Unsupported time unit. Use 'day', 'week', or 'month'.")
        return False

    cutoff_date = datetime.now(timezone.utc) - delta

    logger.info(
        f"Deleting objects with prefix '{prefix}' older "
        f"than '{age}' (cutoff date: {cutoff_date})",
    )

    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    last_modified = obj["LastModified"]
                    if last_modified < cutoff_date:
                        if dry_run:
                            logger.info(
                                f"Dry-run: would delete {obj['Key']} last "
                                f"modified on {last_modified}",
                            )
                        else:
                            delete_file(client, bucket_name, obj["Key"], overwrite=True)
                            logger.info(
                                f"Deleted {obj['Key']} last modified "
                                f"on {last_modified}",
                            )
            if "CommonPrefixes" in page:
                for common_prefix in page["CommonPrefixes"]:
                    folder_prefix = common_prefix["Prefix"]
                    folder_response = client.list_objects_v2(
                        Bucket=bucket_name,
                        Prefix=folder_prefix,
                        MaxKeys=1,
                    )
                    if "Contents" in folder_response:
                        folder_last_modified = folder_response["Contents"][0][
                            "LastModified"
                        ]
                        if folder_last_modified < cutoff_date:
                            if dry_run:
                                logger.info(
                                    f"Dry-run: would delete folder {folder_prefix}"
                                    " last modified on {folder_last_modified}",
                                )
                            else:
                                delete_folder(client, bucket_name, folder_prefix)
                                logger.info(
                                    f"Deleted folder {folder_prefix} last modified"
                                    " on {folder_last_modified}",
                                )
        return True
    except client.exceptions.ClientError as e:
        logger.error(f"Failed to delete old objects and folders: {str(e)}")
        return False


def zip_local_directory_to_s3(
    client: boto3.client,
    local_directory_path: Union[str, Path],
    bucket_name: str,
    object_name: str,
    overwrite: bool = False,
) -> bool:
    """Zips a local directory and uploads it to AWS S3.

    Parameters
    ----------
    client
        The boto3 S3 client instance.
    local_directory_path
        Path to the local directory to be zipped.
    bucket_name
        Name of the S3 bucket.
    object_name
        S3 key (path) where the zip file will be saved.
    overwrite
        If False, will not upload if the file already exists in S3.
        Defaults to False.

    Returns
    -------
    bool
        True if upload was successful, False otherwise.

    Examples
    --------
    >>> import boto3
    >>> from pathlib import Path
    >>> client = boto3.client('s3')
    >>> # Basic usage
    >>> zip_local_directory_to_s3(
    ...     client,
    ...     '/path/to/local/dir',
    ...     'my-bucket',
    ...     'backups/mydir.zip'
    ... )
    True
    >>> # With overwrite parameter
    >>> zip_local_directory_to_s3(
    ...     client,
    ...     Path('/path/to/local/dir'),
    ...     'my-bucket',
    ...     'backups/mydir.zip',
    ...     overwrite=True
    ... )
    True
    """
    try:
        # Convert to Path object if it's a string
        directory_path = Path(local_directory_path)

        # Validate input
        if not directory_path.is_dir():
            logger.error(f"Directory not found: {directory_path}")
            return False

        # Check if file exists in S3 and we're not overwriting
        if not overwrite:
            try:
                client.head_object(Bucket=bucket_name, Key=object_name)
                logger.info(
                    f"File already exists at "
                    f"s3://{bucket_name}/{object_name} "
                    "and overwrite is False. Skipping upload.",
                )
                # Clean up and return without uploading
                return True
            except client.exceptions.ClientError as e:
                # If the error code is 404 (not found), we can proceed with the upload
                if e.response["Error"]["Code"] != "404":
                    raise e

        # Create a temporary zip file in memory
        zip_buffer = BytesIO()

        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            # Recursively find all files in the directory
            for file_path in directory_path.rglob("*"):
                # Include only files (directories are automatically handled by zipfile)
                if file_path.is_file():
                    # Calculate the arcname (path within the zip file)
                    arcname = file_path.relative_to(directory_path)

                    # Add the file to the zip
                    zip_file.write(file_path, arcname=str(arcname))

        # Reset buffer position to the beginning
        zip_buffer.seek(0)

        # Upload the zip file to S3
        client.upload_fileobj(zip_buffer, bucket_name, object_name)

        logger.info(
            f"Successfully uploaded zipped directory to "
            f"s3://{bucket_name}/{object_name}",
        )
        return True

    except Exception as e:
        logger.error(f"Error zipping local directory and uploading to S3: {e}")
        return False


def zip_s3_directory_to_s3(
    client: boto3.client,
    source_bucket_name: str,
    source_prefix: str,
    destination_bucket_name: str,
    destination_object_name: str,
    overwrite: bool = False,
) -> bool:
    """Zips a directory that exists in S3 and saves it to another location in S3.

    Parameters
    ----------
    client
        Initialised boto3 S3 client.
    source_bucket_name
        Name of the source S3 bucket.
    source_prefix
        Prefix (directory path) in the source bucket to zip.
    destination_bucket_name
        Name of the destination S3 bucket.
    destination_object_name
        S3 key (path) where the zip file will be saved.
    overwrite
        If False, will not upload if the file already exists.
        Defaults to False.

    Returns
    -------
    bool
        True if operation was successful, False otherwise

    Examples
    --------
    >>> import boto3
    >>> s3 = boto3.client('s3')
    >>> # Basic usage
    >>> zip_s3_directory_to_s3(
    ...     s3,
    ...     'source-bucket',
    ...     'data/logs/',
    ...     'dest-bucket',
    ...     'archives/logs.zip'
    ... )
    True
    >>> # With overwrite parameter
    >>> zip_s3_directory_to_s3(
    ...     s3,
    ...     'source-bucket',
    ...     'data/logs/',
    ...     'dest-bucket',
    ...     'archives/logs.zip',
    ...     overwrite=True
    ... )
    True
    """
    try:
        # Check if destination file exists and we're not overwriting
        if not overwrite:
            try:
                client.head_object(
                    Bucket=destination_bucket_name,
                    Key=destination_object_name,
                )
                logger.info(
                    f"File already exists at "
                    f"s3://{destination_bucket_name}/{destination_object_name} "
                    "and overwrite is False. Skipping operation.",
                )
                return True
            except client.exceptions.ClientError as e:
                # If the error code is 404 (not found), we can proceed with the upload
                if e.response["Error"]["Code"] != "404":
                    raise e

        # Ensure source_prefix ends with a slash if not empty
        if source_prefix and not source_prefix.endswith("/"):
            source_prefix = f"{source_prefix}/"

        # List all objects in the source directory
        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=source_bucket_name, Prefix=source_prefix)

        # Create a buffer for the zip file
        zip_buffer = BytesIO()

        file_count = 0
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            # Iterate through all objects in the source directory
            for page in pages:
                if "Contents" not in page:
                    continue

                for obj in page["Contents"]:
                    # Get the object key
                    key = obj["Key"]

                    # Skip if it's a directory (ends with '/')
                    if key.endswith("/"):
                        continue

                    # Calculate the arcname (path within the zip file)
                    arcname = key[len(source_prefix) :].lstrip("/")

                    if not arcname:  # Skip if arcname is empty
                        continue

                    try:
                        # Download the object to memory
                        response = client.get_object(Bucket=source_bucket_name, Key=key)
                        content = response["Body"].read()

                        # Add the content to the zip file
                        zip_file.writestr(arcname, content)
                        file_count += 1
                    except Exception as e:
                        logger.warning(f"Failed to process file {key}: {e}")
                        continue

        if file_count == 0:
            logger.warning(
                f"No files found to zip in s3://{source_bucket_name}/{source_prefix}",
            )
            return False

        # Reset buffer position to the beginning
        zip_buffer.seek(0)

        # Upload the zip file to the destination location
        client.upload_fileobj(
            zip_buffer,
            destination_bucket_name,
            destination_object_name,
        )

        logger.info(
            f"Successfully zipped {file_count} files "
            f"from s3://{source_bucket_name}/{source_prefix} "
            f"to s3://{destination_bucket_name}/{destination_object_name}",
        )
        return True

    except Exception as e:
        logger.error(f"Error zipping S3 directory and uploading to S3: {e}")
        return False
