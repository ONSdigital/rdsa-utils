"""Tests for s3_utils.py module."""

import json
import zipfile
from datetime import datetime, timedelta, timezone
from io import BytesIO

import boto3
import pandas as pd
import pytest
from freezegun import freeze_time
from moto import mock_aws

from rdsa_utils.cdp.helpers.s3_utils import (
    check_file,
    copy_file,
    create_folder,
    delete_file,
    delete_folder,
    delete_old_objects_and_folders,
    download_file,
    download_folder,
    file_exists,
    file_size,
    is_s3_directory,
    list_files,
    load_csv,
    load_json,
    md5_sum,
    move_file,
    read_header,
    remove_leading_slash,
    s3_walk,
    upload_file,
    upload_folder,
    validate_bucket_name,
    validate_s3_file_path,
    write_csv,
    write_excel,
    write_string_to_file,
    zip_local_directory_to_s3,
    zip_s3_directory_to_s3,
)
from rdsa_utils.exceptions import InvalidBucketNameError, InvalidS3FilePathError


class TestRemoveLeadingSlash:
    """Tests for remove_leading_slash function."""

    def test_remove_leading_slash_no_slash(self):
        """Test remove_leading_slash with a string without leading slash."""
        assert remove_leading_slash("example/path") == "example/path"

    def test_remove_leading_slash_with_slash(self):
        """Test remove_leading_slash with a string with leading slash."""
        assert remove_leading_slash("/example/path") == "example/path"


class TestValidateBucketName:
    """Test for validate_bucket_name function."""

    def test_validate_bucket_name_valid(self):
        """Test validate_bucket_name with a valid bucket name."""
        assert validate_bucket_name("valid-bucket-name") == "valid-bucket-name"

    def test_validate_bucket_name_invalid_underscore(self):
        """Test validate_bucket_name with an invalid
        bucket name containing underscore.
        """
        with pytest.raises(
            InvalidBucketNameError,
            match="Bucket name must not contain underscores.",
        ):
            validate_bucket_name("invalid_bucket_name")

    def test_validate_bucket_name_too_short(self):
        """Test validate_bucket_name with a bucket name that is too short."""
        with pytest.raises(
            InvalidBucketNameError,
            match="Bucket name must be between 3 and 63 characters long.",
        ):
            validate_bucket_name("ab")

    def test_validate_bucket_name_too_long(self):
        """Test validate_bucket_name with a bucket name that is too long."""
        with pytest.raises(
            InvalidBucketNameError,
            match="Bucket name must be between 3 and 63 characters long.",
        ):
            validate_bucket_name("a" * 64)

    def test_validate_bucket_name_contains_uppercase(self):
        """Test validate_bucket_name with a bucket name
        containing uppercase letters.
        """
        with pytest.raises(
            InvalidBucketNameError,
            match="Bucket name must not contain uppercase letters.",
        ):
            validate_bucket_name("InvalidBucketName")

    def test_validate_bucket_name_starts_with_non_alnum(self):
        """Test validate_bucket_name with a bucket name starting
        with non-alphanumeric character.
        """
        with pytest.raises(
            InvalidBucketNameError,
            match="Bucket name must start and end with a lowercase letter or number.",
        ):
            validate_bucket_name("-invalidname")

    def test_validate_bucket_name_ends_with_non_alnum(self):
        """Test validate_bucket_name with a bucket name ending
        with non-alphanumeric character.
        """
        with pytest.raises(
            InvalidBucketNameError,
            match="Bucket name must start and end with a lowercase letter or number.",
        ):
            validate_bucket_name("invalidname-")

    def test_validate_bucket_name_contains_slash(self):
        """Test validate_bucket_name with a bucket name
        containing forward slash.
        """
        with pytest.raises(
            InvalidBucketNameError,
            match="Bucket name must not contain forward slashes.",
        ):
            validate_bucket_name("invalid/name")


class TestValidateS3FilePath:
    """Tests for validate_s3_file_path function."""

    def test_valid_non_s3_path(self):
        """Test that non-S3 paths are valid when S3 schemes are not allowed."""
        assert (
            validate_s3_file_path("data_folder/data.csv", allow_s3_scheme=False)
            == "data_folder/data.csv"
        )

    def test_invalid_s3_path_when_not_allowed(self):
        """Test that S3 paths raise an error when S3 schemes are not allowed."""
        with pytest.raises(
            InvalidS3FilePathError,
            match="should not contain an S3 URI scheme",
        ):
            validate_s3_file_path("s3a://bucket-name/data.csv", allow_s3_scheme=False)

    def test_valid_s3_path_when_allowed(self):
        """Test that S3 paths are valid when S3 schemes are allowed."""
        assert (
            validate_s3_file_path("s3a://bucket-name/data.csv", allow_s3_scheme=True)
            == "s3a://bucket-name/data.csv"
        )

    def test_invalid_non_s3_path_when_s3_required(self):
        """Test that non-S3 paths raise an error when S3 schemes are required."""
        with pytest.raises(
            InvalidS3FilePathError,
            match="must contain an S3 URI scheme",
        ):
            validate_s3_file_path("data_folder/data.csv", allow_s3_scheme=True)

    def test_valid_s3_path_with_s3_scheme(self):
        """Test that paths with 's3://' scheme are valid when S3 schemes are allowed."""
        assert (
            validate_s3_file_path("s3://bucket-name/data.csv", allow_s3_scheme=True)
            == "s3://bucket-name/data.csv"
        )

    def test_valid_path_without_bucket_name(self):
        """Test that paths without bucket names are valid when no S3 scheme is present."""
        assert (
            validate_s3_file_path("my_folder/data.csv", allow_s3_scheme=False)
            == "my_folder/data.csv"
        )

    def test_invalid_empty_path(self):
        """Test that an empty path raises an error."""
        with pytest.raises(
            InvalidS3FilePathError,
            match="The file path cannot be empty.",
        ):
            validate_s3_file_path("", allow_s3_scheme=False)

    def test_valid_s3_path_with_longer_structure(self):
        """Test that a longer S3 path structure is valid when S3 scheme is allowed."""
        assert (
            validate_s3_file_path(
                "s3a://bucket-name/folder/subfolder/data.csv",
                allow_s3_scheme=True,
            )
            == "s3a://bucket-name/folder/subfolder/data.csv"
        )

    def test_valid_path_without_s3_scheme_with_dots_in_name(self):
        """Test that a path containing dots but without S3 scheme is valid."""
        assert (
            validate_s3_file_path("my.bucket/folder/data.csv", allow_s3_scheme=False)
            == "my.bucket/folder/data.csv"
        )

    def test_invalid_non_s3_path_with_invalid_characters(self):
        """Test that non-S3 paths with invalid characters still pass if no scheme is present."""
        assert (
            validate_s3_file_path("invalid!@#$%^&*/data.csv", allow_s3_scheme=False)
            == "invalid!@#$%^&*/data.csv"
        )


@pytest.fixture
def _aws_credentials():
    """Mock AWS Credentials for moto."""
    boto3.setup_default_session(
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        aws_session_token="testing",
    )


@pytest.fixture
def s3_client(_aws_credentials):
    """Provide a mocked AWS S3 client for testing
    using moto with temporary credentials.
    """
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="test-bucket")
        yield client


class TestFileExists:
    """Tests for file_exists function."""

    def test_file_exists_true(self, s3_client):
        """Test file_exists returns True when the file is in the bucket."""
        s3_client.put_object(
            Bucket="test-bucket",
            Key="test-file.txt",
            Body=b"content",
        )
        assert file_exists(s3_client, "test-bucket", "test-file.txt") is True

    def test_file_exists_false(self, s3_client):
        """Test file_exists returns False when the file is not in the bucket."""
        assert file_exists(s3_client, "test-bucket", "nonexistent.txt") is False


@pytest.fixture
def setup_files(tmp_path):
    """
    Set up local files for upload and download tests.

    Creates a temporary file with content 'Hello, world!'
    for testing upload and download functionality.

    Returns the path of the created local file, which is
    provided to the upload_file and download_file functions.
    """
    local_file = tmp_path / "test_file.txt"
    local_file.write_text("Hello, world!")
    return local_file


class TestUploadFile:
    """Tests for upload_file function."""

    def test_upload_success(self, s3_client, setup_files):
        """Test file is uploaded successfully."""
        assert (
            upload_file(
                s3_client,
                "test-bucket",
                str(setup_files),
                "uploaded.txt",
            )
            is True
        )

    def test_upload_failure_file_not_found(self, s3_client, setup_files):
        """Test upload fails if the local file does not exist."""
        assert (
            upload_file(
                s3_client,
                "test-bucket",
                str(setup_files.parent / "nonexistent.txt"),
            )
            is False
        )

    def test_upload_no_overwrite(self, s3_client, setup_files):
        """Test no overwrite existing file without permission."""
        upload_file(s3_client, "test-bucket", str(setup_files), "uploaded.txt")
        assert (
            upload_file(
                s3_client,
                "test-bucket",
                str(setup_files),
                "uploaded.txt",
            )
            is False
        )


class TestDownloadFile:
    """Tests for download_file function."""

    def test_download_success(self, s3_client, setup_files):
        """Test file is downloaded successfully."""
        s3_client.upload_file(
            str(setup_files),
            "test-bucket",
            "file_to_download.txt",
        )
        download_path = setup_files.parent / "downloaded.txt"
        assert (
            download_file(
                s3_client,
                "test-bucket",
                "file_to_download.txt",
                str(download_path),
            )
            is True
        )

    def test_download_file_not_found(self, s3_client, setup_files):
        """Test download fails if the S3 file does not exist."""
        download_path = setup_files.parent / "downloaded.txt"
        assert (
            download_file(
                s3_client,
                "test-bucket",
                "nonexistent.txt",
                str(download_path),
            )
            is False
        )

    def test_download_no_overwrite_local_file(self, s3_client, setup_files):
        """Test no overwrite existing local file without permission."""
        download_path = setup_files  # Use the same path as the setup file
        s3_client.upload_file(
            str(setup_files),
            "test-bucket",
            "file_to_download.txt",
        )
        # First download
        download_file(
            s3_client,
            "test-bucket",
            "file_to_download.txt",
            str(download_path),
        )
        # Attempt to download again without overwrite permission
        assert (
            download_file(
                s3_client,
                "test-bucket",
                "file_to_download.txt",
                str(download_path),
            )
            is False
        )


@pytest.fixture
def setup_folder(tmp_path):
    """
    Set up local folder and files for upload tests.

    Creates a temporary folder with files and subfolders
    for testing folder upload functionality.

    Returns the path of the created local folder, which
    is provided to the create_folder and upload_folder function.
    """
    folder_path = tmp_path / "test_folder"
    folder_path.mkdir()
    (folder_path / "file1.txt").write_text("Content of file 1")
    (folder_path / "file2.txt").write_text("Content of file 2")
    subfolder = folder_path / "subfolder"
    subfolder.mkdir()
    (subfolder / "file3.txt").write_text("Content of subfolder file")
    return folder_path


class TestCreateFolderOnS3:
    """Tests for create_folder function."""

    def test_create_new_folder(self, s3_client):
        """Test creation of a new folder on S3."""
        assert create_folder(s3_client, "test-bucket", "new_folder/") is True

    def test_folder_already_exists(self, s3_client):
        """Test handling when the folder already exists on S3."""
        s3_client.put_object(Bucket="test-bucket", Key="existing_folder/")
        assert create_folder(s3_client, "test-bucket", "existing_folder/") is True


class TestUploadFolder:
    """Tests for upload_folder function."""

    def test_upload_folder_success(self, s3_client, setup_folder):
        """Test successful upload of a folder to S3."""
        assert (
            upload_folder(
                s3_client,
                "test-bucket",
                str(setup_folder),
                "test_prefix",
                overwrite=True,
            )
            is True
        )

    def test_upload_folder_nonexistent_local(self, s3_client):
        """Test handling when the local folder does not exist."""
        assert (
            upload_folder(
                s3_client,
                "test-bucket",
                "nonexistent_folder",
                "test_prefix",
            )
            is False
        )

    def test_upload_folder_no_overwrite_existing_files(
        self,
        s3_client,
        setup_folder,
    ):
        """Test not overwriting existing files without permission."""
        s3_client.put_object(
            Bucket="test-bucket",
            Key="test_prefix/file1.txt",
            Body=b"Old content",
        )
        assert (
            upload_folder(
                s3_client,
                "test-bucket",
                str(setup_folder),
                "test_prefix",
            )
            is False
        )


class TestS3Walk:
    """Tests for s3_walk function."""

    class TestS3Walk:
        """Tests for s3_walk function."""

        def setup_s3_structure(self, s3_client):
            """Set up a folder structure in S3 for testing."""
            s3_client.put_object(
                Bucket="test-bucket",
                Key="folder1/file1.txt",
                Body=b"content1",
            )
            s3_client.put_object(
                Bucket="test-bucket",
                Key="folder1/file2.txt",
                Body=b"content2",
            )
            s3_client.put_object(
                Bucket="test-bucket",
                Key="folder1/subfolder1/file3.txt",
                Body=b"content3",
            )
            s3_client.put_object(
                Bucket="test-bucket",
                Key="folder2/file4.txt",
                Body=b"content4",
            )
            s3_client.put_object(
                Bucket="test-bucket",
                Key="file5.txt",
                Body=b"content5",
            )

        def test_s3_walk_basic(self, s3_client):
            """Test basic functionality of s3_walk."""
            self.setup_s3_structure(s3_client)
            result = s3_walk(s3_client, "test-bucket", "")
            expected = {
                "": ({"folder2/", "folder1/"}, {"file5.txt"}),
                "folder1/": (set(), {"folder1/"}),
                "folder2/": (set(), {"folder2/"}),
            }
            assert result == expected

        def test_s3_walk_with_prefix(self, s3_client):
            """Test s3_walk with a specific prefix."""
            self.setup_s3_structure(s3_client)
            result = s3_walk(s3_client, "test-bucket", "folder1/")
            expected = {
                "folder1/": (
                    {"subfolder1/"},
                    {"folder1/file1.txt", "folder1/file2.txt"},
                ),
                "folder1/subfolder1/": (set(), {"folder1/subfolder1/"}),
            }
            assert result == expected

        def test_s3_walk_empty_bucket(self, s3_client):
            """Test s3_walk with an empty bucket."""
            result = s3_walk(s3_client, "test-bucket", "")
            expected = {}
            assert result == expected

        def test_s3_walk_nonexistent_prefix(self, s3_client):
            """Test s3_walk with a nonexistent prefix."""
            self.setup_s3_structure(s3_client)
            result = s3_walk(s3_client, "test-bucket", "nonexistent/")
            expected = {}
            assert result == expected

        def test_s3_walk_single_file(self, s3_client):
            """Test s3_walk with a single file in the bucket."""
            s3_client.put_object(
                Bucket="test-bucket",
                Key="single_file.txt",
                Body=b"content",
            )
            result = s3_walk(s3_client, "test-bucket", "")
            expected = {
                "": (set(), {"single_file.txt"}),
            }
            assert result == expected


@pytest.fixture
def s3_client_for_list_files(_aws_credentials):
    """
    Provide a mocked AWS S3 client with temporary
    credentials for testing list_files function.

    Creates a temporary S3 bucket and sets up some
    objects within it for testing.

    Yields the S3 client for use in the test functions.
    """
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="test-bucket")
        # Set up some objects in S3 for testing
        objects = [
            "file1.txt",
            "folder/file2.txt",
            "folder/file3.txt",
            "another_folder/file4.txt",
            "file1.txt.bak",  # To test filter precision
        ]
        for obj in objects:
            client.put_object(
                Bucket="test-bucket",
                Key=obj,
                Body=b"Test content",
            )
        yield client


class TestFileSize:
    """Tests for file_size function."""

    def test_file_size_success(self, s3_client_for_list_files):
        """Test file_size returns correct size for an existing file."""
        s3_client_for_list_files.put_object(
            Bucket="test-bucket",
            Key="test-size-file.txt",
            Body=b"content",
        )
        size = file_size(s3_client_for_list_files, "test-bucket", "test-size-file.txt")
        assert size == 7  # 'content' is 7 bytes long

    def test_file_size_nonexistent(self, s3_client_for_list_files):
        """Test file_size raises an error for a nonexistent file."""
        with pytest.raises(s3_client_for_list_files.exceptions.ClientError):
            file_size(s3_client_for_list_files, "test-bucket", "nonexistent.txt")

    def test_file_size_empty_file(self, s3_client_for_list_files):
        """Test file_size returns 0 for an empty file."""
        s3_client_for_list_files.put_object(
            Bucket="test-bucket",
            Key="empty-file.txt",
            Body=b"",
        )
        size = file_size(s3_client_for_list_files, "test-bucket", "empty-file.txt")
        assert size == 0


class TestMd5sum:
    """Tests for md5_sum function."""

    def test_md5_sum_success(self, s3_client_for_list_files):
        """Test md5_sum returns correct hash for an existing file."""
        s3_client_for_list_files.put_object(
            Bucket="test-bucket",
            Key="test-md5-file.txt",
            Body=b"content",
        )
        md5 = md5_sum(s3_client_for_list_files, "test-bucket", "test-md5-file.txt")
        assert md5 == "9a0364b9e99bb480dd25e1f0284c8555"  # MD5 hash of 'content'

    def test_md5_sum_nonexistent(self, s3_client_for_list_files):
        """Test md5_sum raises an error for a nonexistent file."""
        with pytest.raises(s3_client_for_list_files.exceptions.ClientError):
            md5_sum(s3_client_for_list_files, "test-bucket", "nonexistent.txt")

    def test_md5_sum_empty_file(self, s3_client_for_list_files):
        """Test md5_sum returns correct hash for an empty file."""
        s3_client_for_list_files.put_object(
            Bucket="test-bucket",
            Key="empty-file.txt",
            Body=b"",
        )
        md5 = md5_sum(s3_client_for_list_files, "test-bucket", "empty-file.txt")
        assert md5 == "d41d8cd98f00b204e9800998ecf8427e"  # MD5 hash of an empty string


class TestCheckFile:
    """Tests for check_file function."""

    def test_check_file_exists_and_valid(self, s3_client):
        """Test check_file returns True for an existing valid file."""
        s3_client.put_object(
            Bucket="test-bucket",
            Key="valid-file.txt",
            Body=b"content",
        )
        assert check_file(s3_client, "test-bucket", "valid-file.txt") is True

    def test_check_file_nonexistent(self, s3_client):
        """Test check_file returns False for a nonexistent file."""
        assert check_file(s3_client, "test-bucket", "nonexistent.txt") is False

    def test_check_file_is_directory(self, s3_client):
        """Test check_file returns False when the object is a directory."""
        s3_client.put_object(Bucket="test-bucket", Key="test-folder/")
        assert check_file(s3_client, "test-bucket", "test-folder/") is False

    def test_check_file_empty_file(self, s3_client):
        """Test check_file returns False for an empty file."""
        s3_client.put_object(
            Bucket="test-bucket",
            Key="empty-file.txt",
            Body=b"",
        )
        assert check_file(s3_client, "test-bucket", "empty-file.txt") is False


class TestWriteStringToFile:
    """Tests for write_string_to_file function."""

    def test_write_string_to_file_success(self, s3_client):
        """Test that write_string_to_file writes content successfully."""
        content = b"example content"
        result = write_string_to_file(
            s3_client,
            "test-bucket",
            "test-file.txt",
            content,
        )
        assert result is None

        # Verify the content was written correctly
        response = s3_client.get_object(Bucket="test-bucket", Key="test-file.txt")
        assert response["Body"].read() == content

    def test_write_string_to_file_overwrite(self, s3_client):
        """Test that write_string_to_file overwrites existing content."""
        initial_content = b"initial content"
        new_content = b"new content"

        # Write initial content
        write_string_to_file(
            s3_client,
            "test-bucket",
            "test-file.txt",
            initial_content,
        )

        # Overwrite with new content
        write_string_to_file(
            s3_client,
            "test-bucket",
            "test-file.txt",
            new_content,
        )

        # Verify the content was overwritten correctly
        response = s3_client.get_object(Bucket="test-bucket", Key="test-file.txt")
        assert response["Body"].read() == new_content

    def test_write_string_to_file_empty_content(self, s3_client):
        """Test that write_string_to_file handles empty content."""
        content = b""
        result = write_string_to_file(
            s3_client,
            "test-bucket",
            "test-file.txt",
            content,
        )
        assert result is None

        # Verify the content was written correctly
        response = s3_client.get_object(Bucket="test-bucket", Key="test-file.txt")
        assert response["Body"].read() == content

    def test_write_string_to_file_nonexistent_bucket(self, s3_client):
        """Test that write_string_to_file raises an error for a nonexistent bucket."""
        content = b"example content"
        with pytest.raises(s3_client.exceptions.NoSuchBucket):
            write_string_to_file(
                s3_client,
                "nonexistent-bucket",
                "test-file.txt",
                content,
            )


class TestReadHeader:
    """Tests for read_header function."""

    def test_read_header_success(self, s3_client_for_list_files):
        """Test read_header successfully reads the first line of a file."""
        s3_client_for_list_files.put_object(
            Bucket="test-bucket",
            Key="test-file.txt",
            Body="Header line\nSecond line\nThird line",
        )
        header = read_header(s3_client_for_list_files, "test-bucket", "test-file.txt")
        assert header == "Header line"

    def test_read_header_empty_file(self, s3_client_for_list_files):
        """Test read_header returns an empty string for an empty file."""
        s3_client_for_list_files.put_object(
            Bucket="test-bucket",
            Key="empty-file.txt",
            Body="",
        )
        header = read_header(s3_client_for_list_files, "test-bucket", "empty-file.txt")
        assert header == ""

    def test_read_header_single_line_file(self, s3_client_for_list_files):
        """Test read_header returns the only line in a single-line file."""
        s3_client_for_list_files.put_object(
            Bucket="test-bucket",
            Key="single-line-file.txt",
            Body="Only line",
        )
        header = read_header(
            s3_client_for_list_files,
            "test-bucket",
            "single-line-file.txt",
        )
        assert header == "Only line"

    def test_read_header_nonexistent_file(self, s3_client_for_list_files):
        """Test read_header raises an error for a nonexistent file."""
        with pytest.raises(s3_client_for_list_files.exceptions.ClientError):
            read_header(s3_client_for_list_files, "test-bucket", "nonexistent.txt")


class TestListFiles:
    """Tests for list_files function."""

    def test_list_all_files(self, s3_client_for_list_files):
        """Test listing all files in the bucket."""
        files = list_files(s3_client_for_list_files, "test-bucket")
        assert len(files) == 5  # Ensuring all files are listed

    def test_list_files_with_prefix(self, s3_client_for_list_files):
        """Test listing files filtered by a specific prefix."""
        files = list_files(s3_client_for_list_files, "test-bucket", "folder/")
        assert len(files) == 2
        assert "folder/file2.txt" in files
        assert "folder/file3.txt" in files

    def test_list_files_empty_prefix(self, s3_client_for_list_files):
        """Test listing files with an empty prefix returns all files."""
        files = list_files(s3_client_for_list_files, "test-bucket", "")
        assert len(files) == 5

    def test_list_files_no_match(self, s3_client_for_list_files):
        """Test listing files with a prefix that matches no files."""
        files = list_files(
            s3_client_for_list_files,
            "test-bucket",
            "nonexistent_prefix/",
        )
        assert len(files) == 0

    def test_list_files_pagination(self, s3_client_for_list_files):
        """Test listing >1000 files to verify pagination works correctly."""
        for i in range(1001):
            s3_client_for_list_files.put_object(
                Bucket="test-bucket",
                Key=f"paginated/file_{i:04d}.txt",
                Body=b"Test content",
            )

        files = list_files(s3_client_for_list_files, "test-bucket")
        assert len(files) == 1006
        assert "paginated/file_0000.txt" in files
        assert "paginated/file_1000.txt" in files


@pytest.fixture
def s3_client_for_delete_and_copy(_aws_credentials):
    """
    Provide a mocked AWS S3 client with temporary
    credentials for testing delete_file and copy_file functions.

    Creates two temporary S3 buckets
    ('source-bucket' and 'destination-bucket')
    and sets up some objects within them for testing.

    Yields the S3 client for use in the test functions.
    """
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="source-bucket")
        client.create_bucket(Bucket="destination-bucket")
        # Set up some objects in S3 for testing
        objects = [
            ("source-bucket", "source_file.txt"),
            ("destination-bucket", "dest_file.txt"),
        ]
        for bucket, obj in objects:
            client.put_object(Bucket=bucket, Key=obj, Body=b"Test content")
        yield client


class TestDeleteFile:
    """Tests for delete_file function."""

    def test_delete_file_success(self, s3_client_for_delete_and_copy):
        """Test successful file deletion."""
        assert (
            delete_file(
                s3_client_for_delete_and_copy,
                "source-bucket",
                "source_file.txt",
            )
            is True
        )

    def test_delete_file_nonexistent(self, s3_client_for_delete_and_copy):
        """Test failure when trying to delete a nonexistent
        file without overwrite enabled.
        """
        assert (
            delete_file(
                s3_client_for_delete_and_copy,
                "source-bucket",
                "nonexistent.txt",
            )
            is False
        )

    def test_delete_file_nonexistent_with_overwrite(
        self,
        s3_client_for_delete_and_copy,
    ):
        """Test handling when overwrite is True for a nonexistent file."""
        assert (
            delete_file(
                s3_client_for_delete_and_copy,
                "source-bucket",
                "nonexistent.txt",
                overwrite=True,
            )
            is True
        )


class TestCopyFile:
    """Tests for copy_file function."""

    def test_copy_file_success(self, s3_client_for_delete_and_copy):
        """Test successful file copying."""
        assert (
            copy_file(
                s3_client_for_delete_and_copy,
                "source-bucket",
                "source_file.txt",
                "destination-bucket",
                "new_dest_file.txt",
            )
            is True
        )

    def test_copy_file_failure_existing_destination(
        self,
        s3_client_for_delete_and_copy,
    ):
        """Test failure when the destination file
        exists and overwrite is False.
        """
        assert (
            copy_file(
                s3_client_for_delete_and_copy,
                "source-bucket",
                "source_file.txt",
                "destination-bucket",
                "dest_file.txt",
            )
            is False
        )

    def test_copy_file_overwrite_success(self, s3_client_for_delete_and_copy):
        """Test successful overwrite of an existing destination file."""
        assert (
            copy_file(
                s3_client_for_delete_and_copy,
                "source-bucket",
                "source_file.txt",
                "destination-bucket",
                "dest_file.txt",
                overwrite=True,
            )
            is True
        )


class TestIsS3Directory:
    """Tests for is_s3_directory function."""

    def test_is_s3_directory_true(self, s3_client):
        """Test is_s3_directory returns True when the key is a directory."""
        s3_client.put_object(Bucket="test-bucket", Key="test-folder/")
        assert is_s3_directory(s3_client, "test-bucket", "test-folder/") is True

    def test_is_s3_directory_false(self, s3_client):
        """Test is_s3_directory returns False when the key is not a directory."""
        s3_client.put_object(
            Bucket="test-bucket",
            Key="test-file.txt",
            Body=b"content",
        )
        assert is_s3_directory(s3_client, "test-bucket", "test-file.txt") is False


class TestDownloadFolder:
    """Tests for download_folder function."""

    def test_download_folder_success(self, s3_client, tmp_path):
        """Test download_folder successfully downloads a folder."""
        s3_client.put_object(
            Bucket="test-bucket",
            Key="test-folder/file1.txt",
            Body=b"content1",
        )
        s3_client.put_object(
            Bucket="test-bucket",
            Key="test-folder/file2.txt",
            Body=b"content2",
        )

        local_path = tmp_path / "local-folder"
        success = download_folder(
            s3_client,
            "test-bucket",
            "test-folder/",
            str(local_path),
            overwrite=True,
        )

        assert success is True
        assert (local_path / "file1.txt").exists()
        assert (local_path / "file2.txt").exists()

    def test_download_folder_no_overwrite(self, s3_client, tmp_path):
        """Test download_folder does not overwrite existing
        files if overwrite is False.
        """
        s3_client.put_object(
            Bucket="test-bucket",
            Key="test-folder/file1.txt",
            Body=b"content1",
        )

        local_path = tmp_path / "local-folder"
        local_path.mkdir(parents=True, exist_ok=True)
        (local_path / "file1.txt").write_text("existing content")

        success = download_folder(
            s3_client,
            "test-bucket",
            "test-folder/",
            str(local_path),
            overwrite=False,
        )

        assert success is True
        assert (local_path / "file1.txt").read_text() == "existing content"

    def test_download_folder_not_directory(self, s3_client, tmp_path):
        """Test download_folder returns False when
        the prefix is not a directory.
        """
        s3_client.put_object(
            Bucket="test-bucket",
            Key="not-a-folder.txt",
            Body=b"content",
        )

        local_path = tmp_path / "local-folder"
        success = download_folder(
            s3_client,
            "test-bucket",
            "not-a-folder.txt",
            str(local_path),
            overwrite=True,
        )

        assert success is False


class TestMoveFile:
    """Tests for move_file function."""

    def test_move_file_success(self, s3_client):
        """Test move_file successfully moves a file between buckets."""
        s3_client.create_bucket(Bucket="dest-bucket")
        s3_client.put_object(
            Bucket="test-bucket",
            Key="test-file.txt",
            Body=b"content",
        )

        success = move_file(
            s3_client,
            "test-bucket",
            "test-file.txt",
            "dest-bucket",
            "moved-file.txt",
        )

        assert success is True
        assert (
            s3_client.get_object(Bucket="dest-bucket", Key="moved-file.txt")[
                "Body"
            ].read()
            == b"content"
        )
        assert "Contents" not in s3_client.list_objects_v2(
            Bucket="test-bucket",
            Prefix="test-file.txt",
        )

    def test_move_file_source_not_exist(self, s3_client):
        """Test move_file returns False when the source file does not exist."""
        s3_client.create_bucket(Bucket="dest-bucket")

        success = move_file(
            s3_client,
            "test-bucket",
            "nonexistent.txt",
            "dest-bucket",
            "moved-file.txt",
        )

        assert success is False


class TestDeleteFolder:
    """Tests for delete_folder function."""

    def test_delete_folder_success(self, s3_client):
        """Test delete_folder deletes all objects within a folder."""
        s3_client.put_object(
            Bucket="test-bucket",
            Key="folder/test-file1.txt",
            Body=b"content1",
        )
        s3_client.put_object(
            Bucket="test-bucket",
            Key="folder/test-file2.txt",
            Body=b"content2",
        )

        result = delete_folder(s3_client, "test-bucket", "folder/")
        assert result is True

        # Verify that the folder is empty
        objects = s3_client.list_objects_v2(
            Bucket="test-bucket",
            Prefix="folder/",
        )
        assert "Contents" not in objects

    def test_delete_folder_nonexistent(self, s3_client):
        """Test delete_folder when the folder does not exist."""
        result = delete_folder(s3_client, "test-bucket", "nonexistent-folder/")
        assert result is False


class TestLoadCSV:
    """Tests for load_csv function."""

    data_basic = """col1,col2,col3
1,A,foo
2,B,bar
3,C,baz
"""

    data_multiline = """col1,col2,col3
1,A,"foo
bar"
2,B,"baz
qux"
"""

    @pytest.fixture(scope="class")
    def s3_client(self):
        """Boto3 S3 client fixture for this test class."""
        with mock_aws():
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket="test-bucket")
            yield s3

    def upload_to_s3(self, s3_client, bucket_name, key, data):
        """Upload a string as a CSV file to S3."""
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=data)

    def test_load_csv_basic(self, s3_client):
        """Test loading CSV file."""
        self.upload_to_s3(s3_client, "test-bucket", "test_basic.csv", self.data_basic)
        df = load_csv(s3_client, "test-bucket", "test_basic.csv")
        assert len(df) == 3
        assert len(df.columns) == 3

    def test_load_csv_multiline(self, s3_client):
        """Test loading multiline CSV file."""
        self.upload_to_s3(
            s3_client,
            "test-bucket",
            "test_multiline.csv",
            self.data_multiline,
        )
        df = load_csv(s3_client, "test-bucket", "test_multiline.csv")
        assert len(df) == 2
        assert len(df.columns) == 3

    def test_load_csv_keep_columns(self, s3_client):
        """Test keeping specific columns."""
        self.upload_to_s3(
            s3_client,
            "test-bucket",
            "test_keep_columns.csv",
            self.data_basic,
        )
        df = load_csv(
            s3_client,
            "test-bucket",
            "test_keep_columns.csv",
            keep_columns=["col1", "col2"],
        )
        assert len(df) == 3
        assert len(df.columns) == 2
        assert "col1" in df.columns
        assert "col2" in df.columns
        assert "col3" not in df.columns

    def test_load_csv_drop_columns(self, s3_client):
        """Test dropping specific columns."""
        self.upload_to_s3(
            s3_client,
            "test-bucket",
            "test_drop_columns.csv",
            self.data_basic,
        )
        df = load_csv(
            s3_client,
            "test-bucket",
            "test_drop_columns.csv",
            drop_columns=["col2"],
        )
        assert len(df) == 3
        assert len(df.columns) == 2
        assert "col1" in df.columns
        assert "col3" in df.columns
        assert "col2" not in df.columns

    def test_load_csv_rename_columns(self, s3_client):
        """Test renaming columns."""
        self.upload_to_s3(
            s3_client,
            "test-bucket",
            "test_rename_columns.csv",
            self.data_basic,
        )
        df = load_csv(
            s3_client,
            "test-bucket",
            "test_rename_columns.csv",
            rename_columns={"col1": "new_col1", "col3": "new_col3"},
        )
        assert len(df) == 3
        assert len(df.columns) == 3
        assert "new_col1" in df.columns
        assert "col1" not in df.columns
        assert "new_col3" in df.columns
        assert "col3" not in df.columns

    def test_load_csv_missing_keep_column(self, s3_client):
        """Test error when keep column is missing."""
        self.upload_to_s3(
            s3_client,
            "test-bucket",
            "test_missing_keep_column.csv",
            self.data_basic,
        )
        with pytest.raises(ValueError):
            load_csv(
                s3_client,
                "test-bucket",
                "test_missing_keep_column.csv",
                keep_columns=["col4"],
            )

    def test_load_csv_missing_drop_column(self, s3_client):
        """Test error when drop column is missing."""
        self.upload_to_s3(
            s3_client,
            "test-bucket",
            "test_missing_drop_column.csv",
            self.data_basic,
        )
        with pytest.raises(ValueError):
            load_csv(
                s3_client,
                "test-bucket",
                "test_missing_drop_column.csv",
                drop_columns=["col4"],
            )

    def test_load_csv_missing_rename_column(self, s3_client):
        """Test error when rename column is missing."""
        self.upload_to_s3(
            s3_client,
            "test-bucket",
            "test_missing_rename_column.csv",
            self.data_basic,
        )
        with pytest.raises(ValueError):
            load_csv(
                s3_client,
                "test-bucket",
                "test_missing_rename_column.csv",
                rename_columns={"col4": "new_col4"},
            )

    def test_load_csv_with_encoding(self, s3_client):
        """Test loading CSV with a specific encoding."""
        self.upload_to_s3(
            s3_client,
            "test-bucket",
            "test_encoding.csv",
            self.data_basic,
        )
        df = load_csv(
            s3_client,
            "test-bucket",
            "test_encoding.csv",
            encoding="ISO-8859-1",
        )
        assert len(df) == 3
        assert len(df.columns) == 3

    def test_load_csv_with_custom_delimiter(self, s3_client):
        """Test loading CSV with a custom delimiter."""
        data_with_semicolon = """col1;col2;col3
1;A;foo
2;B;bar
3;C;baz
"""
        self.upload_to_s3(
            s3_client,
            "test-bucket",
            "test_custom_delimiter.csv",
            data_with_semicolon,
        )
        df = load_csv(s3_client, "test-bucket", "test_custom_delimiter.csv", sep=";")
        assert len(df) == 3
        assert len(df.columns) == 3

    def test_load_csv_with_custom_quote(self, s3_client):
        """Test loading CSV with a custom quote character."""
        data_with_custom_quote = """col1,col2,col3
    1,A,foo
    2,B,'bar'
    3,C,'baz'
    """
        self.upload_to_s3(
            s3_client,
            "test-bucket",
            "test_custom_quote.csv",
            data_with_custom_quote,
        )
        df = load_csv(s3_client, "test-bucket", "test_custom_quote.csv", quotechar="'")
        assert len(df) == 3
        assert len(df.columns) == 3
        assert df[df["col3"] == "bar"].shape[0] == 1
        assert df[df["col3"] == "baz"].shape[0] == 1


class TestLoadJSON:
    """Tests for load_json function."""

    @pytest.fixture(scope="class")
    def s3_client(self):
        """Boto3 S3 client fixture for this test class."""
        with mock_aws():
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket="test-bucket")
            yield s3

    def upload_json_to_s3(self, s3_client, bucket_name, key, data):
        """Upload a dictionary as a JSON file to S3."""
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(data))

    def test_load_json_success(self, s3_client):
        """Test load_json successfully reads a JSON file."""
        data = {"name": "John", "age": 30, "city": "Manchester"}
        self.upload_json_to_s3(s3_client, "test-bucket", "test-file.json", data)

        result = load_json(s3_client, "test-bucket", "test-file.json")
        assert result == data

    def test_load_json_nonexistent_file(self, s3_client):
        """Test load_json raises an exception for a nonexistent file."""
        with pytest.raises(Exception):
            load_json(s3_client, "test-bucket", "nonexistent.json")

    def test_load_json_invalid_json(self, s3_client):
        """Test load_json raises an exception when the JSON file is invalid."""
        s3_client.put_object(
            Bucket="test-bucket",
            Key="invalid.json",
            Body="not a valid json",
        )

        with pytest.raises(Exception):
            load_json(s3_client, "test-bucket", "invalid.json")

    def test_load_json_with_encoding(self, s3_client):
        """Test load_json with a specific encoding."""
        data = {"name": "John", "age": 30, "city": "Manchester"}

        # Convert the dictionary to JSON string and encode it in 'utf-16'
        json_data = json.dumps(data).encode("utf-16")

        # Upload the utf-16 encoded JSON file to S3
        s3_client.put_object(
            Bucket="test-bucket",
            Key="test-file-utf16.json",
            Body=json_data,
        )

        # Read the file back, specifying the 'utf-16' encoding
        result = load_json(
            s3_client,
            "test-bucket",
            "test-file-utf16.json",
            encoding="utf-16",
        )
        assert result == data

    def test_load_json_invalid_bucket_name(self, s3_client):
        """Test load_json raises InvalidBucketNameError for invalid bucket name."""
        data = {"name": "John", "age": 30}
        self.upload_json_to_s3(s3_client, "test-bucket", "test-file.json", data)

        with pytest.raises(InvalidBucketNameError):
            load_json(s3_client, "INVALID_BUCKET", "test-file.json")

    def test_load_json_multi_line_success(self, s3_client):
        """Test load_json successfully reads a multi-line JSON file."""
        data = [
            {"event": "start", "timestamp": "2025-02-18T12:00:00Z"},
            {"event": "stop", "timestamp": "2025-02-18T12:05:00Z"},
        ]

        # Convert list of dictionaries into a newline-separated JSON string
        json_lines = "\n".join(json.dumps(entry) for entry in data)

        # Upload to S3
        s3_client.put_object(Bucket="test-bucket", Key="test-log.json", Body=json_lines)

        # Read using multi_line=True
        result = load_json(s3_client, "test-bucket", "test-log.json", multi_line=True)
        assert result == data

    def test_load_json_multi_line_invalid_json(self, s3_client):
        """Test load_json raises an exception when multi-line JSON has an invalid entry."""
        invalid_data = (
            '{"event": "start", "timestamp": "2025-02-18T12:00:00Z"}\nINVALID_JSON_LINE'
        )
        s3_client.put_object(
            Bucket="test-bucket",
            Key="invalid-log.json",
            Body=invalid_data,
        )

        with pytest.raises(Exception):
            load_json(s3_client, "test-bucket", "invalid-log.json", multi_line=True)


class TestWriteCSV:
    """Tests for write_csv function."""

    @pytest.fixture(scope="class")
    def s3_client(self):
        """Boto3 S3 client fixture for this test class."""
        with mock_aws():
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket="test-bucket")
            yield s3

    def test_write_csv_success(self, s3_client):
        """Test that write_csv returns True if successful."""
        data = {"name": ["John"], "age": [30], "city": ["Manchester"]}
        df = pd.DataFrame(data)

        result = write_csv(s3_client, "test-bucket", df, "test_file.csv")
        assert result

    def test_write_csv_read_back(self, s3_client):
        """Test that a file wrtitten by write_csv can be read back and returns
        the same dataframe as input. Uses kwargs.
        """
        data = {"name": ["John"], "age": [30], "city": ["Manchester"]}
        df = pd.DataFrame(data)

        _ = write_csv(s3_client, "test-bucket", df, "test_file.csv", index=False)
        result = load_csv(s3_client, "test-bucket", "test_file.csv")
        pd.testing.assert_frame_equal(df, result)

    def test_write_csv_failure(self, s3_client):
        """Test that write_csv returns False if unable to write.
        Dictionary data does not have to_csv method.
        """
        data = {"name": ["John"], "age": [30], "city": ["Manchester"]}

        result = write_csv(s3_client, "test-bucket", data, "test_file.csv", index=False)
        assert not result


class TestWriteExcel:
    """Tests for write_excel function."""

    @pytest.fixture(scope="class")
    def s3_client(self):
        """Boto3 S3 client fixture for this test class."""
        with mock_aws():
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket="test-bucket")
            yield s3

    def test_write_excel_success(self, s3_client):
        """Test that write_excel returns True if successful."""
        data = {"name": ["John"], "age": [30], "city": ["Manchester"]}
        df = pd.DataFrame(data)

        result = write_excel(s3_client, "test-bucket", df, "test_file.xlsx")
        assert result

    def test_write_excel_read_back(self, s3_client):
        """Test that a file written by write_excel can be read back and returns
        the same dataframe as input. Uses kwargs.
        """
        data = {"name": ["John"], "age": [30], "city": ["Manchester"]}
        df = pd.DataFrame(data)

        _ = write_excel(s3_client, "test-bucket", df, "test_file.xlsx", index=False)

        # Read back the file from S3
        obj = s3_client.get_object(Bucket="test-bucket", Key="test_file.xlsx")
        excel_buffer = BytesIO(obj["Body"].read())

        # Load the Excel file into a DataFrame
        result_df = pd.read_excel(excel_buffer, engine="openpyxl")

        pd.testing.assert_frame_equal(df, result_df)

    def test_write_excel_failure(self, s3_client):
        """Test that write_excel returns False if unable to write.
        Dictionary data does not have a to_excel method.
        """
        data = {"name": ["John"], "age": [30], "city": ["Manchester"]}

        result = write_excel(
            s3_client,
            "test-bucket",
            data,
            "test_file.xlsx",
            index=False,
        )
        assert not result


class TestDeleteOldObjectsAndFolders:
    """Tests for delete_old_objects_and_folders function."""

    def s3_client_structure(self, s3_client):
        """Set up a folder structure in S3 for testing using a fixed current time."""
        fixed_now = datetime(2025, 3, 19, 12, 0, 0, tzinfo=timezone.utc)

        # Freeze time to our fixed_now for creating "old" objects
        with freeze_time(fixed_now - timedelta(days=10)):
            s3_client.put_object(
                Bucket="test-bucket",
                Key="test/old-folder/old-file.txt",
                Body=b"old content",
            )
            s3_client.put_object(
                Bucket="test-bucket",
                Key="test/old-file.txt",
                Body=b"old content",
            )

        # Freeze time to our fixed_now for creating "recent" objects
        with freeze_time(fixed_now - timedelta(days=1)):
            s3_client.put_object(
                Bucket="test-bucket",
                Key="test/recent-folder/recent-file.txt",
                Body=b"recent content",
            )
            s3_client.put_object(
                Bucket="test-bucket",
                Key="test/recent-file.txt",
                Body=b"recent content",
            )

    class TestDeleteByDay:
        """Tests for deleting objects and folders by day."""

        @freeze_time("2025-03-19T12:00:00Z")
        def test_delete_old_objects_and_folders(self, s3_client):
            """Test deleting objects and folders older than 1 day."""
            s3_client = s3_client
            # Use the common prefix "test/"
            TestDeleteOldObjectsAndFolders().s3_client_structure(s3_client)
            result = delete_old_objects_and_folders(
                s3_client,
                "test-bucket",
                "test/",
                "1 day",
            )
            assert result is True

            remaining_objects = s3_client.list_objects_v2(Bucket="test-bucket")
            remaining_keys = [
                obj["Key"] for obj in remaining_objects.get("Contents", [])
            ]

            # Expect the "old" objects (10 days old) to be deleted,
            # while the "recent" objects (1 day old) remain.
            assert "test/recent-folder/recent-file.txt" in remaining_keys
            assert "test/recent-file.txt" in remaining_keys
            assert "test/old-folder/old-file.txt" not in remaining_keys
            assert "test/old-file.txt" not in remaining_keys

    class TestDeleteByWeek:
        """Tests for deleting objects and folders by week."""

        @freeze_time("2025-03-19T12:00:00Z")
        def test_delete_old_objects_and_folders(self, s3_client):
            """Test deleting objects and folders older than 1 week."""
            s3_client = s3_client
            TestDeleteOldObjectsAndFolders().s3_client_structure(s3_client)
            result = delete_old_objects_and_folders(
                s3_client,
                "test-bucket",
                "test/",
                "1 week",
            )
            assert result is True

            remaining_objects = s3_client.list_objects_v2(Bucket="test-bucket")
            remaining_keys = [
                obj["Key"] for obj in remaining_objects.get("Contents", [])
            ]

            # With a 1 week threshold, old objects (10 days old) are deleted
            # and recent objects (1 day old) remain.
            assert "test/recent-folder/recent-file.txt" in remaining_keys
            assert "test/recent-file.txt" in remaining_keys
            assert "test/old-folder/old-file.txt" not in remaining_keys
            assert "test/old-file.txt" not in remaining_keys

    class TestDeleteByMonth:
        """Tests for deleting objects and folders by month."""

        @freeze_time("2025-03-19T12:00:00Z")
        def test_delete_old_objects_and_folders(self, s3_client):
            """Test deleting objects and folders older than 1 month."""
            s3_client = s3_client
            TestDeleteOldObjectsAndFolders().s3_client_structure(s3_client)
            result = delete_old_objects_and_folders(
                s3_client,
                "test-bucket",
                "test/",
                "1 month",
            )
            assert result is True

            remaining_objects = s3_client.list_objects_v2(Bucket="test-bucket")
            remaining_keys = [
                obj["Key"] for obj in remaining_objects.get("Contents", [])
            ]

            # With a 1 month threshold, both old (10 days old) and recent (1 day old)
            # objects should remain because they are newer than 1 month.
            assert "test/recent-folder/recent-file.txt" in remaining_keys
            assert "test/recent-file.txt" in remaining_keys
            assert "test/old-folder/old-file.txt" in remaining_keys
            assert "test/old-file.txt" in remaining_keys


class TestZipLocalDirectoryToS3:
    """Tests for zip_local_directory_to_s3 function."""

    @pytest.fixture(scope="class")
    def s3_client(self):
        """Boto3 S3 client fixture for this test class."""
        with mock_aws():
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket="test-bucket")
            yield s3

    def test_zip_local_directory_to_s3_success(self, s3_client, tmp_path):
        """Test successful zipping and uploading of a local directory."""
        # Create a local directory with files
        local_dir = tmp_path / "test_dir"
        local_dir.mkdir()
        (local_dir / "file1.txt").write_text("Content of file 1")
        (local_dir / "file2.txt").write_text("Content of file 2")

        # Call the function
        result = zip_local_directory_to_s3(
            s3_client,
            local_dir,
            "test-bucket",
            "test_dir.zip",
            overwrite=True,
        )

        # Verify the result
        assert result is True

        # Verify the zip file exists in S3
        response = s3_client.get_object(Bucket="test-bucket", Key="test_dir.zip")
        zip_content = BytesIO(response["Body"].read())

        # Verify the contents of the zip file
        with zipfile.ZipFile(zip_content, "r") as zf:
            assert set(zf.namelist()) == {"file1.txt", "file2.txt"}

    def test_zip_local_directory_to_s3_nonexistent_directory(self, s3_client):
        """Test handling when the local directory does not exist."""
        result = zip_local_directory_to_s3(
            s3_client,
            "nonexistent_dir",
            "test-bucket",
            "test_dir.zip",
        )
        assert result is False

    def test_zip_local_directory_to_s3_no_overwrite(self, s3_client, tmp_path):
        """Test no overwrite of existing S3 object without permission."""
        # Create a local directory with files
        local_dir = tmp_path / "test_dir"
        local_dir.mkdir()
        (local_dir / "file1.txt").write_text("Content of file 1")

        # Upload an existing zip file to S3
        s3_client.put_object(
            Bucket="test-bucket",
            Key="test_dir.zip",
            Body=b"existing content",
        )

        # Call the function without overwrite
        result = zip_local_directory_to_s3(
            s3_client,
            local_dir,
            "test-bucket",
            "test_dir.zip",
            overwrite=False,
        )

        # Verify the result - should be True now since we changed the return value
        # when a file exists and overwrite=False
        assert result is True

        # Verify the existing file was not changed
        response = s3_client.get_object(Bucket="test-bucket", Key="test_dir.zip")
        content = response["Body"].read()
        assert content == b"existing content"


class TestZipS3DirectoryToS3:
    """Tests for zip_s3_directory_to_s3 function."""

    @pytest.fixture(scope="class")
    def s3_client(self):
        """Boto3 S3 client fixture for this test class."""
        with mock_aws():
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket="source-bucket")
            s3.create_bucket(Bucket="destination-bucket")
            yield s3

    def setup_s3_directory(self, s3_client):
        """Set up a directory structure in S3 for testing."""
        s3_client.put_object(
            Bucket="source-bucket",
            Key="folder1/file1.txt",
            Body=b"Content of file 1",
        )
        s3_client.put_object(
            Bucket="source-bucket",
            Key="folder1/file2.txt",
            Body=b"Content of file 2",
        )
        s3_client.put_object(
            Bucket="source-bucket",
            Key="folder1/subfolder/file3.txt",
            Body=b"Content of file 3",
        )

    def test_zip_s3_directory_to_s3_success(self, s3_client):
        """Test successful zipping of an S3 directory and uploading to another bucket."""
        self.setup_s3_directory(s3_client)

        # Call the function
        result = zip_s3_directory_to_s3(
            s3_client,
            "source-bucket",
            "folder1/",
            "destination-bucket",
            "folder1.zip",
            overwrite=True,
        )

        # Verify the result
        assert result is True

        # Verify the zip file exists in the destination bucket
        response = s3_client.get_object(Bucket="destination-bucket", Key="folder1.zip")
        zip_content = BytesIO(response["Body"].read())

        # Verify the contents of the zip file
        with zipfile.ZipFile(zip_content, "r") as zf:
            assert set(zf.namelist()) == {
                "file1.txt",
                "file2.txt",
                "subfolder/file3.txt",
            }

    def test_zip_s3_directory_to_s3_nonexistent_source(self, s3_client):
        """Test handling when the source directory does not exist."""
        result = zip_s3_directory_to_s3(
            s3_client,
            "source-bucket",
            "nonexistent_folder/",
            "destination-bucket",
            "nonexistent_folder.zip",
        )
        assert result is False

    def test_zip_s3_directory_to_s3_no_overwrite(self, s3_client):
        """Test no overwrite of existing S3 object without permission."""
        # Set up test data in source bucket
        self.setup_s3_directory(s3_client)

        # Upload an existing zip file to the destination bucket
        s3_client.put_object(
            Bucket="destination-bucket",
            Key="folder1.zip",
            Body=b"existing content",
        )

        # Call the function without overwrite
        result = zip_s3_directory_to_s3(
            s3_client,
            "source-bucket",
            "folder1/",
            "destination-bucket",
            "folder1.zip",
            overwrite=False,
        )

        # Verify the result - should be True now since we changed the return value
        # when a file exists and overwrite=False
        assert result is True

        # Verify the existing file was not changed
        response = s3_client.get_object(Bucket="destination-bucket", Key="folder1.zip")
        content = response["Body"].read()
        assert content == b"existing content"
