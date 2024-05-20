"""Tests for s3_utils.py module."""

import boto3
import pytest
from moto import mock_aws

from rdsa_utils.cdp.helpers.s3_utils import (
    copy_file,
    create_folder_on_s3,
    delete_file,
    delete_folder,
    download_file,
    download_folder,
    file_exists,
    is_s3_directory,
    list_files,
    move_file,
    remove_leading_slash,
    upload_file,
    upload_folder,
    validate_bucket_name,
)
from rdsa_utils.exceptions import InvalidBucketNameError


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


@pytest.fixture()
def _aws_credentials():
    """Mock AWS Credentials for moto."""
    boto3.setup_default_session(
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        aws_session_token="testing",
    )


@pytest.fixture()
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


@pytest.fixture()
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


@pytest.fixture()
def setup_folder(tmp_path):
    """
    Set up local folder and files for upload tests.

    Creates a temporary folder with files and subfolders
    for testing folder upload functionality.

    Returns the path of the created local folder, which
    is provided to the create_folder_on_s3 and upload_folder function.
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
    """Tests for create_folder_on_s3 function."""

    def test_create_new_folder(self, s3_client):
        """Test creation of a new folder on S3."""
        assert create_folder_on_s3(s3_client, "test-bucket", "new_folder/") is True

    def test_folder_already_exists(self, s3_client):
        """Test handling when the folder already exists on S3."""
        s3_client.put_object(Bucket="test-bucket", Key="existing_folder/")
        assert create_folder_on_s3(s3_client, "test-bucket", "existing_folder/") is True


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


@pytest.fixture()
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


@pytest.fixture()
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
