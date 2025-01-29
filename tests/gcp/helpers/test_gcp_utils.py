"""Tests for gcp_utils.py module."""

from unittest import mock

import pytest
from google.cloud import storage

from rdsa_utils.gcp.helpers.gcp_utils import (
    copy_file,
    create_folder_on_gcs,
    delete_file,
    delete_folder,
    download_file,
    download_folder,
    file_exists,
    is_gcs_directory,
    list_files,
    move_file,
    upload_file,
    upload_folder,
)


@pytest.mark.skip(reason="requires query")
class TestRunBqQuery:
    """Test for run_bq_query function.."""

    def test_expected(self):
        """Test expected functionality."""
        pass


@pytest.mark.skip(reason="requires table path")
class TestGetTableColumns:
    """Test for get_table_columns function.."""

    def test_expected(self):
        """Test expected functionality."""
        pass


@pytest.mark.skip(reason="requires table path")
class TestTableExists:
    """Test for table_exists function."""

    def test_expected(self):
        """Test expected functionality."""
        pass


@pytest.mark.skip(reason="requires GCP")
class TestLoadConfigGcp:
    """Test for load_config_gcp function."""

    def test_expected(self):
        """Test expected functionality."""
        pass


@pytest.fixture
def mock_client():
    """Mock GCS client."""
    return mock.Mock(spec=storage.Client)


@pytest.fixture
def mock_bucket(mock_client):
    """Mock GCS bucket."""
    bucket = mock.Mock(spec=storage.Bucket)
    mock_client.bucket.return_value = bucket
    return bucket


@pytest.fixture
def mock_blob(mock_bucket):
    """Mock GCS blob."""
    blob = mock.Mock(spec=storage.Blob)
    mock_bucket.blob.return_value = blob
    return blob


@pytest.fixture
def mock_list_blobs(mock_client):
    """Mock list_blobs method."""
    mock_client.list_blobs.return_value = iter([mock.Mock()])
    return mock_client.list_blobs


@pytest.fixture
def mock_path():
    """Mock Path object."""
    with mock.patch("rdsa_utils.gcp.helpers.gcp_utils.Path") as mock_path:
        yield mock_path


class TestIsGCSDirectory:
    """Tests for is_gcs_directory function."""

    def test_is_gcs_directory(self, mock_client, mock_list_blobs):
        """Test if a GCS object is a directory."""
        assert is_gcs_directory(
            mock_client,
            "bucket",
            "path",
        ), "Expected path to be recognized as a GCS directory."

    def test_is_not_gcs_directory(self, mock_client):
        """Test if a GCS object is not a directory."""
        mock_client.list_blobs.return_value = iter([])
        assert not is_gcs_directory(
            mock_client,
            "bucket",
            "path",
        ), "Expected path to not be recognized as a GCS directory."


class TestFileExists:
    """Tests for file_exists function."""

    def test_file_exists(self, mock_client, mock_blob):
        """Test if a file exists in GCS bucket."""
        mock_blob.exists.return_value = True
        assert file_exists(
            mock_client,
            "bucket",
            "path",
        ), "Expected file to exist in GCS bucket."

    def test_file_not_exists(self, mock_client, mock_blob):
        """Test if a file does not exist in GCS bucket."""
        mock_blob.exists.return_value = False
        assert not file_exists(
            mock_client,
            "bucket",
            "path",
        ), "Expected file to not exist in GCS bucket."


class TestUploadFile:
    """Tests for upload_file function."""

    @mock.patch("rdsa_utils.gcp.helpers.gcp_utils.Path")
    def test_upload_file_success(self, mock_path, mock_client, mock_blob):
        """Test successful upload of a file to GCS bucket."""
        with mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name",
        ), mock.patch("rdsa_utils.gcp.helpers.gcp_utils.remove_leading_slash"):
            mock_blob.exists.return_value = False
            mock_path.return_value.exists.return_value = True
            assert upload_file(
                mock_client,
                "bucket",
                "/local/path",
                "path",
                overwrite=True,
            ), "Expected file to be uploaded successfully to GCS bucket."

    @mock.patch("rdsa_utils.gcp.helpers.gcp_utils.Path")
    def test_upload_file_already_exists(self, mock_path, mock_client, mock_blob):
        """Test upload fails if file already exists in GCS bucket."""
        with mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name",
        ), mock.patch("rdsa_utils.gcp.helpers.gcp_utils.remove_leading_slash"):
            mock_blob.exists.return_value = True
            mock_path.return_value.exists.return_value = True
            assert not upload_file(
                mock_client,
                "bucket",
                "/local/path",
                "path",
            ), "Expected upload to fail as file already exists in GCS bucket."

    @mock.patch("rdsa_utils.gcp.helpers.gcp_utils.Path")
    def test_upload_file_local_not_exists(self, mock_path, mock_client, mock_blob):
        """Test upload fails if local file does not exist."""
        with mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name",
        ), mock.patch("rdsa_utils.gcp.helpers.gcp_utils.remove_leading_slash"):
            mock_path.return_value.exists.return_value = False
            assert not upload_file(
                mock_client,
                "bucket",
                "/local/path",
                "path",
            ), "Expected upload to fail as local file does not exist."


class TestDownloadFile:
    """Tests for download_file function."""

    @mock.patch("rdsa_utils.gcp.helpers.gcp_utils.Path")
    def test_download_file_success(self, mock_path, mock_client, mock_blob):
        """Test successful download of a file from GCS bucket."""
        with mock.patch("rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name"):
            mock_blob.exists.return_value = True
            mock_path.return_value.exists.return_value = True
            assert download_file(
                mock_client,
                "bucket",
                "path",
                "/local/path",
                overwrite=True,
            ), "Expected file to be downloaded successfully from GCS bucket."

    @mock.patch("rdsa_utils.gcp.helpers.gcp_utils.Path")
    def test_download_file_not_exists(self, mock_path, mock_client, mock_blob):
        """Test download fails if file does not exist in GCS bucket."""
        with mock.patch("rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name"):
            mock_blob.exists.return_value = False
            mock_path.return_value.exists.return_value = True
            assert not download_file(
                mock_client,
                "bucket",
                "path",
                "/local/path",
            ), "Expected download to fail as file does not exist in GCS bucket."

    @mock.patch("rdsa_utils.gcp.helpers.gcp_utils.Path")
    def test_download_file_local_exists_no_overwrite(
        self,
        mock_path,
        mock_client,
        mock_blob,
    ):
        """Test download fails if local file exists and overwrite is False."""
        with mock.patch("rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name"):
            mock_blob.exists.return_value = True
            mock_path.return_value.exists.return_value = True
            assert not download_file(
                mock_client,
                "bucket",
                "path",
                "/local/path",
                overwrite=False,
            ), "Expected download to fail as local file exists and overwrite is False."


class TestDeleteFile:
    """Tests for delete_file function."""

    def test_delete_file_success(self, mock_client, mock_blob):
        """Test successful deletion of a file from GCS bucket."""
        with mock.patch("rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name"):
            mock_blob.exists.return_value = True
            assert delete_file(
                mock_client,
                "bucket",
                "path",
            ), "Expected file to be deleted successfully from GCS bucket."

    def test_delete_file_not_exists(self, mock_client, mock_blob):
        """Test deletion fails if file does not exist in GCS bucket."""
        with mock.patch("rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name"):
            mock_blob.exists.return_value = False
            assert not delete_file(
                mock_client,
                "bucket",
                "path",
            ), "Expected deletion to fail as file does not exist in GCS bucket."

    def test_delete_file_exception(self, mock_client, mock_blob):
        """Test deletion fails if an exception occurs."""
        with mock.patch("rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name"):
            mock_blob.exists.return_value = True
            mock_blob.delete.side_effect = Exception("Deletion failed")
            assert not delete_file(
                mock_client,
                "bucket",
                "path",
            ), "Expected deletion to fail due to an exception."


class TestCopyFile:
    """Tests for copy_file function."""

    def test_copy_file_success(self, mock_client, mock_blob):
        """Test successful copy of a file within GCS buckets."""
        with mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name",
        ), mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.is_gcs_directory",
            return_value=False,
        ):
            mock_blob.exists.return_value = True
            assert copy_file(
                mock_client,
                "source_bucket",
                "source_path",
                "dest_bucket",
                "dest_path",
                overwrite=True,
            ), "Expected file to be copied successfully within GCS buckets."

    def test_copy_file_not_exists(self, mock_client, mock_blob):
        """Test copy fails if source file does not exist in GCS bucket."""
        with mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name",
        ), mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.is_gcs_directory",
            return_value=False,
        ):
            mock_blob.exists.return_value = False
            assert not copy_file(
                mock_client,
                "source_bucket",
                "source_path",
                "dest_bucket",
                "dest_path",
            ), "Expected copy to fail as source file does not exist in GCS bucket."

    def test_copy_file_is_directory(self, mock_client, mock_blob):
        """Test copy fails if source object is a directory."""
        with mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name",
        ), mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.is_gcs_directory",
            return_value=True,
        ):
            assert not copy_file(
                mock_client,
                "source_bucket",
                "source_path",
                "dest_bucket",
                "dest_path",
            ), "Expected copy to fail as source object is a directory."


class TestCreateFolderOnGCS:
    """Tests for create_folder_on_gcs function."""

    def test_create_folder_success(self, mock_client, mock_blob):
        """Test successful creation of a folder in GCS bucket."""
        with mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name",
        ), mock.patch("rdsa_utils.gcp.helpers.gcp_utils.remove_leading_slash"):
            mock_blob.exists.return_value = False
            assert create_folder_on_gcs(
                mock_client,
                "bucket",
                "folder/",
            ), "Expected folder to be created successfully in GCS bucket."

    def test_create_existing_folder(self, mock_client, mock_blob):
        """Test creation succeeds if folder already exists in GCS bucket."""
        with mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name",
        ), mock.patch("rdsa_utils.gcp.helpers.gcp_utils.remove_leading_slash"):
            mock_blob.exists.return_value = True
            assert create_folder_on_gcs(
                mock_client,
                "bucket",
                "folder/",
            ), "Expected creation to succeed as folder already exists in GCS bucket."

    def test_create_folder_exception(self, mock_client, mock_blob):
        """Test creation fails if an exception occurs."""
        with mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name",
        ), mock.patch("rdsa_utils.gcp.helpers.gcp_utils.remove_leading_slash"):
            mock_blob.exists.return_value = False
            mock_blob.upload_from_string.side_effect = Exception("Creation failed")
            assert not create_folder_on_gcs(
                mock_client,
                "bucket",
                "folder/",
            ), "Expected creation to fail due to an exception."


class TestUploadFolder:
    """Tests for upload_folder function."""

    def test_upload_folder_success(self, mock_path, mock_client, mock_blob):
        """Test successful upload of a folder to GCS bucket."""
        mock_path.return_value.is_dir.return_value = True
        mock_path.return_value.rglob.return_value = [mock_path]
        mock_path.return_value.is_file.return_value = True
        mock_blob.exists.return_value = False
        with mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name",
        ), mock.patch("rdsa_utils.gcp.helpers.gcp_utils.remove_leading_slash"):
            assert upload_folder(
                mock_client,
                "bucket",
                "/local/path",
                "prefix",
                overwrite=True,
            ), "Expected folder to be uploaded successfully to GCS bucket."

    def test_upload_folder_local_not_exists(self, mock_path, mock_client):
        """Test upload fails if local folder does not exist."""
        mock_path.return_value.is_dir.return_value = False
        with mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name",
        ), mock.patch("rdsa_utils.gcp.helpers.gcp_utils.remove_leading_slash"):
            assert not upload_folder(
                mock_client,
                "bucket",
                "/local/path",
                "prefix",
                overwrite=True,
            ), "Expected upload to fail as local folder does not exist."

    def test_upload_folder_file_already_exists(self, mock_path, mock_client, mock_blob):
        """Test upload fails if a file in the local folder already exists in GCS bucket."""
        mock_path.return_value.is_dir.return_value = True
        mock_path.return_value.rglob.return_value = [mock_path]
        mock_path.return_value.is_file.return_value = True
        mock_blob.exists.return_value = True
        with mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name",
        ), mock.patch("rdsa_utils.gcp.helpers.gcp_utils.remove_leading_slash"):
            assert not upload_folder(
                mock_client,
                "bucket",
                "/local/path",
                "prefix",
                overwrite=False,
            ), "Expected upload to fail as file already exists in GCS bucket."


class TestListFiles:
    """Tests for list_files function."""

    def test_list_files_success(self, mock_client, mock_list_blobs):
        """Test successful listing of files in GCS bucket."""
        with mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name",
        ), mock.patch("rdsa_utils.gcp.helpers.gcp_utils.remove_leading_slash"):
            mock_blob = mock.Mock()
            mock_blob.name = "file_name"
            mock_list_blobs.return_value = iter([mock_blob])
            assert list_files(mock_client, "bucket", "prefix") == [
                "file_name",
            ], "Expected to successfully list files in GCS bucket."

    def test_list_files_empty(self, mock_client):
        """Test listing returns empty if no files match prefix."""
        with mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name",
        ), mock.patch("rdsa_utils.gcp.helpers.gcp_utils.remove_leading_slash"):
            mock_client.list_blobs.return_value = iter([])
            assert (
                list_files(mock_client, "bucket", "prefix") == []
            ), "Expected to return an empty list as no files match the prefix."


class TestDownloadFolder:
    """Tests for download_folder function."""

    def test_download_folder_success(self, mock_path, mock_client, mock_list_blobs):
        """Test successful download of a folder from GCS bucket."""
        mock_path.return_value.exists.return_value = True
        with mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name",
        ), mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.remove_leading_slash",
        ), mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.is_gcs_directory",
            return_value=True,
        ):
            assert download_folder(
                mock_client,
                "bucket",
                "prefix",
                "/local/path",
                overwrite=True,
            ), "Expected folder to be downloaded successfully from GCS bucket."

    def test_download_folder_not_exists(self, mock_path, mock_client):
        """Test download fails if folder does not exist in GCS bucket."""
        mock_path.return_value.exists.return_value = True
        with mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name",
        ), mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.remove_leading_slash",
        ), mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.is_gcs_directory",
            return_value=False,
        ):
            assert not download_folder(
                mock_client,
                "bucket",
                "prefix",
                "/local/path",
                overwrite=True,
            ), "Expected download to fail as folder does not exist in GCS bucket."


class TestMoveFile:
    """Tests for move_file function."""

    def test_move_file_success(self, mock_client, mock_blob):
        """Test successful move of a file within/between GCS buckets."""
        with mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name",
        ), mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.remove_leading_slash",
        ), mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.is_gcs_directory",
            return_value=False,
        ):
            mock_blob.exists.return_value = True
            assert move_file(
                mock_client,
                "source_bucket",
                "source_path",
                "dest_bucket",
                "dest_path",
            ), "Expected file to be moved successfully within/between GCS buckets."

    def test_move_file_not_exists(self, mock_client, mock_blob):
        """Test move fails if source file does not exist in GCS bucket."""
        with mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name",
        ), mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.remove_leading_slash",
        ), mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.is_gcs_directory",
            return_value=False,
        ):
            mock_blob.exists.return_value = False
            assert not move_file(
                mock_client,
                "source_bucket",
                "source_path",
                "dest_bucket",
                "dest_path",
            ), "Expected move to fail as source file does not exist in GCS bucket."

    def test_move_file_is_directory(self, mock_client):
        """Test move fails if source object is a directory."""
        with mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name",
        ), mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.remove_leading_slash",
        ), mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.is_gcs_directory",
            return_value=True,
        ):
            assert not move_file(
                mock_client,
                "source_bucket",
                "source_path",
                "dest_bucket",
                "dest_path",
            ), "Expected move to fail as source object is a directory."


class TestDeleteFolder:
    """Tests for delete_folder function."""

    def test_delete_folder_success(self, mock_client, mock_list_blobs):
        """Test successful deletion of a folder in GCS bucket."""
        with mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name",
        ), mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.remove_leading_slash",
        ), mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.is_gcs_directory",
            return_value=True,
        ):
            assert delete_folder(
                mock_client,
                "bucket",
                "folder/",
            ), "Expected folder to be deleted successfully in GCS bucket."

    def test_delete_folder_not_exists(self, mock_client):
        """Test deletion fails if folder does not exist in GCS bucket."""
        with mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name",
        ), mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.remove_leading_slash",
        ), mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.is_gcs_directory",
            return_value=False,
        ):
            assert not delete_folder(
                mock_client,
                "bucket",
                "folder/",
            ), "Expected deletion to fail as folder does not exist in GCS bucket."

    def test_delete_folder_exception(self, mock_client, mock_list_blobs):
        """Test deletion fails if an exception occurs."""
        with mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.validate_bucket_name",
        ), mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.remove_leading_slash",
        ), mock.patch(
            "rdsa_utils.gcp.helpers.gcp_utils.is_gcs_directory",
            return_value=True,
        ):
            mock_blob = mock.Mock()
            mock_blob.delete.side_effect = Exception("Deletion failed")
            mock_list_blobs.return_value = iter([mock_blob])
            assert not delete_folder(
                mock_client,
                "bucket",
                "folder/",
            ), "Expected deletion to fail due to an exception."
