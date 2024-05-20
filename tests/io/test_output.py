"""Tests for the output.py module."""

import shutil

import pytest

from rdsa_utils.io.output import zip_folder


class TestZipFolder:
    """Tests for zip_folder function."""

    @pytest.fixture(autouse=True)
    def _setup_and_teardown(self, tmp_path):
        """Set up and tear down for tests."""
        self.tmp_path = tmp_path
        self.source_dir = tmp_path / "source"
        self.source_dir.mkdir()
        self.file_in_source = self.source_dir / "file.txt"
        self.file_in_source.write_text("content")
        yield
        shutil.rmtree(self.tmp_path)

    def test_zip_folder_success(self):
        """Test zipping a directory successfully."""
        output_filename = self.tmp_path / "output.zip"
        assert zip_folder(str(self.source_dir), str(output_filename), overwrite=True)
        assert output_filename.exists()

    def test_zip_folder_no_source_directory(self):
        """Test zipping with a non-existing source directory."""
        non_existing_dir = self.tmp_path / "non_existing"
        output_filename = self.tmp_path / "output.zip"
        assert not zip_folder(str(non_existing_dir), str(output_filename))
        assert not output_filename.exists()

    def test_zip_folder_output_not_zip(self):
        """Test zipping with an output filename not ending in .zip."""
        output_filename = self.tmp_path / "output.txt"
        assert not zip_folder(str(self.source_dir), str(output_filename))
        assert not output_filename.exists()

    def test_zip_folder_overwrite_false(self):
        """Test zipping with overwrite set to False."""
        output_filename = self.tmp_path / "output.zip"
        output_filename.touch()
        assert not zip_folder(
            str(self.source_dir),
            str(output_filename),
            overwrite=False,
        )
        assert output_filename.exists()
        assert output_filename.stat().st_size == 0

    def test_zip_folder_overwrite_true(self):
        """Test zipping with overwrite set to True."""
        output_filename = self.tmp_path / "output.zip"
        output_filename.touch()
        assert zip_folder(str(self.source_dir), str(output_filename), overwrite=True)
        assert output_filename.exists()
        assert output_filename.stat().st_size > 0
