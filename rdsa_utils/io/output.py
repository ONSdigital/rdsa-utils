"""Module containing generic output functionality code."""

import logging
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)


def zip_folder(source_dir: str, output_filename: str, overwrite: bool = False) -> bool:
    """Zip the contents of the specified directory.

    Parameters
    ----------
    source_dir
        The directory whose contents are to be zipped.
    output_filename
        The output zip file name. It must end with '.zip'.
    overwrite
        If True, overwrite the existing zip file if it exists.
        Default is False.

    Returns
    -------
    bool
        True if the directory was zipped successfully, False otherwise.

    Examples
    --------
    >>> zip_folder('/path/to/source_dir', 'output.zip', overwrite=True)
    True
    """
    source_dir_path = Path(source_dir)
    output_filename_path = Path(output_filename)

    if not source_dir_path.exists() or not source_dir_path.is_dir():
        logger.error(
            f"Source directory {source_dir} does not exist or is not a directory.",
        )
        return False

    if output_filename_path.suffix != ".zip":
        logger.error(f"Output filename {output_filename} must have a .zip extension.")
        return False

    if output_filename_path.exists() and not overwrite:
        logger.error(
            f"Output file {output_filename} already exists and "
            "overwrite is set to False.",
        )
        return False

    try:
        # Create the zip file in the same directory as the output_filename
        temp_zip_path = output_filename_path.with_suffix("")
        shutil.make_archive(str(temp_zip_path), "zip", root_dir=source_dir_path)

        # Move the created zip file to the correct output filename
        temp_zip_path_with_extension = temp_zip_path.with_suffix(".zip")
        temp_zip_path_with_extension.rename(output_filename_path)

        logger.info(f"Directory {source_dir} zipped as {output_filename_path}.")
        return True
    except Exception as e:
        logger.error(f"Failed to zip the directory {source_dir}: {e}")
        return False
