import subprocess
from typing import Optional, Union


def delete_file(path: str) -> Union[bool, None]:
    """
    Deletes a file using 'hadoop fs -rm' command.

    This function attempts to delete a file located at `path` using Hadoop Filesystem command.
    If the operation is successful, the function will return `True`. If the file does
    not exist, the function will return `False`.

    Parameters
    ----------
    path : str
        The path to the file to be deleted.

    Returns
    -------
    bool or None
        Returns `True` if the file is successfully deleted, `False` otherwise.

    Examples
    --------
    >>> delete_file('/path/to/file')
    True
    """
    try:
        result = subprocess.run(["hadoop", "fs", "-rm", path], check=True)
        if result.returncode == 0:
            return True
        else:
            return False
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while trying to delete the file: {e}")
        return None


def file_exists(path: str) -> bool:
    """
    Checks whether a file exists in Hadoop filesystem.

    Parameters
    ----------
    path : str
        Path to the file.

    Returns
    -------
    bool
        True if file exists, False otherwise.
    """
    return subprocess.call([f"hadoop fs -test -e {path}"], shell=True) == 0


def create_txt_from_string(
    path: str, string_to_write: str, replace: Optional[bool] = False
) -> None:
    """
    Create a new text file and populate with a given string in HDFS using subprocess call to execute Hadoop commands.

    Parameters
    ----------
    path : str
        The path to the new file to be created, for example, '/some/directory/newfile.txt'.
    string_to_write : str
        The string that will populate the new text file.
    replace : bool, optional
        Flag determining whether an existing file should be replaced. Defaults to False.

    Returns
    -------
    None
        This function doesn't return anything; it's used for its side effect of creating a text file.

    Raises
    ------
    FileNotFoundError
        If `replace` is False and the file already exists.
    """
    if replace and file_exists(path):
        delete_file(path)
    elif not replace and file_exists(path):
        raise FileNotFoundError(
            f"File {path} already exists and replace is set to False."
        )

    subprocess.call([f'echo "{string_to_write}" | hadoop fs -put - {path}'], shell=True)
