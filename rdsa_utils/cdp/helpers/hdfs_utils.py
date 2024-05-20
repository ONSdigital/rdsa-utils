"""Utility functions for interacting with HDFS."""

import subprocess
from pathlib import Path
from typing import List, Optional


def _perform(command: List[str]) -> bool:
    """Execute a command via subprocess, capturing stdout and stderr.

    This function creates a subprocess with the provided command list, then
    communicates with it to retrieve the stdout and stderr. After the
    command execution, it checks the process's return code to determine success
    or failure. A zero return code indicates success.

    Parameters
    ----------
    command
        A list of command elements representing the system
        command to be executed. For example, ['ls', '-l', '/home/user'].

    Returns
    -------
    bool
        True if the command execution is successful (return code 0),
        otherwise False.

    Raises
    ------
    subprocess.TimeoutExpired
        If the process does not complete within the default timeout.
    """
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = process.communicate(
            timeout=15,
        )  # added a timeout for robustness
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate()
    return process.returncode == 0


def change_permissions(
    path: str,
    permission: str,
    recursive: bool = False,
) -> bool:
    """Change directory and file permissions in HDFS.

    Parameters
    ----------
    path
        The path to the file or directory in HDFS.
    permission
        The permission to be set, e.g., 'go+rwx' or '777'.
    recursive
        If True, changes permissions for all subdirectories and
        files within a directory.

    Returns
    -------
    bool
        True if the operation was successful (command return code 0),
        otherwise False.
    """
    command = ["hadoop", "fs", "-chmod"]
    if recursive:
        command.append("-R")
    command.extend([permission, path])
    return _perform(command)


def copy(from_path: str, to_path: str, overwrite: bool = False) -> bool:
    """Copy a file in HDFS.

    Parameters
    ----------
    from_path
        The source path of the file in HDFS.
    to_path
        The target path of the file in HDFS.
    overwrite
        If True, the existing file at the target path will be overwritten,
        default is False.

    Returns
    -------
    bool
        True if the operation was successful (command return code 0),
        otherwise False.

    Raises
    ------
    subprocess.TimeoutExpired
        If the process does not complete within the default timeout.
    """
    command = ["hadoop", "fs", "-cp"]
    if overwrite:
        command.append("-f")
    command.extend([from_path, to_path])
    return _perform(command)


def copy_local_to_hdfs(from_path: str, to_path: str) -> bool:
    """Copy a local file to HDFS.

    Parameters
    ----------
    from_path
        The path to the local file.
    to_path
        The path to the HDFS directory where the file will be copied.

    Returns
    -------
    bool
        True if the operation was successful (command return code 0),
        otherwise False.
    """
    command = ["hadoop", "fs", "-copyFromLocal", from_path, to_path]
    return _perform(command)


def create_dir(path: str) -> bool:
    """Create a directory in HDFS.

    Parameters
    ----------
    path
        The HDFS path where the directory should be created.

    Returns
    -------
    bool
        True if the operation is successful (directory created),
        otherwise False.
    """
    command = ["hadoop", "fs", "-mkdir", path]
    return _perform(command)


def create_txt_from_string(
    path: str,
    string_to_write: str,
    replace: Optional[bool] = False,
) -> None:
    """Create and populate a text file in HDFS.

    Parameters
    ----------
    path
        The path to the new file to be created, for example,
        '/some/directory/newfile.txt'.
    string_to_write
        The string that will populate the new text file.
    replace
        Flag determining whether an existing file should be replaced.
        Defaults to False.

    Returns
    -------
    None
        This function doesn't return anything; it's used for its side effect
        of creating a text file.

    Raises
    ------
    FileNotFoundError
        If `replace` is False and the file already exists.
    """
    if replace and file_exists(path):
        delete_file(path)
    elif not replace and file_exists(path):
        msg = f"File {path} already exists and replace is set to False."
        raise FileNotFoundError(
            msg,
        )

    subprocess.call(
        [f'echo "{string_to_write}" | hadoop fs -put - {path}'],
        shell=True,
    )


def delete_dir(path: str) -> bool:
    """Delete an empty directory from HDFS.

    This function attempts to delete an empty directory in HDFS.
    If the directory is not empty, the deletion will fail.

    Parameters
    ----------
    path
        The HDFS path to the directory to be deleted.

    Returns
    -------
    bool
        True if the operation is successful (directory deleted),
        otherwise False.

    Note
    ----
    This function will only succeed if the directory is empty.
    To delete directories containing files or other directories,
    consider using `delete_path` instead.
    """
    command = ["hadoop", "fs", "-rmdir", path]
    return _perform(command)


def delete_file(path: str) -> bool:
    """Delete a specific file in HDFS.

    This function is used to delete a single file located
    at the specified HDFS path. If the path points to a
    directory, the command will fail.

    Parameters
    ----------
    path
        The path to the file in HDFS to be deleted.

    Returns
    -------
    bool
        True if the file was successfully deleted (command return code 0),
        otherwise False.

    Raises
    ------
    subprocess.TimeoutExpired
        If the process does not complete within the default timeout.

    Note
    ----
    This function is intended for files only. For directory deletions,
    use `delete_dir` or `delete_path`.
    """
    command = ["hadoop", "fs", "-rm", path]
    return _perform(command)


def delete_path(path: str) -> bool:
    """Delete a file or directory in HDFS, including non-empty directories.

    This function is capable of deleting both files and directories.
    When applied to directories, it will recursively delete all contents
    within the directory, making it suitable for removing directories regardless
    of whether they are empty or contain files or other directories.

    Parameters
    ----------
    path
        The path to the file or directory in HDFS to be deleted.

    Returns
    -------
    bool
        True if the file was successfully deleted (command return code 0),
        otherwise False.

    Raises
    ------
    subprocess.TimeoutExpired
        If the process does not complete within the default timeout.

    Warning
    -------
    Use with caution: applying this function to a directory will
    remove all contained files and subdirectories without confirmation.
    """
    command = ["hadoop", "fs", "-rm", "-r", path]
    return _perform(command)


def file_exists(path: str) -> bool:
    """Check whether a file exists in HDFS.

    Parameters
    ----------
    path
        The path to the file in HDFS to be checked for existence.

    Returns
    -------
    bool
        True if the file exists (command return code 0), otherwise False.

    Raises
    ------
    subprocess.TimeoutExpired
        If the process does not complete within the default timeout.
    """
    command = ["hadoop", "fs", "-test", "-e", path]
    return _perform(command)


def get_date_modified(filepath: str) -> str:
    """Return the last modified date of a file in HDFS.

    Parameters
    ----------
    filepath
        The path to the file in HDFS.

    Returns
    -------
    str
        The date the file was last modified.
    """
    command = subprocess.Popen(
        f"hadoop fs -stat %y {filepath}",
        stdout=subprocess.PIPE,
        shell=True,
    )
    return command.stdout.read().decode("utf-8")[0:10]


def is_dir(path: str) -> bool:
    """Test if a directory exists in HDFS.

    Parameters
    ----------
    path
        The HDFS path to the directory to be tested.

    Returns
    -------
    bool
        True if the operation is successful (directory exists), otherwise False.
    """
    command = ["hadoop", "fs", "-test", "-d", path]
    return _perform(command)


def move_local_to_hdfs(from_path: str, to_path: str) -> bool:
    """Move a local file to HDFS.

    Parameters
    ----------
    from_path
        The path to the local file.
    to_path
        The path to the HDFS directory where the file will be moved.

    Returns
    -------
    bool
        True if the operation was successful (command return code 0),
        otherwise False.
    """
    command = ["hadoop", "fs", "-moveFromLocal", from_path, to_path]
    return _perform(command)


def read_dir(path: str) -> List[str]:
    """Read the contents of a directory in HDFS.

    Parameters
    ----------
    path
        The path to the directory in HDFS.

    Returns
    -------
    List[str]
        A list of full paths of the items found in the directory.
    """
    ls = subprocess.Popen(["hadoop", "fs", "-ls", path], stdout=subprocess.PIPE)
    files = [
        line.decode("utf-8").split()[-1]
        for line in ls.stdout
        if "Found" not in line.decode("utf-8")
    ]
    return files


def read_dir_files(path: str) -> List[str]:
    """Read the filenames in a directory in HDFS.

    Parameters
    ----------
    path
        The path to the directory in HDFS.

    Returns
    -------
    List[str]
        A list of filenames in the directory.
    """
    return [Path(p).name for p in read_dir(path)]


def read_dir_files_recursive(path: str, return_path: bool = True) -> List[str]:
    """Recursively reads the contents of a directory in HDFS.

    Parameters
    ----------
    path
        The path to the directory in HDFS.
    return_path
        If True, returns the full path of the files, otherwise
        just the filename.

    Returns
    -------
    List[str]
        A list of files in the directory.
    """
    command = subprocess.Popen(
        f"hadoop fs -ls -R {path} | grep -v ^d | tr -s ' ' | cut -d ' ' -f 8-",
        stdout=subprocess.PIPE,
        shell=True,
    )
    object_list = [obj.decode("utf-8") for obj in command.stdout.read().splitlines()]

    if not return_path:
        return [Path(path).name for path in object_list]

    else:
        return object_list


def rename(from_path: str, to_path: str, overwrite: bool = False) -> bool:
    """Rename (i.e., move using full path) a file in HDFS.

    Parameters
    ----------
    from_path
        The source path of the file in HDFS.
    to_path
        The target path of the file in HDFS.
    overwrite
        If True, the existing file at the target path will be overwritten,
        default is False.

    Returns
    -------
    bool
        True if the operation was successful (command return code 0),
        otherwise False.

    Raises
    ------
    subprocess.TimeoutExpired
        If the process does not complete within the default timeout.
    """
    if overwrite:
        delete_file(to_path)

    command = ["hadoop", "fs", "-mv", from_path, to_path]
    return _perform(command)
