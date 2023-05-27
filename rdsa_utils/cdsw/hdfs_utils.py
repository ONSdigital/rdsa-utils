"""
HDFS Utils
==========

This module provides utility functions for interacting with the Hadoop Distributed File System (HDFS). The utility functions 
are Python wrappers for HDFS shell commands, facilitating the programmatic manipulation of files and directories within HDFS. 

The primary functions in the module include:

- `_perform()`: Execute a system command using a subprocess, capturing the standard output and error.
- `change_permissions()`: Changes directory and file permissions in HDFS.
- `copy()`: Copies a file in the Hadoop filesystem.
- `copy_local_to_hdfs()`: Copies a local file to HDFS.
- `create_dir()`: Creates a directory in HDFS.
- `create_txt_from_string()`: Create a new text file and populate it with a given string in HDFS.
- `delete_dir()`: Deletes a directory from HDFS.
- `delete_file()`: Deletes a file in the Hadoop filesystem.
- `file_exists()`: Checks whether a file exists in HDFS.
- `get_date_modified()`: Returns the last modified date of a file in HDFS.
- `isdir()`: Tests if a directory exists in HDFS.
- `move_local_to_hdfs()`: Moves a local file to HDFS.
- `read_dir()`: Reads the contents of a directory in HDFS.
- `read_dir_files()`: Reads the filenames in a directory in HDFS.
- `read_dir_files_recursive()`: Recursively reads the contents of a directory in HDFS.
- `rename()`: Renames (i.e., moves using full path) a file in the Hadoop filesystem.
- `test()`: Tests if a file exists in HDFS.

Note
----
1. This module assumes that the 'hadoop' command is available on the system path where Python is being run, which means 
   Hadoop must be correctly installed on the system.

2. It's important to note that HDFS shell commands exhibit certain behaviors under specific conditions. For instance, 
   'hadoop fs -put' command will overwrite existing files. These behaviors are also reflected in this utility module.

3. Each function communicates with a subprocess to execute the required HDFS command, capturing the standard output and 
   error. The return codes from these subprocesses determine the success or failure of these operations, with a zero 
   return code indicating success. 

4. Timeouts are incorporated to ensure robustness. If a process does not complete within the specified timeout, a 
   subprocess.TimeoutExpired exception is raised.

Examples
--------
Here are a few examples on how these functions can be used:

1. Test if a file exists in HDFS:
    >>> test("/path/to/hdfs/file")

2. List files in a directory in HDFS:
    >>> read_dir("/path/to/hdfs/directory")

3. Change permissions of a file in HDFS:
    >>> change_permissions("/path/to/hdfs/file", "777")
"""
import os
import subprocess
from typing import List, Optional


def _perform(command: List[str]) -> bool:
    """
    Execute a system command using a subprocess, capturing the standard output and error.

    This function creates a subprocess with the provided command list, then communicates with it
    to retrieve the stdout and stderr. After the command execution, it checks the process's
    return code to determine success or failure. A zero return code indicates success.

    Parameters
    ----------
    command : List[str]
        A list of command elements representing the system command to be executed.
        For example, ['ls', '-l', '/home/user'].

    Returns
    -------
    bool
        True if the command execution is successful (return code 0), otherwise False.

    Raises
    ------
    subprocess.TimeoutExpired
        If the process does not complete within the default timeout.

    """
    try:
        process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate(
            timeout=15
        )  # added a timeout for robustness
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate()
    return process.returncode == 0


def change_permissions(path: str, permission: str, recursive: bool = False) -> bool:
    """
    Changes directory and file permissions in HDFS.

    Parameters
    ----------
    path : str
        The path to the file or directory in HDFS.
    permission : str
        The permission to be set, e.g., 'go+rwx' or '777'.
    recursive : bool, optional
        If True, changes permissions for all subdirectories and files within a directory.

    Returns
    -------
    bool
        True if the operation was successful (command return code 0), otherwise False.
    """
    command = ["hadoop", "fs", "-chmod"]
    if recursive:
        command.append("-R")
    command.extend([permission, path])
    return _perform(command)


def copy(from_path: str, to_path: str, overwrite: bool = False) -> bool:
    """
    Copy a file in the Hadoop filesystem.

    This function uses the Hadoop File System (HDFS) command 'cp'
    to copy a file from the specified source path to the target path.

    Parameters
    ----------
    from_path : str
        The source path of the file in HDFS.
    to_path : str
        The target path of the file in HDFS.
    overwrite : bool, optional
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
    """
    Copies a local file to HDFS.

    Parameters
    ----------
    from_path : str
        The path to the local file.
    to_path : str
        The path to the HDFS directory where the file will be copied.

    Returns
    -------
    bool
        True if the operation was successful (command return code 0), otherwise False.
    """
    command = ["hadoop", "fs", "-copyFromLocal", from_path, to_path]
    return _perform(command)


def create_dir(path: str) -> bool:
    """
    Creates a directory in the Hadoop Distributed File System (HDFS).

    This function wraps the 'hadoop fs -mkdir' command.

    Parameters
    ----------
    path : str
        The HDFS path where the directory should be created.

    Returns
    -------
    bool
        True if the operation is successful (directory created), otherwise False.
    """
    command = ["hadoop", "fs", "-mkdir", path]
    return _perform(command)


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


def delete_dir(path: str) -> bool:
    """
    Deletes a directory from the Hadoop Distributed File System (HDFS).

    This function wraps the 'hadoop fs -rmdir' command.

    Parameters
    ----------
    path : str
        The HDFS path to the directory to be deleted.

    Returns
    -------
    bool
        True if the operation is successful (directory deleted), otherwise False.
    """
    command = ["hadoop", "fs", "-rmdir", path]
    return _perform(command)


def delete_file(path: str) -> bool:
    """
    Delete a file in the Hadoop filesystem.

    This function uses the Hadoop File System (HDFS) command 'rm'
    to delete a file at the specified path.

    Parameters
    ----------
    path : str
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
    """
    command = ["hadoop", "fs", "-rm", path]
    return _perform(command)


def file_exists(path: str) -> bool:
    """
    Checks whether a file exists in the Hadoop filesystem.

    This function uses the Hadoop File System (HDFS) command 'test -e'
    to check the existence of a file at the specified path.

    Parameters
    ----------
    path : str
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
    """
    Returns the last modified date of a file in HDFS.

    Parameters
    ----------
    filepath : str
        The path to the file in HDFS.

    Returns
    -------
    str
        The date the file was last modified.
    """
    command = subprocess.Popen(
        f"hadoop fs -stat %y {filepath}", stdout=subprocess.PIPE, shell=True
    )
    return command.stdout.read().decode("utf-8")[0:10]


def isdir(path: str) -> bool:
    """
    Tests if a directory exists in the Hadoop Distributed File System (HDFS).

    This function wraps the 'hadoop fs -test -d' command.

    Parameters
    ----------
    path : str
        The HDFS path to the directory to be tested.

    Returns
    -------
    bool
        True if the operation is successful (directory exists), otherwise False.
    """
    command = ["hadoop", "fs", "-test", "-d", path]
    return _perform(command)


def move_local_to_hdfs(from_path: str, to_path: str) -> bool:
    """
    Moves a local file to HDFS.

    Parameters
    ----------
    from_path : str
        The path to the local file.
    to_path : str
        The path to the HDFS directory where the file will be moved.

    Returns
    -------
    bool
        True if the operation was successful (command return code 0), otherwise False.
    """
    command = ["hadoop", "fs", "-moveFromLocal", from_path, to_path]
    return _perform(command)


def read_dir(path: str) -> List[str]:
    """
    Reads the contents of a directory in HDFS.

    Parameters
    ----------
    path : str
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
    """
    Reads the filenames in a directory in HDFS.

    Parameters
    ----------
    path : str
        The path to the directory in HDFS.

    Returns
    -------
    List[str]
        A list of filenames in the directory.
    """
    files = [os.path.basename(path) for path in read_dir(path)]
    return files


def read_dir_files_recursive(path: str, return_path: bool = True) -> List[str]:
    """
    Recursively reads the contents of a directory in HDFS.

    Parameters
    ----------
    path : str
        The path to the directory in HDFS.
    return_path : bool, optional
        If True, returns the full path of the files, otherwise just the filename.

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
        return [os.path.basename(path) for path in object_list]
    else:
        return object_list


def rename(from_path: str, to_path: str, overwrite: bool = False) -> bool:
    """
    Rename (i.e., move using full path) a file in the Hadoop filesystem.

    This function uses the Hadoop File System (HDFS) command 'mv'
    to rename a file from the specified source path to the target path.

    Parameters
    ----------
    from_path : str
        The source path of the file in HDFS.
    to_path : str
        The target path of the file in HDFS.
    overwrite : bool, optional
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


def test(path: str) -> bool:
    """
    Tests if a file exists in the Hadoop Distributed File System (HDFS).

    This function wraps the 'hadoop fs -test -e' command.

    Parameters
    ----------
    path : str
        The HDFS path to the file to be tested.

    Returns
    -------
    bool
        True if the operation is successful (file exists), otherwise False.
    """
    command = ["hadoop", "fs", "-test", "-e", path]
    return _perform(command)
