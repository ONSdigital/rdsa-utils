# Contribution Guide

We welcome contributions. To contribute, please follow these guidelines:

## Pull Requests

- All pull requests should be made to the `dev` branch, named `dev_<feature_name>`.
- Please make sure that your code passes all unit tests before submitting a pull request.
- Include unit tests with your code changes whenever possible, preferably written in [pytest](https://docs.pytest.org/en/stable/) format.
- Make sure that all existing unit tests still pass with your code changes.
- Please ensure that your code is compliant with the project's coding style guidelines, which include:
  - Writing docstrings in [Scipy/numpy style format](https://numpydoc.readthedocs.io/en/latest/format.html).
  - Using [type hints](https://docs.python.org/3/library/typing.html) in Python functions.
  - Adhering to the [PEP 8 style guide](https://www.python.org/dev/peps/pep-0008/) for Python code.
  - No wildcard imports.
  - Import PySpark functions as `F`.
  - Providing well-documented and easy-to-understand code, including clear variable and function names, as well as explanatory comments where necessary.
- If you are making a significant change to the codebase, please make sure to update the documentation to reflect the changes.
- If you are adding new functionality, please provide examples of how to use it in the project's documentation or in a separate README file.
- If you are fixing a bug, please include a description of the bug and how your changes address it.
- If you are adding a new dependency, please include a brief explanation of why it is necessary and what it does.
- If you are making significant changes to the project's architecture or design, please discuss your ideas with the project maintainers first to ensure they align with the project's goals and vision.

## Issues

If you find a bug or would like to request a feature, please open an issue on the project's [GitHub page](https://github.com/ONSdigital/rdsa-utils/issues). When opening an issue, please provide as much detail as possible, including:

- A clear and descriptive title.
- A description of the problem you're experiencing, including steps to reproduce it.
- Any error messages or logs related to the issue.
- Your operating system and Python version (if relevant).

Please search through the existing issues before opening a new one to avoid duplicates. If you find an existing issue that covers your problem, please add any additional information as a comment. Issues will be triaged and prioritized by the project maintainers.

If you would like to contribute to the project by fixing an existing issue, please leave a comment on the issue to let the maintainers know that you are working on it.

## Getting Started

### Installing Python

Before getting started, you need to have Python 3.7 or higher installed on your system. You can download Python from the [official website](https://www.python.org/downloads/). Make sure to add Python to your `PATH` during the installation process.

Alternatively, you can use [Anaconda](https://www.anaconda.com/download) to create a Python 3.7 or higher virtual environment. Anaconda is a popular Python distribution that comes with many pre-installed scientific computing packages and tools. Here's how to create a new environment with Anaconda:

1. Download and install Anaconda from the [official website](https://www.anaconda.com/download).
2. Open the Anaconda prompt.
3. Create a new virtual environment with Python 3.7 or higher:

    ```
    conda create --name myenv python=3.7
    ```
4. Activate the virtual environment:

    ```
    conda activate myenv
    ```

### Installing Poetry

Poetry is a dependency management and packaging tool for Python projects. To install Poetry into your virtual environment, use the following command:

```
pip install poetry
```

For more detailed installation instructions and troubleshooting, please visit the [official Poetry documentation](https://python-poetry.org/docs/#installation).

### Clone the Repository

Clone the repository to your local machine:

```
git clone git@github.com:ONSdigital/rdsa-utils.git
cd rdsa-utils
```

### Set Up the Development Environment

To set up the development environment and install the required dependencies, run the following command:

```
poetry install
```

This command will create a virtual environment and install all the dependencies specified in the `pyproject.toml` file.

If you're working on **Google Cloud Platform (GCP)** and want to save memory on Cloud Shell, you can use the `--no-cache` option:

```
poetry install --no-cache
```

This command installs the required dependencies without using the cache, which can save memory on the Cloud Shell.

### Activate the Virtual Environment

To activate the virtual environment, run the following command:

```
poetry shell
```

### Running Tests

You can run tests (if any) using the following command:

```
poetry run pytest
```

### Installing Pre-commit Hooks in Your Development Environment

To improve code quality and maintain a consistent coding style, we use pre-commit hooks in this repository. The following steps will guide you through the process of setting up pre-commit hooks in your development environment.

#### Installation Steps

1. **Install pre-commit**: Open a terminal and run the following command to install the `pre-commit` package:

```
pip install pre-commit
```

This command installs the pre-commit tool, which manages the pre-commit hooks for this repository.

2. **Install pre-commit hooks**: Run the following command to install the pre-commit hooks defined in the `.pre-commit-config.yaml` file:

```
pre-commit install
```

This command sets up the pre-commit hooks to run automatically before each commit.

#### Usage

Now that you have installed the pre-commit hooks, they will run automatically before each commit. If any of the hooks fail, the commit will be aborted, and you will need to fix the issues before trying again.

You can also run the pre-commit hooks manually at any time using the following command:

```
pre-commit run --all-files
```

This command runs all the pre-commit hooks on all the files in the repository, allowing you to check your code before committing.

By following these steps, you can ensure that your development environment is set up to use the pre-commit hooks, helping to maintain a clean and consistent codebase.