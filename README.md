# rdsa-utils

A suite of pyspark, pandas and general pipeline utils for **Reproducible Data Science and Analysis (RDSA)** projects.

`rdsa-utils` is a Python codebase built with Python 3.7 and higher, and uses Poetry for dependency management and packaging.

## Prerequisites

- Python 3.7 or higher
- Poetry

## Getting Started for Developers

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
