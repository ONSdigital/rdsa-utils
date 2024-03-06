This site contains the project documentation for `rdsa-utils`, a suite of pyspark, pandas, and general pipeline utils for **Reproducible Data Science and Analysis (RDSA)** projects.


## Table Of Contents

1. [API Reference](reference.md)
2. [Contribution Guide](contribution_guide.md)
2. [Branching & Deployment Guide](branch_and_deploy_guide.md)

Quickly find what you're looking for depending on your use case by looking at the different pages.

## Prerequisites

The following prerequisites are required for `rdsa-utils`:

- Python 3.8 or higher

## Dependency Update: PySpark

To optimise the installation process and accommodate users with pre-installed environments, 
`pyspark` is now classified as a **development dependency**. This adjustment avoids 
potential conflicts in environments where `pyspark` is already available, 
such as **Cloudera Data Platform**.

### For Users:

- **If your environment does not have `pyspark` pre-installed:** You will need to 
manually install `pyspark` to utilise features dependent on it. This can be done by 
running `pip install pyspark==<version>` when setting up your environment, 
replacing `<version>` with the specific version required for your project.

- **If `pyspark` is pre-installed in your environment:** No additional action is required. 
This change ensures seamless integration without overwriting or conflicting with the 
existing `pyspark` installation.

This modification streamlines `rdsa-utils` for various use cases, enhancing both 
flexibility and user experience.
