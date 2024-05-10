# 🧰 rdsa-utils 

[![Deploy to PyPI](https://github.com/ONSdigital/rdsa-utils/actions/workflows/deploy_pypi.yaml/badge.svg?branch=main)](https://github.com/ONSdigital/rdsa-utils/actions/workflows/deploy_pypi.yaml)
[![Deploy MkDocs](https://github.com/ONSdigital/rdsa-utils/actions/workflows/deploy_mkdocs.yaml/badge.svg?branch=main)](https://github.com/ONSdigital/rdsa-utils/actions/workflows/deploy_mkdocs.yaml)
[![PyPI version](https://badge.fury.io/py/rdsa-utils.svg)](https://pypi.org/project/rdsa-utils/)

A suite of PySpark, Pandas, and general pipeline utils for **Reproducible Data Science and Analysis (RDSA)** projects.

The RDSA team sits within the Economic Statistics Change Directorate, and uses cutting-edge data science and engineering skills to produce the next generation of economic statistics. Current priorities include overhauling legacy systems and developing new systems for key statistics. More information about work at RDSA can be found here: [Using Data Science for Next-Gen Statistics](https://dataingovernment.blog.gov.uk/2023/02/14/using-data-science-for-next-gen-statistics/).

`rdsa-utils` is a Python codebase built with Python 3.8 and higher, and uses `setup.py`, `setup.cfg`, and `pyproject.toml` for dependency management and packaging.

## 📋 Prerequisites 

- Python 3.8 or higher

## 💾 Installation 

`rdsa-utils` is available for installation via [PyPI](https://pypi.org/project/rdsa-utils/) and can also be found on [GitHub Releases](https://github.com/ONSdigital/rdsa-utils/releases) for direct downloads and version history.

To install via `pip`, simply run:

```bash
pip install rdsa-utils
```

## 📖 Documentation and Further Information 

Our documentation is automatically generated using **GitHub Actions** and **MkDocs**. For an in-depth understanding of `rdsa-utils`, how to contribute to `rdsa-utils`, and more, please refer to our [MkDocs-generated documentation](https://onsdigital.github.io/rdsa-utils/).

## 📘 Further Reading on Reproducible Analytical Pipelines

While `rdsa-utils` provides essential tools for data processing, it's just one part of the broader development process needed to build and maintain a robust, high-quality codebase. Following best practices and using the right tools are crucial for success.

We highly recommend checking out the following resources to learn more about creating Reproducible Analytical Pipelines (RAP), which focus on important areas such as version control, modular code development, unit testing, and peer review -- all essential for developing these pipelines:

- [Reproducible Analytical Pipelines (RAP) Resource](https://analysisfunction.civilservice.gov.uk/support/reproducible-analytical-pipelines/) - This resource offers an overview of Reproducible Analytical Pipelines, covering benefits, case studies, and guidelines on building a RAP. It discusses minimising manual steps, using open source software like R or Python, enhancing quality assurance through peer review, and ensuring auditability with version control. It also addresses challenges and considerations for implementing RAPs, such as data access restrictions or confidentiality, and underscores the importance of collaborative development.

- [Quality Assurance of Code for Analysis and Research](https://best-practice-and-impact.github.io/qa-of-code-guidance/intro.html) - This book details methods and practices for ensuring high-quality coding in research and analysis, including unit testing and peer reviews.

- [PySpark Introduction and Training Book](https://best-practice-and-impact.github.io/ons-spark/intro.html) - An introduction to using PySpark for large-scale data processing.

## 🛡️ Licence

Unless stated otherwise, the codebase is released under the [MIT License][mit].
This covers both the codebase and any sample code in the documentation.

The documentation is [© Crown copyright][copyright] and available under the terms of the [Open Government 3.0][ogl] licence.

[mit]: LICENSE
[copyright]: http://www.nationalarchives.gov.uk/information-management/re-using-public-sector-information/uk-government-licensing-framework/crown-copyright/
[ogl]: http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/
