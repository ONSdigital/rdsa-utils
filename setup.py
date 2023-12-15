"""Setup script for creating package from code."""
from setuptools import find_packages, setup

from rdsa_utils import __version__

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='rdsa-utils',
    version=__version__,
    author='Reproducible Data Science & Analysis, ONS',
    author_email = (
        'Diego.Lara.De.Andres@ons.gov.uk, '
        'Meg.Scammell@ons.gov.uk, '
        'Dominic.Bean@ons.gov.uk'
    ),
    description=(
        'A suite of pyspark, pandas and general pipeline utils '
        'for Reproducible Data Science and Analysis (RDSA) projects.'
    ),
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/ONSdigital/rdsa-utils',
    packages=find_packages(),
    python_requires='>=3.8, <3.11',
    install_requires=requirements,
    extras_require={
        'dev': [
            'bump2version>=1.0.0',
            'pre-commit>=2.6.0',
            'ruff>=0.0.270',
            'chispa>=0.9.2',
            'coverage[toml]>=7.1.0',
            'pytest>=7.1.0',
            'pytest-cov>=4.0.0',
            'pytest-lazy-fixture>=0.6.0',
            'pytest-mock>=3.8.0',
        ],
        'doc': [
            'mkdocs>=1.4.2',
            'mkdocs-tech-docs-template>=0.1.2',
            'mkdocstrings[python]>=0.22.0',
            'mkdocs-git-revision-date-localized-plugin>=1.2.1',
            'mkdocs-jupyter>=0.24.3',
        ],
    },
)
