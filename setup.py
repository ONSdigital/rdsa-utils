"""Setup script for creating package from code."""
from setuptools import setup

def read_requirements():
    """Read and return a list of requirements from 'requirements.txt'."""
    with open('requirements.txt') as f:
        return f.read().splitlines()

def get_extra_requirements():
    """Return extra requirements for development and documentation."""
    return {
        'dev': [
            'bump2version>=1.0.0',
            'pre-commit>=2.6.0',
            'ruff>=0.0.270',
            'chispa>=0.9.2',
            'coverage[toml]>=7.1.0',
            'pytest>=7.1.0, <8.0.0',  # Temporarily pin pytest due to https://github.com/TvoroG/pytest-lazy-fixture/issues/65
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
    }

if __name__ == '__main__':
    setup(
        install_requires=read_requirements(),
        extras_require=get_extra_requirements(),
    )
