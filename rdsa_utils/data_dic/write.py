from pathlib import Path

import markdown
from bs4 import BeautifulSoup
from jinja2 import Environment, FileSystemLoader

from rdsa_utils import PACKAGE_PATH


def markdown_file_to_html_with_theme(
    markdown_file: str, output_file: str = "data_dictionary.html"
) -> None:
    """Convert a markdown file to themed HTML using a Jinja2 template.

    Parameters
    ----------
    markdown_file : str
        Path to the input markdown file.
    output_file : str, optional
        Path to the output HTML file, defaults to "data_dictionary.html".

    Returns
    -------
    None
        This function writes the themed HTML to an output file.

    Raises
    ------
    FileNotFoundError
        If the input markdown file or Jinja2 template file does not exist.

    Notes
    -----
    This function reads the contents of a markdown file, converts it to HTML using
    the Python Markdown library, extracts the H1 content using BeautifulSoup,
    loads a Jinja2 template, applies the template to the HTML content,
    and saves the themed HTML to an output file.

    The function also supports the conversion of Markdown tables to HTML tables
    using the `markdown.extensions.tables` extension.

    The input and output file paths can be either relative or absolute. If a relative
    path is given, it is interpreted as relative to the current working directory.

    Examples
    --------
    >>> markdown_file_to_html_with_theme("example.md", "template.html", "output.html")
    """
    # Convert the input file paths to Path objects
    markdown_path = Path(markdown_file)
    output_path = Path(output_file)

    # Hard-Coded Theme Path
    template_path = Path(PACKAGE_PATH / "data_dic" / "theme.html")

    # Raise an error if the input files do not exist
    if not markdown_path.exists():
        raise FileNotFoundError(f"Markdown file not found: {markdown_path}")
    if not template_path.exists():
        raise FileNotFoundError(f"Template file not found: {template_path}")

    # Read the content of the Markdown file
    with open(markdown_path, "r") as f:
        markdown_content = f.read()

    # Convert Markdown to HTML with tables extension
    html = markdown.markdown(markdown_content, extensions=["tables"])

    # Extract the H1 content using BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    h1_text = soup.h1.string if soup.h1 else ""

    # Load the Jinja2 template
    templates_path = template_path.parent
    env = Environment(loader=FileSystemLoader(str(templates_path)))
    template = env.get_template(template_path.name)

    # Apply the template to the HTML content
    themed_html = template.render(content=html, title=h1_text)

    # Save the themed HTML to an output file
    with open(output_path, "w") as f:
        f.write(themed_html)
