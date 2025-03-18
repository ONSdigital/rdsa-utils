"""Generates a PySpark log analysis report using Papermill and nbconvert."""

import logging
from pathlib import Path
from typing import Dict, List

import papermill as pm
from nbconvert import HTMLExporter
from traitlets.config import Config

logger = logging.getLogger(__name__)

NOTEBOOK_TEMPLATE = str(Path(__file__).parent / "pyspark_log_report_template.ipynb")


def generate_report(logs_data: List[Dict], output_path: str) -> None:
    """Generate a PySpark log analysis report using Papermill.

    This function executes a Jupyter notebook template with the
    provided logs data, converts the executed notebook to HTML,
    and saves the HTML report to the specified output path.

    It ensures that specific cells (e.g., code cells) are ignored in
    the final HTML output.

    Parameters
    ----------
    logs_data
        A list of dictionaries containing PySpark log metrics and cost details.
    output_path
        The path where the generated HTML report will be saved.

    Examples
    --------
    >>> sample_logs = [
    >>>     {
    >>>         "file_path": "user/test/eventlog_v2_spark-1234/events_1_spark-1234",
    >>>         "log_metrics": {"Pipeline Name": "TestApp", "Timestamp": 1739793526775},
    >>>         "cost_metrics": {
    >>>             "runtime": {"milliseconds": 10000},
    >>>             "costs": {"pipeline_cost": 0.0001},
    >>>         },
    >>>     },
    >>>     {
    >>>         "file_path": "user/test/eventlog_v2_spark-5678/events_1_spark-5678",
    >>>         "log_metrics": {"Pipeline Name": "TestApp", "Timestamp": 1739793626775},
    >>>         "cost_metrics": {
    >>>             "runtime": {"milliseconds": 12000},
    >>>             "costs": {"pipeline_cost": 0.0002},
    >>>         },
    >>>     },
    >>> ]
    >>> generate_report(sample_logs, "/path/to/output.html")
    """
    # Execute the notebook with logs_data as a parameter
    executed_notebook = pm.execute_notebook(
        NOTEBOOK_TEMPLATE,
        None,
        parameters={"logs_data": logs_data},
        kernel_name="python3",  # Ensure a valid kernel is specified
    )

    # Configure nbconvert to remove input cells tagged as "hide_input"
    c = Config()
    c.HTMLExporter.exclude_input = True

    # Convert executed notebook to HTML while ignoring specific cells
    html_exporter = HTMLExporter(config=c)
    body, _ = html_exporter.from_notebook_node(executed_notebook)

    # Save HTML report
    output_path = Path(output_path)
    with output_path.open("w", encoding="utf-8") as f:
        f.write(body)

    logger.info(f"Report generated: {output_path}")
