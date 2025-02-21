"""Tests for ec2_pricing module."""

import sqlite3

import pytest

from rdsa_utils.helpers.pyspark_log_parser.ec2_pricing import (
    InstanceType,
    calculate_emr_surcharge,
    calculate_pipeline_cost,
    extract_instance_specs,
    fetch_from_aws,
    fetch_from_sqlite,
    fetch_pricing,
    get_matching_instance,
)


class TestCalculateEmrSurcharge:
    """Tests for calculate_emr_surcharge function."""

    def test_general_purpose(self):
        """Test surcharge for General Purpose family."""
        assert calculate_emr_surcharge("General Purpose", 1.0) == 1.25

    def test_compute_optimized(self):
        """Test surcharge for Compute Optimized family."""
        assert calculate_emr_surcharge("Compute Optimized", 1.0) == 1.25

    def test_memory_optimized(self):
        """Test surcharge for Memory Optimized family."""
        assert calculate_emr_surcharge("Memory Optimized", 1.0) == 1.25

    def test_unknown_family(self):
        """Test surcharge for unknown family."""
        assert calculate_emr_surcharge("Unknown Family", 1.0) == 1.25


class TestCalculatePipelineCost:
    """Tests for calculate_pipeline_cost function."""

    def test_calculate_pipeline_cost_with_surcharge(self):
        """Test pipeline cost calculation with EMR surcharge."""
        parsed_metrics = {
            "Timestamp": 1739793526775,
            "Pipeline Name": "ExamplePipeline",
            "Total Cores": 8,
            "Total Memory": 12,
            "Start Time": 1739793526775,
            "End Time": 1739793626775,
        }
        result = calculate_pipeline_cost(
            parsed_metrics,
            fetch_data=False,
            apply_emr_surcharge=True,
        )
        assert result["surcharge_applied"] is True

    def test_calculate_pipeline_cost_without_surcharge(self):
        """Test pipeline cost calculation without EMR surcharge."""
        parsed_metrics = {
            "Timestamp": 1739793526775,
            "Pipeline Name": "ExamplePipeline",
            "Total Cores": 8,
            "Total Memory": 12,
            "Start Time": 1739793526775,
            "End Time": 1739793626775,
        }
        result = calculate_pipeline_cost(
            parsed_metrics,
            fetch_data=False,
            apply_emr_surcharge=False,
        )
        assert result["surcharge_applied"] is False


class TestExtractInstanceSpecs:
    """Tests for extract_instance_specs function."""

    def test_valid_instance_type(self):
        """Test extraction of valid instance type specs."""
        specs = extract_instance_specs("m5.xlarge")
        assert specs == {
            "vcpu": 4,
            "memory_gb": 16,
            "family": "General Purpose",
        }

    def test_invalid_instance_type(self):
        """Test extraction of invalid instance type specs."""
        specs = extract_instance_specs("invalid.type")
        assert specs is None


class TestFetchFromSqlite:
    """Tests for fetch_from_sqlite function."""

    def test_fetch_from_sqlite(self, mocker):
        """Test fetching instance data from SQLite."""
        mocker.patch(
            "rdsa_utils.helpers.pyspark_log_parser.ec2_pricing.get_db_path",
            return_value=":memory:",
        )
        mocker.patch("sqlite3.connect", side_effect=sqlite3.connect)
        instances = fetch_from_sqlite()
        assert isinstance(instances, list)


class TestFetchFromAws:
    """Tests for fetch_from_aws function."""

    def test_fetch_from_aws(self, mocker):
        """Test fetching instance data from AWS."""
        mocker.patch(
            "requests.get",
            return_value=mocker.Mock(status_code=200, json=lambda: {}),
        )
        instances = fetch_from_aws()
        assert isinstance(instances, list)


class TestFetchPricing:
    """Tests for fetch_pricing function."""

    def test_fetch_pricing_from_aws(self, mocker):
        """Test fetching pricing data from AWS."""
        mocker.patch(
            "rdsa_utils.helpers.pyspark_log_parser.ec2_pricing.fetch_from_aws",
            return_value=[],
        )
        instances = fetch_pricing(fetch_data=True)
        assert isinstance(instances, list)

    def test_fetch_pricing_from_sqlite(self, mocker):
        """Test fetching pricing data from SQLite."""
        mocker.patch(
            "rdsa_utils.helpers.pyspark_log_parser.ec2_pricing.fetch_from_sqlite",
            return_value=[],
        )
        instances = fetch_pricing(fetch_data=False)
        assert isinstance(instances, list)


class TestGetMatchingInstance:
    """Tests for get_matching_instance function."""

    def test_get_matching_instance(self, mocker):
        """Test finding a matching instance."""
        mocker.patch(
            "rdsa_utils.helpers.pyspark_log_parser.ec2_pricing.fetch_pricing",
            return_value=[
                InstanceType(
                    name="m5.xlarge",
                    vcpu=4,
                    memory_gb=16,
                    ec2_price=0.192,
                    family="General Purpose",
                ),
            ],
        )
        instance = get_matching_instance(memory_gb=8, cores=4, fetch_data=False)
        assert instance.name == "m5.xlarge"

    def test_no_matching_instance(self, mocker):
        """Test no matching instance found."""
        mocker.patch(
            "rdsa_utils.helpers.pyspark_log_parser.ec2_pricing.fetch_pricing",
            return_value=[],
        )
        with pytest.raises(
            ValueError,
            match="No instances available to match the requirements.",
        ):
            get_matching_instance(memory_gb=8, cores=4, fetch_data=False)
