"""Tests for init_aws_ec2_db.py module."""

import sqlite3

import pytest

from rdsa_utils.helpers.pyspark_log_parser.ec2_pricing import InstanceType
from rdsa_utils.helpers.pyspark_log_parser.init_aws_ec2_db import (
    init_db,
    update_pricing_data,
)


class TestInitDb:
    """Tests for init_db function."""

    def test_init_db(self, tmp_path):
        """Test initialising the database schema."""
        db_path = tmp_path / "test_db.sqlite"
        init_db(db_path)
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = {row[0] for row in cursor.fetchall()}
            assert "instance_types" in tables
            assert "metadata" in tables


class TestUpdatePricingData:
    """Tests for update_pricing_data function."""

    def test_update_pricing_data(self, tmp_path):
        """Test updating the database with new pricing data."""
        db_path = tmp_path / "test_db.sqlite"
        init_db(db_path)
        instances = [
            InstanceType(
                name="m5.large",
                vcpu=2,
                memory_gb=8,
                ec2_price=0.096,
                family="General Purpose",
            ),
            InstanceType(
                name="m5.xlarge",
                vcpu=4,
                memory_gb=16,
                ec2_price=0.192,
                family="General Purpose",
            ),
        ]
        update_pricing_data(db_path, instances)
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM instance_types")
            count = cursor.fetchone()[0]
            assert count == 2
            cursor.execute("SELECT value FROM metadata WHERE key='last_updated'")
            last_updated = cursor.fetchone()[0]
            assert last_updated is not None


@pytest.mark.initialise(reason="Not required to test main function.")
class TestMain:
    """Tests for main function."""

    def test_main(self):
        """Test main function."""
        pass
