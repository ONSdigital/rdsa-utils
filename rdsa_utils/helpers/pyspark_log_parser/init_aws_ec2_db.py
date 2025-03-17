"""Module to initialise SQLite database with EC2 pricing data from AWS."""

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from rdsa_utils.helpers.pyspark_log_parser.ec2_pricing import (
    InstanceType,
    fetch_from_aws,
)

logger = logging.getLogger(__name__)


def init_db(db_path: Path) -> None:
    """Initialise the SQLite database with schema."""
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # Create instance types table
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS instance_types (
            name TEXT PRIMARY KEY,
            vcpu INTEGER,
            memory_gb REAL,
            ec2_price REAL,
            family TEXT,
            last_updated TIMESTAMP
        )
        """,
        )

        # Create metadata table to track last update
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """,
        )

        conn.commit()


def update_pricing_data(db_path: Path, instances: List[InstanceType]) -> None:
    """Update the database with new pricing data."""
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # Begin transaction
        cursor.execute("BEGIN TRANSACTION")

        try:
            # Clear existing data
            cursor.execute("DELETE FROM instance_types")

            # Update instance types using a dictionary to ensure uniqueness
            instances_dict = {instance.name: instance for instance in instances}

            # Insert unique instances
            for instance in instances_dict.values():
                cursor.execute(
                    """
                INSERT OR REPLACE INTO instance_types
                (name, vcpu, memory_gb, ec2_price, family, last_updated)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        instance.name,
                        instance.vcpu,
                        instance.memory_gb,
                        instance.ec2_price,
                        instance.family,
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )

            # Update last_updated metadata
            cursor.execute(
                """
            INSERT OR REPLACE INTO metadata (key, value)
            VALUES ('last_updated', ?)
            """,
                (datetime.now(timezone.utc).isoformat(),),
            )

            # Commit transaction
            cursor.execute("COMMIT")

            logger.info(
                f"Successfully updated {len(instances)} instances in the database",
            )

        except Exception as e:
            cursor.execute("ROLLBACK")
            logger.info(f"Error updating database: {e}")
            raise


def main() -> None:
    """Initialise the database with EC2 pricing data."""
    # Create data directory if it doesn't exist
    module_dir = Path(__file__).parent
    data_dir = module_dir / "data"
    data_dir.mkdir(exist_ok=True)

    db_path = data_dir / "ec2_pricing.db"

    # Initialise database
    logger.info(f"Initializing database at: {db_path}")
    init_db(db_path)

    # Fetch pricing data from AWS
    instances = fetch_from_aws()

    if not instances:
        logger.info("Failed to fetch pricing data from AWS")
        return

    # Update database with fetched pricing
    update_pricing_data(db_path, instances)

    # Verify the update
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM instance_types")
        count = cursor.fetchone()[0]
        cursor.execute("SELECT value FROM metadata WHERE key='last_updated'")
        last_updated = cursor.fetchone()[0]

        logger.info("\nDatabase Update Summary:")
        logger.info(f"Total instances: {count}")
        logger.info(f"Last updated: {last_updated}")

        # logger.info some sample data
        logger.info("\nSample Instance Data:")
        cursor.execute(
            """
            SELECT name, vcpu, memory_gb, ec2_price, family
            FROM instance_types
            LIMIT 5
        """,
        )
        for row in cursor.fetchall():
            logger.info(f"Instance: {row[0]}")
            logger.info(f"  vCPU: {row[1]}")
            logger.info(f"  Memory: {row[2]}GB")
            logger.info(f"  Price: ${row[3]}/hour")
            logger.info(f"  Family: {row[4]}")


if __name__ == "__main__":
    main()
