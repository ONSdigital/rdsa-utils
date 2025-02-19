"""Module to fetch and parse EC2 pricing data from AWS API."""

import logging
import re
import sqlite3
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


def calculate_emr_surcharge(instance_family: str, ec2_price: float) -> float:
    """Calculate EMR pricing surcharge based on instance family.

    Parameters
    ----------
    instance_family
        The instance family (e.g., 'General Purpose', 'Compute Optimized')
    ec2_price
        The base EC2 price per hour

    Returns
    -------
    float
        The EMR price per hour including surcharge
    """
    # EMR pricing typically adds a percentage surcharge on top of EC2 price
    # These percentages are approximations and might need adjustment
    surcharge_rates = {
        "General Purpose": 0.25,  # 25% surcharge
        "Compute Optimized": 0.25,  # 25% surcharge
        "Memory Optimized": 0.25,  # 25% surcharge
        "Storage Optimized": 0.25,  # 25% surcharge
        "Accelerated Computing": 0.25,  # 25% surcharge
    }

    surcharge_rate = surcharge_rates.get(
        instance_family,
        0.25,
    )  # Default to 25% if family not found
    return ec2_price * (1 + surcharge_rate)


def calculate_pipeline_cost(
    parsed_metrics: Dict[str, Any],
    fetch_data: bool = False,
) -> Dict[str, Any]:
    """Calculate EMR costs from parsed Spark metrics.

    Parameters
    ----------
    parsed_metrics
        Output from parse_pyspark_logs function containing parsed metrics
    fetch_data
        Whether to fetch fresh data from AWS

    Returns
    -------
    Dict[str, Any]
        Cost analysis including instance recommendations and costs
    """
    # Extract memory and cores configuration from parsed metrics
    memory_per_executor = parsed_metrics.get("Memory Per Executor", 0)  # Already in GB
    total_cores = parsed_metrics.get("Total Cores", 0)
    total_executors = parsed_metrics.get("Total Executors", 1)

    # Calculate total memory required
    total_memory_gb = memory_per_executor * total_executors

    # Convert runtime from minutes to hours
    runtime_ms = (
        parsed_metrics.get("Executor Run Time", 0) * 60 * 1000
    )  # Convert minutes to ms
    runtime_hours = runtime_ms / (1000 * 60 * 60)

    # Get peak memory in GB (already converted in parse_pyspark_logs)
    peak_memory_gb = parsed_metrics.get("Peak Execution Memory", 0)

    # Get matching instance type
    instance = get_matching_instance(
        memory_gb=total_memory_gb,
        cores=total_cores,
        fetch_data=fetch_data,
    )
    if not instance:
        error_msg = (
            f"No suitable instance type found for {total_memory_gb}GB memory "
            f"and {total_cores} cores",
        )
        raise ValueError(error_msg)

    # Calculate EMR price based on EC2 price
    emr_price = calculate_emr_surcharge(instance.family, instance.ec2_price)

    # Calculate costs
    pipeline_cost = runtime_hours * emr_price

    return {
        "configuration": {
            "memory_requested_gb": total_memory_gb,
            "cores_requested": total_cores,
            "peak_memory_gb": peak_memory_gb,
            "total_executors": total_executors,
        },
        "instance_recommendation": {
            "type": instance.name,
            "family": instance.family,
            "vcpu": instance.vcpu,
            "memory_gb": instance.memory_gb,
            "ec2_price": instance.ec2_price,
            "emr_price": emr_price,
        },
        "runtime": {"milliseconds": runtime_ms, "hours": runtime_hours},
        "costs": {
            "pipeline_cost": round(pipeline_cost, 4),
            "ec2_cost": round(runtime_hours * instance.ec2_price, 4),
            "emr_surcharge": round(runtime_hours * (emr_price - instance.ec2_price), 4),
        },
        "utilization": {
            "memory_utilization": (
                peak_memory_gb / total_memory_gb if total_memory_gb > 0 else 0
            ),
            "cost_per_hour": emr_price,
        },
    }


@dataclass
class InstanceType:
    """Represents an EC2 instance type with its specifications and pricing."""

    name: str
    vcpu: int
    memory_gb: float
    ec2_price: float
    family: str = ""


def get_db_path() -> Path:
    """Get the path to the SQLite database."""
    module_dir = Path(__file__).parent
    return module_dir / "data" / "ec2_pricing.db"


def extract_instance_specs(instance_type: str) -> Optional[Dict]:
    """Extract vCPU and memory specs from instance type naming convention."""
    # Parse instance size multiplier
    size_multipliers = {
        "nano": 0.25,
        "micro": 0.5,
        "small": 1,
        "medium": 1,
        "large": 2,
        "xlarge": 4,
        "2xlarge": 8,
        "3xlarge": 12,
        "4xlarge": 16,
        "8xlarge": 32,
        "9xlarge": 36,
        "12xlarge": 48,
        "16xlarge": 64,
        "18xlarge": 72,
        "24xlarge": 96,
        "metal": 96,
    }

    # Parse instance family base specs
    family_base_specs = {
        "t3": {"vcpu": 2, "memory_gb": 0.5, "family": "General Purpose"},
        "t4g": {"vcpu": 2, "memory_gb": 0.5, "family": "General Purpose"},
        "m4": {"vcpu": 2, "memory_gb": 8, "family": "General Purpose"},
        "m5": {"vcpu": 2, "memory_gb": 8, "family": "General Purpose"},
        "m5a": {"vcpu": 2, "memory_gb": 8, "family": "General Purpose"},
        "m5d": {"vcpu": 2, "memory_gb": 8, "family": "General Purpose"},
        "m6a": {"vcpu": 2, "memory_gb": 8, "family": "General Purpose"},
        "r4": {"vcpu": 2, "memory_gb": 15.25, "family": "Memory Optimized"},
        "r5": {"vcpu": 2, "memory_gb": 16, "family": "Memory Optimized"},
        "r5a": {"vcpu": 2, "memory_gb": 16, "family": "Memory Optimized"},
        "r5b": {"vcpu": 2, "memory_gb": 16, "family": "Memory Optimized"},
        "r6a": {"vcpu": 2, "memory_gb": 16, "family": "Memory Optimized"},
        "x2gd": {"vcpu": 2, "memory_gb": 32, "family": "Memory Optimized"},
        "c4": {"vcpu": 2, "memory_gb": 3.75, "family": "Compute Optimized"},
        "c5": {"vcpu": 2, "memory_gb": 4, "family": "Compute Optimized"},
        "c5a": {"vcpu": 2, "memory_gb": 4, "family": "Compute Optimized"},
        "c6a": {"vcpu": 2, "memory_gb": 4, "family": "Compute Optimized"},
    }

    # Parse instance type (e.g., "r6a.xlarge")
    match = re.match(r"([a-z]+\d+[a-z]*?)\.([a-z0-9]+)", instance_type)
    if not match:
        return None

    family, size = match.groups()

    if family not in family_base_specs or size not in size_multipliers:
        return None

    base_specs = family_base_specs[family]
    multiplier = size_multipliers[size]

    return {
        "vcpu": int(base_specs["vcpu"] * multiplier),
        "memory_gb": base_specs["memory_gb"] * multiplier,
        "family": base_specs["family"],
    }


def fetch_from_sqlite() -> List[InstanceType]:
    """Fetch EC2 pricing data from local SQLite database."""
    instances = []
    db_path = get_db_path()

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT name, vcpu, memory_gb, ec2_price, family
                FROM instance_types
            """,
            )

            for row in cursor.fetchall():
                instances.append(
                    InstanceType(
                        name=row[0],
                        vcpu=row[1],
                        memory_gb=row[2],
                        ec2_price=row[3],
                        family=row[4],
                    ),
                )

        return instances
    except Exception as e:
        logger.error(f"Error fetching from SQLite: {e}")
        return []


def fetch_from_aws() -> List[InstanceType]:
    """Fetch EC2 pricing data from AWS API."""
    url = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/eu-west-2/index.json"

    try:
        logger.info("Fetching EC2 pricing data from AWS...")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        logger.info("Processing instance data...")

        # Dictionary to store unique instances with their lowest price
        unique_instances = {}
        products = data.get("products", {})

        # Get EC2 Linux instances
        instance_products = {}
        for sku, product in products.items():
            attrs = product.get("attributes", {})
            if (
                attrs.get("operatingSystem") == "Linux"
                and attrs.get("tenancy") == "Dedicated"
                and attrs.get("capacitystatus") == "Used"
            ):

                instance_type = attrs.get("instanceType")
                if instance_type:
                    instance_products[sku] = {
                        "type": instance_type,
                        "family": attrs.get("instanceFamily", ""),
                    }

        # Get pricing
        terms = data.get("terms", {}).get("OnDemand", {})
        for sku, product_info in instance_products.items():
            if sku in terms:
                price_dims = list(terms[sku].values())[0].get("priceDimensions", {})
                if price_dims:
                    price_info = list(price_dims.values())[0]
                    base_price = float(price_info.get("pricePerUnit", {}).get("USD", 0))

                    instance_type = product_info["type"]
                    specs = extract_instance_specs(instance_type)

                    if specs:
                        # Only keep the lowest price for each instance type
                        if (
                            instance_type not in unique_instances
                            or base_price < unique_instances[instance_type].ec2_price
                        ):

                            unique_instances[instance_type] = InstanceType(
                                name=instance_type,
                                vcpu=specs["vcpu"],
                                memory_gb=specs["memory_gb"],
                                ec2_price=base_price,
                                family=specs["family"],
                            )

        return list(unique_instances.values())

    except Exception as e:
        logger.info(f"Error fetching EC2 pricing from AWS: {e}")
        return []


@lru_cache(maxsize=1)
def fetch_pricing(fetch_data: bool = False) -> List[InstanceType]:
    """Fetch EC2 instance pricing from AWS or local SQLite database.

    Parameters
    ----------
    fetch_data
        If True, fetch from AWS API. If False, use local SQLite database.

    Returns
    -------
    List[InstanceType]
        List of instance types with pricing information
    """
    if fetch_data:
        instances = fetch_from_aws()
        if not instances:
            return fetch_from_sqlite()  # Fallback to SQLite if AWS fails
        return instances
    else:
        return fetch_from_sqlite()


def get_matching_instance(
    memory_gb: float,
    cores: int,
    instances: Optional[List[InstanceType]] = None,
    fetch_data: bool = False,
) -> Optional[InstanceType]:
    """Find most cost-effective instance meeting requirements.

    Parameters
    ----------
    memory_gb
        Required memory in GB
    cores
        Required number of CPU cores
    instances
        Pre-fetched list of instances (optional)
    fetch_data
        Whether to fetch fresh data from AWS

    Returns
    -------
    Optional[InstanceType]
        Most cost-effective instance meeting requirements
    """
    if instances is None:
        instances = fetch_pricing(fetch_data=fetch_data)

    if not instances:
        # Fallback to base instance if both AWS and SQLite fail
        return InstanceType("m5.xlarge", 4, 16, 0.192, "General Purpose")

    # Find instances that meet minimum requirements
    valid_instances = [
        i for i in instances if i.memory_gb >= memory_gb and i.vcpu >= cores
    ]

    if not valid_instances:
        return None

    # Return the cheapest valid instance
    return min(valid_instances, key=lambda x: x.ec2_price)
