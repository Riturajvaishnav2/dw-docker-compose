"""
Iceberg catalog initialization utility for alembic migrations.

Provides dynamic catalog name resolution based on ICEBERG_NAMESPACE environment variable.
"""

import os
import logging

logger = logging.getLogger(__name__)


def get_catalog_name() -> str:
    """
    Get the Iceberg catalog name from environment.

    Returns:
        Catalog name from ICEBERG_NAMESPACE env var, defaults to "ee"

    Example:
        catalog = get_catalog_name()
        # Returns: "ee" or value of ICEBERG_NAMESPACE
    """
    catalog = os.getenv("ICEBERG_NAMESPACE", "ee")
    logger.info(f"Using Iceberg catalog: {catalog}")
    return catalog


def get_catalog_config() -> dict:
    """
    Get complete catalog configuration from environment.

    Returns:
        Dictionary with catalog name and configuration

    Example:
        config = get_catalog_config()
        # {
        #     'name': 'ee',
        #     'type': 'rest',
        #     'uri': 'http://localhost:8181',
        #     'warehouse': 's3://warehouse'
        # }
    """
    catalog_name = get_catalog_name()
    catalog_type = os.getenv("ICEBERG_CATALOG_TYPE", "rest")
    catalog_uri = os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
    catalog_warehouse = os.getenv("CATALOG_WAREHOUSE", "warehouse")

    config = {
        "name": catalog_name,
        "type": catalog_type,
        "uri": catalog_uri,
        "warehouse": f"s3://{catalog_warehouse}/{catalog_name}",
    }

    logger.info(f"Catalog config: {config}")
    return config
