"""
Spark catalog initialization utility.

Provides dynamic catalog registration based on ICEBERG_NAMESPACE environment variable.
"""

import os
import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def register_iceberg_catalog(spark: SparkSession, catalog_name: str = None) -> str:
    """
    Register Iceberg catalog dynamically based on ICEBERG_NAMESPACE environment variable.

    Structure: catalog (ee) -> namespaces (bronze, silver, gold) -> tables
    Example: ee.silver.dim_client

    Args:
        spark: Active SparkSession
        catalog_name: Catalog name (e.g., "ee"). Defaults to ICEBERG_NAMESPACE env var.

    Returns:
        The registered catalog name
    """
    # Determine catalog name (tenant/namespace like "ee")
    if not catalog_name:
        catalog_name = os.getenv("ICEBERG_NAMESPACE", "ee")

    catalog_uri = os.getenv("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181")
    catalog_warehouse = os.getenv("CATALOG_WAREHOUSE", "warehouse")
    s3_endpoint = os.getenv("OCI_S3_ENDPOINT", "http://minio:9000")
    s3_access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

    # Register catalog using Spark 3.x configuration API (not SQL CREATE CATALOG)
    spark.conf.set(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(f"spark.sql.catalog.{catalog_name}.type", "rest")
    spark.conf.set(f"spark.sql.catalog.{catalog_name}.uri", catalog_uri)
    spark.conf.set(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    spark.conf.set(f"spark.sql.catalog.{catalog_name}.s3.endpoint", s3_endpoint)
    spark.conf.set(f"spark.sql.catalog.{catalog_name}.s3.path-style-access", "true")
    spark.conf.set(f"spark.sql.catalog.{catalog_name}.s3.access-key-id", s3_access_key)
    spark.conf.set(f"spark.sql.catalog.{catalog_name}.s3.secret-access-key", s3_secret_key)
    spark.conf.set(f"spark.sql.catalog.{catalog_name}.warehouse", f"s3://{catalog_warehouse}/{catalog_name}")

    # Set as default catalog
    spark.conf.set("spark.sql.defaultCatalog", catalog_name)

    logger.info(f"✓ Registered Iceberg catalog: {catalog_name}")
    logger.info(f"  URI: {catalog_uri}")
    logger.info(f"  Warehouse: s3://{catalog_warehouse}/{catalog_name}")

    return catalog_name


def get_catalog_name() -> str:
    """
    Get the Iceberg catalog name from environment.

    Returns:
        Catalog name from ICEBERG_NAMESPACE, defaults to "ee"
    """
    return os.getenv("ICEBERG_NAMESPACE", "ee")
