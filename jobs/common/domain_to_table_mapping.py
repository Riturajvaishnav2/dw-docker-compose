"""
Domain-to-Iceberg table mapping configuration.

Maps tenant domains and layers to their corresponding Iceberg tables
managed by the iceberg-alembic migration framework.
included
Usage:
    from domain_to_table_mapping import DomainTableMapping
    mapping = DomainTableMapping()
    table_config = mapping.get_table_config(domain="Activation", layer="bronze")
    # Returns:
    # {
    #   'iceberg_namespace': 'orange',
    #   'table_name': 'activation',
    #   'full_path': 'iceberg.orange.activation',
    #   'warehouse_location': 's3://warehouse/orange/bronze/activation'
    # }
"""

import os
from dataclasses import dataclass
from typing import Optional


def normalize_warehouse_path(path: str) -> str:
    """
    Normalize warehouse path to s3:// format.
    Handles both formats:
    - "s3://warehouse" → "s3://warehouse"
    - "warehouse_test" → "s3://warehouse_test"
    """
    if not path:
        return path
    path = path.rstrip("/")
    if not path.startswith("s3://") and not path.startswith("s3a://"):
        return f"s3://{path}"
    if path.startswith("s3a://"):
        return "s3://" + path[6:]
    return path


def warehouse_root() -> str:
    """Get warehouse root path from environment or defaults."""
    catalog_warehouse = os.getenv("CATALOG_WAREHOUSE", "s3://warehouse")
    normalized = normalize_warehouse_path(catalog_warehouse)
    return normalized.rstrip("/")


@dataclass
class TableConfig:
    """Configuration for an Iceberg table."""
    iceberg_namespace: str
    table_name: str
    warehouse_location: str
    source_domain: str
    layer: str

    @property
    def full_path(self) -> str:
        """Return fully qualified table path with backticks for table names containing dots."""
        # Use backticks for table names with dots (Spark requirement)
        quoted_table = f"`{self.table_name}`" if '.' in self.table_name else self.table_name
        return f"{os.getenv('ICEBERG_NAMESPACE')}.{self.iceberg_namespace}.{quoted_table}"


class DomainTableMapping:
    """
    Maps domain + layer combinations to Iceberg tables.

    Supports the following domains across multiple layers:
    - Activation → orange.activation (managed by 20260529_002, bronze only)
    - Rating → orange.rating (managed by 20260529_003, bronze only)
    - Settlement → orange.settlement (managed by 20260529_001, bronze only)
    - Settlement → orange.settlement (managed by 20260617_007, gold only)
    - Traffic → orange.traffic (managed by 20260604_005/006, bronze and silver)
    """

    @classmethod
    def _get_namespace(cls) -> str:
        """Get the Iceberg namespace from environment variable."""
        return os.getenv("ICEBERG_NAMESPACE", "orange")

    @property
    def DOMAIN_MAPPINGS(self) -> dict:
        """Migration-managed tables across bronze, silver, and gold layers.

        Multi-tenant structure:
        - Catalog (dynamic): ICEBERG_NAMESPACE (ee, orange, vodafone, etc.)
        - Namespace (fixed): bronze, silver, gold (layer names)
        - Table: actual table names

        Full path: {catalog}.{namespace}.{table}
        Example: ee.bronze.activation
        """
        return {
            ("activation", "bronze"): {
                "namespace": "bronze",
                "table_name": "activation",
                "migration_revision": "20260529_002_create_activation_table",
            },
            ("rating", "bronze"): {
                "namespace": "bronze",
                "table_name": "rating",
                "migration_revision": "20260529_003_create_rating_table",
            },
            ("settlement", "bronze"): {
                "namespace": "bronze",
                "table_name": "settlement",
                "migration_revision": "20260529_001_create_settlement_table",
            },
            ("traffic", "bronze"): {
                "namespace": "bronze",
                "table_name": "imsi_level_traffic",
                "migration_revision": "20260604_005_create_traffic_bronze_table",
            },
            ("traffic", "silver"): {
                "namespace": "silver",
                "table_name": "traffic",
                "migration_revision": "20260604_006_create_traffic_silver_table",
            },
            ("settlement", "gold"): {
                "namespace": "gold",
                "table_name": "settlement",
                "migration_revision": "20260617_007_create_settlement_gold_table",
            },
            ("forecast", "bronze"): {
                "namespace": "bronze",
                "table_name": "iot_forecast_raw",
                "migration_revision": "20260703_001_create_forecast_bronze_table",
            },
            ("forecast", "silver"): {
                "namespace": "silver",
                "table_name": "fact_iot_forecast",
                "migration_revision": "20260703_002_create_forecast_silver_tables",
            },
            ("forecast", "gold"): {
                "namespace": "gold",
                "table_name": "forecast_by_client_date",
                "migration_revision": "20260703_003_create_forecast_gold_tables",
            },
        }

    def get_table_config(
        self,
        domain: str,
        layer: str = "bronze",
        warehouse_root: str = None,
    ) -> Optional[TableConfig]:
        """
        Get table configuration for a domain + layer combination.

        Args:
            domain: Data domain (e.g., 'Activation', 'rating', 'settlement')
            layer: Data layer (currently only 'bronze' is supported)
            warehouse_root: Warehouse path root (defaults to CATALOG_WAREHOUSE env var)

        Returns:
            TableConfig if mapping exists, None otherwise

        Example:
            config = DomainTableMapping().get_table_config(
                domain="Activation",
                layer="bronze"
            )
            # Returns TableConfig for iceberg.<namespace>.activation
        """
        if warehouse_root is None:
            catalog_warehouse = os.getenv("CATALOG_WAREHOUSE", "s3://warehouse")
            warehouse_root = normalize_warehouse_path(catalog_warehouse).rstrip("/")

        # Normalize domain for lookup (case-insensitive)
        key = (domain.lower(), layer.lower())

        mappings = self.DOMAIN_MAPPINGS
        if key not in mappings:
            return None

        mapping = mappings[key]
        location = f"{warehouse_root}/{mapping['namespace']}/{mapping['table_name']}"

        return TableConfig(
            iceberg_namespace=mapping["namespace"],
            table_name=mapping["table_name"],
            warehouse_location=location,
            source_domain=domain,
            layer=layer,
        )

    def is_managed_by_alembic(self, domain: str, layer: str) -> bool:
        """
        Check if a domain + layer combination is managed by iceberg-alembic.

        Args:
            domain: Data domain
            layer: Data layer

        Returns:
            True if the table is created and managed by migrations, False otherwise
        """
        mappings = self.DOMAIN_MAPPINGS
        return (domain.lower(), layer.lower()) in mappings

    def get_migration_revision(self, domain: str, layer: str) -> Optional[str]:
        """
        Get the migration revision that manages this domain + layer.

        Args:
            domain: Data domain
            layer: Data layer

        Returns:
            Migration revision ID if found, None otherwise
        """
        key = (domain.lower(), layer.lower())
        mappings = self.DOMAIN_MAPPINGS
        if key not in mappings:
            return None
        return mappings[key]["migration_revision"]

    def list_managed_tables(self) -> dict:
        """
        List all tables managed by iceberg-alembic migrations.

        Returns:
            Dictionary mapping (domain, layer) -> TableConfig

        Example:
            tables = DomainTableMapping().list_managed_tables()
            for (domain, layer), config in tables.items():
                print(f"{domain}/{layer} -> {config.full_path}")
        """
        result = {}
        mappings = self.DOMAIN_MAPPINGS
        for (domain, layer) in mappings.keys():
            config = self.get_table_config(domain=domain, layer=layer)
            if config:
                result[(domain, layer)] = config
        return result


def validate_domain_and_layer(domain: str, layer: str) -> tuple[bool, Optional[str]]:
    """
    Validate that a domain + layer combination has a corresponding Iceberg table.

    Args:
        domain: Data domain
        layer: Data layer

    Returns:
        Tuple of (is_valid, error_message)
        - (True, None) if valid
        - (False, error_msg) if invalid
    """
    mapping = DomainTableMapping()

    if not mapping.is_managed_by_alembic(domain, layer):
        error = (
            f"Domain '{domain}' with layer '{layer}' is not yet configured. "
            f"It may need an iceberg-alembic migration. "
            f"Supported combinations:\n"
        )
        for (d, l), config in mapping.list_managed_tables().items():
            error += f"  - {d.capitalize()}/{l}\n"
        return False, error

    return True, None


if __name__ == "__main__":
    # Example usage
    mapping = DomainTableMapping()

    print("=" * 60)
    print("Domain-to-Iceberg Table Mapping")
    print("=" * 60)

    for (domain, layer), config in mapping.list_managed_tables().items():
        print(f"\nDomain: {domain.upper()}")
        print(f"  Layer: {layer}")
        print(f"  Table: {config.full_path}")
        print(f"  Location: {config.warehouse_location}")
        print(f"  Migration: {mapping.get_migration_revision(domain, layer)}")

    print("\n" + "=" * 60)
    print("Example: Get activation table config")
    print("=" * 60)
    config = mapping.get_table_config(domain="Activation", layer="bronze")
    if config:
        print(f"✓ {config.full_path}")
        print(f"  Location: {config.warehouse_location}")

    print("\n" + "=" * 60)
    print("Example: Validate invalid domain")
    print("=" * 60)
    is_valid, error = validate_domain_and_layer(domain="forecast", layer="silver")
    if not is_valid:
        print(f"✗ {error}")
