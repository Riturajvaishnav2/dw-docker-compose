from __future__ import annotations

from iceberg_alembic.config import CatalogConfig


def load_config() -> CatalogConfig:
    """Load the runtime configuration for migrations."""

    return CatalogConfig.from_sources()


def main() -> None:
    config = load_config()
    print(
        f"Loaded catalog '{config.catalog_name}' "
        f"({config.catalog_type}) warehouse={config.warehouse}"
    )


if __name__ == "__main__":
    main()

