from __future__ import annotations

import sys

from iceberg_alembic.config import CatalogConfig
from iceberg_alembic.operations import IcebergOperations
from iceberg_alembic.state import LocalMigrationStore


def main() -> int:
    config = CatalogConfig.from_sources()
    store = LocalMigrationStore(config)
    records = [record for record in store.all() if record.status == "success"]

    if not records:
        print("No successful migrations have been applied yet.")
        return 1

    operations = IcebergOperations(config=config, dry_run=False)
    missing: list[str] = []

    for record in records:
        if not record.table_name:
            continue
        identifier = (record.namespace, record.table_name)
        if not operations.catalog.table_exists(identifier):
            missing.append(f"{record.namespace}.{record.table_name}")

    if missing:
        print("Missing migrated Iceberg tables:", ", ".join(missing))
        return 1

    print(
        "Verified migrated Iceberg tables:",
        ", ".join(f"{record.namespace}.{record.table_name}" for record in records if record.table_name),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
