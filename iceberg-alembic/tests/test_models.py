from __future__ import annotations

from datetime import datetime, timezone

from iceberg_alembic.models import MigrationRecord


def test_migration_record_fields() -> None:
    record = MigrationRecord(
        revision="20260529_001",
        down_revision=None,
        namespace="orange",
        table_name="customers",
        operation="create_table",
        applied_at=datetime.now(timezone.utc),
        applied_by="pytest",
        snapshot_id_before=None,
        snapshot_id_after=123,
        metadata_location_before=None,
        metadata_location_after="s3://${CATALOG_WAREHOUSE}/customers/metadata/v1.json",
        checksum="abc123",
        status="success",
    )

    assert record.revision == "20260529_001"
    assert record.snapshot_id_after == 123
    assert record.status == "success"
