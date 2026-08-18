from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from iceberg_alembic.config import CatalogConfig
from iceberg_alembic.models import MigrationRecord


class LocalMigrationStore:
    """Local state file until the Iceberg metadata table is implemented."""

    def __init__(self, config: CatalogConfig):
        self.config = config
        self.path = config.state_path or Path(".iceberg-alembic-state.json")

    def bootstrap(self) -> Path:
        if not self.path.exists():
            self._write([])
        return self.path

    def all(self) -> list[MigrationRecord]:
        if not self.path.exists():
            return []

        records = json.loads(self.path.read_text())
        return [self._deserialize(record) for record in records]

    def applied_revisions(self) -> list[str]:
        return [record.revision for record in self.all() if record.status == "success"]

    def latest(self) -> MigrationRecord | None:
        records = [record for record in self.all() if record.status == "success"]
        return records[-1] if records else None

    def append(self, record: MigrationRecord) -> None:
        records = self.all()
        records.append(record)
        self._write(records)

    def pop_success(self) -> MigrationRecord | None:
        records = self.all()
        for index in range(len(records) - 1, -1, -1):
            if records[index].status == "success":
                record = records.pop(index)
                self._write(records)
                return record
        return None

    def _write(self, records: list[MigrationRecord]) -> None:
        self.path.write_text(json.dumps([self._serialize(record) for record in records], indent=2))

    @staticmethod
    def _serialize(record: MigrationRecord) -> dict[str, str | int | None]:
        payload = asdict(record)
        payload["applied_at"] = record.applied_at.isoformat()
        return payload

    @staticmethod
    def _deserialize(payload: dict[str, str | int | None]) -> MigrationRecord:
        return MigrationRecord(
            revision=str(payload["revision"]),
            down_revision=str(payload["down_revision"]) if payload["down_revision"] is not None else None,
            namespace=str(payload["namespace"]),
            table_name=str(payload["table_name"]),
            operation=str(payload["operation"]),
            applied_at=datetime.fromisoformat(str(payload["applied_at"])),
            applied_by=str(payload["applied_by"]),
            snapshot_id_before=int(payload["snapshot_id_before"]) if payload["snapshot_id_before"] is not None else None,
            snapshot_id_after=int(payload["snapshot_id_after"]) if payload["snapshot_id_after"] is not None else None,
            metadata_location_before=(
                str(payload["metadata_location_before"])
                if payload["metadata_location_before"] is not None
                else None
            ),
            metadata_location_after=(
                str(payload["metadata_location_after"])
                if payload["metadata_location_after"] is not None
                else None
            ),
            checksum=str(payload["checksum"]),
            status=str(payload["status"]),
        )
