from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class MigrationRecord:
    revision: str
    down_revision: str | None
    namespace: str
    table_name: str
    operation: str
    applied_at: datetime
    applied_by: str
    snapshot_id_before: int | None
    snapshot_id_after: int | None
    metadata_location_before: str | None
    metadata_location_after: str | None
    checksum: str
    status: str

