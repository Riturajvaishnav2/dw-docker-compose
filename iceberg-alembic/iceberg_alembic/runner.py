from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from iceberg_alembic.config import CatalogConfig
from iceberg_alembic.exceptions import ChecksumError, MigrationError
from iceberg_alembic.models import MigrationRecord
from iceberg_alembic.operations import IcebergOperations
from iceberg_alembic.revisions import Revision, load_revisions, order_revisions
from iceberg_alembic.state import LocalMigrationStore


class MigrationRunner:
    def __init__(
        self,
        config: CatalogConfig,
        *,
        root: Path | None = None,
        dry_run: bool = False,
    ):
        self.config = config
        self.root = root or Path.cwd()
        self.dry_run = dry_run
        self.store = LocalMigrationStore(config)
        self.operations = IcebergOperations(config=config, dry_run=dry_run)

    def init(self) -> Path:
        return self.store.bootstrap()

    def current(self) -> MigrationRecord | None:
        return self.store.latest()

    def history(self) -> list[MigrationRecord]:
        return self.store.all()

    def upgrade(self, target: str = "head") -> list[str]:
        revisions = order_revisions(load_revisions(self.root))
        self._verify_checksums(revisions)
        applied = set(self.store.applied_revisions())
        planned: list[Revision] = []
        for revision in revisions:
            if revision.revision in applied:
                continue
            planned.append(revision)
            if target != "head" and revision.revision == target:
                break

        if target != "head" and planned and planned[-1].revision != target and target not in applied:
            raise MigrationError(f"Unknown or unreachable target revision: {target}")
        if target != "head" and target in applied:
            return []

        applied_revisions: list[str] = []
        for revision in planned:
            self.apply_revision(revision)
            applied_revisions.append(revision.revision)
        return applied_revisions

    def downgrade(self, target: str) -> list[str]:
        records = [record for record in self.store.all() if record.status == "success"]
        if not records:
            return []

        if target == "-1":
            to_revert = records[-1:]
        elif target == "base":
            to_revert = list(reversed(records))
        else:
            to_revert = []
            for record in reversed(records):
                if record.revision == target:
                    break
                to_revert.append(record)
            else:
                raise MigrationError(f"Unknown downgrade target: {target}")

        revisions = {revision.revision: revision for revision in load_revisions(self.root)}
        reverted: list[str] = []
        for record in to_revert:
            revision = revisions.get(record.revision)
            if revision is None:
                raise MigrationError(f"Missing revision file for applied revision {record.revision}")
            self.revert_revision(revision)
            reverted.append(revision.revision)
        return reverted

    def apply_revision(self, revision: Revision) -> None:
        self.operations.reset_last_action()
        action = None
        before = None
        after = None
        try:
            revision.module.upgrade(self.operations)
            action = self.operations.last_action()
            before, after = self.operations.last_state_change()

            if not self.dry_run:
                self.store.append(
                    MigrationRecord(
                        revision=revision.revision,
                        down_revision=revision.down_revision,
                        namespace=action.namespace if action else self.config.namespace,
                        table_name=action.table_name if action else "",
                        operation=action.operation if action else "upgrade",
                        applied_at=datetime.now(timezone.utc),
                        applied_by=self.config.applied_by,
                        snapshot_id_before=before.snapshot_id if before else None,
                        snapshot_id_after=after.snapshot_id if after else None,
                        metadata_location_before=before.metadata_location if before else None,
                        metadata_location_after=after.metadata_location if after else None,
                        checksum=revision.checksum,
                        status="success",
                    )
                )
        except Exception:
            action = action or self.operations.last_action()
            before, after = self.operations.last_state_change()
            if not self.dry_run:
                self.store.append(
                    MigrationRecord(
                        revision=revision.revision,
                        down_revision=revision.down_revision,
                        namespace=action.namespace if action else self.config.namespace,
                        table_name=action.table_name if action else "",
                        operation=action.operation if action else "upgrade",
                        applied_at=datetime.now(timezone.utc),
                        applied_by=self.config.applied_by,
                        snapshot_id_before=before.snapshot_id if before else None,
                        snapshot_id_after=after.snapshot_id if after else None,
                        metadata_location_before=before.metadata_location if before else None,
                        metadata_location_after=after.metadata_location if after else None,
                        checksum=revision.checksum,
                        status="failed",
                    )
                )
            raise

    def revert_revision(self, revision: Revision) -> None:
        revision.module.downgrade(self.operations)
        if not self.dry_run:
            self.store.pop_success()

    def _verify_checksums(self, revisions: list[Revision]) -> None:
        checksums = {revision.revision: revision.checksum for revision in revisions}
        for record in self.store.all():
            if record.status != "success":
                continue
            expected = checksums.get(record.revision)
            if expected and expected != record.checksum:
                raise ChecksumError(
                    f"Revision {record.revision} has changed since it was applied"
                )
