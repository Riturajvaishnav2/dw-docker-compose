from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError, TableAlreadyExistsError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import NestedField

from iceberg_alembic.config import CatalogConfig

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class OperationAction:
    operation: str
    namespace: str
    table_name: str


@dataclass(slots=True)
class StateSnapshot:
    snapshot_id: int | None
    metadata_location: str | None


class IcebergOperations:
    """Small subset of the planned Alembic-like operations API."""

    def __init__(self, config: CatalogConfig, dry_run: bool = False):
        self.config = config
        self.dry_run = dry_run
        self._catalog: Catalog | None = None
        self._last_action: OperationAction | None = None
        self._last_before = StateSnapshot(snapshot_id=None, metadata_location=None)
        self._last_after = StateSnapshot(snapshot_id=None, metadata_location=None)

    @property
    def catalog(self) -> Catalog:
        if self._catalog is None:
            self._catalog = load_catalog(self.config.catalog_name, **self._catalog_properties())
        return self._catalog

    def create_namespace(self, namespace: str) -> None:
        if self.dry_run:
            return

        try:
            self.catalog.create_namespace(namespace)
        except NamespaceAlreadyExistsError:
            return

    def create_table(
        self,
        namespace: str,
        table_name: str,
        columns: list[dict[str, Any]],
        partition_by: list[str] | None = None,
        location: str | None = None,
        properties: dict[str, Any] | None = None,
    ) -> None:
        self._last_action = OperationAction("create_table", namespace, table_name)
        self._last_before = self.capture_state(namespace, table_name)
        if self.dry_run:
            self._last_after = self._last_before
            return

        identifier = (namespace, table_name)
        if self.catalog.table_exists(identifier):
            self._last_after = self.capture_state(namespace, table_name)
            return

        self.create_namespace(namespace)
        table_location = location or self._default_table_location(namespace, table_name)
        self.catalog.create_table(
            identifier=identifier,
            schema=self._build_schema(columns),
            location=table_location,
            partition_spec=self._build_partition_spec(columns, partition_by or []),
            properties=properties or {},
        )
        self._last_after = self.capture_state(namespace, table_name)

    def drop_table(self, namespace: str, table_name: str) -> None:
        self._last_action = OperationAction("drop_table", namespace, table_name)
        self._last_before = self.capture_state(namespace, table_name)
        if self.dry_run:
            self._last_after = StateSnapshot(snapshot_id=None, metadata_location=None)
            return

        try:
            self.catalog.drop_table((namespace, table_name))
        except NoSuchTableError:
            self._last_after = StateSnapshot(snapshot_id=None, metadata_location=None)
            return
        self._last_after = self.capture_state(namespace, table_name)

    def add_column(
        self,
        namespace: str,
        table_name: str,
        column_name: str,
        column_type: str,
        doc: str | None = None,
    ) -> None:
        """Add a new column to an existing table."""
        self._last_action = OperationAction("add_column", namespace, table_name)
        self._last_before = self.capture_state(namespace, table_name)
        if self.dry_run:
            self._last_after = self._last_before
            return

        identifier = (namespace, table_name)
        try:
            table = self.catalog.load_table(identifier)
            table.update_schema().add_column(
                name=column_name,
                type=column_type,
                doc=doc,
            ).commit()
        except NoSuchTableError:
            return

        self._last_after = self.capture_state(namespace, table_name)

    def rename_column(
        self,
        namespace: str,
        table_name: str,
        old_name: str,
        new_name: str,
    ) -> None:
        """Rename a column in an existing table."""
        self._last_action = OperationAction("rename_column", namespace, table_name)
        self._last_before = self.capture_state(namespace, table_name)
        if self.dry_run:
            self._last_after = self._last_before
            return

        identifier = (namespace, table_name)
        try:
            table = self.catalog.load_table(identifier)
            table.update_schema().rename_column(old_name, new_name).commit()
        except NoSuchTableError:
            return

        self._last_after = self.capture_state(namespace, table_name)

    def drop_column(
        self,
        namespace: str,
        table_name: str,
        column_name: str,
    ) -> None:
        """Remove a column from an existing table."""
        self._last_action = OperationAction("drop_column", namespace, table_name)
        self._last_before = self.capture_state(namespace, table_name)
        if self.dry_run:
            self._last_after = self._last_before
            return

        identifier = (namespace, table_name)
        try:
            table = self.catalog.load_table(identifier)
            table.update_schema().delete_column(column_name).commit()
        except NoSuchTableError:
            return

        self._last_after = self.capture_state(namespace, table_name)

    def rename_table(
        self,
        old_namespace: str,
        old_name: str,
        new_namespace: str,
        new_name: str,
    ) -> None:
        """Rename or move a table to a different namespace."""
        old_identifier = (old_namespace, old_name)
        new_identifier = (new_namespace, new_name)

        self._last_action = OperationAction("rename_table", old_namespace, old_name)
        self._last_before = self.capture_state(old_namespace, old_name)
        if self.dry_run:
            self._last_after = self._last_before
            return

        try:
            old_table = self.catalog.load_table(old_identifier)
        except NoSuchTableError:
            return

        if new_namespace != old_namespace:
            self.create_namespace(new_namespace)

        self.catalog.rename_table(old_identifier, new_identifier)
        self._last_after = self.capture_state(new_namespace, new_name)

    def update_column_type(
        self,
        namespace: str,
        table_name: str,
        column_name: str,
        new_type: str,
    ) -> None:
        """Update the type of an existing column."""
        self._last_action = OperationAction("update_column_type", namespace, table_name)
        self._last_before = self.capture_state(namespace, table_name)
        if self.dry_run:
            self._last_after = self._last_before
            return

        identifier = (namespace, table_name)
        try:
            table = self.catalog.load_table(identifier)
            table.update_schema().update_column(column_name, type=new_type).commit()
        except NoSuchTableError:
            return

        self._last_after = self.capture_state(namespace, table_name)

    def capture_state(self, namespace: str, table_name: str) -> StateSnapshot:
        if self.dry_run:
            return StateSnapshot(snapshot_id=None, metadata_location=None)

        identifier = (namespace, table_name)
        if not self.catalog.table_exists(identifier):
            return StateSnapshot(snapshot_id=None, metadata_location=None)

        table = self.catalog.load_table(identifier)
        current_snapshot = table.current_snapshot()
        return StateSnapshot(
            snapshot_id=current_snapshot.snapshot_id if current_snapshot else None,
            metadata_location=table.metadata_location,
        )

    def last_action(self) -> OperationAction | None:
        return self._last_action

    def last_state_change(self) -> tuple[StateSnapshot, StateSnapshot]:
        return self._last_before, self._last_after

    def reset_last_action(self) -> None:
        self._last_action = None
        self._last_before = StateSnapshot(snapshot_id=None, metadata_location=None)
        self._last_after = StateSnapshot(snapshot_id=None, metadata_location=None)


    def _build_schema(self, columns: list[dict[str, Any]]) -> Schema:
        fields = [
            NestedField(
                id=index,
                name=column["name"],
                type=column["type"],
                required=column.get("required", False),
                doc=column.get("source_name"),
            )
            for index, column in enumerate(columns, start=1)
        ]
        return Schema(*fields)

    def _build_partition_spec(self, columns: list[dict[str, Any]], partition_by: list[str]) -> PartitionSpec:
        if not partition_by:
            return PartitionSpec()

        field_ids = {column["name"]: index for index, column in enumerate(columns, start=1)}
        partition_fields = [
            PartitionField(
                source_id=field_ids[column_name],
                field_id=1000 + offset,
                transform=IdentityTransform(),
                name=column_name,
            )
            for offset, column_name in enumerate(partition_by, start=1)
        ]
        return PartitionSpec(*partition_fields)

    def _default_table_location(self, namespace: str, table_name: str) -> str:
        return f"{self.config.warehouse.rstrip('/')}/{namespace}/{table_name}"

    def _catalog_properties(self) -> dict[str, str]:
        properties: dict[str, str] = {
            "type": self.config.catalog_type,
            "warehouse": self.config.warehouse,
        }
        if self.config.uri:
            properties["uri"] = self.config.uri

        properties.update(self._normalized_properties())
        self._inject_storage_credentials(properties)
        return properties

    def _normalized_properties(self) -> dict[str, str]:
        mappings = {
            "s3_endpoint": "s3.endpoint",
            "s3_region": "s3.region",
            "s3_path_style_access": "s3.path-style-access",
            "s3_checksum_enabled": "s3.checksum-enabled",
            "s3_chunked_encoding_enabled": "s3.chunked-encoding-enabled",
            "s3_access_key_id": "s3.access-key-id",
            "s3_secret_access_key": "s3.secret-access-key",
            "s3_session_token": "s3.session-token",
            "client_region": "client.region",
            "client_access_key_id": "client.access-key-id",
            "client_secret_access_key": "client.secret-access-key",
            "client_session_token": "client.session-token",
        }
        normalized: dict[str, str] = {}
        for key, value in self.config.properties.items():
            normalized[mappings.get(key, key)] = value
        return normalized

    def _inject_storage_credentials(self, properties: dict[str, str]) -> None:
        env = os.environ
        env_file_values = self._load_project_env()
        fallback_mappings = {
            "s3.endpoint": env.get("OCI_S3_ENDPOINT") or env_file_values.get("OCI_S3_ENDPOINT"),
            "s3.region": env.get("OCI_REGION") or env_file_values.get("OCI_REGION"),
            "s3.access-key-id": env.get("OCI_ACCESS_KEY_ID") or env_file_values.get("OCI_ACCESS_KEY_ID"),
            "s3.secret-access-key": env.get("OCI_SECRET_ACCESS_KEY") or env_file_values.get("OCI_SECRET_ACCESS_KEY"),
            "client.region": env.get("OCI_REGION") or env_file_values.get("OCI_REGION"),
            "client.access-key-id": env.get("OCI_ACCESS_KEY_ID") or env_file_values.get("OCI_ACCESS_KEY_ID"),
            "client.secret-access-key": env.get("OCI_SECRET_ACCESS_KEY") or env_file_values.get("OCI_SECRET_ACCESS_KEY"),
        }
        for key, value in fallback_mappings.items():
            if value and key not in properties:
                properties[key] = value

    def _load_project_env(self) -> dict[str, str]:
        candidate_paths = []
        if self.config.config_path:
            candidate_paths.append(self.config.config_path.parent.parent / ".env")
        candidate_paths.append(Path.cwd().parent / ".env")
        candidate_paths.append(Path.cwd() / ".env")

        for path in candidate_paths:
            if path.exists():
                return _parse_dotenv(path)
        return {}


def _parse_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values
