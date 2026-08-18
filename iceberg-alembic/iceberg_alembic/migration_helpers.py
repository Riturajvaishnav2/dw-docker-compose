from __future__ import annotations

from typing import Any


AUDIT_COLUMNS: list[dict[str, Any]] = [
    {"name": "_source_bucket", "type": "string", "source_name": "_source_bucket", "required": True},
    {"name": "_source_key", "type": "string", "source_name": "_source_key", "required": True},
    {"name": "_source_file_name", "type": "string", "source_name": "_source_file_name", "required": True},
    {"name": "_source_format", "type": "string", "source_name": "_source_format", "required": True},
    {"name": "_tenant", "type": "string", "source_name": "_tenant", "required": True},
    {"name": "_layer", "type": "string", "source_name": "_layer", "required": True},
    {"name": "_stage", "type": "string", "source_name": "_stage", "required": True},
    {"name": "_ingested_at", "type": "timestamp", "source_name": "_ingested_at", "required": True},
    {"name": "_ingest_date", "type": "date", "source_name": "_ingest_date", "required": True},
]

DEFAULT_TABLE_PROPERTIES = {
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "snappy",
    "write.spark.accept-any-schema": "true",
}


def with_audit_columns(columns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [*columns, *AUDIT_COLUMNS]
