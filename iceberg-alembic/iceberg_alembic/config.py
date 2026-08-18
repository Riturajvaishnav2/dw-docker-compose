from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 fallback
    import tomli as tomllib


class CatalogConfig(BaseModel):
    """Configuration needed to initialize an Iceberg catalog and runtime."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    catalog_name: str = Field(default="datawarehouse-local", alias="name")
    catalog_type: str = Field(default="rest", alias="type")
    uri: str | None = "http://localhost:8181"
    warehouse: str = Field(default_factory=lambda: f"s3://{os.getenv('CATALOG_WAREHOUSE')}/")
    namespace: str = "dw"
    applied_by: str = "iceberg-alembic"
    config_path: Path | None = None
    state_path: Path | None = None
    spark_url: str | None = None
    trino_url: str | None = "https://localhost:8443"
    properties: dict[str, str] = Field(default_factory=dict)

    @classmethod
    def default_path(cls) -> Path:
        return Path("iceberg-alembic.toml")

    @classmethod
    def resolve_config_path(cls, path: str | Path | None = None) -> Path | None:
        candidate = path or os.environ.get("ICEBERG_ALEMBIC_CONFIG")
        if candidate:
            resolved = Path(candidate).expanduser()
            return resolved if resolved.exists() else resolved

        default_path = cls.default_path()
        return default_path if default_path.exists() else None

    @classmethod
    def from_sources(cls, path: str | Path | None = None) -> "CatalogConfig":
        file_data = cls._read_file(path)
        env_data = cls._read_env()

        merged: dict[str, Any] = {}
        merged.update(file_data)
        merged.update({key: value for key, value in env_data.items() if value is not None})

        instance = cls.model_validate(merged)
        instance.config_path = cls.resolve_config_path(path)
        instance.state_path = cls._resolve_state_path(
            explicit_path=merged.get("state_path"),
            config_path=instance.config_path,
        )
        return instance

    @classmethod
    def _read_file(cls, path: str | Path | None = None) -> dict[str, Any]:
        config_path = cls.resolve_config_path(path)
        if not config_path or not config_path.exists():
            return {}

        with config_path.open("rb") as handle:
            data = tomllib.load(handle)

        catalog = data.get("catalog", {})
        execution = data.get("execution", {})

        file_data: dict[str, Any] = {
            "name": catalog.get("name"),
            "type": catalog.get("type"),
            "uri": catalog.get("uri"),
            "warehouse": catalog.get("warehouse"),
            "namespace": catalog.get("namespace"),
            "state_path": execution.get("state_path"),
            "spark_url": execution.get("spark_url"),
            "trino_url": execution.get("trino_url"),
            "applied_by": execution.get("applied_by"),
            "properties": catalog.get("properties", {}),
        }
        return {key: value for key, value in file_data.items() if value is not None}

    @staticmethod
    def _read_env() -> dict[str, Any]:
        env = os.environ
        properties = {
            key.removeprefix("ICEBERG_ALEMBIC_PROPERTY_").lower(): value
            for key, value in env.items()
            if key.startswith("ICEBERG_ALEMBIC_PROPERTY_")
            and value != ""
        }
        env_data = {
            "name": env.get("ICEBERG_ALEMBIC_CATALOG_NAME"),
            "type": env.get("ICEBERG_ALEMBIC_CATALOG_TYPE"),
            "uri": env.get("ICEBERG_ALEMBIC_CATALOG_URI"),
            "warehouse": env.get("ICEBERG_ALEMBIC_WAREHOUSE"),
            "namespace": env.get("ICEBERG_ALEMBIC_NAMESPACE"),
            "applied_by": env.get("ICEBERG_ALEMBIC_APPLIED_BY"),
            "state_path": env.get("ICEBERG_ALEMBIC_STATE_PATH"),
            "spark_url": env.get("ICEBERG_ALEMBIC_SPARK_URL"),
            "trino_url": env.get("ICEBERG_ALEMBIC_TRINO_URL"),
            "properties": properties or None,
        }
        return {
            key: value
            for key, value in env_data.items()
            if value not in (None, "")
        }

    @classmethod
    def _resolve_state_path(
        cls,
        explicit_path: str | Path | None,
        config_path: Path | None,
    ) -> Path:
        if explicit_path:
            candidate = Path(explicit_path).expanduser()
        elif config_path:
            candidate = config_path.parent / ".iceberg-alembic-state.json"
        else:
            candidate = Path(".iceberg-alembic-state.json")

        return candidate.resolve()
