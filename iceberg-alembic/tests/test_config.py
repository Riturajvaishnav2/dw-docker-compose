from __future__ import annotations

from pathlib import Path

from iceberg_alembic.config import CatalogConfig


def _clear_iceberg_alembic_env(monkeypatch) -> None:
    for key in list(__import__("os").environ):
        if key.startswith("ICEBERG_ALEMBIC_"):
            monkeypatch.delenv(key, raising=False)


def test_catalog_config_loads_from_toml(tmp_path: Path, monkeypatch) -> None:
    _clear_iceberg_alembic_env(monkeypatch)
    config_file = tmp_path / "iceberg-alembic.toml"
    config_file.write_text(
        """
[catalog]
name = "dev"
type = "rest"
uri = "http://catalog:8181"
warehouse = "s3://warehouse"
namespace = "dw"

[execution]
applied_by = "pytest"
trino_url = "http://trino:8080"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = CatalogConfig.from_sources(config_file)

    assert config.catalog_name == "dev"
    assert config.catalog_type == "rest"
    assert config.uri == "http://catalog:8181"
    assert config.warehouse == "s3://warehouse"
    assert config.applied_by == "pytest"
    assert config.trino_url == "http://trino:8080"
    assert config.config_path == config_file


def test_catalog_config_env_overrides(monkeypatch) -> None:
    _clear_iceberg_alembic_env(monkeypatch)
    monkeypatch.setenv("ICEBERG_ALEMBIC_WAREHOUSE", "file:///tmp/warehouse")
    monkeypatch.setenv("ICEBERG_ALEMBIC_CATALOG_TYPE", "sql")
    monkeypatch.setenv("ICEBERG_ALEMBIC_PROPERTY_REGION", "us-east-1")

    config = CatalogConfig.from_sources()

    assert config.catalog_type == "sql"
    assert config.warehouse == "file:///tmp/warehouse"
    assert config.properties == {"region": "us-east-1"}
