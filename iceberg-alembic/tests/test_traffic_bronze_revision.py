from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


def test_traffic_bronze_revision_has_expected_chain_and_columns() -> None:
    revision_path = Path(__file__).resolve().parents[1] / "migrations" / "versions" / "20260604_005_create_traffic_bronze_table.py"
    spec = spec_from_file_location("traffic_bronze_revision", revision_path)
    module = module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)

    assert module.revision == "20260604_005_create_traffic_bronze_table"
    assert module.down_revision == "20260529_003_create_rating_table"
    assert callable(module.upgrade)
    assert callable(module.downgrade)
    assert len(module.TRAFFIC_BRONZE_COLUMNS) == 24
    assert module.TRAFFIC_BRONZE_COLUMNS[0]["name"] == "client_operator"
    assert module.TRAFFIC_BRONZE_COLUMNS[-10]["name"] == "file_direction"
    assert module.TRAFFIC_BRONZE_COLUMNS[-1]["name"] == "_ingest_date"
