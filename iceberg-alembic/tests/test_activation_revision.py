from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


def test_activation_revision_has_expected_chain_and_columns() -> None:
    revision_path = Path(__file__).resolve().parents[1] / "migrations" / "versions" / "20260529_002_create_activation_table.py"
    spec = spec_from_file_location("activation_revision", revision_path)
    module = module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)

    assert module.revision == "20260529_002_create_activation_table"
    assert module.down_revision == "20260529_001_create_settlement_table"
    assert callable(module.upgrade)
    assert callable(module.downgrade)
    assert len(module.ACTIVATION_COLUMNS) == 47
    assert module.ACTIVATION_COLUMNS[0]["name"] == "traffic_direction"
    assert module.ACTIVATION_COLUMNS[-10]["name"] == "regular_segment"
    assert module.ACTIVATION_COLUMNS[-1]["name"] == "_ingest_date"
