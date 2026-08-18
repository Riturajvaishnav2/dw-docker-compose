from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


def test_settlement_revision_has_upgrade_and_downgrade() -> None:
    revision_path = Path(__file__).resolve().parents[1] / "migrations" / "versions" / "20260529_001_create_settlement_table.py"
    spec = spec_from_file_location("settlement_revision", revision_path)
    module = module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)

    assert module.revision == "20260529_001_create_settlement_table"
    assert module.down_revision is None
    assert callable(module.upgrade)
    assert callable(module.downgrade)
    assert len(module.SETTLEMENT_COLUMNS) == 46
    assert module.SETTLEMENT_COLUMNS[0]["name"] == "home_pmn"
    assert module.SETTLEMENT_COLUMNS[-10]["name"] == "settled_amount_incl_tax"
    assert module.SETTLEMENT_COLUMNS[-1]["name"] == "_ingest_date"
