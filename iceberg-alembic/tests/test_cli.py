from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from iceberg_alembic.cli import cli


def test_upgrade_command_exists_and_supports_dry_run(tmp_path: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["upgrade", "head", "--dry-run"],
        env={"ICEBERG_ALEMBIC_STATE_PATH": str(tmp_path / ".iceberg-alembic-state.json")},
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert "Planned revisions:" in result.output or "No pending revisions." in result.output


def test_init_creates_local_state_file(tmp_path: Path) -> None:
    config_path = tmp_path / "iceberg-alembic.toml"
    config_path.write_text(
        "\n".join(
            [
                "[catalog]",
                'name = "test"',
                'type = "rest"',
                'uri = "http://localhost:8181"',
                'warehouse = "s3://${CATALOG_WAREHOUSE}/"',
                'namespace = "dw"',
            ]
        )
    )

    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=str(tmp_path)):
        local_config = Path("iceberg-alembic.toml")
        local_config.write_text(config_path.read_text())
        result = runner.invoke(cli, ["init"], catch_exceptions=False)
        assert result.exit_code == 0
        assert Path(".iceberg-alembic-state.json").exists()
