from __future__ import annotations

from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from iceberg_alembic.config import CatalogConfig
from iceberg_alembic.runner import MigrationRunner

console = Console()


@click.group()
@click.option(
    "--config",
    "config_path",
    type=click.Path(path_type=Path, dir_okay=False),
    help="Path to iceberg-alembic.toml",
)
@click.pass_context
def cli(ctx: click.Context, config_path: Path | None) -> None:
    """CLI entrypoint for Iceberg migrations."""

    config = CatalogConfig.from_sources(config_path)
    ctx.obj = {
        "config": config,
        "runner": MigrationRunner(config=config),
    }


@cli.command("version")
def version() -> None:
    click.echo("iceberg-alembic 0.1.0")


@cli.command("init")
@click.pass_context
def init(ctx: click.Context) -> None:
    runner: MigrationRunner = ctx.obj["runner"]
    path = runner.init()
    console.print(f"Initialized local migration state at [bold]{path}[/bold]")


@cli.command("upgrade")
@click.argument("target", default="head")
@click.option("--dry-run", is_flag=True, help="Show the revisions that would run.")
@click.pass_context
def upgrade(ctx: click.Context, target: str, dry_run: bool) -> None:
    config: CatalogConfig = ctx.obj["config"]
    runner = MigrationRunner(config=config, dry_run=dry_run)
    applied = runner.upgrade(target=target)
    if dry_run:
        if applied:
            console.print(f"Planned revisions: {', '.join(applied)}")
        else:
            console.print("No pending revisions.")
        return

    if applied:
        console.print(f"Applied revisions: {', '.join(applied)}")
    else:
        console.print("No pending revisions.")


@cli.command("downgrade")
@click.argument("target")
@click.option("--dry-run", is_flag=True, help="Show the revisions that would be reverted.")
@click.pass_context
def downgrade(ctx: click.Context, target: str, dry_run: bool) -> None:
    config: CatalogConfig = ctx.obj["config"]
    runner = MigrationRunner(config=config, dry_run=dry_run)
    reverted = runner.downgrade(target=target)
    if dry_run:
        if reverted:
            console.print(f"Planned downgrades: {', '.join(reverted)}")
        else:
            console.print("No applied revisions to revert.")
        return

    if reverted:
        console.print(f"Reverted revisions: {', '.join(reverted)}")
    else:
        console.print("No applied revisions to revert.")


@cli.command("current")
@click.pass_context
def current(ctx: click.Context) -> None:
    runner: MigrationRunner = ctx.obj["runner"]
    record = runner.current()
    if record is None:
        console.print("No applied revisions.")
        return

    console.print(
        f"{record.revision} ({record.namespace}.{record.table_name}) at {record.applied_at.isoformat()}"
    )


@cli.command("history")
@click.pass_context
def history(ctx: click.Context) -> None:
    runner: MigrationRunner = ctx.obj["runner"]
    records = runner.history()
    if not records:
        console.print("No migration history.")
        return

    table = Table(title="Iceberg Migration History")
    table.add_column("Revision")
    table.add_column("Status")
    table.add_column("Operation")
    table.add_column("Target")
    table.add_column("Applied At")
    for record in records:
        table.add_row(
            record.revision,
            record.status,
            record.operation,
            f"{record.namespace}.{record.table_name}".rstrip("."),
            record.applied_at.isoformat(),
        )
    console.print(table)
