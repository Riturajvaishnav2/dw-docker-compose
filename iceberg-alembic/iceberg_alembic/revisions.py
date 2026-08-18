from __future__ import annotations

import hashlib
import importlib.util
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType

from iceberg_alembic.exceptions import MigrationError


@dataclass(slots=True)
class Revision:
    revision: str
    down_revision: str | None
    path: Path
    checksum: str
    module: ModuleType


def revisions_path(root: Path | None = None) -> Path:
    base = root or Path.cwd()
    return base / "migrations" / "versions"


def load_revisions(root: Path | None = None) -> list[Revision]:
    path = revisions_path(root)
    revisions: list[Revision] = []
    for revision_path in sorted(path.glob("*.py")):
        module_name = f"iceberg_alembic_revision_{revision_path.stem}"
        spec = importlib.util.spec_from_file_location(module_name, revision_path)
        if spec is None or spec.loader is None:
            raise MigrationError(f"Unable to load revision from {revision_path}")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        revision = getattr(module, "revision", None)
        if not revision:
            raise MigrationError(f"Revision file {revision_path} is missing `revision`")

        revisions.append(
            Revision(
                revision=revision,
                down_revision=getattr(module, "down_revision", None),
                path=revision_path,
                checksum=checksum_file(revision_path),
                module=module,
            )
        )
    return revisions


def order_revisions(revisions: list[Revision]) -> list[Revision]:
    by_id = {revision.revision: revision for revision in revisions}
    children: dict[str | None, list[Revision]] = {}
    for revision in revisions:
        children.setdefault(revision.down_revision, []).append(revision)

    ordered: list[Revision] = []
    current = children.get(None, [])
    if len(current) != 1 and revisions:
        raise MigrationError("Expected a single root revision chain")

    while current:
        node = current.pop(0)
        ordered.append(node)
        next_nodes = children.get(node.revision, [])
        if len(next_nodes) > 1:
            raise MigrationError(f"Branching revision history is not supported at {node.revision}")
        current.extend(next_nodes)

    if len(ordered) != len(revisions):
        missing = sorted(set(by_id).difference(revision.revision for revision in ordered))
        raise MigrationError(f"Unable to resolve full revision order: {', '.join(missing)}")

    return ordered


def checksum_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
