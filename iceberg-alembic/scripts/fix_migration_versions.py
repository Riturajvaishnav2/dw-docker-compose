#!/usr/bin/env python3
"""
Automatically fix migration versioning and dependency chain.

This script:
1. Scans all migration files in versions/
2. Orders them chronologically
3. Fixes revision and down_revision fields
4. Updates checksums in state file
5. Logs all changes

Safe to run multiple times (idempotent).
"""

import os
import re
import json
import hashlib
import sys
from pathlib import Path
from datetime import datetime


class MigrationFixer:
    def __init__(self, base_dir: str = "."):
        # Auto-detect: try /app first (docker), fallback to . (local)
        if os.path.exists("/app") and os.access("/app", os.W_OK):
            self.base_dir = Path("/app")
        else:
            self.base_dir = Path(".")

        self.versions_dir = self.base_dir / "iceberg-alembic" / "migrations" / "versions"
        self.state_file = self.base_dir / "iceberg-alembic" / ".iceberg-alembic-state.json"
        self.log_file = self.base_dir / "iceberg-alembic" / "migration_fix.log"
        self.migrations = []
        self.changes = []

        # Ensure log directory exists
        try:
            self.log_file.parent.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            pass  # Directory already exists and is readable

    def log(self, message: str, level: str = "INFO"):
        """Log messages to file and stdout."""
        timestamp = datetime.now().isoformat()
        log_msg = f"[{timestamp}] [{level}] {message}"
        print(log_msg)

        with open(self.log_file, "a") as f:
            f.write(log_msg + "\n")

    def scan_migrations(self):
        """Scan and parse all migration files."""
        if not self.versions_dir.exists():
            self.log(f"Versions directory not found: {self.versions_dir}", "ERROR")
            return False

        try:
            files = sorted([
                f for f in self.versions_dir.glob("*.py")
                if f.name != "__pycache__" and f.is_file()
            ])

            for filepath in files:
                match = re.match(r"(\d{8})_(\d{3})_(.+)\.py", filepath.name)
                if match:
                    date_str = match.group(1)
                    version_str = match.group(2)
                    description = match.group(3)

                    # Read file content
                    with open(filepath, "r") as f:
                        content = f.read()

                    # Extract current revision
                    rev_match = re.search(r'revision = "([^"]+)"', content)
                    current_revision = rev_match.group(1) if rev_match else None

                    # Extract current down_revision
                    down_match = re.search(r'down_revision = (None|"([^"]*)")', content)
                    current_down = None
                    if down_match:
                        current_down = down_match.group(1)

                    # Calculate checksum
                    checksum = hashlib.sha256(content.encode()).hexdigest()

                    self.migrations.append({
                        "filepath": filepath,
                        "filename": filepath.name,
                        "date": int(date_str),
                        "version": int(version_str),
                        "description": description,
                        "current_revision": current_revision,
                        "current_down_revision": current_down,
                        "content": content,
                        "checksum": checksum,
                    })

            self.log(f"Found {len(self.migrations)} migration files")
            return True

        except Exception as e:
            self.log(f"Error scanning migrations: {e}", "ERROR")
            return False

    def fix_migrations(self):
        """Fix revision and down_revision in all migration files."""
        if not self.migrations:
            self.log("No migrations found", "WARN")
            return False

        try:
            # Sort by date, then version
            self.migrations.sort(key=lambda x: (x["date"], x["version"]))

            for i, mig in enumerate(self.migrations):
                expected_revision = f"{mig['date']}_{mig['version']:03d}_{mig['description']}"

                # Determine expected down_revision
                if i == 0:
                    expected_down = "None"
                    expected_down_str = "None"
                else:
                    prev_mig = self.migrations[i - 1]
                    expected_down_str = f'"{prev_mig["date"]}_{prev_mig["version"]:03d}_{prev_mig["description"]}"'
                    expected_down = expected_down_str

                # Check if changes needed
                needs_fix = (
                    mig["current_revision"] != expected_revision or
                    str(mig["current_down_revision"]) != expected_down
                )

                if needs_fix:
                    # Fix the file content
                    content = mig["content"]

                    # Update revision
                    content = re.sub(
                        r'revision = "[^"]+"',
                        f'revision = "{expected_revision}"',
                        content
                    )

                    # Update down_revision
                    content = re.sub(
                        r'down_revision = (?:None|"[^"]+")',
                        f'down_revision = {expected_down}',
                        content
                    )

                    # Write back to file
                    with open(mig["filepath"], "w") as f:
                        f.write(content)

                    # Record change
                    self.changes.append({
                        "file": mig["filename"],
                        "old_revision": mig["current_revision"],
                        "new_revision": expected_revision,
                        "old_down_revision": mig["current_down_revision"],
                        "new_down_revision": expected_down,
                    })

                    self.log(
                        f"Fixed {mig['filename']}: "
                        f"{mig['current_revision']} → {expected_revision}",
                        "INFO"
                    )

            if self.changes:
                self.log(f"Fixed {len(self.changes)} migration files", "INFO")
            else:
                self.log("All migrations already have correct versioning", "INFO")

            return True

        except Exception as e:
            self.log(f"Error fixing migrations: {e}", "ERROR")
            return False

    def update_state_file(self):
        """Update checksums in state file if it exists and has entries."""
        if not self.state_file.exists():
            self.log("State file not found (migrations not yet applied)", "INFO")
            return True

        try:
            with open(self.state_file, "r") as f:
                state = json.load(f)

            if not isinstance(state, list):
                self.log("State file format incorrect", "WARN")
                return False

            # Build checksum map
            checksum_map = {mig["current_revision"]: mig["checksum"] for mig in self.migrations}

            # Update checksums in state
            updated_count = 0
            for entry in state:
                if entry.get("revision") in checksum_map:
                    old_checksum = entry.get("checksum")
                    new_checksum = checksum_map[entry["revision"]]

                    if old_checksum != new_checksum:
                        entry["checksum"] = new_checksum
                        updated_count += 1
                        self.log(
                            f"Updated checksum for {entry['revision']}"
                        )

            # Write back state file
            with open(self.state_file, "w") as f:
                json.dump(state, f, indent=2)

            if updated_count > 0:
                self.log(f"Updated {updated_count} checksums in state file", "INFO")
            else:
                self.log("All checksums already up to date", "INFO")

            return True

        except Exception as e:
            self.log(f"Error updating state file: {e}", "ERROR")
            return False

    def print_summary(self):
        """Print summary of all migrations."""
        print("\n" + "="*80)
        print("MIGRATION CHAIN SUMMARY")
        print("="*80)

        for i, mig in enumerate(self.migrations):
            print(f"\n{i+1}. {mig['filename']}")
            print(f"   Revision: {mig['current_revision']}")
            print(f"   Down Rev: {mig['current_down_revision']}")

        if self.changes:
            print("\n" + "="*80)
            print(f"CHANGES APPLIED ({len(self.changes)} files fixed)")
            print("="*80)
            for change in self.changes:
                print(f"\n✓ {change['file']}")
                print(f"  Revision: {change['old_revision']} → {change['new_revision']}")
                print(f"  Down Rev: {change['old_down_revision']} → {change['new_down_revision']}")
        else:
            print("\n✓ All migrations are correctly versioned")

        print("\n" + "="*80 + "\n")

    def run(self):
        """Execute the complete fix process."""
        self.log("Starting migration version fix", "INFO")

        if not self.scan_migrations():
            return False

        if not self.fix_migrations():
            return False

        if not self.update_state_file():
            return False

        self.print_summary()
        self.log("Migration fix completed successfully", "INFO")
        return True


def main():
    fixer = MigrationFixer()  # Auto-detects base_dir
    success = fixer.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
