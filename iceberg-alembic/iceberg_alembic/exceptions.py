class MigrationError(Exception):
    """Base exception for iceberg-alembic failures."""


class ChecksumError(MigrationError):
    """Raised when a previously applied revision has been modified."""


class LockError(MigrationError):
    """Raised when the migration lock cannot be acquired."""

