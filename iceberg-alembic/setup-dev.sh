#!/bin/bash
set -e

echo "Setting up iceberg-alembic development environment..."

# Check Python version
PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
echo "Found Python: $PYTHON_VERSION"

if [[ ! "$PYTHON_VERSION" =~ ^3\.(10|11) ]]; then
    echo "ERROR: Python 3.10 or 3.11 required, found $PYTHON_VERSION"
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source .venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip setuptools wheel

# Install package with SQL support
echo "Installing iceberg-alembic with PostgreSQL support..."
pip install -e .

# Initialize migration state
if [ ! -f ".iceberg-alembic-state.json" ]; then
    echo "Initializing migration state..."
    iceberg-migrate init
else
    echo "Migration state already initialized."
fi

echo ""
echo "✓ Development environment ready!"
echo ""
echo "Next steps:"
echo "1. Ensure docker compose stack is running:"
echo "   docker compose up --build -d"
echo ""
echo "2. Source the virtual environment:"
echo "   source .venv/bin/activate"
echo ""
echo "3. Configure catalog (edit iceberg-alembic.toml or set env vars)"
echo ""
echo "4. Run migrations:"
echo "   iceberg-migrate upgrade head --dry-run"
echo "   iceberg-migrate upgrade head"
echo ""
echo "5. Verify:"
echo "   iceberg-migrate current"
echo "   iceberg-migrate history"
echo ""
echo "For more details, see DEVELOPMENT.md"
