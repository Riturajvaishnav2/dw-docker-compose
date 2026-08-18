FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy and install iceberg-alembic package
COPY iceberg-alembic/ /app/iceberg-alembic/

WORKDIR /app/iceberg-alembic
RUN pip install -e . && pip install --upgrade pip setuptools
WORKDIR /app

# Copy scripts for management and utilities
COPY scripts/ /app/scripts/

# Make scripts executable and add to PATH
RUN chmod +x /app/scripts/*.sh && \
    chmod +x /app/iceberg-alembic/scripts/*.py && \
    chmod +x /app/iceberg-alembic/scripts/*.sh && \
    ln -s /app/scripts /scripts && \
    chmod 755 /app/scripts && \
    chmod 755 /app/iceberg-alembic/scripts

# Create directory for migrations
RUN mkdir -p /migrations

WORKDIR /migrations

# Copy entrypoint script
COPY docker/iceberg-alembic/entrypoint.sh /entrypoint.sh
COPY docker/iceberg-alembic/verify_migrations.py /verify_migrations.py
RUN chmod +x /entrypoint.sh

# Set PATH to include scripts directory and Python bin
ENV PATH="/app/scripts:/usr/local/bin:${PATH}"


# Health check
HEALTHCHECK --interval=10s --timeout=10s --retries=5 --start-period=120s \
  CMD python /verify_migrations.py || exit 1

# Run entrypoint script (auto-runs migrations then keeps container alive)
ENTRYPOINT ["/entrypoint.sh"]
