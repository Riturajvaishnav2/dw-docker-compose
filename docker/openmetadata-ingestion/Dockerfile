# Uses the official OpenMetadata ingestion image which has all connectors
# (Trino, Airflow, Superset) and apache-airflow pre-installed.
# Entrypoint is overridden in docker-compose so the internal Airflow does not
# start (we use this container solely as a CLI runner).
#
# The Pygments pin resolves a version conflict between diff-cover (bundled in
# the image) and openmetadata-ingestion 1.4.0.
FROM openmetadata/ingestion:1.4.0

USER airflow
RUN pip install --no-cache-dir \
    "Pygments>=2.19.1,<3.0.0" \
    "clickhouse-sqlalchemy>=0.3.0" \
    "pinotdb>=0.4.0"

WORKDIR /opt/airflow
