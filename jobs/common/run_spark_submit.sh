#!/usr/bin/env bash
set -euo pipefail

JOB_PATH="${1:?Usage: run_spark_submit.sh /path/to/job.py}"
shift || true

## SPARK_PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1,org.apache.iceberg:iceberg-aws-bundle:1.10.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
SPARK_PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.iceberg:iceberg-aws-bundle:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

# Injected via docker-compose environment block.
_ENDPOINT="${OCI_S3_ENDPOINT:?Error: OCI_S3_ENDPOINT not set}"
_ACCESS_KEY="${OCI_ACCESS_KEY_ID:?Error: OCI_ACCESS_KEY_ID not set}"
_SECRET_KEY="${OCI_SECRET_ACCESS_KEY:?Error: OCI_SECRET_ACCESS_KEY not set}"
_REGION="${OCI_REGION:-uk-london-1}"
_SSL="${USE_OCI_STORAGE:-false}"
_ICEBERG_URI="${ICEBERG_CATALOG_URI:-http://iceberg-rest:8181}"
_CATALOG_WAREHOUSE="${CATALOG_WAREHOUSE:?Error: CATALOG_WAREHOUSE not set}"

# Set Python path for imports
export PYTHONPATH="/opt/airflow:${PYTHONPATH:-}"

spark-submit \
  --master "local[*]" \
  --driver-memory 4g \
  --packages "${SPARK_PACKAGES}" \
  --conf "spark.driver.maxResultSize=2g" \
  --conf "spark.sql.shuffle.partitions=20" \
  --conf "spark.default.parallelism=20" \
  --conf "spark.sql.files.maxPartitionBytes=268435456" \
  --conf "spark.sql.files.openCostInBytes=134217728" \
  --conf "spark.shuffle.manager=sort" \
  --conf "spark.local.dir=/tmp/spark" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
  --conf "spark.sql.adaptive.skewJoin.enabled=true" \
  --conf "spark.sql.parquet.enableVectorizedReader=true" \
  --conf "spark.sql.parquet.filterPushdown=true" \
  --conf "spark.sql.iceberg.handle-timestamp-without-timezone=true" \
  --conf "spark.rpc.askTimeout=600s" \
  --conf "spark.network.timeout=600s" \
  --conf "spark.driver.heartbeatInterval=60s" \
  --conf "spark.executor.heartbeatInterval=60s" \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.iceberg.type=rest" \
  --conf "spark.sql.catalog.iceberg.uri=${_ICEBERG_URI}" \
  --conf "spark.sql.catalog.iceberg.warehouse=s3://${_CATALOG_WAREHOUSE}/" \
  --conf "spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
  --conf "spark.sql.catalog.iceberg.s3.endpoint=${_ENDPOINT}" \
  --conf "spark.sql.catalog.iceberg.s3.path-style-access=true" \
  --conf "spark.sql.catalog.iceberg.s3.access-key-id=${_ACCESS_KEY}" \
  --conf "spark.sql.catalog.iceberg.s3.secret-access-key=${_SECRET_KEY}" \
  --conf "spark.sql.catalog.iceberg.s3.region=${_REGION}" \
  --conf "spark.sql.catalog.iceberg.s3.checksum-enabled=false" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.endpoint=${_ENDPOINT}" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.access.key=${_ACCESS_KEY}" \
  --conf "spark.hadoop.fs.s3a.secret.key=${_SECRET_KEY}" \
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=${_SSL}" \
  --conf "spark.hadoop.fs.s3a.connection.maximum=200" \
  --conf "spark.hadoop.fs.s3a.connection.timeout=600000" \
  --conf "spark.hadoop.fs.s3a.socket.timeout=600000" \
  --conf "spark.hadoop.fs.s3a.attempts.maximum=10" \
  --conf "spark.hadoop.fs.s3a.retry.limit=10" \
  --conf "spark.hadoop.fs.s3a.fast.upload=true" \
  --conf "spark.sql.defaultCatalog=iceberg" \
  --conf "spark.sql.session.timeZone=UTC" \
  "${JOB_PATH}" "$@"
