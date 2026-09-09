#!/usr/bin/env bash
set -euo pipefail

# Uploads local seed data (./data, mounted at /opt/airflow/data) into the
# landing bucket, mirroring the same "OCI:" rclone remote used by
# bootstrap_ingestion_prefixes.sh. OCI_* vars point at MinIO for local dev
# and at real Oracle Cloud Object Storage when USE_OCI_STORAGE=true.

if [ "${USE_OCI_STORAGE:-false}" != "true" ]; then
  echo "USE_OCI_STORAGE is not 'true' — skipping upload_to_oracle_s3.sh (local MinIO dev doesn't need seed data pre-uploaded)."
  exit 0
fi

LANDING_BUCKET="${LANDING_BUCKET:-landing}"
LOCAL_DATA_DIR="${LOCAL_DATA_DIR:-/opt/airflow/data}"

export RCLONE_CONFIG=/dev/null
export RCLONE_CONFIG_OCI_TYPE=s3
export RCLONE_CONFIG_OCI_PROVIDER=Other
export RCLONE_CONFIG_OCI_ACCESS_KEY_ID="${OCI_ACCESS_KEY_ID}"
export RCLONE_CONFIG_OCI_SECRET_ACCESS_KEY="${OCI_SECRET_ACCESS_KEY}"
export RCLONE_CONFIG_OCI_ENDPOINT="${OCI_S3_ENDPOINT}"
export RCLONE_CONFIG_OCI_REGION="${OCI_REGION}"

if [ ! -d "${LOCAL_DATA_DIR}" ] || [ -z "$(ls -A "${LOCAL_DATA_DIR}" 2>/dev/null)" ]; then
  echo "No local seed data found under ${LOCAL_DATA_DIR} — nothing to upload."
  exit 0
fi

echo "Uploading ${LOCAL_DATA_DIR} to OCI:${LANDING_BUCKET} ..."
rclone copy "${LOCAL_DATA_DIR}" "OCI:${LANDING_BUCKET}" --s3-no-check-bucket --progress
echo "Upload to OCI:${LANDING_BUCKET} complete."
