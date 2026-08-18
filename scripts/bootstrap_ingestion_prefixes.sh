#!/usr/bin/env bash
set -euo pipefail

LANDING_BUCKET="${LANDING_BUCKET:-landing}"

# Vars available inside container (mapped by docker-compose from STORAGE_*)

export RCLONE_CONFIG=/dev/null

export RCLONE_CONFIG_OCI_TYPE=s3
export RCLONE_CONFIG_OCI_PROVIDER=Other
export RCLONE_CONFIG_OCI_ACCESS_KEY_ID="${OCI_ACCESS_KEY_ID}"
export RCLONE_CONFIG_OCI_SECRET_ACCESS_KEY="${OCI_SECRET_ACCESS_KEY}"
export RCLONE_CONFIG_OCI_ENDPOINT="${OCI_S3_ENDPOINT}"
export RCLONE_CONFIG_OCI_REGION="${OCI_REGION}"

domains=(
  Discovery Forecasting Negotiation Activation Traffic Rating Settling
)
layers=(
  raw bronze silver gold diamond
)
tenants=(
  EE Orange
)
tenant_layers=(
  Silver Gold
)
stages=(
  Discovery Forecasting Negotiation Activation Traffic Rating Settling
)

for domain in "${domains[@]}"; do
  for layer in "${layers[@]}"; do
    marker_key="${domain}/${layer}/.keep"
    marker_path="OCI:${LANDING_BUCKET}/${marker_key}"

    if ! rclone lsf "${marker_path}" --s3-no-check-bucket >/dev/null 2>&1; then
      echo "" | rclone rcat "${marker_path}" --s3-no-check-bucket
      printf 'Created prefix marker: %s\n' "${marker_path}"
    else
      printf 'Prefix marker already exists: %s\n' "${marker_path}"
    fi
  done
done

for tenant in "${tenants[@]}"; do
  for layer in "${tenant_layers[@]}"; do
    for stage in "${stages[@]}"; do
      marker_key="tenant/${tenant}/${layer}/${stage}/.keep"
      marker_path="OCI:${LANDING_BUCKET}/${marker_key}"

      if ! rclone lsf "${marker_path}" --s3-no-check-bucket >/dev/null 2>&1; then
        echo "" | rclone rcat "${marker_path}" --s3-no-check-bucket
        printf 'Created prefix marker: %s\n' "${marker_path}"
      else
        printf 'Prefix marker already exists: %s\n' "${marker_path}"
      fi
    done
  done
done
