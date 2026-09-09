import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def env_or_default(name: str, default: str) -> str:
    return os.environ.get(name) or default


SOURCE_BUCKET = env_or_default("LANDING_BUCKET", "landing")
SOURCE_PREFIX = env_or_default("TENANT_SOURCE_PREFIX", "tenant").strip("/")


def discover_settlement_files(**context):
    """Discover all Gold settlement files across all tenant directories.

    Only picks files from tenant/{tenant_name}/Gold/settlement/,
    avoiding Bronze/settlement or Silver/settlement directories.
    """
    import boto3

    storage_endpoint = env_or_default("OCI_S3_ENDPOINT", "http://minio:9000")

    s3 = boto3.client(
        "s3",
        endpoint_url=storage_endpoint,
        aws_access_key_id=env_or_default("OCI_ACCESS_KEY_ID", ""),
        aws_secret_access_key=env_or_default("OCI_SECRET_ACCESS_KEY", ""),
        region_name=env_or_default("OCI_REGION", "uk-london-1"),
    )

    file_configs = []

    try:
        # Discover all tenant directories (case-insensitive)
        paginator = s3.get_paginator("list_objects_v2")
        tenant_dirs = []

        for page in paginator.paginate(Bucket=SOURCE_BUCKET, Prefix=f"{SOURCE_PREFIX}/", Delimiter="/"):
            for common_prefix in page.get("CommonPrefixes", []):
                parts = common_prefix["Prefix"].split("/")
                if len(parts) >= 2 and parts[0] == SOURCE_PREFIX and parts[1]:
                    tenant_name_original = parts[1]
                    tenant_name_lower = tenant_name_original.lower()
                    tenant_dirs.append({
                        "original": tenant_name_original,
                        "lower": tenant_name_lower
                    })

        if not tenant_dirs:
            print(f"No tenant directories found in s3://{SOURCE_BUCKET}/{SOURCE_PREFIX}/")
            return []

        # For each tenant, discover the actual Gold/settlement directory (case-insensitive)
        for tenant in tenant_dirs:
            # First find the actual Gold directory (case-insensitive)
            gold_dir = None
            gold_prefix = f"{SOURCE_PREFIX}/{tenant['original']}/"
            for page in paginator.paginate(Bucket=SOURCE_BUCKET, Prefix=gold_prefix, Delimiter="/"):
                for prefix in page.get("CommonPrefixes", []):
                    dir_name = prefix["Prefix"].split("/")[-2]  # Extract dir name
                    if dir_name.lower() == "gold":
                        gold_dir = dir_name
                        break
                if gold_dir:
                    break

            if not gold_dir:
                continue  # This tenant has no Gold directory

            # Now find the actual settlement directory within Gold (case-insensitive)
            settlement_dir = None
            settlement_search_prefix = f"{SOURCE_PREFIX}/{tenant['original']}/{gold_dir}/"
            for page in paginator.paginate(Bucket=SOURCE_BUCKET, Prefix=settlement_search_prefix, Delimiter="/"):
                for prefix in page.get("CommonPrefixes", []):
                    dir_name = prefix["Prefix"].split("/")[-2]  # Extract dir name
                    if dir_name.lower() == "settlement":
                        settlement_dir = dir_name
                        break
                if settlement_dir:
                    break

            if not settlement_dir:
                continue  # This tenant has no settlement directory

            # List files in the actual Gold/settlement/ directory
            settlement_prefix = f"{SOURCE_PREFIX}/{tenant['original']}/{gold_dir}/{settlement_dir}/"
            for settlement_page in paginator.paginate(Bucket=SOURCE_BUCKET, Prefix=settlement_prefix):
                for obj in settlement_page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith("/"):
                        continue
                    # Skip processed/failed/invalid/original (already archived)
                    if any(part.lower() in {"processed", "failed", "invalid", "original"} for part in key.split("/")):
                        continue
                    if key.lower().endswith((".parquet", ".csv")):
                        file_configs.append({
                            "file_key": key,
                            "tenant": tenant["lower"],
                        })

    except Exception as exc:
        raise AirflowException(f"Failed to list settlement files from S3: {exc}")

    if not file_configs:
        print(f"No unprocessed Gold settlement files found in any tenant directory")
        return []

    file_configs.sort(key=lambda item: item["file_key"])
    print(f"Found {len(file_configs)} Gold settlement file(s) across all tenants")
    return file_configs


def build_load_command(file_config: dict) -> str:
    tenant = file_config["tenant"]
    exports = {
        "FILE_KEY": file_config["file_key"],
        "TENANT": tenant,
        "ICEBERG_NAMESPACE": tenant,
    }
    export_cmd = " && ".join(
        f"export {name}='{value}'" for name, value in exports.items()
    )
    return (
        f"{export_cmd} && "
        "sed 's/\\r$//' /opt/airflow/jobs/common/run_spark_submit.sh | "
        "bash -s -- /opt/airflow/jobs/ingestion/settlement/load_gold.py"
    )


def build_load_commands(**context):
    file_configs = context["ti"].xcom_pull(task_ids="discover_settlement_files") or []
    return [build_load_command(file_config) for file_config in file_configs]


def archive_original_files(**context):
    """Copy original settlement files to original/{date}/ within their source directory."""
    import boto3

    file_configs = context["ti"].xcom_pull(task_ids="discover_settlement_files") or []
    if not file_configs:
        print("No files to archive")
        return

    storage_endpoint = env_or_default("OCI_S3_ENDPOINT", "http://minio:9000")
    access_key = env_or_default("OCI_ACCESS_KEY_ID", "")
    secret_key = env_or_default("OCI_SECRET_ACCESS_KEY", "")

    if not access_key or not secret_key:
        raise AirflowException("OCI_ACCESS_KEY_ID or OCI_SECRET_ACCESS_KEY not set")

    print(f"Connecting to S3: {storage_endpoint}")
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=storage_endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=env_or_default("OCI_REGION", "uk-london-1"),
        )
    except Exception as exc:
        raise AirflowException(f"Failed to create S3 client: {exc}")

    archive_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    archived_count = 0
    print(f"Processing {len(file_configs)} file(s) for archival to {SOURCE_BUCKET}")

    for file_config in file_configs:
        source_key = file_config["file_key"]
        parts = source_key.rsplit("/", 1)
        source_dir = parts[0]
        filename = parts[1]
        archive_key = f"{source_dir}/original/{archive_date}/{filename}"

        try:
            print(f"Archiving: {source_key} → {archive_key}")
            copy_source = {"Bucket": SOURCE_BUCKET, "Key": source_key}
            s3.copy_object(
                CopySource=copy_source,
                Bucket=SOURCE_BUCKET,
                Key=archive_key,
            )
            archived_count += 1
            print(f"✓ Archived: {archive_key}")
        except Exception as exc:
            print(f"✗ Failed to archive {source_key}: {exc}")

    print(f"Archived {archived_count}/{len(file_configs)} settlement files to {SOURCE_BUCKET}")


with DAG(
    dag_id="gold_settlement_ingest",
    start_date=datetime(2024, 1, 1),
    schedule="*/59 * * * *",  # Run every 59 minutes
    catchup=False,
    max_active_runs=1,
    tags=["iceberg", "settlement", "gold", "tenant"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    description=(
        "Discover and load Gold settlement files from all tenant directories "
        "(tenant/{tenant_name}/Gold/settlement/) into Iceberg table. "
        "Runs daily at 11:59 AM and 11:59 PM. "
        "Only picks Gold layer files, avoiding Bronze and Silver settlement directories."
    ),
) as dag:
    discover_files = PythonOperator(
        task_id="discover_settlement_files",
        python_callable=discover_settlement_files,
        do_xcom_push=True,
    )

    archive_files = PythonOperator(
        task_id="archive_settlement_originals",
        python_callable=archive_original_files,
    )

    prepare_load_commands = PythonOperator(
        task_id="prepare_settlement_load_commands",
        python_callable=build_load_commands,
        do_xcom_push=True,
    )

    load_settlement = BashOperator.partial(
        task_id="load_gold_settlement_to_iceberg",
        execution_timeout=timedelta(hours=1),
        retries=0,
        max_active_tis_per_dagrun=1,
    ).expand(
        bash_command=prepare_load_commands.output
    )

    discover_files >> archive_files >> prepare_load_commands >> load_settlement
