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


def discover_traffic_files(**context):
    """Discover all Bronze/Traffic files across all tenant directories."""
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
        # Discover tenant directories (case-insensitive)
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=SOURCE_BUCKET, Prefix=f"{SOURCE_PREFIX}/", Delimiter="/"):
            for common_prefix in page.get("CommonPrefixes", []):
                parts = common_prefix["Prefix"].split("/")
                if len(parts) >= 2 and parts[0] == SOURCE_PREFIX and parts[1]:
                    tenant_name_original = parts[1]
                    tenant_name_lower = tenant_name_original.lower()

                    bronze_dir = None
                    bronze_search_prefix = f"{SOURCE_PREFIX}/{tenant_name_original}/"
                    for bronze_page in paginator.paginate(Bucket=SOURCE_BUCKET, Prefix=bronze_search_prefix, Delimiter="/"):
                        for bronze_prefix_obj in bronze_page.get("CommonPrefixes", []):
                            dir_name = bronze_prefix_obj["Prefix"].split("/")[-2]
                            if dir_name.lower() == "bronze":
                                bronze_dir = dir_name
                                break
                        if bronze_dir:
                            break

                    if not bronze_dir:
                        continue

                    traffic_dir = None
                    traffic_search_prefix = f"{SOURCE_PREFIX}/{tenant_name_original}/{bronze_dir}/"
                    for traffic_page in paginator.paginate(Bucket=SOURCE_BUCKET, Prefix=traffic_search_prefix, Delimiter="/"):
                        for traffic_prefix_obj in traffic_page.get("CommonPrefixes", []):
                            dir_name = traffic_prefix_obj["Prefix"].split("/")[-2]
                            if dir_name.lower() == "traffic":
                                traffic_dir = dir_name
                                break
                        if traffic_dir:
                            break

                    if not traffic_dir:
                        continue

                    traffic_prefix = f"{SOURCE_PREFIX}/{tenant_name_original}/{bronze_dir}/{traffic_dir}/"
                    for traffic_page in paginator.paginate(Bucket=SOURCE_BUCKET, Prefix=traffic_prefix):
                        for obj in traffic_page.get("Contents", []):
                            key = obj["Key"]
                            if key.endswith("/"):
                                continue
                            if any(part.lower() in {"processed", "failed", "original"} for part in key.split("/")):
                                continue
                            if key.lower().endswith((".parquet", ".csv")):
                                file_configs.append({
                                    "file_key": key,
                                    "tenant": tenant_name_lower,
                                })
    except Exception as exc:
        raise AirflowException(f"Failed to list traffic files from S3: {exc}")

    if not file_configs:
        print(f"No unprocessed traffic files found in s3://{SOURCE_BUCKET}/{SOURCE_PREFIX}/*/Bronze/Traffic/")
        return []

    file_configs.sort(key=lambda item: item["file_key"])
    print(f"Found {len(file_configs)} traffic file(s)")
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
        "bash -s -- /opt/airflow/jobs/ingestion/traffic/load_bronze.py"
    )


def build_load_commands(**context):
    file_configs = context["ti"].xcom_pull(task_ids="discover_traffic_files") or []
    return [build_load_command(file_config) for file_config in file_configs]


def archive_original_files(**context):
    """Copy original traffic files to original/{date}/ within their source directory."""
    import boto3

    file_configs = context["ti"].xcom_pull(task_ids="discover_traffic_files") or []
    if not file_configs:
        print("No files to archive")
        return

    storage_endpoint = env_or_default("OCI_S3_ENDPOINT", "http://minio:9000")
    s3 = boto3.client(
        "s3",
        endpoint_url=storage_endpoint,
        aws_access_key_id=env_or_default("OCI_ACCESS_KEY_ID", ""),
        aws_secret_access_key=env_or_default("OCI_SECRET_ACCESS_KEY", ""),
        region_name=env_or_default("OCI_REGION", "uk-london-1"),
    )

    archive_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    archived_count = 0

    for file_config in file_configs:
        source_key = file_config["file_key"]
        parts = source_key.rsplit("/", 1)
        source_dir = parts[0]
        filename = parts[1]
        archive_key = f"{source_dir}/original/{archive_date}/{filename}"

        try:
            copy_source = {"Bucket": SOURCE_BUCKET, "Key": source_key}
            s3.copy_object(
                CopySource=copy_source,
                Bucket=SOURCE_BUCKET,
                Key=archive_key,
            )
            archived_count += 1
            print(f"Archived: {source_key} → {archive_key}")
        except Exception as exc:
            print(f"Warning: Failed to archive {source_key}: {exc}")

    print(f"Archived {archived_count}/{len(file_configs)} traffic files")


def load_silver_dimensions_and_fact(**context):
    """Load silver dimension and fact tables from bronze data."""
    import subprocess
    file_configs = context["ti"].xcom_pull(task_ids="discover_traffic_files") or []

    if not file_configs:
        print("No traffic files found, skipping silver load")
        return

    tenants = list(set(fc["tenant"] for fc in file_configs))

    for tenant in tenants:
        print(f"\n{'='*70}")
        print(f"Loading silver layer for tenant: {tenant}")
        print(f"{'='*70}")

        # Load dimensions
        print(f"Loading dimensions for {tenant}...")
        dim_result = subprocess.run(
            [
                "bash",
                "-c",
                f"export TENANT={tenant} && export ICEBERG_NAMESPACE={tenant} && "
                "sed 's/\\r$//' /opt/airflow/jobs/common/run_spark_submit.sh | "
                "bash -s -- /opt/airflow/jobs/ingestion/traffic/load_silver.py"
            ],
            timeout=3600,
        )

        if dim_result.returncode != 0:
            raise AirflowException(f"Failed to load dimensions for tenant {tenant}")

        # Load fact
        print(f"Loading fact table for {tenant}...")
        fact_result = subprocess.run(
            [
                "bash",
                "-c",
                f"export TENANT={tenant} && export ICEBERG_NAMESPACE={tenant} && "
                "sed 's/\\r$//' /opt/airflow/jobs/common/run_spark_submit.sh | "
                "bash -s -- /opt/airflow/jobs/ingestion/traffic/load_silver_fact.py"
            ],
            timeout=3600,
        )

        if fact_result.returncode != 0:
            raise AirflowException(f"Failed to load fact table for tenant {tenant}")

        print(f"Completed silver layer load for {tenant}")


def load_gold_layer(**context):
    """Load gold layer tables (daily and monthly) from silver tables."""
    import subprocess
    file_configs = context["ti"].xcom_pull(task_ids="discover_traffic_files") or []

    if not file_configs:
        print("No traffic files found, skipping gold load")
        return

    tenants = list(set(fc["tenant"] for fc in file_configs))

    for tenant in tenants:
        print(f"\n{'='*70}")
        print(f"Loading gold layer for tenant: {tenant}")
        print(f"{'='*70}")

        # Load gold daily
        print(f"Loading gold daily table for {tenant}...")
        daily_result = subprocess.run(
            [
                "bash",
                "-c",
                f"export TENANT={tenant} && export ICEBERG_NAMESPACE={tenant} && "
                "sed 's/\\r$//' /opt/airflow/jobs/common/run_spark_submit.sh | "
                "bash -s -- /opt/airflow/jobs/ingestion/traffic/load_gold_daily.py"
            ],
            timeout=3600,
        )

        if daily_result.returncode != 0:
            raise AirflowException(f"Failed to load gold daily table for tenant {tenant}")

        # Load gold monthly
        print(f"Loading gold monthly table for {tenant}...")
        monthly_result = subprocess.run(
            [
                "bash",
                "-c",
                f"export TENANT={tenant} && export ICEBERG_NAMESPACE={tenant} && "
                "sed 's/\\r$//' /opt/airflow/jobs/common/run_spark_submit.sh | "
                "bash -s -- /opt/airflow/jobs/ingestion/traffic/load_gold_monthly.py"
            ],
            timeout=3600,
        )

        if monthly_result.returncode != 0:
            raise AirflowException(f"Failed to load gold monthly table for tenant {tenant}")

        print(f"Completed gold layer load for {tenant}")


with DAG(
    dag_id="bronze_traffic_ingest",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["iceberg", "traffic", "bronze", "silver", "gold", "tenant"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    description=(
        "Discover and load Bronze/Traffic files, populate silver dimensions and fact, "
        "then load gold daily and monthly tables"
    ),
) as dag:
    discover_files = PythonOperator(
        task_id="discover_traffic_files",
        python_callable=discover_traffic_files,
        do_xcom_push=True,
    )

    archive_files = PythonOperator(
        task_id="archive_traffic_originals",
        python_callable=archive_original_files,
    )

    prepare_load_commands = PythonOperator(
        task_id="prepare_traffic_load_commands",
        python_callable=build_load_commands,
        do_xcom_push=True,
    )

    load_traffic = BashOperator.partial(
        task_id="load_bronze_traffic_to_iceberg",
        execution_timeout=timedelta(hours=1),
        retries=0,
        max_active_tis_per_dagrun=1,
    ).expand(
        bash_command=prepare_load_commands.output
    )

    load_silver = PythonOperator(
        task_id="load_silver_dimensions_and_fact",
        python_callable=load_silver_dimensions_and_fact,
        execution_timeout=timedelta(hours=2),
    )

    load_gold = PythonOperator(
        task_id="load_gold_layer",
        python_callable=load_gold_layer,
        execution_timeout=timedelta(hours=2),
    )

    discover_files >> archive_files >> prepare_load_commands >> load_traffic >> load_silver >> load_gold
