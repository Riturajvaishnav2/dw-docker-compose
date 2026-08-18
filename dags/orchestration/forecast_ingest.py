import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator


def env_or_default(name: str, default: str) -> str:
    return os.environ.get(name) or default


def discover_and_check_files(**context):
    """Discover files and branch: Skip everything if no files found."""
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

                    forecast_dir = None
                    forecast_search_prefix = f"{SOURCE_PREFIX}/{tenant_name_original}/{bronze_dir}/"
                    for forecast_page in paginator.paginate(Bucket=SOURCE_BUCKET, Prefix=forecast_search_prefix, Delimiter="/"):
                        for forecast_prefix_obj in forecast_page.get("CommonPrefixes", []):
                            dir_name = forecast_prefix_obj["Prefix"].split("/")[-2]
                            if dir_name.lower() == "forecast":
                                forecast_dir = dir_name
                                break
                        if forecast_dir:
                            break

                    if not forecast_dir:
                        continue

                    forecast_prefix = f"{SOURCE_PREFIX}/{tenant_name_original}/{bronze_dir}/{forecast_dir}/"
                    for forecast_page in paginator.paginate(Bucket=SOURCE_BUCKET, Prefix=forecast_prefix):
                        for obj in forecast_page.get("Contents", []):
                            key = obj["Key"]
                            if key.endswith("/"):
                                continue
                            key_lower = key.lower()
                            key_parts = key.split("/")
                            try:
                                forecast_idx = next(i for i, p in enumerate(key_parts) if p.lower() == "forecast")
                                if forecast_idx + 1 < len(key_parts) - 1:
                                    next_dir = key_parts[forecast_idx + 1].lower()
                                    if next_dir in {"processed", "failed", "original", "invalid"}:
                                        continue
                            except StopIteration:
                                continue

                            if key_lower.endswith((".parquet", ".csv")):
                                file_configs.append({
                                    "file_key": key,
                                    "tenant": tenant_name_lower,
                                })
    except Exception as e:
        print(f"Error discovering files: {e}")
        raise

    # Push discovered files to XCom
    context["ti"].xcom_push(key="file_configs", value=file_configs)

    # Branch decision: If no files found, skip all downstream tasks
    if not file_configs:
        print("❌ No forecast files found. Skipping all downstream tasks.")
        return []  # Skip all downstream tasks

    print(f"✅ Found {len(file_configs)} forecast file(s). Proceeding with pipeline.")
    return "archive_forecast_originals"


SOURCE_BUCKET = env_or_default("LANDING_BUCKET", "landing")
SOURCE_PREFIX = env_or_default("TENANT_SOURCE_PREFIX", "tenant").strip("/")


def archive_original_files(**context):
    """Copy original forecast files to original/{date}/ within their source directory."""
    import boto3

    file_configs = context["ti"].xcom_pull(task_ids="discover_forecast_files", key="file_configs") or []
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

        # Prevent double-archiving: only archive files from root Forecast directory
        # file_key should be: tenant/XX/Bronze/Forecast/filename.csv
        # NOT: tenant/XX/Bronze/Forecast/failed/date/filename.csv
        key_parts = source_key.split("/")
        try:
            forecast_idx = next(i for i, p in enumerate(key_parts) if p.lower() == "forecast")
            # Ensure the file is directly in Forecast dir (only 1 more part after Forecast = filename)
            if forecast_idx + 1 != len(key_parts) - 1:
                print(f"Skipping already-archived file: {source_key}")
                continue
        except StopIteration:
            print(f"Warning: Could not find Forecast directory in path: {source_key}")
            continue

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

    print(f"Archived {archived_count}/{len(file_configs)} forecast files")


def move_files_to_error_directory(**context):
    """Move failed files to error/{date}/ directory."""
    import boto3
    from datetime import datetime, timezone

    file_configs = context["ti"].xcom_pull(task_ids="discover_forecast_files", key="file_configs") or []
    if not file_configs:
        print("No files to move to error directory")
        return

    storage_endpoint = env_or_default("OCI_S3_ENDPOINT", "http://minio:9000")
    s3 = boto3.client(
        "s3",
        endpoint_url=storage_endpoint,
        aws_access_key_id=env_or_default("OCI_ACCESS_KEY_ID", ""),
        aws_secret_access_key=env_or_default("OCI_SECRET_ACCESS_KEY", ""),
        region_name=env_or_default("OCI_REGION", "uk-london-1"),
    )

    error_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    error_count = 0

    for file_config in file_configs:
        source_key = file_config["file_key"]

        # Only move files from root Forecast directory
        key_parts = source_key.split("/")
        try:
            forecast_idx = next(i for i, p in enumerate(key_parts) if p.lower() == "forecast")
            if forecast_idx + 1 != len(key_parts) - 1:
                continue
        except StopIteration:
            continue

        parts = source_key.rsplit("/", 1)
        source_dir = parts[0]
        filename = parts[1]
        error_key = f"{source_dir}/error/{error_date}/{filename}"

        try:
            copy_source = {"Bucket": SOURCE_BUCKET, "Key": source_key}
            s3.copy_object(
                CopySource=copy_source,
                Bucket=SOURCE_BUCKET,
                Key=error_key,
            )
            error_count += 1
            print(f"Moved to error: {source_key} → {error_key}")
        except Exception as exc:
            print(f"Warning: Failed to move {source_key} to error: {exc}")

    print(f"Moved {error_count}/{len(file_configs)} files to error directory")


def build_load_command(file_config: dict) -> str:
    """Build bash command to load forecast bronze data.
    Uses IDENTICAL pattern to traffic_ingest - no arguments to load script,
    instead FILE_KEY is passed via environment variable."""
    tenant = file_config["tenant"]
    exports = {
        "FILE_KEY": file_config["file_key"],
        "ICEBERG_NAMESPACE": tenant,
    }
    export_cmd = " && ".join(
        f"export {name}='{value}'" for name, value in exports.items()
    )
    return (
        f"{export_cmd} && "
        "sed 's/\\r$//' /opt/airflow/jobs/common/run_spark_submit.sh | "
        "bash -s -- /opt/airflow/jobs/ingestion/forecast/load_bronze.py"
    )


def build_load_commands(**context):
    file_configs = context["ti"].xcom_pull(task_ids="discover_forecast_files", key="file_configs") or []
    return [build_load_command(file_config) for file_config in file_configs]




def load_silver_dimensions_and_fact(**context):
    """Load silver dimension and fact tables from bronze data."""
    import subprocess

    file_configs = context["ti"].xcom_pull(task_ids="discover_forecast_files", key="file_configs") or []

    if not file_configs:
        print("No forecast files found, skipping silver load")
        return

    tenants = list(set(fc["tenant"] for fc in file_configs))

    try:
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
                    f"export ICEBERG_NAMESPACE={tenant} && "
                    "sed 's/\\r$//' /opt/airflow/jobs/common/run_spark_submit.sh | "
                    "bash -s -- /opt/airflow/jobs/ingestion/forecast/load_silver.py"
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
                    f"export ICEBERG_NAMESPACE={tenant} && "
                    "sed 's/\\r$//' /opt/airflow/jobs/common/run_spark_submit.sh | "
                    "bash -s -- /opt/airflow/jobs/ingestion/forecast/load_silver_fact.py"
                ],
                timeout=3600,
            )

            if fact_result.returncode != 0:
                raise AirflowException(f"Failed to load fact table for tenant {tenant}")

            print(f"Completed silver layer load for {tenant}")
    except Exception as e:
        print(f"❌ Silver layer load failed: {e}")
        move_files_to_error_directory(**context)
        raise


def load_gold_layer(**context):
    """Load gold layer tables (daily and monthly) from silver tables."""
    import subprocess

    file_configs = context["ti"].xcom_pull(task_ids="discover_forecast_files", key="file_configs") or []

    if not file_configs:
        print("No forecast files found, skipping gold load")
        return

    tenants = list(set(fc["tenant"] for fc in file_configs))

    try:
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
                    f"export ICEBERG_NAMESPACE={tenant} && "
                    "sed 's/\\r$//' /opt/airflow/jobs/common/run_spark_submit.sh | "
                    "bash -s -- /opt/airflow/jobs/ingestion/forecast/load_gold_daily.py"
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
                    f"export ICEBERG_NAMESPACE={tenant} && "
                    "sed 's/\\r$//' /opt/airflow/jobs/common/run_spark_submit.sh | "
                    "bash -s -- /opt/airflow/jobs/ingestion/forecast/load_gold_monthly.py"
                ],
                timeout=3600,
            )

            if monthly_result.returncode != 0:
                raise AirflowException(f"Failed to load gold monthly table for tenant {tenant}")

            print(f"Completed gold layer load for {tenant}")
    except Exception as e:
        print(f"❌ Gold layer load failed: {e}")
        move_files_to_error_directory(**context)
        raise


with DAG(
    dag_id="bronze_forecast_ingest",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["iceberg", "forecast", "bronze", "silver", "gold", "tenant"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    description=(
        "Discover and load Bronze/Forecast files, populate silver dimensions and fact, "
        "then load gold daily and monthly tables"
    ),
) as dag:
    discover_files = BranchPythonOperator(
        task_id="discover_forecast_files",
        python_callable=discover_and_check_files,
    )

    archive_files = PythonOperator(
        task_id="archive_forecast_originals",
        python_callable=archive_original_files,
    )

    prepare_load_commands = PythonOperator(
        task_id="prepare_forecast_load_commands",
        python_callable=build_load_commands,
        do_xcom_push=True,
    )

    load_forecast = BashOperator.partial(
        task_id="load_bronze_forecast_to_iceberg",
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

    # Branch: If no files found (returns []), skip all downstream; otherwise continue pipeline
    discover_files >> archive_files >> prepare_load_commands >> load_forecast >> load_silver >> load_gold
