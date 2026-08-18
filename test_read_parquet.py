#!/usr/bin/env python3
"""Test the read_parquet function with local parquet files"""
import sys
import os
import tempfile
sys.path.insert(0, "/opt/airflow")

from pyspark.sql import SparkSession
from dataclasses import dataclass
import logging

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Mock boto3 client for testing
class MockS3Client:
    def download_file(self, bucket, key, local_path):
        """Mock download - just copy the file"""
        import shutil
        if os.path.exists(key):
            shutil.copy2(key, local_path)
        else:
            raise FileNotFoundError(f"Source file not found: {key}")

# Mock Config
@dataclass
class MockConfig:
    source_bucket: str = "landing_test"
    storage_endpoint: str = "http://minio:9000"
    aws_access_key: str = "test"
    aws_secret_key: str = "test"

# Import the actual read_parquet function
from jobs.ingestion.traffic.load_bronze import read_parquet

# Create Spark session
spark = SparkSession.builder \
    .appName("test-read-parquet") \
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
    .getOrCreate()

s3_client = MockS3Client()
config = MockConfig()

# Test files
test_files = [
    "/opt/platform/data/agg_KPNMM_WAREHOUSE_TRAFFIC_202411_KPNMM_TI_20241101.parquet",
    "/opt/platform/data/agg_KPNMM_WAREHOUSE_TRAFFIC_202411_KPNMM_TI_20241102.parquet",
]

print("=" * 80)
print("TESTING read_parquet FUNCTION WITH LOCAL PARQUET FILES")
print("=" * 80)

total_rows = 0
success_count = 0

for idx, file_path in enumerate(test_files, 1):
    print(f"\n[Test {idx}] Reading: {os.path.basename(file_path)}")
    print("-" * 80)

    if not os.path.exists(file_path):
        print(f"✗ File not found: {file_path}")
        continue

    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    print(f"  File size: {file_size_mb:.1f} MB")

    try:
        # Test with local file path
        df, temp_file_path = read_parquet(spark, s3_client, file_path, config)

        row_count = df.count()
        col_count = len(df.columns)

        total_rows += row_count
        success_count += 1

        print(f"  ✓ Read successful!")
        print(f"    - Rows: {row_count:,}")
        print(f"    - Columns: {col_count}")
        print(f"    - Temp file: {temp_file_path}")

        # Show sample schema
        print(f"\n  Schema (first 5 columns):")
        for field in df.schema[:5]:
            print(f"    - {field.name}: {field.dataType}")
        if col_count > 5:
            print(f"    ... and {col_count - 5} more columns")

        # Clean up temp file
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)
            print(f"\n  ✓ Temp file cleaned up: {os.path.basename(temp_file_path)}")

    except Exception as e:
        print(f"  ✗ Failed: {str(e)}")
        import traceback
        traceback.print_exc()

print("\n" + "=" * 80)
print("TEST SUMMARY")
print("=" * 80)
print(f"✓ Files processed: {success_count}/2")
print(f"✓ Total rows read: {total_rows:,}")
if success_count == 2:
    print("✓ Both files processed successfully!")
print("=" * 80)

spark.stop()
