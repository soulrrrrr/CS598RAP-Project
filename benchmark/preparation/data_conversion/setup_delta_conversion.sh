#!/bin/bash
set -e

echo "=========================================="
echo "TPC-DS Delta Lake Conversion via Docker"
echo "=========================================="

# Configuration
PROJECT_ROOT="/Users/chongbinyao/dev/CS598RAP-Project"
PARQUET_DATA_PATH="${PROJECT_ROOT}/data/tpcds_raw"
CONVERSION_SCRIPT="${PROJECT_ROOT}/benchmark/preparation/data_conversion/convert_parquet_to_delta_docker.py"
DOCKER_COMPOSE_FILE="${PROJECT_ROOT}/iceberg/docker-compose.yml"

echo ""
echo "Using existing Spark-Iceberg container for Delta conversion..."

echo ""
echo "Step 1: Copying conversion script to Docker container..."
docker cp "${CONVERSION_SCRIPT}" spark-iceberg:/tmp/convert_parquet_to_delta.py

echo ""
echo "Step 2: Installing Delta Lake in container..."
docker exec spark-iceberg pip uninstall -y delta-spark pyspark || true
docker exec spark-iceberg pip install delta-spark==3.2.0

echo ""
echo "Step 3: Running Delta conversion inside Docker container..."
echo "This may take several minutes..."
echo ""
docker exec spark-iceberg python3 /tmp/convert_parquet_to_delta.py

echo ""
echo "Step 4: Copying Delta data back to host..."
mkdir -p "${PROJECT_ROOT}/data/delta"
docker cp spark-iceberg:/tmp/delta/. "${PROJECT_ROOT}/data/delta/"

echo ""
echo "=========================================="
echo "✅ Delta Lake conversion complete!"
echo "=========================================="
echo ""
echo "The Delta Lake tables are now available at: ${PROJECT_ROOT}/data/delta"
