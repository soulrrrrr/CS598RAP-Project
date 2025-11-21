#!/bin/bash
set -e

echo "=========================================="
echo "TPC-DS Iceberg Conversion via Docker"
echo "=========================================="

# Configuration
PROJECT_ROOT="/Users/chongbinyao/dev/CS598RAP-Project"
PARQUET_DATA_PATH="${PROJECT_ROOT}/data/tpcds_raw"
CONVERSION_SCRIPT="${PROJECT_ROOT}/benchmark/preparation/convert_parquet_to_iceberg_docker.py"
DOCKER_COMPOSE_FILE="${PROJECT_ROOT}/iceberg/docker-compose.yml"

echo ""
echo "Step 1: Cleaning up any existing Docker containers..."
docker compose -f "${DOCKER_COMPOSE_FILE}" down

echo ""
echo "Step 2: Starting Docker services..."
docker compose -f "${DOCKER_COMPOSE_FILE}" up -d --wait

echo ""
echo "Step 3: Waiting for services to be ready..."
sleep 10

echo ""
echo "Step 4: Copying Parquet data to Docker container..."
docker cp "${PARQUET_DATA_PATH}/." spark-iceberg:/tmp/tpcds_parquet/

echo ""
echo "Step 5: Copying conversion script to Docker container..."
docker cp "${CONVERSION_SCRIPT}" spark-iceberg:/tmp/convert_parquet_to_iceberg.py

echo ""
echo "Step 6: Running conversion inside Docker container..."
echo "This may take several minutes..."
echo ""
docker exec spark-iceberg python3 /tmp/convert_parquet_to_iceberg.py

echo ""
echo "=========================================="
echo "✅ Iceberg conversion complete!"
echo "=========================================="
echo ""
echo "The Iceberg tables are now available in the 'demo.tpcds' database."
echo "You can query them using DuckDB with the iceberg extension."
