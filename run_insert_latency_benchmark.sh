#!/bin/bash
set -e

echo "=========================================="
echo "TPC-DS Insert Latency Benchmark"
echo "DuckLake vs RESTful Iceberg"
echo "=========================================="
echo ""

# Configuration
PROJECT_ROOT="/Users/chongbinyao/dev/CS598RAP-Project"
DOCKER_COMPOSE_FILE="${PROJECT_ROOT}/iceberg/docker-compose.yml"

echo "Step 1: Starting Docker services for Iceberg REST catalog..."
echo ""
docker compose -f "${DOCKER_COMPOSE_FILE}" down
docker compose -f "${DOCKER_COMPOSE_FILE}" up -d --wait

echo ""
echo "Step 2: Waiting for services to be ready..."
sleep 10

echo ""
echo "Step 3: Running insert latency benchmark..."
echo "This will test batch sizes: 100, 1000, 10000, 100000 rows"
echo ""

# Activate virtual environment and run benchmark
source venv/bin/activate
export PYTHONPATH="${PROJECT_ROOT}"

python3 benchmark/execution/run_insert_latency_benchmark.py

echo ""
echo "Step 4: Generating visualizations..."
echo ""

python3 benchmark/visualization/create_insert_latency_visualizations.py

echo ""
echo "Step 5: Stopping Docker services..."
docker compose -f "${DOCKER_COMPOSE_FILE}" down

echo ""
echo "=========================================="
echo "✅ Insert Latency Benchmark Complete!"
echo "=========================================="
echo ""
echo "Results saved to:"
echo "  - results/insert_latency_results_*.json"
echo "  - results/insert_latency_results_*.csv"
echo "  - results/insert_latency_analysis.png"
echo "  - results/insert_latency_scaling.png"
echo ""
