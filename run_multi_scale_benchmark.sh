#!/bin/bash
# Automated 5GB and 10GB benchmarks (no sudo required)
# Uses DuckDB to generate Parquet instead of TPC-DS tool

set -e

echo "=========================================="
echo "Multi-Scale TPC-DS Benchmark: 5GB & 10GB"
echo "=========================================="
echo ""
echo "Formats benchmarked: DuckLake, Iceberg, Delta"
echo "(Parquet generated via DuckDB, used only for conversion)"
echo ""

# Activate virtual environment
source venv/bin/activate

# Create directories
mkdir -p data_backups
mkdir -p results/multi_scale

# Function to backup data
backup_data() {
    local scale=$1
    echo ""
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Backing up ${scale}GB data..."
    mkdir -p "data_backups/${scale}gb"

    for format in ducklake iceberg_local delta; do
        if [ -d "data/${format}" ]; then
            rm -rf "data_backups/${scale}gb/${format}"
            cp -r "data/${format}" "data_backups/${scale}gb/${format}"
            echo "  ✓ Backed up ${format}"
        fi
    done
}

# Function to run benchmark
run_benchmark() {
    local scale=$1
    echo ""
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Running benchmark for ${scale}GB..."

    # Update config to match current scale
    echo "  Updating config scale_factor to ${scale}GB..."
    python3 update_config_scale.py ${scale}

    mkdir -p "results/multi_scale/${scale}gb"

    python benchmark/execution/run_comparison_benchmark.py

    # Copy results
    cp results/comparison_results.json "results/multi_scale/${scale}gb/"
    cp results/comparison_query_results.csv "results/multi_scale/${scale}gb/"
    cp results/*.png "results/multi_scale/${scale}gb/" 2>/dev/null || true

    echo "  ✓ Benchmark complete for ${scale}GB"
}

# Function to generate data for a scale
generate_data() {
    local scale=$1
    echo ""
    echo "=========================================="
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Generating ${scale}GB data"
    echo "=========================================="

    # Use unified data generation script to generate all formats
    python3 generate_data.py --scale ${scale} --format all

    # Post-processing for Iceberg: copy to local filesystem and stop Docker
    echo ""
    echo "Post-processing: Moving Iceberg to local filesystem..."
    docker exec mc mc mirror minio/warehouse/tpcds /tmp/iceberg_local_copy
    rm -rf data/iceberg_local
    docker cp mc:/tmp/iceberg_local_copy data/iceberg_local

    echo "Stopping Docker..."
    docker compose -f iceberg/docker-compose.yml down

    echo ""
    echo "✅ All formats generated for ${scale}GB!"
}

# ==============================================================================
# 5GB BENCHMARK
# ==============================================================================

echo ""
echo "╔════════════════════════════════════════╗"
echo "║       STARTING 5GB BENCHMARK           ║"
echo "╚════════════════════════════════════════╝"
echo ""

generate_data 5
backup_data 5
run_benchmark 5

echo ""
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Generating 5GB visualizations..."
python3 benchmark/visualization/create_query_execution_charts.py

echo ""
echo "✅ 5GB Benchmark Complete!"

# ==============================================================================
# 10GB BENCHMARK
# ==============================================================================

echo ""
echo "╔════════════════════════════════════════╗"
echo "║       STARTING 10GB BENCHMARK          ║"
echo "╚════════════════════════════════════════╝"
echo ""

generate_data 10
backup_data 10
run_benchmark 10

echo ""
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Generating 10GB visualizations..."
python3 benchmark/visualization/create_query_execution_charts.py

echo ""
echo "✅ 10GB Benchmark Complete!"

# ==============================================================================
# CROSS-SCALE ANALYSIS
# ==============================================================================

echo ""
echo "╔════════════════════════════════════════╗"
echo "║     CREATING CROSS-SCALE ANALYSIS      ║"
echo "╚════════════════════════════════════════╝"
echo ""

python3 benchmark/visualization/create_scale_analysis.py

# ==============================================================================
# FINAL SUMMARY
# ==============================================================================

echo ""
echo "=========================================="
echo "     ALL BENCHMARKS COMPLETE! ✅"
echo "=========================================="
echo ""
echo "Results location: results/multi_scale/"
echo ""
echo "Per-scale results:"
echo "  • 1GB:  results/multi_scale/1gb/"
echo "  • 5GB:  results/multi_scale/5gb/"
echo "  • 10GB: results/multi_scale/10gb/"
echo ""
echo "Cross-scale analysis:"
echo "  • results/multi_scale/cross_scale_analysis.png"
echo "  • results/multi_scale/detailed_scaling.png"
echo ""
echo "Formats benchmarked:"
echo "  ✓ DuckLake (native)"
echo "  ✓ Apache Iceberg (local filesystem)"
echo "  ✓ Delta Lake"
echo ""
echo "Note: Parquet generated via DuckDB (no sudo required)"
echo "      Parquet NOT included in performance benchmarks"
echo ""
echo "Completed at: $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="
