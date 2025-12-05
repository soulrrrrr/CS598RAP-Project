# TPC-DS Lakehouse Format Comparison Benchmark

## Overview

This project benchmarks three open table formats using DuckDB as the query engine:
- **DuckLake** (DuckDB native - baseline)
- **Apache Iceberg** (local filesystem)
- **Delta Lake**

Based on the CIDR 2023 paper "Analyzing and Comparing Lakehouse Storage Systems" with modifications for DuckDB.

## Table of Contents

- [Setup](#setup)
- [Quick Start](#quick-start)
- [Data Generation](#data-generation)
- [Running Benchmarks](#running-benchmarks)
- [Named Benchmark Runs](#named-benchmark-runs)
- [Multi-Scale Testing](#multi-scale-testing)
- [Insert Latency Benchmark](#insert-latency-benchmark)
- [Visualizations](#visualizations)
- [Data Verification](#data-verification)
- [Project Structure](#project-structure)

## Setup

### Environment Setup

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Install Docker (required for Iceberg/Delta setup)
# https://docs.docker.com/get-docker/
```

### System Requirements

- Python 3.8+
- Docker and Docker Compose
- 16GB+ RAM recommended for 10GB datasets
- 50GB+ free disk space

## Quick Start

### 1. Generate Sample Data (1GB)

```bash
# Generate all formats for 1GB dataset
python3 generate_data.py --scale 1 --format all

# For Iceberg: Post-processing to use local filesystem
docker exec mc mc mirror minio/warehouse/tpcds /tmp/iceberg_local_copy
docker cp mc:/tmp/iceberg_local_copy data/iceberg_local
docker compose -f iceberg/docker-compose.yml down
```

### 2. Run Benchmark

```bash
# Run benchmark with custom name
python3 run_benchmark.py --scale 1 --name quick_test
```

### 3. View Results

```bash
# List all runs
python3 list_runs.py

# Results are saved in: results/quick_test/
```

## Data Generation

### Unified Data Generation Command

```bash
# Generate all formats at once (recommended)
python3 generate_data.py --scale <SCALE> --format all

# SCALE: 1 (1GB), 5 (5GB), or 10 (10GB)
```

### Generate Individual Formats

```bash
# Parquet only (source of truth)
python3 generate_data.py --scale 5 --format parquet

# DuckLake only (fastest - direct generation)
python3 generate_data.py --scale 5 --format ducklake

# DuckLake from existing Parquet
python3 generate_data.py --scale 5 --format ducklake --from-parquet

# Delta Lake (requires Parquet first)
python3 generate_data.py --scale 5 --format delta

# Apache Iceberg (requires Parquet first)
python3 generate_data.py --scale 5 --format iceberg
```

### Iceberg Post-Processing

After generating Iceberg format, copy data to local filesystem to eliminate network overhead:

```bash
docker exec mc mc mirror minio/warehouse/tpcds /tmp/iceberg_local_copy
docker cp mc:/tmp/iceberg_local_copy data/iceberg_local
docker compose -f iceberg/docker-compose.yml down
```

### Dataset Sizes

| Scale | Data Size | Store Sales Rows | Generation Time | Typical Query Time |
|-------|-----------|------------------|-----------------|-------------------|
| 1     | ~1GB      | 2,880,052        | ~30s            | ~0.02s           |
| 5     | ~5GB      | 14,400,265       | ~2-3min         | ~0.08s           |
| 10    | ~10GB     | 28,800,530       | ~5-6min         | ~0.14s           |

## Running Benchmarks

### Basic Benchmark Run

```bash
# Run with custom name
python3 run_benchmark.py --scale 5 --name baseline

# Run without name (uses timestamp YYYYMMDD_HHMMSS)
python3 run_benchmark.py --scale 5
```

This will:
1. Update config to match scale factor
2. Run comparison benchmark (DuckLake, Iceberg, Delta)
3. Execute 10 TPC-DS queries with 3 warm runs each
4. Generate visualizations
5. Save results to `results/<name>/`

### Manual Benchmark Execution

```bash
# Step 1: Update config
python3 update_config_scale.py 5

# Step 2: Run benchmark
python3 benchmark/execution/run_comparison_benchmark.py

# Step 3: Generate visualizations (optional)
python3 benchmark/visualization/create_query_execution_charts.py
```

## Named Benchmark Runs

### Directory Structure

```
results/
├── baseline/                 # Your named run
│   ├── comparison_results.json
│   ├── comparison_query_results.csv
│   ├── query_execution_times.png
│   ├── execution_summary.png
│   └── run_metadata.json
├── optimized/                # Another named run
└── 20250104_120000/         # Timestamp run
```

### List All Runs

```bash
# Simple list
python3 list_runs.py

# Detailed information
python3 list_runs.py --detailed
```

### Compare Two Runs

```bash
# Compare named runs
python3 compare_runs.py baseline optimized

# Compare timestamp runs
python3 compare_runs.py 20250104_120000 20250104_150000
```

Comparison includes:
- Average query times by format
- Query-by-query performance differences
- Visual indicators (🟢 improved, 🔴 regressed, ⚪ similar)
- Improvement/regression counts

### Common Workflows

#### Baseline and Optimization Comparison

```bash
# 1. Run baseline
python3 run_benchmark.py --scale 5 --name baseline

# 2. Make system changes

# 3. Run optimized version
python3 run_benchmark.py --scale 5 --name optimized

# 4. Compare
python3 compare_runs.py baseline optimized
```

#### Configuration Testing

```bash
# Test different configurations
python3 run_benchmark.py --scale 5 --name config_default
python3 run_benchmark.py --scale 5 --name config_tuned

# Compare
python3 compare_runs.py config_default config_tuned
```

## Multi-Scale Testing

### Automated Multi-Scale Benchmark

```bash
# Runs 5GB and 10GB benchmarks automatically
./run_multi_scale_benchmark.sh
```

This script:
1. Generates 5GB data (all formats)
2. Runs 5GB benchmark
3. Generates 10GB data (all formats)
4. Runs 10GB benchmark
5. Creates cross-scale analysis

Runtime: ~2-3 hours

### Cross-Scale Analysis

```bash
# Analyze all available scales (auto-detects 1GB, 5GB, 10GB, etc.)
python3 benchmark/visualization/create_scale_analysis.py
```

Creates:
- `results/scale_comparison.png` - Bar chart across scales
- `results/scaling_efficiency.png` - Line chart showing trends
- `results/scale_summary_table.png` - Summary statistics

## Insert Latency Benchmark

Compares insert performance between DuckLake (local) and Iceberg (REST catalog + MinIO).

### Running Insert Latency Test

```bash
./run_insert_latency_benchmark.sh
```

**Test Configuration:**
- Batch sizes: 100, 1K, 10K, 100K rows
- Test tables: store_sales, customer
- Metrics: Insert time, throughput, overhead percentage

**Runtime:** ~10-15 minutes

**Results:**
- `results/insert_latency_results_*.json`
- `results/insert_latency_analysis.png`
- `results/insert_latency_scaling.png`

### What Gets Measured

**DuckLake (Baseline):**
- Local database writes
- No network overhead
- Minimal metadata management

**Iceberg REST:**
- REST catalog API calls
- MinIO (S3) storage writes
- Metadata versioning
- Network latency

## Visualizations

### Generate Visualizations

```bash
# For latest results
python3 benchmark/visualization/create_query_execution_charts.py

# For specific named run
python3 benchmark/visualization/create_query_execution_charts.py --run baseline

# Cross-scale analysis
python3 benchmark/visualization/create_scale_analysis.py
```

### Visualization Outputs

**Query Execution Charts:**
- `query_execution_times.png` - Bar chart of query times by format
- `execution_summary.png` - Performance summary table

**Cross-Scale Analysis:**
- `scale_comparison.png` - Performance across dataset sizes
- `scaling_efficiency.png` - Scaling trends
- `scale_summary_table.png` - Statistics table

**Insert Latency:**
- `insert_latency_analysis.png` - Insert performance comparison
- `insert_latency_scaling.png` - Throughput scaling

## Data Verification

### Verify Data Scale

```bash
# Check all formats and detect scale automatically
python3 verify_data_scale.py
```

Output shows:
- Row counts for each format (DuckLake, Delta, Iceberg)
- Detected scale factor
- Config file consistency check

### Manual Verification

```bash
# DuckLake
python3 -c "
import duckdb
conn = duckdb.connect()
conn.execute('INSTALL ducklake; LOAD ducklake')
conn.execute(\"ATTACH 'ducklake:data/ducklake/metadata.ducklake' AS db (DATA_PATH 'data/ducklake/data_files', OVERRIDE_DATA_PATH true)\")
count = conn.execute('SELECT COUNT(*) FROM db.store_sales').fetchone()[0]
print(f'DuckLake: {count:,} rows')
"

# Delta
python3 -c "
import duckdb
conn = duckdb.connect()
conn.execute('INSTALL delta; LOAD delta')
count = conn.execute(\"SELECT COUNT(*) FROM delta_scan('data/delta/store_sales')\").fetchone()[0]
print(f'Delta: {count:,} rows')
"

# Iceberg
python3 -c "
import duckdb
conn = duckdb.connect()
conn.execute('INSTALL iceberg; LOAD iceberg')
conn.execute('SET unsafe_enable_version_guessing=true')
count = conn.execute(\"SELECT COUNT(*) FROM iceberg_scan('data/iceberg_local/store_sales', allow_moved_paths=true)\").fetchone()[0]
print(f'Iceberg: {count:,} rows')
"
```

### Expected Row Counts

**Scale Factor 1 (1GB):**
- store_sales: 2,880,052 rows
- customer: 100,000 rows
- item: 18,000 rows

**Scale Factor 5 (5GB):**
- store_sales: 14,400,265 rows
- customer: 500,000 rows
- item: 90,000 rows

**Scale Factor 10 (10GB):**
- store_sales: 28,800,530 rows
- customer: 1,000,000 rows
- item: 180,000 rows

## Project Structure

```
.
├── generate_data.py                # Unified data generation
├── run_benchmark.py                # Run benchmark with naming
├── list_runs.py                    # List all benchmark runs
├── compare_runs.py                 # Compare two runs
├── update_config_scale.py          # Update config scale factor
├── verify_data_scale.py            # Verify data consistency
│
├── benchmark/
│   ├── core/
│   │   └── query_executor.py       # Query execution engine
│   ├── data_generation/
│   │   └── archived/               # Old generation scripts
│   ├── execution/
│   │   └── run_comparison_benchmark.py
│   ├── metrics/
│   │   └── collector.py            # Metrics collection
│   ├── preparation/
│   │   ├── setup_delta_conversion.sh
│   │   └── setup_iceberg_conversion.sh
│   └── visualization/
│       ├── create_query_execution_charts.py
│       ├── create_scale_analysis.py
│       ├── create_insert_latency_visualizations.py
│       └── archived/               # Old visualization scripts
│
├── config/
│   └── benchmark_config.yaml       # Centralized configuration
│
├── data/
│   ├── tpcds_raw/                  # Parquet files (source)
│   ├── ducklake/                   # DuckLake database
│   ├── iceberg_local/              # Iceberg tables
│   └── delta/                      # Delta Lake tables
│
├── results/
│   ├── <run_name>/                 # Named benchmark runs
│   └── multi_scale/                # Multi-scale results
│       ├── 1gb/
│       ├── 5gb/
│       └── 10gb/
│
└── _queries/                       # TPC-DS SQL query files
```

## Key Scripts and Commands

### Data Management

```bash
# Generate all formats for 5GB
python3 generate_data.py --scale 5 --format all

# Verify data scale
python3 verify_data_scale.py

# Update config scale
python3 update_config_scale.py 5
```

### Benchmark Execution

```bash
# Named run
python3 run_benchmark.py --scale 5 --name experiment1

# List all runs
python3 list_runs.py

# Compare runs
python3 compare_runs.py run1 run2
```

### Visualization

```bash
# Query execution charts
python3 benchmark/visualization/create_query_execution_charts.py

# Cross-scale analysis
python3 benchmark/visualization/create_scale_analysis.py

# For specific run
python3 benchmark/visualization/create_query_execution_charts.py --run baseline
```

### Shell Scripts

```bash
# Multi-scale benchmark (5GB + 10GB)
./run_multi_scale_benchmark.sh

# Insert latency benchmark
./run_insert_latency_benchmark.sh
```

## Configuration

### Benchmark Config (`config/benchmark_config.yaml`)

Key settings:
- `scale_factor`: Dataset size (1, 5, or 10)
- `queries`: List of TPC-DS query IDs to run
- `num_query_runs`: Number of warm runs per query (default: 3)
- Paths to data directories
- DuckDB configuration (memory limit, threads)

### TPC-DS Queries

10 representative queries tested:
- Q3, Q7, Q19, Q25, Q34, Q42, Q52, Q55, Q73, Q79

Query SQL files located in `_queries/` directory.

## Typical Workflows

### Daily Development Testing

```bash
# Quick 1GB test
python3 generate_data.py --scale 1 --format all
python3 run_benchmark.py --scale 1 --name dev_$(date +%Y%m%d)
```

### Performance Regression Testing

```bash
# Baseline
python3 run_benchmark.py --scale 5 --name v1.0_baseline

# After changes
python3 run_benchmark.py --scale 5 --name v1.1_candidate

# Compare
python3 compare_runs.py v1.0_baseline v1.1_candidate
```

### Comprehensive Testing

```bash
# Full multi-scale test
./run_multi_scale_benchmark.sh

# Analyze results
python3 benchmark/visualization/create_scale_analysis.py
```

## Tips and Best Practices

1. **Always use Parquet as source of truth**: Generate Parquet first, then convert to other formats to ensure data consistency

2. **Name your runs descriptively**: Use names like `baseline`, `optimized`, `config_v2` rather than generic names

3. **Verify data before benchmarking**: Run `python3 verify_data_scale.py` to ensure all formats have consistent data

4. **Clean up old runs**: Delete unnecessary runs to save disk space:
   ```bash
   rm -rf results/old_run_name
   ```

5. **Use appropriate scales**:
   - 1GB for quick development tests
   - 5GB for standard benchmarks
   - 10GB for comprehensive analysis

6. **Monitor Docker resources**: For Iceberg/Delta conversion, ensure Docker has sufficient memory allocated

## Troubleshooting

### Issue: "Parquet files not found"
**Solution:** Generate Parquet first:
```bash
python3 generate_data.py --scale 5 --format parquet
```

### Issue: Docker container not found
**Solution:** Conversion scripts start Docker automatically, but if issues persist:
```bash
docker compose -f iceberg/docker-compose.yml up -d
sleep 10
```

### Issue: Data scale mismatch
**Solution:** Regenerate data for all formats from same Parquet source:
```bash
rm -rf data/ducklake data/delta data/iceberg_local
python3 generate_data.py --scale 5 --format all
```

### Issue: Out of disk space
**Solution:** Clean up old runs and data backups:
```bash
rm -rf data_backups/
rm -rf results/old_*
```

## References

- CIDR 2023 Paper: "Analyzing and Comparing Lakehouse Storage Systems"
- LHBench Repository: https://github.com/lhbench/lhbench
- DuckDB Iceberg Extension: https://duckdb.org/docs/extensions/iceberg
- DuckDB Delta Extension: https://duckdb.org/docs/extensions/delta
