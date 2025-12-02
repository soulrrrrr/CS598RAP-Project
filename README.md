# TPC-DS Lakehouse Format Comparison Benchmark

## Overview
This repository provides a comprehensive TPC-DS benchmark framework to compare the performance of three lakehouse table formats:
- **Native Parquet** (baseline)
- **Delta Lake**
- **Apache Iceberg**

All benchmarks use **DuckDB** as the query engine.

## Components
- **TPC-DS-Tool/** — Official TPC-DS data generation tool
- **benchmark/** — Benchmark framework modules (query execution, metrics collection, visualization)
- **delta/** — Delta Lake conversion scripts and Docker setup
- **iceberg/** — Iceberg setup with Spark, MinIO (S3), and REST catalog via Docker
- **_queries/** — TPC-DS query SQL files
- **results/** — Benchmark results and visualization charts
- **config/** — Benchmark configuration (YAML)

## Quick Start

### 1. Prerequisites
```bash
# Install Python dependencies
python3 -m venv venv
source venv/bin/activate  # On macOS/Linux
pip install -r requirements.txt
curl https://install.duckdb.org | sh # duckdb
echo 'export PATH=$HOME/.duckdb/cli/latest:$PATH' >> ~/.bashrc
source ~/.bashrc # add duckdb to PATH

# Install Docker (required for Iceberg and Delta setup)
# https://docs.docker.com/engine/install/ubuntu/
```

### 2. Get TPC-DS Data
**Option A: Download pre-generated 1GB dataset**
- [Download from Google Drive](https://drive.google.com/file/d/1Z2NGxApeZzETB_DGMxRoXMcZaoRuDPMz/view?usp=sharing)
- Extract to `data/tpcds_raw/`

**Option B: Generate data yourself**
```bash
./tpcds_gen.sh
./ducklake/setup_ducklake.sh
./iceberg/setup_iceberg.sh
sudo DATA_ROOT=/mydata ./delta/start.sh # DATA_ROOT is your data path
```

### 3. Convert Data to Table Formats

**Delta Lake:**
```bash
./benchmark/preparation/setup_delta_conversion.sh
```

**Iceberg:**
```bash
./benchmark/preparation/setup_iceberg_conversion.sh
```

### 4. Run Benchmarks

**Full 3-way comparison (Native vs Delta vs Iceberg):**
```bash
source venv/bin/activate
python run_comparison_benchmark.py
```

Results will be saved to `results/` directory with JSON data and PNG charts.

## Testing

### Unit Tests

**1. Test Data Generation**
```bash
python test_generation.py
```
Tests TPC-DS data generation functionality and configuration loading.

**2. Test Query Execution (Native Parquet)**
```bash
python test_query.py
```
Verifies query executor can read and query native Parquet files.

**3. Test Iceberg Connection**
```bash
# Ensure Iceberg Docker containers are running first
docker compose -f iceberg/docker-compose.yml up -d

python test_iceberg_connection.py
```
Tests DuckDB connection to Iceberg tables via MinIO S3 and REST catalog.

### Integration Tests

**4. Mini Benchmark (Quick Test)**
```bash
python test_mini_benchmark.py
```
Runs 2 TPC-DS queries (Q3, Q19) on native format only. Quick smoke test.

**5. Full Comparison Benchmark**
```bash
python run_comparison_benchmark.py
```
Runs complete benchmark across all 3 formats:
- Queries: Q3, Q19 (configurable in script)
- Execution: 1 cold run + 3 warm runs per query
- Metrics: Execution time, median, percentiles
- Output: JSON results + comprehensive visualization chart

### Expected Output

After running the full benchmark, you should see:
```
results/
├── comparison_results.json          # Raw benchmark data
├── comparison_query_results.csv     # CSV export
├── comprehensive_benchmark_chart.png # Main visualization
├── query_comparison.png             # Query-by-query comparison
├── format_summary.png               # Format averages
└── query_details.png                # Cold vs warm run details
```

### Viewing Results

The main visualization (`comprehensive_benchmark_chart.png`) shows:
- **Top panel:** Query-by-query execution times for all 3 formats
- **Bottom panel:** Average performance across all queries with speedup multipliers

Example results (1GB dataset):
- Native Parquet: **16.8ms** average (baseline)
- Delta Lake: **22.7ms** average (1.35x slower)
- Apache Iceberg: **47.7ms** average (2.84x slower)

## Interactive Query Mode

For ad-hoc querying:
```bash
python query.py -i
```

Then select format (iceberg/ducklake) and provide SQL file path.

## Cleanup

**Stop Docker containers:**
```bash
# Iceberg
docker compose -f iceberg/docker-compose.yml down

# Delta (if using Docker-based conversion)
docker compose -f delta/docker-compose.yml down
```

## Configuration

Edit `config/benchmark_config.yaml` to customize:
- TPC-DS scale factor
- Query list
- Data paths
- DuckDB memory settings
- Number of warm runs
