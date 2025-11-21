# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository implements a TPC-DS benchmark framework to compare the performance of three open table formats (Apache Iceberg, DuckLake, Delta Lake) using DuckDB as the query engine. The project follows the methodology from the CIDR 2023 paper "Analyzing and Comparing Lakehouse Storage Systems."

Key differences from the original paper:
- Uses DuckDB instead of Apache Spark as the query engine
- Tests Iceberg, DuckLake (DuckDB-native), and Delta Lake formats
- Uses Docker containers for Iceberg setup with Spark, MinIO (S3-compatible storage), and Iceberg REST catalog

## Common Commands

### Setup and Environment

```bash
# Install Python dependencies (use virtual environment)
python3 -m venv my_venv
source my_venv/bin/activate  # On macOS/Linux
pip3 install -r requirements.txt
```

### Data Generation

```bash
# Generate TPC-DS data (or download from provided link)
./tpcds_gen.sh

# Data should be placed in /mydata/data1gb for the setup scripts
```

### Setup Table Formats

#### Iceberg Setup
```bash
# Start Docker containers for Spark, MinIO, and Iceberg REST catalog
# Then load TPC-DS data into Iceberg format
./iceberg/setup_iceberg.sh

# Stop containers when done
sudo docker compose -f iceberg/docker-compose.yml down
```

#### DuckLake Setup
```bash
# Create DuckLake database with TPC-DS schema and load data
./ducklake/setup_ducklake.sh
```

#### Delta Setup
```bash
# Generate Delta Lake tables
cd delta
./start.sh
```

### Running Queries

```bash
# Interactive mode - query either Iceberg or DuckLake
python3 query.py -i

# Single query mode
python3 query.py iceberg path/to/query.sql
python3 query.py ducklake path/to/query.sql
```

## Architecture

### Table Format Handlers

The project is organized by table format, each with its own directory and setup process:

1. **Iceberg** (`iceberg/`): Uses Docker Compose to orchestrate:
   - `spark-iceberg`: Spark container for data loading into Iceberg format
   - `rest`: Apache Iceberg REST catalog
   - `minio`: S3-compatible object storage
   - `mc`: MinIO client for bucket management
   - Setup script copies TPC-DS data and schema definition into Spark container, then runs `setup_spark_iceberg.py` to load data

2. **DuckLake** (`ducklake/`): DuckDB's native table format:
   - Uses DuckDB extension `ducklake`
   - Schema defined in `tpcds_no_dbgen.sql`
   - Data loading script in `load_data.sql`
   - Metadata stored in `metadata.ducklake`, data files in `data_files/`

3. **Delta Lake** (`delta/`):
   - Script `delta_generate.py` handles conversion to Delta format
   - Setup via `start.sh`

### Query Execution (`query.py`)

The main query interface provides:
- **Querier class**: Manages DuckDB connection and initializes both Iceberg and DuckLake extensions
  - `init_iceberg()`: Sets up Iceberg catalog connection to REST API at localhost:8181, configures S3 secret for MinIO
  - `init_ducklake()`: Attaches DuckLake metadata and data paths
  - `query()`: Switches schema context and executes SQL from file
- **Modes**:
  - Interactive: `-i` flag for repeated queries
  - Single query: `python3 query.py <query_type> <sql_path>`

### TPC-DS Schema (`tpcds_schema.py`)

Defines all 24 TPC-DS table schemas using PySpark StructType definitions. Used by `iceberg/setup_spark_iceberg.py` when loading data into Iceberg format. Schema includes:
- Dimension tables: customer, item, store, date_dim, time_dim, etc.
- Fact tables: store_sales, catalog_sales, web_sales, inventory
- Returns tables: store_returns, catalog_returns, web_returns

### Docker Infrastructure (Iceberg)

Docker Compose configuration (`iceberg/docker-compose.yml`) defines:
- Network: `iceberg_net` for inter-service communication
- Services share AWS credentials (admin/password) for MinIO access
- Warehouse location: `s3://warehouse/` in MinIO
- REST catalog endpoint: `http://localhost:8181`
- Spark UI: port 8888 (Jupyter), 8080 (Spark Master)
- MinIO console: port 9001, API: port 9000

## Key Files

- `query.py`: Main entry point for querying Iceberg/DuckLake tables via DuckDB
- `tpcds_schema.py`: PySpark schema definitions for all TPC-DS tables
- `benchmark_implementation_plan.md`: Detailed implementation plan for benchmark framework
- `_queries/`: Directory containing TPC-DS query SQL files (query_3.sql, query_10.sql, etc.)
- `requirements.txt`: Python dependencies (duckdb, pyarrow, polars)

## Implementation Notes

### When Working with Iceberg:
- Ensure Docker containers are running before querying
- Iceberg tables are accessed via `iceberg_scan()` function or by setting schema to `demo.tpcds`
- Data is stored in MinIO S3 buckets, metadata in REST catalog
- Connection requires httpfs and iceberg extensions in DuckDB

### When Working with DuckLake:
- DuckLake databases must be attached with metadata path and data folder path
- Uses `ATTACH 'ducklake:metadata.ducklake' AS my_ducklake (DATA_PATH 'ducklake/data_files')`
- Can copy entire databases using `COPY FROM DATABASE source TO target`

### When Working with Delta:
- Delta Lake support via DuckDB delta extension
- Use `delta_scan()` function to query Delta tables
- Data loading handled by `delta_generate.py`

## Benchmark Framework (Planned)

The `benchmark_implementation_plan.md` outlines a comprehensive benchmark system with:
1. **Load Performance**: Measure time to load TPC-DS tables into each format
2. **Query Performance**: Run TPC-DS queries with cold/warm runs, track execution metrics
3. **Update Performance**: TPC-DS Refresh benchmark with merge operations
4. **Micro Merge**: Test varying update sizes and measure query slowdown
5. **Metadata Performance**: Test with varying file counts (1K to 200K files)

Key modules to implement:
- `benchmark/core/loader.py`: Format-specific data loaders
- `benchmark/core/query_executor.py`: Query execution and timing
- `benchmark/core/merge_ops.py`: Update/merge operations
- `benchmark/core/metadata_ops.py`: Metadata handling tests
- `benchmark/metrics/collector.py`: Metrics collection and storage

## Paths and Locations

- TPC-DS data location: `/mydata/data1gb` (configurable)
- Iceberg warehouse: `iceberg/warehouse/` (mounted to Docker)
- DuckLake metadata: `ducklake/metadata.ducklake`
- DuckLake data: `ducklake/data_files/`
- Query files: `_queries/`

## References

- CIDR 2023 Paper: https://www.cidrdb.org/cidr2023/papers/p92-jain.pdf
- LHBench Repository: https://github.com/lhbench/lhbench
- DuckDB Iceberg Extension: https://duckdb.org/docs/extensions/iceberg
- DuckDB Delta Extension: https://duckdb.org/docs/extensions/delta
