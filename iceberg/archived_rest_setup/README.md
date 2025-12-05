# Archived REST Catalog Setup (Pre-v2.1.0)

This directory contains the **old Iceberg setup scripts** that used REST catalog + MinIO (S3-compatible storage). These files are archived for reference but are **no longer used** in the benchmark.

## Why Archived?

As of **version 2.1.0 (December 1, 2025)**, the benchmark was changed to use **direct local filesystem access** to Iceberg tables instead of REST catalog + S3. This eliminated significant network overhead that made performance comparisons invalid.

**Performance Impact:**
- **Before (REST + S3)**: Iceberg was 136% slower than DuckLake (50.1ms vs 21.2ms)
- **After (Local FS)**: Iceberg is only 29% slower than DuckLake (26.8ms vs 20.7ms)
- **Improvement**: 4.7x faster, making the comparison valid

## Archived Files

### 1. `setup_iceberg.sh`
**Purpose:** End-to-end script to:
1. Start Docker containers (Spark, MinIO, REST catalog)
2. Load TPC-DS data into Iceberg via Spark
3. Copy data from MinIO to local filesystem
4. Stop Docker containers

**Status:** Replaced by direct local filesystem access

### 2. `setup_spark_iceberg.py`
**Purpose:** PySpark script that loads TPC-DS .dat files into Iceberg format using REST catalog

**Configuration:**
- Catalog: `demo` (REST catalog at http://rest:8181)
- Warehouse: `s3://warehouse` (MinIO)
- Database: `tpcds`

**Status:** No longer needed for benchmark queries (still useful for initial data creation)

### 3. `convert_parquet_to_iceberg_docker.py`
**Purpose:** Convert Parquet files to Iceberg format using Docker Spark container

**Status:** Alternative approach, archived

### 4. `test_iceberg_connection.py`
**Purpose:** Test DuckDB connection to Iceberg via REST catalog and S3

**Configuration:**
- S3 endpoint: `localhost:9000`
- Credentials: admin/password
- Used `httpfs` extension

**Status:** Replaced by `test_iceberg_local_access.py` (uses local filesystem)

## Current Approach (v2.1.0+)

The benchmark now uses **direct local filesystem access**:

```python
# New approach (benchmark/core/query_executor.py)
table_path = self.data_path / table_name
conn.execute(f"""
    CREATE OR REPLACE VIEW {table_name} AS
    SELECT * FROM iceberg_scan('{table_path}', allow_moved_paths=true)
""")
```

**Benefits:**
- No Docker containers needed for queries
- No REST API overhead
- No S3 network latency
- Fair comparison against DuckLake and Delta Lake
- 4.7x faster Iceberg queries

## When to Use These Files

These archived files may still be useful if you need to:
1. **Create new Iceberg tables from scratch** (using Spark + REST catalog)
2. **Understand the old REST-based architecture**
3. **Test against REST catalog** for comparison purposes

For normal benchmark usage, these files are **not needed**.

## Migration Guide

If you're upgrading from pre-v2.1.0:

1. **Stop using REST catalog for queries:**
   - Remove `httpfs` extension loading
   - Remove S3 configuration (`s3_endpoint`, credentials, etc.)
   - Update paths from `s3://warehouse/tpcds/table` to `data/iceberg_local/table`

2. **Copy Iceberg data to local filesystem:**
   ```bash
   # If data exists in MinIO container
   docker exec mc mc mirror minio/warehouse/tpcds /tmp/iceberg_local_copy
   docker cp mc:/tmp/iceberg_local_copy data/iceberg_local
   ```

3. **Update configuration:**
   ```yaml
   # config/benchmark_config.yaml
   paths:
     iceberg: "data/iceberg_local"  # Changed from "data/iceberg"
   ```

4. **Use new test file:**
   ```bash
   python test_iceberg_local_access.py  # Not test_iceberg_connection.py
   ```

## References

- See [CHANGES.md](../../CHANGES.md) for detailed changelog
- See [README.md](../../README.md) for current setup instructions
- See [benchmark/core/query_executor.py](../../benchmark/core/query_executor.py) for current implementation

---

**Archived Date:** December 1, 2025
**Version:** Pre-v2.1.0
**Reason:** Network overhead eliminated by local filesystem access
