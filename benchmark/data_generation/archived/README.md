# Archived Data Generation Scripts

These scripts have been superseded by the unified data generation script `generate_data.py` at the project root.

## Archived Files

1. **setup_ducklake_direct.py** - Previously used for direct DuckLake generation
2. **generate_parquet_from_duckdb.py** - Previously used for Parquet generation

## Why Archived?

These scripts were replaced by a single unified script that:
- Provides a cleaner interface with command-line arguments
- Supports all formats (Parquet, DuckLake, Delta, Iceberg)
- Has better error handling and progress reporting
- Eliminates code duplication

## Migration Guide

**Old approach:**
```bash
python3 benchmark/data_generation/setup_ducklake_direct.py 5
python3 benchmark/data_generation/generate_parquet_from_duckdb.py 5 data/tpcds_raw
```

**New approach:**
```bash
# Generate all formats for 5GB
python3 generate_data.py --scale 5 --format all

# Or generate individual formats
python3 generate_data.py --scale 5 --format parquet
python3 generate_data.py --scale 5 --format ducklake
python3 generate_data.py --scale 5 --format delta
python3 generate_data.py --scale 5 --format iceberg
```

## See Also

- Main data generation script: `generate_data.py`
- Documentation: `CLAUDE.md`
