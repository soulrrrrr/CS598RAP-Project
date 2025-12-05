# Archived DuckLake Setup Scripts

This script has been superseded by the unified data generation script `generate_data.py` at the project root.

## Archived Files

1. **setup_ducklake_from_parquet.py** - Previously used for DuckLake generation from Parquet

## Why Archived?

This script was replaced by the unified `generate_data.py` which provides:
- Consistent interface with other formats
- Better error handling
- Progress reporting
- Command-line argument support

## Migration Guide

**Old approach:**
```bash
python3 ducklake/setup_ducklake_from_parquet.py
```

**New approach:**
```bash
# Generate DuckLake from Parquet
python3 generate_data.py --scale 5 --format ducklake --from-parquet

# Generate DuckLake directly (faster)
python3 generate_data.py --scale 5 --format ducklake
```

## See Also

- Main data generation script: `generate_data.py`
- Documentation: `CLAUDE.md`
