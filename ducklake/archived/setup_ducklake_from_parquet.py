#!/usr/bin/env python3
"""Setup DuckLake database from existing Parquet files."""

import duckdb
from pathlib import Path
import sys

# Setup paths
script_dir = Path(__file__).parent
project_root = script_dir.parent
output_dir = project_root / "data" / "ducklake"
metadata_file = output_dir / "metadata.ducklake"
data_files_dir = output_dir / "data_files"
parquet_dir = project_root / "data" / "tpcds_raw"

# TPC-DS tables
TABLES = [
    'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales',
    'customer', 'customer_address', 'customer_demographics', 'date_dim',
    'household_demographics', 'income_band', 'inventory', 'item',
    'promotion', 'reason', 'ship_mode', 'store', 'store_returns',
    'store_sales', 'time_dim', 'warehouse', 'web_page', 'web_returns',
    'web_sales', 'web_site'
]

print("=" * 60)
print("DuckLake Setup from Parquet Files")
print("=" * 60)
print(f"📍 Parquet Source: {parquet_dir}")
print(f"📍 Output Dir:     {output_dir}")
print(f"📍 Metadata:       {metadata_file}")
print(f"📍 Data Files:     {data_files_dir}")
print("=" * 60)

# Check if Parquet files exist
if not (parquet_dir / "call_center.parquet").exists():
    print(f"❌ Error: Parquet files not found in {parquet_dir}")
    sys.exit(1)

# Remove existing DuckLake data if it exists
if output_dir.exists():
    print(f"⚠️  Removing existing DuckLake data in {output_dir}")
    import shutil
    shutil.rmtree(output_dir)

# Create output directory
print(f"📂 Creating output directory...")
output_dir.mkdir(parents=True, exist_ok=True)

# Connect to DuckDB
print("🔌 Connecting to DuckDB...")
conn = duckdb.connect(':memory:')

# Install and load ducklake extension
print("📦 Installing DuckLake extension...")
conn.execute("INSTALL ducklake")
conn.execute("LOAD ducklake")

# Load schema from SQL file
schema_file = script_dir / "tpcds_no_dbgen.sql"
if schema_file.exists():
    print(f"📋 Loading schema from {schema_file}...")
    with open(schema_file, 'r') as f:
        schema_sql = f.read()
    conn.execute(schema_sql)
else:
    print("⚠️  Schema file not found, creating tables from Parquet directly")

# Load data from Parquet files
print("\n📥 Loading data from Parquet files into memory...")
for i, table in enumerate(TABLES, 1):
    parquet_file = parquet_dir / f"{table}.parquet"
    if not parquet_file.exists():
        print(f"  ⚠️  [{i:2d}/24] {table}: File not found, skipping")
        continue

    print(f"  [{i:2d}/24] Loading {table}...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT * FROM read_parquet('{parquet_file}')
    """)

    # Get row count
    row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"           ✓ {row_count:,} rows loaded")

# Attach DuckLake database
print(f"\n🔗 Attaching DuckLake database...")
conn.execute(f"""
    ATTACH 'ducklake:{metadata_file}' AS my_ducklake
    (DATA_PATH '{data_files_dir}')
""")

# Copy data from memory to DuckLake
print("🚀 Copying data to DuckLake format...")
conn.execute("COPY FROM DATABASE memory TO my_ducklake")

# Verify
print("\n📊 Verifying DuckLake tables...")
result = conn.execute("""
    SELECT COUNT(*) as table_count
    FROM information_schema.tables
    WHERE table_catalog = 'my_ducklake'
""").fetchone()

table_count = result[0]
print(f"✅ Successfully created {table_count} tables in DuckLake")

# Show table counts
print("\n📊 Table Row Counts in DuckLake:")
print("-" * 60)
tables_info = conn.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_catalog = 'my_ducklake'
      AND table_schema = 'main'
    ORDER BY table_name
""").fetchall()

total_rows = 0
for (table_name,) in tables_info:
    row_count = conn.execute(
        f"SELECT COUNT(*) FROM my_ducklake.main.{table_name}"
    ).fetchone()[0]
    total_rows += row_count
    print(f"  {table_name:30s} {row_count:>12,} rows")

print("-" * 60)
print(f"  {'TOTAL':30s} {total_rows:>12,} rows")
print("-" * 60)

conn.close()

print("\n" + "=" * 60)
print("✅ DuckLake setup complete!")
print("=" * 60)
print(f"   Metadata: {metadata_file}")
print(f"   Data:     {data_files_dir}")
print("=" * 60)
