"""Generate TPC-DS Parquet data directly from DuckDB (no sudo required)."""

import duckdb
import sys
from pathlib import Path

def generate_tpcds_parquet(scale_factor: int, output_dir: str):
    """
    Generate TPC-DS data in Parquet format using DuckDB's built-in TPC-DS extension.

    Args:
        scale_factor: Scale factor (1 = 1GB, 5 = 5GB, 10 = 10GB)
        output_dir: Output directory for Parquet files
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    print(f"Generating TPC-DS Parquet data (Scale Factor: {scale_factor}GB)")
    print(f"Output directory: {output_path}")
    print("")

    # Create DuckDB connection
    conn = duckdb.connect()

    # Install and load TPC-DS extension
    print("Installing TPC-DS extension...")
    conn.execute("INSTALL tpcds")
    conn.execute("LOAD tpcds")

    # Set scale factor
    conn.execute(f"CALL dsdgen(sf={scale_factor})")

    # List of TPC-DS tables
    tables = [
        'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales',
        'customer', 'customer_address', 'customer_demographics', 'date_dim',
        'household_demographics', 'income_band', 'inventory', 'item',
        'promotion', 'reason', 'ship_mode', 'store', 'store_returns',
        'store_sales', 'time_dim', 'warehouse', 'web_page', 'web_returns',
        'web_sales', 'web_site'
    ]

    print(f"Exporting {len(tables)} tables to Parquet...")
    total_rows = 0

    for table in tables:
        # Export to Parquet
        parquet_file = output_path / table

        # Count rows
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        total_rows += row_count

        # Export to Parquet with compression
        conn.execute(f"""
            COPY {table} TO '{parquet_file}'
            (FORMAT 'parquet', COMPRESSION 'snappy', ROW_GROUP_SIZE 100000)
        """)

        print(f"  ✓ {table:30s} {row_count:>12,} rows")

    conn.close()

    print("")
    print("="*70)
    print("✅ SUCCESS!")
    print("="*70)
    print(f"Total Tables: {len(tables)}")
    print(f"Total Rows:   {total_rows:,}")
    print(f"Location:     {output_path}")
    print("="*70)

if __name__ == "__main__":
    # Get scale factor from environment variable or command line
    import os

    scale = int(sys.argv[1]) if len(sys.argv) > 1 else int(os.getenv("SCALE_FACTOR", "1"))
    output_dir = sys.argv[2] if len(sys.argv) > 2 else f"data/tpcds_raw"

    generate_tpcds_parquet(scale, output_dir)
