#!/usr/bin/env python3
"""
Generate TPC-DS data directly to DuckLake format.
This is the streamlined approach - no intermediate Parquet files needed.
"""

import duckdb
import time
from pathlib import Path
import sys

def main():
    # Configuration - accept scale factor from command line
    SCALE_FACTOR = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    DUCKLAKE_DIR = Path("data/ducklake")
    METADATA_FILE = DUCKLAKE_DIR / "metadata.ducklake"
    DATA_FILES_DIR = DUCKLAKE_DIR / "data_files"

    print("=" * 70)
    print("TPC-DS Direct DuckLake Generation")
    print("=" * 70)
    print(f"Scale Factor: {SCALE_FACTOR}GB")
    print(f"Output: {DUCKLAKE_DIR}")
    print("=" * 70)

    # Remove existing DuckLake data
    if DUCKLAKE_DIR.exists():
        print(f"\n⚠️  Removing existing DuckLake data...")
        import shutil
        shutil.rmtree(DUCKLAKE_DIR)

    # Create output directory
    DUCKLAKE_DIR.mkdir(parents=True, exist_ok=True)
    print(f"✓ Created output directory")

    # Connect to DuckDB
    print("\n🔌 Connecting to DuckDB...")
    conn = duckdb.connect(':memory:')

    try:
        # Step 1: Generate TPC-DS data in memory
        print(f"\n📊 Generating TPC-DS data (Scale Factor: {SCALE_FACTOR})...")
        start_gen = time.time()

        conn.execute("INSTALL tpcds")
        conn.execute("LOAD tpcds")
        conn.execute(f"CALL dsdgen(sf={SCALE_FACTOR})")

        gen_time = time.time() - start_gen
        print(f"✓ Generated in-memory data in {gen_time:.2f}s")

        # Step 2: Export directly to DuckLake
        print(f"\n💾 Exporting to DuckLake format...")
        start_export = time.time()

        # Install and load DuckLake extension
        conn.execute("INSTALL ducklake")
        conn.execute("LOAD ducklake")

        # Attach DuckLake database
        attach_sql = f"""
            ATTACH 'ducklake:{METADATA_FILE}' AS ducklake_db
            (DATA_PATH '{DATA_FILES_DIR}')
        """
        conn.execute(attach_sql)

        # Copy all data from memory to DuckLake
        conn.execute("COPY FROM DATABASE memory TO ducklake_db")

        export_time = time.time() - start_export
        print(f"✓ Exported to DuckLake in {export_time:.2f}s")

        # Step 3: Verify
        print(f"\n📋 Verifying tables...")
        tables_info = conn.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_catalog = 'ducklake_db'
              AND table_schema = 'main'
            ORDER BY table_name
        """).fetchall()

        print(f"\n✓ Created {len(tables_info)} tables:")
        total_rows = 0
        for (table_name,) in tables_info:
            row_count = conn.execute(
                f"SELECT COUNT(*) FROM ducklake_db.main.{table_name}"
            ).fetchone()[0]
            total_rows += row_count
            print(f"   {table_name:30s} {row_count:>12,} rows")

        total_time = gen_time + export_time

        print("\n" + "=" * 70)
        print("✅ SUCCESS!")
        print("=" * 70)
        print(f"Total Tables: {len(tables_info)}")
        print(f"Total Rows:   {total_rows:,}")
        print(f"Generation:   {gen_time:.2f}s")
        print(f"Export:       {export_time:.2f}s")
        print(f"Total Time:   {total_time:.2f}s")
        print(f"\nDuckLake Location:")
        print(f"  Metadata: {METADATA_FILE}")
        print(f"  Data:     {DATA_FILES_DIR}")
        print("=" * 70)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
