#!/usr/bin/env python3
"""
Unified TPC-DS Data Generation Script

This script generates TPC-DS benchmark data in different formats:
- parquet: Raw Parquet files (source of truth)
- ducklake: DuckDB's native table format
- delta: Delta Lake format (requires Docker)
- iceberg: Apache Iceberg format (requires Docker)
- all: Generate all formats

Usage:
    python3 generate_data.py --scale 1 --format parquet
    python3 generate_data.py --scale 5 --format ducklake
    python3 generate_data.py --scale 10 --format all
"""

import argparse
import duckdb
import subprocess
import sys
import time
from pathlib import Path
from typing import List


# TPC-DS table list
TPCDS_TABLES = [
    'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales',
    'customer', 'customer_address', 'customer_demographics', 'date_dim',
    'household_demographics', 'income_band', 'inventory', 'item',
    'promotion', 'reason', 'ship_mode', 'store', 'store_returns',
    'store_sales', 'time_dim', 'warehouse', 'web_page', 'web_returns',
    'web_sales', 'web_site'
]


class DataGenerator:
    """TPC-DS data generator for multiple formats."""

    def __init__(self, scale_factor: int):
        """
        Initialize data generator.

        Args:
            scale_factor: Scale factor (1 = 1GB, 5 = 5GB, 10 = 10GB)
        """
        self.scale_factor = scale_factor
        self.project_root = Path(__file__).parent
        self.parquet_dir = self.project_root / "data" / "tpcds_raw"
        self.ducklake_dir = self.project_root / "data" / "ducklake"
        self.delta_dir = self.project_root / "data" / "delta"
        self.iceberg_dir = self.project_root / "data" / "iceberg_local"

    def print_header(self, title: str):
        """Print formatted header."""
        print("\n" + "=" * 70)
        print(title)
        print("=" * 70)

    def print_success(self, message: str):
        """Print success message."""
        print(f"✅ {message}")

    def print_error(self, message: str):
        """Print error message."""
        print(f"❌ {message}")

    def generate_parquet(self) -> bool:
        """
        Generate TPC-DS data in Parquet format.

        Returns:
            True if successful, False otherwise
        """
        self.print_header(f"Generating TPC-DS Parquet Data (Scale Factor: {self.scale_factor}GB)")

        try:
            # Create output directory
            self.parquet_dir.mkdir(parents=True, exist_ok=True)
            print(f"📍 Output directory: {self.parquet_dir}")

            # Create DuckDB connection
            print("🔌 Connecting to DuckDB...")
            conn = duckdb.connect()

            # Install and load TPC-DS extension
            print("📦 Installing TPC-DS extension...")
            conn.execute("INSTALL tpcds")
            conn.execute("LOAD tpcds")

            # Generate data
            print(f"📊 Generating TPC-DS data (Scale Factor: {self.scale_factor})...")
            start_time = time.time()
            conn.execute(f"CALL dsdgen(sf={self.scale_factor})")
            gen_time = time.time() - start_time
            print(f"✓ Generated in {gen_time:.2f}s")

            # Export to Parquet
            print(f"\n💾 Exporting {len(TPCDS_TABLES)} tables to Parquet...")
            total_rows = 0

            for i, table in enumerate(TPCDS_TABLES, 1):
                # Get row count
                row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                total_rows += row_count

                # Export to Parquet
                parquet_file = self.parquet_dir / f"{table}.parquet"
                conn.execute(f"""
                    COPY {table} TO '{parquet_file}'
                    (FORMAT 'parquet', COMPRESSION 'snappy', ROW_GROUP_SIZE 100000)
                """)

                print(f"  [{i:2d}/24] {table:30s} {row_count:>12,} rows")

            conn.close()

            export_time = time.time() - start_time - gen_time

            # Summary
            self.print_header("Parquet Generation Complete!")
            print(f"Total Tables:     {len(TPCDS_TABLES)}")
            print(f"Total Rows:       {total_rows:,}")
            print(f"Generation Time:  {gen_time:.2f}s")
            print(f"Export Time:      {export_time:.2f}s")
            print(f"Total Time:       {time.time() - start_time:.2f}s")
            print(f"Location:         {self.parquet_dir}")
            print("=" * 70)

            return True

        except Exception as e:
            self.print_error(f"Failed to generate Parquet: {e}")
            import traceback
            traceback.print_exc()
            return False

    def generate_ducklake(self, from_parquet: bool = False) -> bool:
        """
        Generate DuckLake format.

        Args:
            from_parquet: If True, generate from existing Parquet files.
                         If False, generate directly from TPC-DS.

        Returns:
            True if successful, False otherwise
        """
        if from_parquet:
            self.print_header(f"Generating DuckLake from Parquet (Scale Factor: {self.scale_factor}GB)")

            # Check if Parquet files exist
            if not (self.parquet_dir / "call_center.parquet").exists():
                self.print_error(f"Parquet files not found in {self.parquet_dir}")
                print(f"💡 Run: python3 generate_data.py --scale {self.scale_factor} --format parquet")
                return False
        else:
            self.print_header(f"Generating DuckLake Directly (Scale Factor: {self.scale_factor}GB)")

        try:
            metadata_file = self.ducklake_dir / "metadata.ducklake"
            data_files_dir = self.ducklake_dir / "data_files"

            # Remove existing DuckLake data
            if self.ducklake_dir.exists():
                print(f"⚠️  Removing existing DuckLake data...")
                import shutil
                shutil.rmtree(self.ducklake_dir)

            # Create output directory
            self.ducklake_dir.mkdir(parents=True, exist_ok=True)
            print(f"✓ Created output directory: {self.ducklake_dir}")

            # Connect to DuckDB
            print("🔌 Connecting to DuckDB...")
            conn = duckdb.connect(':memory:')

            # Install extensions
            print("📦 Installing extensions...")
            conn.execute("INSTALL ducklake")
            conn.execute("LOAD ducklake")

            if from_parquet:
                # Load from Parquet
                print(f"\n📥 Loading data from Parquet files...")
                start_time = time.time()

                for i, table in enumerate(TPCDS_TABLES, 1):
                    parquet_file = self.parquet_dir / f"{table}.parquet"
                    if not parquet_file.exists():
                        print(f"  ⚠️  [{i:2d}/24] {table}: File not found, skipping")
                        continue

                    conn.execute(f"""
                        CREATE OR REPLACE TABLE {table} AS
                        SELECT * FROM read_parquet('{parquet_file}')
                    """)

                    row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    print(f"  [{i:2d}/24] {table:30s} {row_count:>12,} rows")

                load_time = time.time() - start_time
                print(f"✓ Loaded data in {load_time:.2f}s")

            else:
                # Generate directly from TPC-DS
                print(f"\n📊 Generating TPC-DS data (Scale Factor: {self.scale_factor})...")
                start_time = time.time()

                conn.execute("INSTALL tpcds")
                conn.execute("LOAD tpcds")
                conn.execute(f"CALL dsdgen(sf={self.scale_factor})")

                gen_time = time.time() - start_time
                print(f"✓ Generated in-memory data in {gen_time:.2f}s")

            # Export to DuckLake
            print(f"\n💾 Exporting to DuckLake format...")
            export_start = time.time()

            attach_sql = f"""
                ATTACH 'ducklake:{metadata_file}' AS ducklake_db
                (DATA_PATH '{data_files_dir}')
            """
            conn.execute(attach_sql)
            conn.execute("COPY FROM DATABASE memory TO ducklake_db")

            export_time = time.time() - export_start
            print(f"✓ Exported to DuckLake in {export_time:.2f}s")

            # Verify
            print(f"\n📋 Verifying tables...")
            tables_info = conn.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_catalog = 'ducklake_db'
                  AND table_schema = 'main'
                ORDER BY table_name
            """).fetchall()

            total_rows = 0
            for (table_name,) in tables_info:
                row_count = conn.execute(
                    f"SELECT COUNT(*) FROM ducklake_db.main.{table_name}"
                ).fetchone()[0]
                total_rows += row_count

            conn.close()

            # Summary
            self.print_header("DuckLake Generation Complete!")
            print(f"Total Tables:   {len(tables_info)}")
            print(f"Total Rows:     {total_rows:,}")
            print(f"Total Time:     {time.time() - start_time:.2f}s")
            print(f"Metadata:       {metadata_file}")
            print(f"Data:           {data_files_dir}")
            print("=" * 70)

            return True

        except Exception as e:
            self.print_error(f"Failed to generate DuckLake: {e}")
            import traceback
            traceback.print_exc()
            return False

    def generate_delta(self) -> bool:
        """
        Generate Delta Lake format from Parquet files using Docker.

        Returns:
            True if successful, False otherwise
        """
        self.print_header(f"Generating Delta Lake Format (Scale Factor: {self.scale_factor}GB)")

        # Check if Parquet files exist
        if not (self.parquet_dir / "call_center.parquet").exists():
            self.print_error(f"Parquet files not found in {self.parquet_dir}")
            print(f"💡 Run: python3 generate_data.py --scale {self.scale_factor} --format parquet")
            return False

        try:
            # Run setup script
            script_path = self.project_root / "benchmark" / "preparation" / "data_conversion" / "setup_delta_conversion.sh"

            print(f"🐳 Running Delta conversion via Docker...")
            print(f"📍 Script: {script_path}")

            result = subprocess.run(
                [str(script_path)],
                cwd=self.project_root,
                capture_output=False,
                text=True
            )

            if result.returncode == 0:
                self.print_success("Delta Lake generation complete!")
                return True
            else:
                self.print_error(f"Delta conversion failed with exit code {result.returncode}")
                return False

        except Exception as e:
            self.print_error(f"Failed to generate Delta: {e}")
            import traceback
            traceback.print_exc()
            return False

    def generate_iceberg(self) -> bool:
        """
        Generate Iceberg format from Parquet files using Docker.

        Returns:
            True if successful, False otherwise
        """
        self.print_header(f"Generating Iceberg Format (Scale Factor: {self.scale_factor}GB)")

        # Check if Parquet files exist
        if not (self.parquet_dir / "call_center.parquet").exists():
            self.print_error(f"Parquet files not found in {self.parquet_dir}")
            print(f"💡 Run: python3 generate_data.py --scale {self.scale_factor} --format parquet")
            return False

        try:
            # Run setup script
            script_path = self.project_root / "benchmark" / "preparation" / "data_conversion" / "setup_iceberg_conversion.sh"

            print(f"🐳 Running Iceberg conversion via Docker...")
            print(f"📍 Script: {script_path}")

            result = subprocess.run(
                [str(script_path)],
                cwd=self.project_root,
                capture_output=False,
                text=True
            )

            if result.returncode == 0:
                self.print_success("Iceberg generation complete!")
                return True
            else:
                self.print_error(f"Iceberg conversion failed with exit code {result.returncode}")
                return False

        except Exception as e:
            self.print_error(f"Failed to generate Iceberg: {e}")
            import traceback
            traceback.print_exc()
            return False


def main():
    """Main execution."""
    parser = argparse.ArgumentParser(
        description="Generate TPC-DS benchmark data in various formats",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 1GB Parquet data
  python3 generate_data.py --scale 1 --format parquet

  # Generate 5GB DuckLake data directly
  python3 generate_data.py --scale 5 --format ducklake

  # Generate 10GB data in all formats
  python3 generate_data.py --scale 10 --format all

  # Generate DuckLake from existing Parquet
  python3 generate_data.py --scale 5 --format ducklake --from-parquet
        """
    )

    parser.add_argument(
        '--scale',
        type=int,
        required=True,
        choices=[1, 5, 10],
        help='Scale factor (1 = 1GB, 5 = 5GB, 10 = 10GB)'
    )

    parser.add_argument(
        '--format',
        type=str,
        required=True,
        choices=['parquet', 'ducklake', 'delta', 'iceberg', 'all'],
        help='Format to generate'
    )

    parser.add_argument(
        '--from-parquet',
        action='store_true',
        help='Generate DuckLake from existing Parquet files (only for ducklake format)'
    )

    args = parser.parse_args()

    # Create generator
    generator = DataGenerator(args.scale)

    # Track success
    results = {}

    # Generate requested format(s)
    if args.format == 'all':
        # Generate all formats in order: Parquet → DuckLake → Delta → Iceberg
        print("\n🚀 Generating ALL formats...")
        results['parquet'] = generator.generate_parquet()
        results['ducklake'] = generator.generate_ducklake(from_parquet=True)
        results['delta'] = generator.generate_delta()
        results['iceberg'] = generator.generate_iceberg()

    elif args.format == 'parquet':
        results['parquet'] = generator.generate_parquet()

    elif args.format == 'ducklake':
        results['ducklake'] = generator.generate_ducklake(from_parquet=args.from_parquet)

    elif args.format == 'delta':
        results['delta'] = generator.generate_delta()

    elif args.format == 'iceberg':
        results['iceberg'] = generator.generate_iceberg()

    # Print final summary
    print("\n" + "=" * 70)
    print("📊 GENERATION SUMMARY")
    print("=" * 70)
    for format_name, success in results.items():
        status = "✅ Success" if success else "❌ Failed"
        print(f"{format_name:15s} {status}")
    print("=" * 70)

    # Exit with error if any failed
    if not all(results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()