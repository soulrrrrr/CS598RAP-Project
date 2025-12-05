"""Generate TPC-DS data using DuckDB's built-in extension."""

import duckdb
import time
from pathlib import Path
from typing import Dict, Any
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))

from benchmark.utils.logger import setup_logger
from benchmark.utils.config_loader import ConfigLoader


class TPCDSGenerator:
    """Generate TPC-DS benchmark data using DuckDB."""

    # All 24 TPC-DS tables
    TPCDS_TABLES = [
        'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales',
        'customer', 'customer_address', 'customer_demographics', 'date_dim',
        'household_demographics', 'income_band', 'inventory', 'item',
        'promotion', 'reason', 'ship_mode', 'store', 'store_returns',
        'store_sales', 'time_dim', 'warehouse', 'web_page', 'web_returns',
        'web_sales', 'web_site'
    ]

    def __init__(self, config: ConfigLoader):
        """
        Initialize TPC-DS generator.

        Args:
            config: Configuration loader
        """
        self.config = config
        self.scale_factor = config.get('benchmark.scale_factor', 1)
        self.output_path = Path(config.get('paths.tpcds_raw'))
        self.output_path.mkdir(parents=True, exist_ok=True)

        self.logger = setup_logger(
            name="tpcds_generator",
            log_file=config.get('paths.logs') + "/tpcds_generation.log",
            level=config.get('logging.level', 'INFO')
        )

        self.conn = duckdb.connect(':memory:')
        self.metrics = {}

    def generate_data(self, export_format: str = 'both') -> Dict[str, Any]:
        """
        Generate TPC-DS data at specified scale factor.

        Args:
            export_format: 'parquet', 'ducklake', or 'both' (default: 'both')

        Returns:
            Dictionary with generation metrics
        """
        self.logger.info(f"Starting TPC-DS data generation (scale factor: {self.scale_factor})")

        try:
            # Install and load TPC-DS extension
            self._setup_extension()

            # Generate data in-memory
            self._generate_in_memory()

            # Export based on format
            if export_format in ['parquet', 'both']:
                # Export to Parquet files (for conversion to Iceberg/Delta)
                self._export_to_parquet()

            if export_format in ['ducklake', 'both']:
                # Export directly to DuckLake format
                self._export_to_ducklake()

            # Collect metrics
            self._collect_metrics()

            self.logger.info("TPC-DS data generation completed successfully")
            return self.metrics

        except Exception as e:
            self.logger.error(f"Error generating TPC-DS data: {e}", exc_info=True)
            raise

    def _setup_extension(self):
        """Install and load TPC-DS extension."""
        self.logger.info("Installing TPC-DS extension...")
        self.conn.execute("INSTALL tpcds")
        self.conn.execute("LOAD tpcds")
        self.logger.info("TPC-DS extension loaded")

    def _generate_in_memory(self):
        """Generate TPC-DS tables in memory."""
        self.logger.info(f"Generating TPC-DS data (SF={self.scale_factor})...")

        start_time = time.time()

        # Generate all TPC-DS tables
        self.conn.execute(f"CALL dsdgen(sf={self.scale_factor})")

        generation_time = time.time() - start_time
        self.metrics['generation_time_seconds'] = generation_time
        self.logger.info(f"Data generation completed in {generation_time:.2f}s")

    def _export_to_parquet(self):
        """Export all tables to Parquet format."""
        self.logger.info(f"Exporting tables to Parquet: {self.output_path}")

        export_start = time.time()
        table_metrics = []

        for table_name in self.TPCDS_TABLES:
            self.logger.info(f"  Exporting {table_name}...")

            table_start = time.time()

            # Export to Parquet
            output_file = self.output_path / f"{table_name}.parquet"
            self.conn.execute(f"""
                COPY {table_name}
                TO '{output_file}'
                (FORMAT PARQUET, COMPRESSION 'SNAPPY')
            """)

            # Get table stats
            row_count = self.conn.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()[0]

            file_size = output_file.stat().st_size if output_file.exists() else 0

            table_time = time.time() - table_start

            table_metrics.append({
                'table_name': table_name,
                'row_count': row_count,
                'file_size_bytes': file_size,
                'file_size_mb': file_size / (1024 * 1024),
                'export_time_seconds': table_time
            })

            self.logger.info(
                f"    {row_count:,} rows, "
                f"{file_size / (1024 * 1024):.2f} MB, "
                f"{table_time:.2f}s"
            )

        export_time = time.time() - export_start
        self.metrics['export_time_seconds'] = export_time
        self.metrics['table_metrics'] = table_metrics

        # Calculate totals
        total_rows = sum(t['row_count'] for t in table_metrics)
        total_size = sum(t['file_size_bytes'] for t in table_metrics)

        self.metrics['total_rows'] = total_rows
        self.metrics['total_size_bytes'] = total_size
        self.metrics['total_size_gb'] = total_size / (1024 ** 3)

        self.logger.info(f"Export completed in {export_time:.2f}s")
        self.logger.info(f"Total: {total_rows:,} rows, {total_size / (1024 ** 3):.2f} GB")

    def _export_to_ducklake(self):
        """Export all tables to DuckLake format."""
        from pathlib import Path

        ducklake_dir = Path(self.config.get('paths.ducklake', 'data/ducklake'))
        ducklake_metadata = Path(self.config.get('paths.ducklake_metadata', 'data/ducklake/metadata.ducklake'))
        ducklake_data = Path(self.config.get('paths.ducklake_data', 'data/ducklake/data_files'))

        self.logger.info(f"Exporting tables to DuckLake: {ducklake_dir}")

        # Remove existing DuckLake data
        if ducklake_dir.exists():
            import shutil
            shutil.rmtree(ducklake_dir)

        ducklake_dir.mkdir(parents=True, exist_ok=True)

        export_start = time.time()

        # Install and load ducklake extension
        self.conn.execute("INSTALL ducklake")
        self.conn.execute("LOAD ducklake")

        # Attach DuckLake database
        self.logger.info(f"  Attaching DuckLake database...")
        attach_sql = f"""
            ATTACH 'ducklake:{ducklake_metadata}' AS ducklake_db
            (DATA_PATH '{ducklake_data}')
        """
        self.conn.execute(attach_sql)

        # Copy all data from memory to DuckLake
        self.logger.info(f"  Copying {len(self.TPCDS_TABLES)} tables to DuckLake...")
        self.conn.execute("COPY FROM DATABASE memory TO ducklake_db")

        export_time = time.time() - export_start

        # Verify and collect stats
        table_metrics = []
        for table_name in self.TPCDS_TABLES:
            row_count = self.conn.execute(
                f"SELECT COUNT(*) FROM ducklake_db.main.{table_name}"
            ).fetchone()[0]

            table_metrics.append({
                'table_name': table_name,
                'row_count': row_count
            })

        total_rows = sum(t['row_count'] for t in table_metrics)

        self.metrics['ducklake_export_time_seconds'] = export_time
        self.metrics['ducklake_table_count'] = len(table_metrics)
        self.metrics['ducklake_total_rows'] = total_rows

        self.logger.info(f"DuckLake export completed in {export_time:.2f}s")
        self.logger.info(f"DuckLake: {total_rows:,} total rows in {len(table_metrics)} tables")

    def _collect_metrics(self):
        """Collect final metrics."""
        self.metrics['scale_factor'] = self.scale_factor
        self.metrics['num_tables'] = len(self.TPCDS_TABLES)
        self.metrics['output_path'] = str(self.output_path)
        self.metrics['total_time_seconds'] = (
            self.metrics['generation_time_seconds'] +
            self.metrics['export_time_seconds']
        )

    def get_table_list(self) -> list:
        """Get list of generated table files."""
        return [str(f) for f in self.output_path.glob("*.parquet")]


def main():
    """Main entry point for TPC-DS generation."""
    # Load configuration
    config = ConfigLoader()

    # Generate data
    generator = TPCDSGenerator(config)
    metrics = generator.generate_data()

    # Print summary
    print("\n" + "=" * 60)
    print("TPC-DS Data Generation Summary")
    print("=" * 60)
    print(f"Scale Factor: {metrics['scale_factor']}GB")
    print(f"Total Tables: {metrics['num_tables']}")
    print(f"Total Rows: {metrics['total_rows']:,}")
    print(f"Total Size: {metrics['total_size_gb']:.2f} GB")
    print(f"Generation Time: {metrics['generation_time_seconds']:.2f}s")
    print(f"Export Time: {metrics['export_time_seconds']:.2f}s")
    print(f"Total Time: {metrics['total_time_seconds']:.2f}s")
    print(f"Output Path: {metrics['output_path']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
