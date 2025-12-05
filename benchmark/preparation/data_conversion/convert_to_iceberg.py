"""Convert TPC-DS Parquet data to Iceberg format using PySpark."""

import time
from pathlib import Path
from typing import Dict, Any
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))

from pyspark.sql import SparkSession
from benchmark.utils.logger import setup_logger
from benchmark.utils.config_loader import ConfigLoader


class IcebergConverter:
    """Convert Parquet data to Iceberg format."""

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
        Initialize Iceberg converter.

        Args:
            config: Configuration loader
        """
        self.config = config
        self.source_path = Path(config.get('paths.tpcds_raw'))
        self.target_path = Path(config.get('paths.iceberg'))
        self.target_path.mkdir(parents=True, exist_ok=True)

        self.logger = setup_logger(
            name="iceberg_converter",
            log_file=config.get('paths.logs') + "/iceberg_conversion.log",
            level=config.get('logging.level', 'INFO')
        )

        self.spark = None
        self.metrics = {}

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Iceberg configuration."""
        self.logger.info("Creating Spark session with Iceberg support...")

        # Get Spark configuration from config
        driver_memory = self.config.get('delta.spark.driver_memory', '4g')
        executor_memory = self.config.get('delta.spark.executor_memory', '4g')

        spark = SparkSession.builder \
            .appName("TPC-DS to Iceberg Converter") \
            .config("spark.driver.memory", driver_memory) \
            .config("spark.executor.memory", executor_memory) \
            .config("spark.sql.extensions",
                   "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.local.type", "hadoop") \
            .config("spark.sql.catalog.local.warehouse", str(self.target_path)) \
            .getOrCreate()

        self.logger.info("Spark session created successfully")
        return spark

    def convert(self) -> Dict[str, Any]:
        """
        Convert all TPC-DS tables to Iceberg format.

        Returns:
            Dictionary with conversion metrics
        """
        self.logger.info("Starting Iceberg conversion...")
        self.logger.info(f"Source: {self.source_path}")
        self.logger.info(f"Target: {self.target_path}")

        try:
            # Create Spark session
            self.spark = self._create_spark_session()

            # Convert all tables
            table_metrics = []
            total_start = time.time()

            for table_name in self.TPCDS_TABLES:
                metrics = self._convert_table(table_name)
                table_metrics.append(metrics)

            total_time = time.time() - total_start

            # Collect overall metrics
            self.metrics = {
                'source_path': str(self.source_path),
                'target_path': str(self.target_path),
                'num_tables': len(self.TPCDS_TABLES),
                'table_metrics': table_metrics,
                'total_time_seconds': total_time,
                'total_rows': sum(t['row_count'] for t in table_metrics),
                'total_size_bytes': sum(t.get('size_bytes', 0) for t in table_metrics)
            }

            self.logger.info(f"Iceberg conversion completed in {total_time:.2f}s")
            return self.metrics

        except Exception as e:
            self.logger.error(f"Error during Iceberg conversion: {e}", exc_info=True)
            raise

        finally:
            if self.spark:
                self.spark.stop()
                self.logger.info("Spark session stopped")

    def _convert_table(self, table_name: str) -> Dict[str, Any]:
        """
        Convert a single table to Iceberg format.

        Args:
            table_name: Name of the table to convert

        Returns:
            Dictionary with table conversion metrics
        """
        self.logger.info(f"Converting {table_name}...")

        start_time = time.time()

        # Read Parquet file
        source_file = self.source_path / f"{table_name}.parquet"
        if not source_file.exists():
            raise FileNotFoundError(f"Source file not found: {source_file}")

        df = self.spark.read.parquet(str(source_file))
        row_count = df.count()

        # Write to Iceberg format
        iceberg_table = f"local.default.{table_name}"

        df.writeTo(iceberg_table) \
            .using("iceberg") \
            .createOrReplace()

        conversion_time = time.time() - start_time

        # Get table size (approximate)
        table_path = self.target_path / "default" / table_name
        size_bytes = sum(f.stat().st_size for f in table_path.rglob("*") if f.is_file())

        metrics = {
            'table_name': table_name,
            'row_count': row_count,
            'size_bytes': size_bytes,
            'size_mb': size_bytes / (1024 * 1024),
            'conversion_time_seconds': conversion_time
        }

        self.logger.info(
            f"  {table_name}: {row_count:,} rows, "
            f"{size_bytes / (1024 * 1024):.2f} MB, "
            f"{conversion_time:.2f}s"
        )

        return metrics


def main():
    """Main entry point for Iceberg conversion."""
    # Load configuration
    config = ConfigLoader()

    # Convert to Iceberg
    converter = IcebergConverter(config)
    metrics = converter.convert()

    # Print summary
    print("\n" + "=" * 60)
    print("Iceberg Conversion Summary")
    print("=" * 60)
    print(f"Tables Converted: {metrics['num_tables']}")
    print(f"Total Rows: {metrics['total_rows']:,}")
    print(f"Total Size: {metrics['total_size_bytes'] / (1024 ** 3):.2f} GB")
    print(f"Total Time: {metrics['total_time_seconds']:.2f}s")
    print(f"Output Path: {metrics['target_path']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
