"""
Insert Latency Benchmark: DuckLake vs RESTful Iceberg

Compares insert performance between DuckLake (local) and Iceberg (RESTful catalog).
Tests various batch sizes to understand latency characteristics.
"""

import duckdb
import time
import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any
import sys
from datetime import datetime

sys.path.append(str(Path(__file__).parent.parent.parent))
from benchmark.utils.logger import setup_logger
from benchmark.utils.config_loader import ConfigLoader


class InsertLatencyBenchmark:
    """Benchmark insert latency for DuckLake and RESTful Iceberg."""

    def __init__(self, config_path: str = None):
        """Initialize benchmark."""
        self.logger = setup_logger("insert_latency_benchmark")

        # Load configuration
        config_file = config_path or "config/benchmark_config.yaml"
        config_loader = ConfigLoader(config_file)
        self.config = config_loader.get_all()

        # Batch sizes to test (from config or defaults)
        self.batch_sizes = self.config.get('insert_latency', {}).get('batch_sizes', [100, 1000, 10000, 100000])

        # Test tables (from config or defaults)
        self.test_tables = self.config.get('insert_latency', {}).get('test_tables', ['store_sales', 'customer'])

        # Results storage
        self.results = []

        # Cached sample data (to avoid regenerating TPC-DS multiple times)
        self.sample_data_cache = {}

    def setup_ducklake_connection(self) -> duckdb.DuckDBPyConnection:
        """Setup DuckLake connection."""
        self.logger.info("Setting up DuckLake connection...")

        conn = duckdb.connect(':memory:')

        # Configure DuckDB
        memory_limit = self.config['duckdb'].get('memory_limit', '8GB')
        threads = self.config['duckdb'].get('threads', 4)

        conn.execute(f"SET memory_limit = '{memory_limit}'")
        conn.execute(f"SET threads TO {threads}")

        # Install and load ducklake extension
        conn.execute("INSTALL ducklake")
        conn.execute("LOAD ducklake")

        return conn

    def setup_iceberg_rest_connection(self) -> duckdb.DuckDBPyConnection:
        """Setup Iceberg connection with REST catalog."""
        self.logger.info("Setting up Iceberg REST connection...")

        conn = duckdb.connect(':memory:')

        # Configure DuckDB
        memory_limit = self.config['duckdb'].get('memory_limit', '8GB')
        threads = self.config['duckdb'].get('threads', 4)

        conn.execute(f"SET memory_limit = '{memory_limit}'")
        conn.execute(f"SET threads TO {threads}")

        # Install and load iceberg extension
        conn.execute("INSTALL iceberg")
        conn.execute("LOAD iceberg")
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")

        # Configure S3/MinIO credentials
        conn.execute("SET s3_region='us-east-1'")
        conn.execute("SET s3_access_key_id='admin'")
        conn.execute("SET s3_secret_access_key='password'")
        conn.execute("SET s3_endpoint='localhost:9000'")
        conn.execute("SET s3_use_ssl=false")
        conn.execute("SET s3_url_style='path'")

        return conn

    def create_ducklake_test_table(self, conn: duckdb.DuckDBPyConnection, table_name: str):
        """Create test table in DuckLake."""
        self.logger.info(f"Creating DuckLake test table: {table_name}")

        if table_name == 'store_sales':
            conn.execute("""
                CREATE OR REPLACE TABLE store_sales_test (
                    ss_sold_date_sk INTEGER,
                    ss_sold_time_sk INTEGER,
                    ss_item_sk INTEGER,
                    ss_customer_sk INTEGER,
                    ss_cdemo_sk INTEGER,
                    ss_hdemo_sk INTEGER,
                    ss_addr_sk INTEGER,
                    ss_store_sk INTEGER,
                    ss_promo_sk INTEGER,
                    ss_ticket_number BIGINT,
                    ss_quantity INTEGER,
                    ss_wholesale_cost DECIMAL(7,2),
                    ss_list_price DECIMAL(7,2),
                    ss_sales_price DECIMAL(7,2),
                    ss_ext_discount_amt DECIMAL(7,2),
                    ss_ext_sales_price DECIMAL(7,2),
                    ss_ext_wholesale_cost DECIMAL(7,2),
                    ss_ext_list_price DECIMAL(7,2),
                    ss_ext_tax DECIMAL(7,2),
                    ss_coupon_amt DECIMAL(7,2),
                    ss_net_paid DECIMAL(7,2),
                    ss_net_paid_inc_tax DECIMAL(7,2),
                    ss_net_profit DECIMAL(7,2)
                )
            """)
        elif table_name == 'customer':
            conn.execute("""
                CREATE OR REPLACE TABLE customer_test (
                    c_customer_sk INTEGER,
                    c_customer_id VARCHAR,
                    c_current_cdemo_sk INTEGER,
                    c_current_hdemo_sk INTEGER,
                    c_current_addr_sk INTEGER,
                    c_first_shipto_date_sk INTEGER,
                    c_first_sales_date_sk INTEGER,
                    c_salutation VARCHAR,
                    c_first_name VARCHAR,
                    c_last_name VARCHAR,
                    c_preferred_cust_flag VARCHAR,
                    c_birth_day INTEGER,
                    c_birth_month INTEGER,
                    c_birth_year INTEGER,
                    c_birth_country VARCHAR,
                    c_login VARCHAR,
                    c_email_address VARCHAR,
                    c_last_review_date_sk INTEGER
                )
            """)

    def get_sample_data(self, table_name: str, num_rows: int) -> pd.DataFrame:
        """Get sample data from TPC-DS for inserts (cached)."""
        # Check cache first
        cache_key = f"{table_name}_{num_rows}"
        if cache_key in self.sample_data_cache:
            return self.sample_data_cache[cache_key]

        # Generate TPC-DS data
        temp_conn = duckdb.connect(':memory:')
        temp_conn.execute("INSTALL tpcds")
        temp_conn.execute("LOAD tpcds")
        temp_conn.execute("CALL dsdgen(sf=0.1)")

        # Get sample rows as DataFrame
        sample_df = temp_conn.execute(f"SELECT * FROM {table_name} LIMIT {num_rows}").fetchdf()
        temp_conn.close()

        # Cache the result
        self.sample_data_cache[cache_key] = sample_df

        return sample_df

    def benchmark_ducklake_insert(self, table_name: str, batch_size: int) -> Dict[str, Any]:
        """Benchmark insert operation on DuckLake."""
        self.logger.info(f"Benchmarking DuckLake insert: {table_name}, batch_size={batch_size}")

        conn = self.setup_ducklake_connection()

        try:
            # Create test table
            self.create_ducklake_test_table(conn, table_name)

            # Get sample data
            sample_df = self.get_sample_data(table_name, batch_size)

            # Measure insert time
            start_time = time.time()

            test_table = f"{table_name}_test"
            conn.execute(f"INSERT INTO {test_table} SELECT * FROM sample_df")

            insert_time = time.time() - start_time

            # Verify row count
            row_count = conn.execute(f"SELECT COUNT(*) FROM {test_table}").fetchone()[0]

            result = {
                'format': 'ducklake',
                'table': table_name,
                'batch_size': batch_size,
                'insert_time_seconds': insert_time,
                'rows_inserted': row_count,
                'rows_per_second': row_count / insert_time if insert_time > 0 else 0,
                'status': 'success'
            }

            self.logger.info(
                f"  DuckLake {table_name}: {insert_time:.3f}s, "
                f"{row_count} rows, {result['rows_per_second']:.0f} rows/s"
            )

        except Exception as e:
            self.logger.error(f"DuckLake insert failed: {e}")
            result = {
                'format': 'ducklake',
                'table': table_name,
                'batch_size': batch_size,
                'status': 'failed',
                'error': str(e)
            }

        finally:
            conn.close()

        return result

    def benchmark_iceberg_rest_insert(self, table_name: str, batch_size: int) -> Dict[str, Any]:
        """Benchmark insert operation on Iceberg with REST catalog."""
        self.logger.info(f"Benchmarking Iceberg REST insert: {table_name}, batch_size={batch_size}")

        conn = self.setup_iceberg_rest_connection()

        try:
            # Get sample data
            sample_df = self.get_sample_data(table_name, batch_size)

            # Create unique test table name
            import random
            test_table_name = f"{table_name}_test_{random.randint(1000, 9999)}"

            # Create Iceberg table from sample data (empty schema)
            conn.execute(f"""
                CREATE TABLE {test_table_name}
                AS SELECT * FROM sample_df LIMIT 0
            """)

            # Measure insert time
            start_time = time.time()

            conn.execute(f"""
                INSERT INTO {test_table_name}
                SELECT * FROM sample_df
            """)

            insert_time = time.time() - start_time

            # Verify row count
            row_count = conn.execute(f"SELECT COUNT(*) FROM {test_table_name}").fetchone()[0]

            # Clean up
            try:
                conn.execute(f"DROP TABLE IF EXISTS {test_table_name}")
            except:
                pass

            result = {
                'format': 'iceberg_rest',
                'table': table_name,
                'batch_size': batch_size,
                'insert_time_seconds': insert_time,
                'rows_inserted': row_count,
                'rows_per_second': row_count / insert_time if insert_time > 0 else 0,
                'status': 'success'
            }

            self.logger.info(
                f"  Iceberg REST {table_name}: {insert_time:.3f}s, "
                f"{row_count} rows, {result['rows_per_second']:.0f} rows/s"
            )

        except Exception as e:
            self.logger.error(f"Iceberg REST insert failed: {e}")
            result = {
                'format': 'iceberg_rest',
                'table': table_name,
                'batch_size': batch_size,
                'status': 'failed',
                'error': str(e)
            }

        finally:
            conn.close()

        return result

    def run_benchmark(self):
        """Run complete insert latency benchmark."""
        self.logger.info("=" * 80)
        self.logger.info("Starting Insert Latency Benchmark")
        self.logger.info("=" * 80)

        for table_name in self.test_tables:
            self.logger.info(f"\nTesting table: {table_name}")

            for batch_size in self.batch_sizes:
                self.logger.info(f"\n  Batch size: {batch_size}")

                # Benchmark DuckLake
                ducklake_result = self.benchmark_ducklake_insert(table_name, batch_size)
                self.results.append(ducklake_result)

                # Benchmark Iceberg REST
                iceberg_result = self.benchmark_iceberg_rest_insert(table_name, batch_size)
                self.results.append(iceberg_result)

                # Calculate overhead if both succeeded
                if (ducklake_result['status'] == 'success' and
                    iceberg_result['status'] == 'success'):
                    overhead = (
                        (iceberg_result['insert_time_seconds'] - ducklake_result['insert_time_seconds']) /
                        ducklake_result['insert_time_seconds'] * 100
                    )
                    self.logger.info(f"  Overhead: {overhead:.1f}%")

        self.logger.info("\n" + "=" * 80)
        self.logger.info("Benchmark Complete")
        self.logger.info("=" * 80)

    def save_results(self):
        """Save benchmark results to files."""
        results_dir = Path(self.config['paths']['results'])
        results_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save JSON
        json_file = results_dir / f"insert_latency_results_{timestamp}.json"
        with open(json_file, 'w') as f:
            json.dump({
                'metadata': {
                    'timestamp': timestamp,
                    'batch_sizes': self.batch_sizes,
                    'tables': self.test_tables
                },
                'results': self.results
            }, f, indent=2)

        self.logger.info(f"Results saved to {json_file}")

        # Save CSV
        df = pd.DataFrame(self.results)
        csv_file = results_dir / f"insert_latency_results_{timestamp}.csv"
        df.to_csv(csv_file, index=False)

        self.logger.info(f"Results saved to {csv_file}")

        # Generate summary
        self.generate_summary()

    def generate_summary(self):
        """Generate and print summary statistics."""
        df = pd.DataFrame([r for r in self.results if r['status'] == 'success'])

        if df.empty:
            self.logger.warning("No successful results to summarize")
            return

        self.logger.info("\n" + "=" * 80)
        self.logger.info("SUMMARY STATISTICS")
        self.logger.info("=" * 80)

        for table in self.test_tables:
            table_df = df[df['table'] == table]

            if table_df.empty:
                continue

            self.logger.info(f"\nTable: {table}")
            self.logger.info("-" * 40)

            for batch_size in self.batch_sizes:
                batch_df = table_df[table_df['batch_size'] == batch_size]

                if batch_df.empty:
                    continue

                ducklake = batch_df[batch_df['format'] == 'ducklake']
                iceberg = batch_df[batch_df['format'] == 'iceberg_rest']

                if ducklake.empty or iceberg.empty:
                    continue

                ducklake_time = ducklake.iloc[0]['insert_time_seconds']
                iceberg_time = iceberg.iloc[0]['insert_time_seconds']
                overhead = (iceberg_time - ducklake_time) / ducklake_time * 100

                self.logger.info(f"\nBatch size: {batch_size}")
                self.logger.info(f"  DuckLake:     {ducklake_time:.3f}s ({ducklake.iloc[0]['rows_per_second']:.0f} rows/s)")
                self.logger.info(f"  Iceberg REST: {iceberg_time:.3f}s ({iceberg.iloc[0]['rows_per_second']:.0f} rows/s)")
                self.logger.info(f"  Overhead:     {overhead:.1f}%")


def main():
    """Main entry point."""
    benchmark = InsertLatencyBenchmark()

    try:
        benchmark.run_benchmark()
        benchmark.save_results()

    except KeyboardInterrupt:
        benchmark.logger.info("\nBenchmark interrupted by user")
        benchmark.save_results()

    except Exception as e:
        benchmark.logger.error(f"Benchmark failed: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()
