"""Query executor for benchmarking different table formats."""

import duckdb
import time
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from statistics import median
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))

from benchmark.utils.logger import setup_logger


class QueryExecutor:
    """Execute queries and collect performance metrics."""

    def __init__(self, format_type: str, data_path: str, config: dict, logger=None, ducklake_metadata: str = None, ducklake_data: str = None):
        """
        Initialize query executor.

        Args:
            format_type: Format to query ('ducklake', 'iceberg', 'delta')
            data_path: Path to data files
            config: DuckDB configuration dictionary
            logger: Optional logger instance
            ducklake_metadata: Path to DuckLake metadata file (for ducklake format)
            ducklake_data: Path to DuckLake data directory (for ducklake format)
        """
        self.format_type = format_type
        self.data_path = Path(data_path)
        self.config = config
        self.ducklake_metadata = ducklake_metadata
        self.ducklake_data = ducklake_data
        self.logger = logger or setup_logger(f"query_executor_{format_type}")

        # Create DuckDB connection
        self.conn = duckdb.connect(':memory:')
        self._configure_duckdb()
        self._setup_format()

        self.query_results = []

    def _configure_duckdb(self):
        """Configure DuckDB with optimal settings."""
        self.logger.info("Configuring DuckDB...")

        # Set memory limit
        memory_limit = self.config.get('memory_limit', '8GB')
        self.conn.execute(f"SET memory_limit = '{memory_limit}'")

        # Set thread count
        threads = self.config.get('threads', 4)
        self.conn.execute(f"SET threads TO {threads}")

        # Performance settings
        if self.config.get('enable_object_cache', True):
            self.conn.execute("SET enable_object_cache = true")

        if not self.config.get('preserve_insertion_order', True):
            self.conn.execute("SET preserve_insertion_order = false")

        self.logger.info(
            f"DuckDB configured: memory={memory_limit}, threads={threads}"
        )

    def _setup_format(self):
        """Setup extensions and configuration for specific format."""
        self.logger.info(f"Setting up format: {self.format_type}")

        if self.format_type == 'ducklake':
            # DuckLake format
            self.conn.execute("INSTALL ducklake")
            self.conn.execute("LOAD ducklake")

            # Attach DuckLake database
            if self.ducklake_metadata and self.ducklake_data:
                attach_sql = f"""
                    ATTACH 'ducklake:{self.ducklake_metadata}' AS ducklake_db
                    (DATA_PATH '{self.ducklake_data}', OVERRIDE_DATA_PATH true)
                """
                self.conn.execute(attach_sql)
                self.logger.info(f"DuckLake database attached: {self.ducklake_metadata}")
            else:
                raise ValueError("DuckLake format requires ducklake_metadata and ducklake_data paths")

        elif self.format_type == 'iceberg':
            self.conn.execute("INSTALL iceberg")
            self.conn.execute("LOAD iceberg")
            # Enable version guessing for local Iceberg tables
            self.conn.execute("SET unsafe_enable_version_guessing=true")

            self.logger.info("Iceberg extension loaded with local filesystem access")

        elif self.format_type == 'delta':
            self.conn.execute("INSTALL delta")
            self.conn.execute("LOAD delta")
            self.logger.info("Delta extension loaded")

        else:
            raise ValueError(f"Unknown format type: {self.format_type}")

    def create_views(self, tables: List[str]):
        """
        Create views for all tables based on format type.

        Args:
            tables: List of table names to create views for
        """
        self.logger.info(f"Creating views for {len(tables)} tables...")

        for table_name in tables:
            self._create_table_view(table_name)

        self.logger.info("Views created successfully")

    def _create_table_view(self, table_name: str):
        """Create a view for a single table."""
        if self.format_type == 'ducklake':
            # For DuckLake, reference the attached database
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW {table_name} AS
                SELECT * FROM ducklake_db.main.{table_name}
            """)

        elif self.format_type == 'iceberg':
            # For Iceberg, use iceberg_scan with local filesystem path
            # This avoids REST catalog and S3/MinIO overhead
            table_path = self.data_path / table_name
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW {table_name} AS
                SELECT * FROM iceberg_scan('{table_path}', allow_moved_paths=true)
            """)

        elif self.format_type == 'delta':
            # For Delta, use delta_scan function
            table_path = self.data_path / table_name
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW {table_name} AS
                SELECT * FROM delta_scan('{table_path}')
            """)

    def execute_query(
        self,
        query: str,
        query_id: str,
        num_runs: int = 3
    ) -> Dict[str, Any]:
        """
        Execute a query multiple times and collect metrics.

        The first run is a cold run (not included in statistics).
        Subsequent runs are warm runs, and the median time is reported.

        Args:
            query: SQL query to execute
            query_id: Identifier for the query
            num_runs: Number of warm runs (excludes cold run)

        Returns:
            Dictionary with query metrics
        """
        self.logger.info(f"Executing query {query_id}...")

        warm_times = []
        cold_time = None
        row_count = 0

        try:
            # Cold run (includes compilation and first-time costs)
            self.logger.debug(f"  Cold run...")
            cold_start = time.time()
            result = self.conn.execute(query)
            rows = result.fetchall()
            cold_time = time.time() - cold_start
            row_count = len(rows)

            self.logger.debug(f"  Cold run: {cold_time:.3f}s, {row_count} rows")

            # Warm runs
            for run in range(num_runs):
                self.logger.debug(f"  Warm run {run + 1}/{num_runs}...")

                # Checkpoint to clear some caches
                self.conn.execute("CHECKPOINT")

                warm_start = time.time()
                result = self.conn.execute(query)
                _ = result.fetchall()
                warm_time = time.time() - warm_start

                warm_times.append(warm_time)
                self.logger.debug(f"    Time: {warm_time:.3f}s")

            # Calculate statistics
            median_time = median(warm_times)
            min_time = min(warm_times)
            max_time = max(warm_times)

            metrics = {
                'query_id': query_id,
                'format': self.format_type,
                'status': 'success',
                'cold_run_time_seconds': cold_time,
                'warm_run_times_seconds': warm_times,
                'median_time_seconds': median_time,
                'min_time_seconds': min_time,
                'max_time_seconds': max_time,
                'rows_returned': row_count,
                'num_runs': num_runs
            }

            self.logger.info(
                f"  Query {query_id}: "
                f"cold={cold_time:.3f}s, "
                f"median={median_time:.3f}s, "
                f"rows={row_count}"
            )

        except Exception as e:
            self.logger.error(f"  Query {query_id} failed: {e}")
            metrics = {
                'query_id': query_id,
                'format': self.format_type,
                'status': 'failed',
                'error': str(e),
                'error_type': type(e).__name__
            }

        self.query_results.append(metrics)
        return metrics

    def execute_query_with_profiling(
        self,
        query: str,
        query_id: str
    ) -> Dict[str, Any]:
        """
        Execute a query with detailed profiling information.

        Args:
            query: SQL query to execute
            query_id: Identifier for the query

        Returns:
            Dictionary with query metrics including profiling data
        """
        self.logger.info(f"Executing query {query_id} with profiling...")

        try:
            # Enable profiling
            self.conn.execute("PRAGMA enable_profiling='json'")
            self.conn.execute("PRAGMA profiling_output='profiling_output.json'")

            # Execute query
            start_time = time.time()
            result = self.conn.execute(query)
            rows = result.fetchall()
            execution_time = time.time() - start_time

            # Get profiling information
            try:
                profile_result = self.conn.execute(
                    "SELECT * FROM pragma_last_profiling_output()"
                ).fetchdf()
                has_profile = True
            except:
                profile_result = None
                has_profile = False

            metrics = {
                'query_id': query_id,
                'format': self.format_type,
                'status': 'success',
                'execution_time_seconds': execution_time,
                'rows_returned': len(rows),
                'has_profiling': has_profile
            }

            if has_profile and profile_result is not None:
                # Extract useful profiling metrics
                metrics['profiling_data'] = profile_result.to_dict()

            self.logger.info(
                f"  Query {query_id}: "
                f"time={execution_time:.3f}s, "
                f"rows={len(rows)}"
            )

        except Exception as e:
            self.logger.error(f"  Query {query_id} failed: {e}")
            metrics = {
                'query_id': query_id,
                'format': self.format_type,
                'status': 'failed',
                'error': str(e),
                'error_type': type(e).__name__
            }

        return metrics

    def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """
        Get statistics for a table.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with table statistics
        """
        try:
            # Get row count
            row_count = self.conn.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()[0]

            # Get column count
            columns = self.conn.execute(
                f"DESCRIBE {table_name}"
            ).fetchall()

            return {
                'table_name': table_name,
                'row_count': row_count,
                'column_count': len(columns),
                'columns': [col[0] for col in columns]
            }

        except Exception as e:
            self.logger.error(f"Error getting stats for {table_name}: {e}")
            return {
                'table_name': table_name,
                'error': str(e)
            }

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.logger.info("Database connection closed")
