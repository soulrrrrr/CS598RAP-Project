#!/usr/bin/env python3
"""Main benchmark runner for TPC-DS lakehouse format comparison."""

import argparse
import sys
from pathlib import Path
import time

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from benchmark.utils.config_loader import ConfigLoader
from benchmark.utils.logger import setup_logger
from benchmark.preparation.generate_tpcds import TPCDSGenerator
from benchmark.preparation.convert_to_iceberg import IcebergConverter
from benchmark.preparation.convert_to_delta import DeltaConverter
from benchmark.core.query_executor import QueryExecutor
from benchmark.metrics.collector import MetricsCollector


class BenchmarkRunner:
    """Main benchmark orchestrator."""

    TPCDS_TABLES = [
        'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales',
        'customer', 'customer_address', 'customer_demographics', 'date_dim',
        'household_demographics', 'income_band', 'inventory', 'item',
        'promotion', 'reason', 'ship_mode', 'store', 'store_returns',
        'store_sales', 'time_dim', 'warehouse', 'web_page', 'web_returns',
        'web_sales', 'web_site'
    ]

    def __init__(self, config_path: str = "config/benchmark_config.yaml"):
        """
        Initialize benchmark runner.

        Args:
            config_path: Path to configuration file
        """
        self.config = ConfigLoader(config_path)

        # Setup logger
        self.logger = setup_logger(
            name="benchmark_runner",
            log_file=self.config.get('paths.logs') + "/benchmark.log",
            level=self.config.get('logging.level', 'INFO'),
            console=self.config.get('logging.console', True)
        )

        # Initialize metrics collector
        self.metrics = MetricsCollector(self.config.get('paths.results'))

        self.logger.info("=" * 70)
        self.logger.info("TPC-DS Lakehouse Benchmark")
        self.logger.info("=" * 70)
        self.logger.info(f"Scale Factor: {self.config.get('benchmark.scale_factor')}GB")
        self.logger.info(f"Formats: {', '.join(self.config.get('benchmark.formats'))}")
        self.logger.info("=" * 70)

    def run_all(self, skip_data_prep: bool = False):
        """
        Run complete benchmark suite.

        Args:
            skip_data_prep: Skip data generation and conversion if True
        """
        try:
            if not skip_data_prep:
                # Phase 1: Data Preparation
                self.prepare_data()

            # Phase 2: Query Benchmarks
            self.run_query_benchmarks()

            # Phase 3: Save and Report Results
            self.finalize_results()

            self.logger.info("\n✅ Benchmark completed successfully!")

        except Exception as e:
            self.logger.error(f"\n❌ Benchmark failed: {e}", exc_info=True)
            raise

    def prepare_data(self):
        """Prepare data: generate TPC-DS and convert to all formats."""
        self.logger.info("\n" + "=" * 70)
        self.logger.info("PHASE 1: DATA PREPARATION")
        self.logger.info("=" * 70)

        # Step 1: Generate TPC-DS data
        self.logger.info("\nStep 1: Generating TPC-DS data...")
        generator = TPCDSGenerator(self.config)
        gen_metrics = generator.generate_data()
        self.metrics.add_data_generation_metrics(gen_metrics)
        self.logger.info(f"✓ Generated {gen_metrics['total_size_gb']:.2f} GB in {gen_metrics['total_time_seconds']:.2f}s")

        # Step 2: Convert to Iceberg (if needed)
        if 'iceberg' in self.config.get('benchmark.formats'):
            self.logger.info("\nStep 2: Converting to Iceberg format...")
            iceberg_converter = IcebergConverter(self.config)
            iceberg_metrics = iceberg_converter.convert()
            self.metrics.add_format_conversion_metrics('iceberg', iceberg_metrics)
            self.logger.info(f"✓ Converted to Iceberg in {iceberg_metrics['total_time_seconds']:.2f}s")

        # Step 3: Convert to Delta (if needed)
        if 'delta' in self.config.get('benchmark.formats'):
            self.logger.info("\nStep 3: Converting to Delta format...")
            delta_converter = DeltaConverter(self.config)
            delta_metrics = delta_converter.convert()
            self.metrics.add_format_conversion_metrics('delta', delta_metrics)
            self.logger.info(f"✓ Converted to Delta in {delta_metrics['total_time_seconds']:.2f}s")

        self.logger.info("\n✓ Data preparation completed")

    def run_query_benchmarks(self):
        """Run query benchmarks for all formats."""
        self.logger.info("\n" + "=" * 70)
        self.logger.info("PHASE 2: QUERY BENCHMARKS")
        self.logger.info("=" * 70)

        formats = self.config.get('benchmark.formats')
        queries = self.config.get('benchmark.queries')
        num_runs = self.config.get('benchmark.num_query_runs', 3)

        self.logger.info(f"\nRunning {len(queries)} queries on {len(formats)} formats")
        self.logger.info(f"Each query: 1 cold run + {num_runs} warm runs")

        # Load queries from files
        query_specs = self._load_queries(queries)

        # Run benchmarks for each format
        for format_name in formats:
            self._run_format_benchmark(format_name, query_specs, num_runs)

    def _run_format_benchmark(self, format_name: str, query_specs: list, num_runs: int):
        """Run benchmark for a specific format."""
        self.logger.info(f"\n{'=' * 70}")
        self.logger.info(f"Format: {format_name.upper()}")
        self.logger.info(f"{'=' * 70}")

        # Get data path for this format
        if format_name == 'native':
            data_path = self.config.get('paths.tpcds_raw')
        elif format_name == 'iceberg':
            data_path = self.config.get('paths.iceberg')
        elif format_name == 'delta':
            data_path = self.config.get('paths.delta')
        else:
            raise ValueError(f"Unknown format: {format_name}")

        # Create query executor
        executor = QueryExecutor(
            format_type=format_name,
            data_path=data_path,
            config=self.config.get('duckdb'),
            logger=self.logger
        )

        try:
            # Create views for all tables
            self.logger.info("Creating table views...")
            executor.create_views(self.TPCDS_TABLES)
            self.logger.info("✓ Views created")

            # Execute queries
            results = []
            for query_id, query_text in query_specs:
                result = executor.execute_query(query_text, query_id, num_runs)
                results.append(result)

            # Add results to metrics
            self.metrics.add_query_results(results)

            # Print format summary
            successful = sum(1 for r in results if r['status'] == 'success')
            failed = len(results) - successful

            self.logger.info(f"\n✓ {format_name}: {successful} successful, {failed} failed")

        finally:
            executor.close()

    def _load_queries(self, query_ids: list) -> list:
        """
        Load query SQL from files.

        Args:
            query_ids: List of query IDs to load

        Returns:
            List of (query_id, query_text) tuples
        """
        query_specs = []
        queries_path = Path(self.config.get('paths.queries'))

        for query_id in query_ids:
            query_file = queries_path / f"query_{query_id}.sql"

            if not query_file.exists():
                self.logger.warning(f"Query file not found: {query_file}")
                continue

            with open(query_file, 'r') as f:
                query_text = f.read().strip()

            if query_text:
                query_specs.append((f"Q{query_id}", query_text))
            else:
                self.logger.warning(f"Empty query file: {query_file}")

        self.logger.info(f"Loaded {len(query_specs)} queries")
        return query_specs

    def finalize_results(self):
        """Save results and print summary."""
        self.logger.info("\n" + "=" * 70)
        self.logger.info("PHASE 3: RESULTS")
        self.logger.info("=" * 70)

        # Save JSON
        json_path = self.metrics.save_json()
        self.logger.info(f"\n✓ Saved JSON: {json_path}")

        # Save CSV
        csv_path = self.metrics.save_query_results_csv()
        self.logger.info(f"✓ Saved CSV: {csv_path}")

        # Print summary
        self.metrics.print_summary()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Run TPC-DS benchmark for lakehouse formats'
    )
    parser.add_argument(
        '--config',
        default='config/benchmark_config.yaml',
        help='Path to configuration file'
    )
    parser.add_argument(
        '--skip-data-prep',
        action='store_true',
        help='Skip data generation and conversion (use existing data)'
    )
    parser.add_argument(
        '--data-prep-only',
        action='store_true',
        help='Only run data preparation, skip benchmarks'
    )

    args = parser.parse_args()

    # Create runner
    runner = BenchmarkRunner(args.config)

    if args.data_prep_only:
        # Only prepare data
        runner.prepare_data()
    else:
        # Run full benchmark
        runner.run_all(skip_data_prep=args.skip_data_prep)


if __name__ == "__main__":
    main()
