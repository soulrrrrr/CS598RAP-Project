"""Run comparison benchmark between DuckLake, Delta Lake, and Apache Iceberg formats."""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from benchmark.utils.config_loader import ConfigLoader
from benchmark.core.query_executor import QueryExecutor
from benchmark.metrics.collector import MetricsCollector
from benchmark.metrics.visualizer import BenchmarkVisualizer
from benchmark.utils.logger import setup_logger

# Load configuration
config = ConfigLoader()
logger = setup_logger("comparison_benchmark")

# Load queries from config
def load_queries():
    """Load queries from SQL files specified in config."""
    queries = {}
    query_dir = Path(config.get('paths.queries'))
    query_ids = config.get('benchmark.queries')

    for query_num in query_ids:
        query_file = query_dir / f"query_{query_num}.sql"
        if query_file.exists():
            with open(query_file, 'r') as f:
                query_sql = f.read()
                queries[f'Q{query_num}'] = query_sql
        else:
            logger.warning(f"Query file not found: {query_file}")

    return queries

# Load queries from files
QUERIES = load_queries()

# TPC-DS tables
TABLES = [
    'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales',
    'customer', 'customer_address', 'customer_demographics', 'date_dim',
    'household_demographics', 'income_band', 'inventory', 'item',
    'promotion', 'reason', 'ship_mode', 'store', 'store_returns',
    'store_sales', 'time_dim', 'warehouse', 'web_page', 'web_returns',
    'web_sales', 'web_site'
]

def main():
    logger.info("=" * 60)
    logger.info("Starting DuckLake vs Delta vs Iceberg Comparison Benchmark")
    logger.info("=" * 60)

    # Initialize metrics collector
    results_dir = config.get('paths.results')
    collector = MetricsCollector(results_dir)

    # Test formats - DuckLake as baseline
    formats_to_test = [
        ('ducklake', config.get('paths.ducklake')),
        ('iceberg', config.get('paths.iceberg')),  # Local filesystem Iceberg
        ('delta', config.get('paths.delta'))
    ]

    for format_type, data_path in formats_to_test:
        logger.info(f"\n{'=' * 60}")
        logger.info(f"Testing format: {format_type.upper()}")
        logger.info(f"{'=' * 60}\n")

        # Create query executor
        duckdb_config = {
            'memory_limit': config.get('duckdb.memory_limit'),
            'threads': config.get('duckdb.threads'),
            'enable_object_cache': config.get('duckdb.enable_object_cache'),
            'preserve_insertion_order': config.get('duckdb.preserve_insertion_order')
        }

        # Get DuckLake paths if needed
        ducklake_metadata = None
        ducklake_data = None
        if format_type == 'ducklake':
            ducklake_metadata = config.get('paths.ducklake_metadata')
            ducklake_data = config.get('paths.ducklake_data')

        executor = QueryExecutor(
            format_type=format_type,
            data_path=data_path,
            config=duckdb_config,
            logger=logger,
            ducklake_metadata=ducklake_metadata,
            ducklake_data=ducklake_data
        )

        # Create views
        try:
            logger.info(f"Creating views for {len(TABLES)} tables...")
            executor.create_views(TABLES)
            logger.info("Views created successfully\n")
        except Exception as e:
            logger.error(f"Failed to create views: {e}")
            executor.close()
            continue

        # Run queries
        num_runs = config.get('benchmark.num_query_runs', 3)
        for query_id, query_sql in QUERIES.items():
            try:
                metrics = executor.execute_query(
                    query=query_sql,
                    query_id=query_id,
                    num_runs=num_runs
                )
                collector.add_query_results([metrics])
            except Exception as e:
                logger.error(f"Error executing {query_id}: {e}")

        executor.close()

    # Save results
    logger.info("\n" + "=" * 60)
    logger.info("Saving Results")
    logger.info("=" * 60)

    json_file = collector.save_json(f"{results_dir}/comparison_results.json")
    csv_file = collector.save_query_results_csv(f"{results_dir}/comparison_query_results.csv")

    logger.info(f"JSON results: {json_file}")
    logger.info(f"CSV results: {csv_file}")

    # Generate visualizations
    logger.info("\n" + "=" * 60)
    logger.info("Generating Visualizations")
    logger.info("=" * 60)

    try:
        # Use new simplified visualization script
        import subprocess
        result = subprocess.run(
            ['python3', 'benchmark/visualization/create_query_execution_charts.py'],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            logger.info("Visualizations created successfully")
        else:
            logger.error(f"Visualization script failed: {result.stderr}")
    except Exception as e:
        logger.error(f"Error creating visualizations: {e}")

    # Print summary
    logger.info("\n" + "=" * 60)
    logger.info("Benchmark Summary")
    logger.info("=" * 60)

    summary = collector.get_summary_statistics()
    logger.info(f"\nTotal Queries: {summary['total_queries']}")
    logger.info(f"Successful: {summary['successful_queries']}")
    logger.info(f"Failed: {summary['failed_queries']}")

    logger.info("\nResults by Format:")
    for format_name, format_data in summary['by_format'].items():
        logger.info(f"\n  {format_name.upper()}:")
        logger.info(f"    Total: {format_data['total']}")
        logger.info(f"    Successful: {format_data['successful']}")
        if format_data['successful'] > 0:
            logger.info(f"    Avg Time: {format_data['avg_time']:.4f}s")

    logger.info("\nQuery-by-Query Comparison:")
    comparison = collector.get_format_comparison()
    for query_id, formats in comparison.items():
        logger.info(f"\n  {query_id}:")
        for fmt, data in formats.items():
            if 'time' in data:
                logger.info(f"    {fmt}: {data['time']:.4f}s ({data['rows']} rows)")
            else:
                logger.info(f"    {fmt}: ERROR")

    logger.info("\n" + "=" * 60)
    logger.info("Benchmark Complete!")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
