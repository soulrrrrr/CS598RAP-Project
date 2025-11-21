"""Run comparison benchmark between Native Parquet and Iceberg formats."""

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

# Query definitions
QUERIES = {
    'Q3': """
        SELECT dt.d_year, item.i_brand_id, item.i_brand,
               SUM(ss_ext_sales_price) sum_agg
        FROM date_dim dt, store_sales, item
        WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
          AND store_sales.ss_item_sk = item.i_item_sk
          AND item.i_manufact_id = 128
          AND dt.d_moy = 11
        GROUP BY dt.d_year, item.i_brand, item.i_brand_id
        ORDER BY dt.d_year, sum_agg DESC, item.i_brand_id
        LIMIT 100
    """,
    'Q19': """
        SELECT i_brand_id, i_brand, i_manufact_id, i_manufact,
               SUM(ss_ext_sales_price) ext_price
        FROM date_dim, store_sales, item, customer, customer_address, store
        WHERE d_date_sk = ss_sold_date_sk
          AND ss_item_sk = i_item_sk
          AND i_manager_id = 8
          AND d_moy = 11
          AND d_year = 1998
          AND ss_customer_sk = c_customer_sk
          AND c_current_addr_sk = ca_address_sk
          AND SUBSTR(ca_zip, 1, 5) <> SUBSTR(s_zip, 1, 5)
          AND ss_store_sk = s_store_sk
        GROUP BY i_brand, i_brand_id, i_manufact_id, i_manufact
        ORDER BY ext_price DESC, i_brand, i_brand_id, i_manufact_id, i_manufact
        LIMIT 100
    """
}

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
    logger.info("Starting Native vs Iceberg Comparison Benchmark")
    logger.info("=" * 60)

    # Initialize metrics collector
    results_dir = config.get('paths.results')
    collector = MetricsCollector(results_dir)

    # Test formats
    formats_to_test = [
        ('native', config.get('paths.tpcds_raw')),
        ('iceberg', 's3://warehouse'),  # Not used for iceberg due to S3 direct access
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

        executor = QueryExecutor(
            format_type=format_type,
            data_path=data_path,
            config=duckdb_config,
            logger=logger
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
        for query_id, query_sql in QUERIES.items():
            try:
                metrics = executor.execute_query(
                    query=query_sql,
                    query_id=query_id,
                    num_runs=3
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
        visualizer = BenchmarkVisualizer(json_file, results_dir)
        visualizer.create_all_visualizations()
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
