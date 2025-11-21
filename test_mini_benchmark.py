#!/usr/bin/env python3
"""Mini benchmark test - run a few TPC-DS queries on native format."""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

from benchmark.utils.config_loader import ConfigLoader
from benchmark.core.query_executor import QueryExecutor
from benchmark.metrics.collector import MetricsCollector

print("=" * 70)
print("Mini Benchmark Test - Native Format Only")
print("=" * 70)

try:
    # Load config
    config = ConfigLoader()

    # Create metrics collector
    metrics = MetricsCollector(config.get('paths.results'))

    # Test queries (subset)
    test_queries = [3, 19]  # Just 2 queries for quick test

    print(f"\nTesting {len(test_queries)} TPC-DS queries")
    print(f"Format: Native Parquet")
    print(f"Runs: 1 cold + 3 warm per query")

    # Create executor
    print("\nInitializing query executor...")
    executor = QueryExecutor(
        format_type='native',
        data_path=config.get('paths.tpcds_raw'),
        config=config.get('duckdb')
    )

    # All tables needed for TPC-DS queries
    all_tables = [
        'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales',
        'customer', 'customer_address', 'customer_demographics', 'date_dim',
        'household_demographics', 'income_band', 'inventory', 'item',
        'promotion', 'reason', 'ship_mode', 'store', 'store_returns',
        'store_sales', 'time_dim', 'warehouse', 'web_page', 'web_returns',
        'web_sales', 'web_site'
    ]

    print("Creating table views...")
    executor.create_views(all_tables)
    print("✓ Views created")

    # Load and run queries
    results = []
    queries_path = Path(config.get('paths.queries'))

    print("\nRunning queries:")
    print("-" * 70)

    for query_id in test_queries:
        query_file = queries_path / f"query_{query_id}.sql"

        if not query_file.exists():
            print(f"⚠️  Query {query_id}: file not found")
            continue

        # Read query
        with open(query_file, 'r') as f:
            query_text = f.read().strip()

        # Execute query
        result = executor.execute_query(query_text, f"Q{query_id}", num_runs=3)
        results.append(result)

        # Print result
        status_icon = "✓" if result['status'] == 'success' else "✗"
        if result['status'] == 'success':
            print(f"{status_icon} Query {query_id}: "
                  f"cold={result['cold_run_time_seconds']:.3f}s, "
                  f"median={result['median_time_seconds']:.3f}s, "
                  f"rows={result['rows_returned']}")
        else:
            print(f"{status_icon} Query {query_id}: FAILED - {result.get('error', 'Unknown error')}")

    # Add results to metrics
    metrics.add_query_results(results)

    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    successful = sum(1 for r in results if r['status'] == 'success')
    failed = len(results) - successful

    print(f"Queries run: {len(results)}")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")

    if successful > 0:
        avg_time = sum(r['median_time_seconds'] for r in results if r['status'] == 'success') / successful
        print(f"  Average time: {avg_time:.3f}s")

    # Save results
    json_file = metrics.save_json("test_results.json")
    csv_file = metrics.save_query_results_csv("test_results.csv")

    print(f"\n✓ Results saved:")
    print(f"  JSON: {json_file}")
    print(f"  CSV: {csv_file}")

    print("\n" + "=" * 70)
    print("✓ MINI BENCHMARK COMPLETE!")
    print("=" * 70)

    executor.close()

except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
