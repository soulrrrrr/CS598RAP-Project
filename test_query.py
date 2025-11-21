#!/usr/bin/env python3
"""Test querying the generated TPC-DS data."""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

from benchmark.utils.config_loader import ConfigLoader
from benchmark.core.query_executor import QueryExecutor

print("=" * 60)
print("Testing Query Execution")
print("=" * 60)

try:
    # Load config
    config = ConfigLoader()

    # Create query executor for native format
    print("\n1. Creating query executor (native Parquet)...")
    executor = QueryExecutor(
        format_type='native',
        data_path=config.get('paths.tpcds_raw'),
        config=config.get('duckdb')
    )
    print("   ✓ Executor created")

    # Create views for all tables
    print("\n2. Creating table views...")
    tables = ['store_sales', 'customer', 'item', 'date_dim']
    executor.create_views(tables)
    print(f"   ✓ Created views for {len(tables)} tables")

    # Test simple query
    print("\n3. Running test query...")
    test_query = """
    SELECT
        COUNT(*) as total_sales,
        SUM(ss_sales_price) as total_revenue
    FROM store_sales
    WHERE ss_sold_date_sk IS NOT NULL
    """

    result = executor.execute_query(test_query, "test_q1", num_runs=2)

    print("\n" + "=" * 60)
    print("QUERY RESULTS")
    print("=" * 60)
    print(f"Status: {result['status']}")
    print(f"Cold run: {result.get('cold_run_time_seconds', 0):.3f}s")
    print(f"Median warm time: {result.get('median_time_seconds', 0):.3f}s")
    print(f"Rows returned: {result.get('rows_returned', 0)}")

    # Test join query
    print("\n4. Running join query...")
    join_query = """
    SELECT
        d.d_year,
        COUNT(*) as num_sales,
        SUM(ss_sales_price) as total_revenue
    FROM store_sales ss
    JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
    GROUP BY d.d_year
    ORDER BY d.d_year
    LIMIT 5
    """

    result2 = executor.execute_query(join_query, "test_q2", num_runs=2)

    print("\n" + "=" * 60)
    print("JOIN QUERY RESULTS")
    print("=" * 60)
    print(f"Status: {result2['status']}")
    print(f"Cold run: {result2.get('cold_run_time_seconds', 0):.3f}s")
    print(f"Median warm time: {result2.get('median_time_seconds', 0):.3f}s")
    print(f"Rows returned: {result2.get('rows_returned', 0)}")

    # Get table stats
    print("\n5. Table statistics...")
    stats = executor.get_table_stats('store_sales')
    print(f"   store_sales: {stats['row_count']:,} rows, {stats['column_count']} columns")

    print("\n" + "=" * 60)
    print("✓ ALL TESTS PASSED!")
    print("=" * 60)

    executor.close()

except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
