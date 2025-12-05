#!/usr/bin/env python3
"""Test direct access to local Iceberg files without REST catalog."""

import duckdb
import time

print("=" * 70)
print("Testing Direct Local Iceberg Access")
print("=" * 70)

# Connect to DuckDB
conn = duckdb.connect(':memory:')

try:
    # Install and load extensions
    print("\n1. Loading extensions...")
    conn.execute("INSTALL iceberg")
    conn.execute("LOAD iceberg")
    conn.execute("SET unsafe_enable_version_guessing = true")
    print("   ✓ Iceberg extension loaded")
    print("   ✓ Version guessing enabled")

    # Test direct access to a local Iceberg table
    print("\n2. Testing direct access to store_sales table...")
    iceberg_path = "data/iceberg_local/store_sales"

    start_time = time.time()
    result = conn.execute(f"""
        SELECT COUNT(*) as row_count
        FROM iceberg_scan('{iceberg_path}', allow_moved_paths=true)
    """).fetchone()
    query_time = time.time() - start_time

    print(f"   ✓ Query successful!")
    print(f"   Row count: {result[0]:,}")
    print(f"   Query time: {query_time:.3f}s")

    # Test with a more complex query
    print("\n3. Testing aggregation query...")
    start_time = time.time()
    result = conn.execute(f"""
        SELECT
            COUNT(*) as total_sales,
            SUM(ss_sales_price) as total_revenue
        FROM iceberg_scan('{iceberg_path}', allow_moved_paths=true)
        WHERE ss_sold_date_sk IS NOT NULL
    """).fetchone()
    query_time = time.time() - start_time

    print(f"   ✓ Query successful!")
    print(f"   Total sales: {result[0]:,}")
    print(f"   Total revenue: ${result[1]:,.2f}")
    print(f"   Query time: {query_time:.3f}s")

    # Test date_dim table
    print("\n4. Testing date_dim table...")
    iceberg_path = "data/iceberg_local/date_dim"

    start_time = time.time()
    result = conn.execute(f"""
        SELECT COUNT(*) as row_count
        FROM iceberg_scan('{iceberg_path}', allow_moved_paths=true)
    """).fetchone()
    query_time = time.time() - start_time

    print(f"   ✓ Query successful!")
    print(f"   Row count: {result[0]:,}")
    print(f"   Query time: {query_time:.3f}s")

    print("\n" + "=" * 70)
    print("✅ ALL TESTS PASSED - Direct local Iceberg access works!")
    print("=" * 70)

except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()
    exit(1)
finally:
    conn.close()
