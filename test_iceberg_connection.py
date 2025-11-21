"""Test Iceberg connection from DuckDB."""

import duckdb

print("Testing Iceberg connection...")

# Create connection
conn = duckdb.connect(':memory:')

# Load extensions
print("1. Loading Iceberg extension...")
conn.execute("INSTALL iceberg")
conn.execute("LOAD iceberg")
conn.execute("INSTALL httpfs")
conn.execute("LOAD httpfs")

# Configure S3
print("2. Configuring S3/MinIO access...")
conn.execute("SET s3_endpoint='localhost:9000'")
conn.execute("SET s3_access_key_id='admin'")
conn.execute("SET s3_secret_access_key='password'")
conn.execute("SET s3_use_ssl=false")
conn.execute("SET s3_url_style='path'")
conn.execute("SET unsafe_enable_version_guessing=true")

# Test reading a small table
print("3. Testing read from Iceberg table (store)...")
try:
    result = conn.execute("""
        SELECT COUNT(*) as count
        FROM iceberg_scan('s3://warehouse/tpcds/store', allow_moved_paths=true)
    """).fetchone()
    print(f"   SUCCESS: Found {result[0]} rows in store table")
except Exception as e:
    print(f"   ERROR: {e}")

# Test reading another table
print("4. Testing read from Iceberg table (customer)...")
try:
    result = conn.execute("""
        SELECT COUNT(*) as count
        FROM iceberg_scan('s3://warehouse/tpcds/customer', allow_moved_paths=true)
    """).fetchone()
    print(f"   SUCCESS: Found {result[0]} rows in customer table")
except Exception as e:
    print(f"   ERROR: {e}")

print("\nConnection test complete!")
conn.close()
