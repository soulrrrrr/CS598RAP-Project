"""Convert TPC-DS Parquet data to Iceberg format using Docker Spark container."""

import sys
from pyspark.sql import SparkSession

# List of all TPC-DS tables
TPCDS_TABLES = [
    'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales',
    'customer', 'customer_address', 'customer_demographics', 'date_dim',
    'household_demographics', 'income_band', 'inventory', 'item',
    'promotion', 'reason', 'ship_mode', 'store', 'store_returns',
    'store_sales', 'time_dim', 'warehouse', 'web_page', 'web_returns',
    'web_sales', 'web_site'
]

def create_spark_session():
    """Create Spark Session with Iceberg REST catalog."""
    print("🚀 Starting Spark Session with Iceberg support...")
    return (
        SparkSession.builder
            .appName("Convert TPCDS Parquet to Iceberg")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtension")
            .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.demo.type", "rest")
            .config("spark.sql.catalog.demo.uri", "http://rest:8181")
            .config("spark.sql.catalog.demo.warehouse", "s3://warehouse")
            .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000")
            .config("spark.sql.catalog.demo.s3.access-key-id", "admin")
            .config("spark.sql.catalog.demo.s3.secret-access-key", "password")
            .config("spark.sql.catalog.demo.s3.path-style-access", "true")
            .getOrCreate()
    )

def convert_table(spark, table_name, parquet_base_path, catalog_name, database_name):
    """
    Convert a single table from Parquet to Iceberg.

    Args:
        spark: SparkSession
        table_name: Name of the table
        parquet_base_path: Base path where Parquet files are stored
        catalog_name: Iceberg catalog name
        database_name: Database/namespace name
    """
    full_table_name = f"{catalog_name}.{database_name}.{table_name}"
    parquet_path = f"{parquet_base_path}/{table_name}.parquet"

    print(f"\n--- Converting {table_name} ---")
    print(f"    Source: {parquet_path}")
    print(f"    Target: {full_table_name}")

    try:
        # Read Parquet file
        print(f"    1. Reading Parquet data...")
        df = spark.read.parquet(parquet_path)
        row_count = df.count()
        print(f"    2. Found {row_count:,} rows")

        # Write to Iceberg
        print(f"    3. Writing to Iceberg...")
        df.write \
            .format("iceberg") \
            .mode("overwrite") \
            .saveAsTable(full_table_name)

        print(f"    ✅ Success: {table_name} converted")

        # Validate
        print(f"    4. Validating...")
        result_count = spark.sql(f"SELECT COUNT(*) FROM {full_table_name}").collect()[0][0]
        print(f"    ✅ Validation: {result_count:,} rows in Iceberg table")

        return True

    except Exception as e:
        print(f"    ❌ Error converting {table_name}: {e}")
        return False

def main():
    """Main execution."""
    spark = None
    try:
        # Configuration
        CATALOG_NAME = "demo"
        DATABASE_NAME = "tpcds"
        PARQUET_BASE_PATH = "/tmp/tpcds_parquet"

        # Create Spark session
        spark = create_spark_session()

        # Create database
        full_db_name = f"{CATALOG_NAME}.{DATABASE_NAME}"
        print(f"\n📚 Creating database: {full_db_name}")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_db_name}")

        # Convert all tables
        print(f"\n🔄 Converting {len(TPCDS_TABLES)} tables...")
        successful = 0
        failed = 0

        for table_name in TPCDS_TABLES:
            if convert_table(spark, table_name, PARQUET_BASE_PATH, CATALOG_NAME, DATABASE_NAME):
                successful += 1
            else:
                failed += 1

        # Summary
        print("\n" + "=" * 60)
        print("🎉 Conversion Complete!")
        print("=" * 60)
        print(f"Successful: {successful}/{len(TPCDS_TABLES)}")
        print(f"Failed: {failed}/{len(TPCDS_TABLES)}")
        print("=" * 60)

    except Exception as e:
        print(f"\n🔥 Fatal error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if spark:
            print("\n🛑 Stopping Spark Session")
            spark.stop()

if __name__ == "__main__":
    main()
