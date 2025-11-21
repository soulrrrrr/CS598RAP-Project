"""Convert TPC-DS Parquet data to Delta Lake format."""

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
    """Create Spark Session with Delta Lake support."""
    print("🚀 Starting Spark Session with Delta Lake support...")
    print("   Using PySpark 3.4.1 + Delta Lake 2.4.0")

    return (
        SparkSession.builder
            .appName("Convert TPCDS Parquet to Delta Lake")
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
            .getOrCreate()
    )

def convert_table(spark, table_name, parquet_base_path, delta_base_path):
    """
    Convert a single table from Parquet to Delta Lake.

    Args:
        spark: SparkSession
        table_name: Name of the table
        parquet_base_path: Base path where Parquet files are stored
        delta_base_path: Base path to store Delta tables
    """
    parquet_path = f"{parquet_base_path}/{table_name}.parquet"
    delta_path = f"{delta_base_path}/{table_name}"

    print(f"\n--- Converting {table_name} ---")
    print(f"    Source: {parquet_path}")
    print(f"    Target: {delta_path}")

    try:
        # Read Parquet file
        print(f"    1. Reading Parquet data...")
        df = spark.read.parquet(parquet_path)
        row_count = df.count()
        print(f"    2. Found {row_count:,} rows")

        # Write to Delta Lake
        print(f"    3. Writing to Delta Lake...")
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(delta_path)

        print(f"    ✅ Success: {table_name} converted")

        # Validate
        print(f"    4. Validating...")
        delta_df = spark.read.format("delta").load(delta_path)
        result_count = delta_df.count()
        print(f"    ✅ Validation: {result_count:,} rows in Delta table")

        return True

    except Exception as e:
        print(f"    ❌ Error converting {table_name}: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main execution."""
    spark = None
    try:
        # Configuration
        PARQUET_BASE_PATH = "/data/tpcds_raw"
        DELTA_BASE_PATH = "/data/delta"

        print("=" * 70)
        print("TPC-DS Parquet to Delta Lake Conversion")
        print("=" * 70)
        print(f"Source: {PARQUET_BASE_PATH}")
        print(f"Target: {DELTA_BASE_PATH}")
        print("=" * 70)

        # Create Spark session
        spark = create_spark_session()

        # Convert all tables
        print(f"\n🔄 Converting {len(TPCDS_TABLES)} tables...")
        successful = 0
        failed = 0

        for table_name in TPCDS_TABLES:
            if convert_table(spark, table_name, PARQUET_BASE_PATH, DELTA_BASE_PATH):
                successful += 1
            else:
                failed += 1

        # Summary
        print("\n" + "=" * 70)
        print("🎉 Conversion Complete!")
        print("=" * 70)
        print(f"Successful: {successful}/{len(TPCDS_TABLES)}")
        print(f"Failed: {failed}/{len(TPCDS_TABLES)}")
        print("=" * 70)

        if failed > 0:
            sys.exit(1)

    except Exception as e:
        print(f"\n🔥 Fatal error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark:
            print("\n🛑 Stopping Spark Session")
            spark.stop()

if __name__ == "__main__":
    main()
