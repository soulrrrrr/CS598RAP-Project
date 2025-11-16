import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import tpcds_schema

def create_spark_session():
    """
    Creates and returns a Spark Session with the Iceberg REST catalog enabled.
    """
    print("🚀 Starting Spark Session and enabling Iceberg...")
    return (
        SparkSession.builder
            .appName("Load TPCDS to Iceberg REST Catalog")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
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

def get_tpcds_schemas():
    """
    Imports and assembles the TPC-DS schema dictionary from tpcds_schema.py.
    The dictionary "key" must exactly match the .dat file name (without the extension).
    
    Returns: A dictionary { 'table_name': StructType }
    """
    print("📚 Loading all TPC-DS Schema definitions from tpcds_schema.py...")
    
    # The dictionary key (e.g., "customer_address") must match the .dat file name
    # (e.g., "customer_address.dat").
    # The dictionary value (e.g., tpcds_schema.schema_customer_address)
    # is the variable we import from tpcds_schema.py.
    
    return {
        # --- 24 TPC-DS Tables ---
        "customer_address": tpcds_schema.schema_customer_address,
        "customer_demographics": tpcds_schema.schema_customer_demographics,
        "date_dim": tpcds_schema.schema_date_dim,
        "warehouse": tpcds_schema.schema_warehouse,
        "ship_mode": tpcds_schema.schema_ship_mode,
        "time_dim": tpcds_schema.schema_time_dim,
        "reason": tpcds_schema.schema_reason,
        "income_band": tpcds_schema.schema_income_band,
        "item": tpcds_schema.schema_item,
        "store": tpcds_schema.schema_store,
        "call_center": tpcds_schema.schema_call_center,
        "customer": tpcds_schema.schema_customer,
        "web_site": tpcds_schema.schema_web_site,
        "store_returns": tpcds_schema.schema_store_returns,
        "household_demographics": tpcds_schema.schema_household_demographics,
        "web_page": tpcds_schema.schema_web_page,
        "promotion": tpcds_schema.schema_promotion,
        "catalog_page": tpcds_schema.schema_catalog_page,
        "inventory": tpcds_schema.schema_inventory,
        "catalog_returns": tpcds_schema.schema_catalog_returns,
        "web_returns": tpcds_schema.schema_web_returns,
        "web_sales": tpcds_schema.schema_web_sales,
        "catalog_sales": tpcds_schema.schema_catalog_sales,
        "store_sales": tpcds_schema.schema_store_sales
    }

def load_table(spark, full_db_name, table_name, schema, data_path_prefix):
    """
    Executes the full loading process for a single table:
    1. Read data from .dat file (using the provided schema)
    2. Write to Iceberg using .saveAsTable (mode "append")
    3. Validate the loaded data
    """
    full_table_name = f"{full_db_name}.{table_name}"
    # Assume .dat file name matches the table name
    input_path = f"{data_path_prefix}/{table_name}.dat"

    print(f"\n--- Processing table: {full_table_name} ---")
    print(f"      Data source: {input_path}")

    # 1. Load TPC-DS .dat file
    try:
        print(f"      1. Reading data from {input_path}...")
        df = (
            spark.read
                .schema(schema)
                .option("delimiter", "|")
                .option("nullValue", "") # Treat empty strings as NULL
                .csv(input_path)
        )
    except Exception as e:
        print(f"      ❌ Error: Could not read file {input_path}. Skipping this table. \n        Error message: {e}")
        return

    # 2. Write to Iceberg
    try:
        # This is the key change:
        # .mode("append") automatically handles:
        #   - If the table does not exist, create a new table with the df's schema and write to it
        #   - If the table already exists, just append the data
        # This replaces manual CREATE TABLE and .writeTo().append()
        print(f"      2. Writing (Append/Create) data to Iceberg table {full_table_name}...")
        (
            df.write
                .format("iceberg")      # Ensure using iceberg format
                .mode("append")         # Key mode: append or create
                .saveAsTable(full_table_name) # Write to table
        )
        print(f"      ✅ Success: Data loaded into {full_table_name}")
    except Exception as e:
        print(f"      ❌ Error: Failed to write to Iceberg. \n        Error message: {e}")
        return

    # 3. (Optional) Validate data
    try:
        print(f"      3. Validating data (LIMIT 10):")
        spark.sql(f"SELECT * FROM {full_table_name} LIMIT 10").show()
        
        print(f"      3. Validating data (COUNT):")
        spark.sql(f"SELECT COUNT(*) AS total_rows FROM {full_table_name}").show()
    except Exception as e:
        print(f"      ⚠️ Warning: Validation query failed. \n        Error message: {e}")

# -------------------------------------------------------
# Main execution flow
# -------------------------------------------------------
def main():
    spark = None
    try:
        spark = create_spark_session()
        
        # --- Configuration ---
        ICEBERG_CATALOG = "demo"
        DATABASE_NAME = "tpcds"
        FULL_DB_NAME = f"{ICEBERG_CATALOG}.{DATABASE_NAME}"
        # The base path where your TPC-DS .dat files are located
        TPCDS_DATA_PATH_PREFIX = "/tmp/data1gb" 
        # --- End Configuration ---

        # 1. Create database (namespace)
        print(f"Ensuring Database exists: {FULL_DB_NAME}")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {FULL_DB_NAME}")

        # 2. Get all schemas to load
        schemas_to_load = get_tpcds_schemas()
        if not schemas_to_load:
            print("⚠️ Warning: No schemas defined in 'get_tpcds_schemas'. Exiting.")
            return
            
        print(f"🔎 Found {len(schemas_to_load)} tables to process: {list(schemas_to_load.keys())}")

        # 3. Loop through and process each table
        for table_name, schema in schemas_to_load.items():
            load_table(
                spark=spark,
                full_db_name=FULL_DB_NAME,
                table_name=table_name,
                schema=schema,
                data_path_prefix=TPCDS_DATA_PATH_PREFIX
            )

        print("\n🎉 --- All tables processed ---")

    except Exception as e:
        print(f"\n🔥🔥🔥 An unexpected error occurred: {e}", file=sys.stderr)
    finally:
        if spark:
            print("🛑 Stopping Spark Session.")
            spark.stop()

if __name__ == "__main__":
    main()