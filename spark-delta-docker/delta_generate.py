from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import tpcds_schema

def create_spark_session():
    builder = SparkSession.builder \
        .appName("DeltaTest") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    return configure_spark_with_delta_pip(builder).getOrCreate()


def get_tpcds_schemas():
    return {
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

def load_table(spark, db, table, schema, data_path, delta_base):
    input_path = f"{data_path}/{table}.dat"
    delta_path = f"{delta_base}/{db}/{table}"

    print(f"\n--- Loading {table} ---")
    df = spark.read.schema(schema).option("delimiter","|").option("nullValue","").csv(input_path)

    # Save as Delta table
    df.write.format("delta").mode("overwrite").save(delta_path)
    # May not need the below line as we will do delta_scan from duckDB
    # spark.sql(f"CREATE TABLE IF NOT EXISTS {db}.{table} USING DELTA LOCATION '{delta_path}'")
    print(f"{table} ✅ done")

def main():
    spark = create_spark_session()
    DATABASE = "tpcds"
    DATA_PATH = "/home/spark/data1gb/"
    DELTA_PATH = "/home/spark/data/delta_tables"

    # May not need the below line as we will do delta_scan from duckDB
    # spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")

    schemas = get_tpcds_schemas()
    for table, schema in schemas.items():
        load_table(spark, DATABASE, table, schema, DATA_PATH, DELTA_PATH)

    spark.stop()
    print("🎉 All tables loaded!")

if __name__ == "__main__":
    main()
