-- Load TPC-DS data from Parquet files into memory
-- This script reads from the data/tpcds_raw Parquet files

CREATE OR REPLACE TABLE call_center AS SELECT * FROM read_parquet('../data/tpcds_raw/call_center.parquet');
CREATE OR REPLACE TABLE catalog_page AS SELECT * FROM read_parquet('../data/tpcds_raw/catalog_page.parquet');
CREATE OR REPLACE TABLE catalog_returns AS SELECT * FROM read_parquet('../data/tpcds_raw/catalog_returns.parquet');
CREATE OR REPLACE TABLE catalog_sales AS SELECT * FROM read_parquet('../data/tpcds_raw/catalog_sales.parquet');
CREATE OR REPLACE TABLE customer_address AS SELECT * FROM read_parquet('../data/tpcds_raw/customer_address.parquet');
CREATE OR REPLACE TABLE customer AS SELECT * FROM read_parquet('../data/tpcds_raw/customer.parquet');
CREATE OR REPLACE TABLE customer_demographics AS SELECT * FROM read_parquet('../data/tpcds_raw/customer_demographics.parquet');
CREATE OR REPLACE TABLE date_dim AS SELECT * FROM read_parquet('../data/tpcds_raw/date_dim.parquet');
CREATE OR REPLACE TABLE household_demographics AS SELECT * FROM read_parquet('../data/tpcds_raw/household_demographics.parquet');
CREATE OR REPLACE TABLE income_band AS SELECT * FROM read_parquet('../data/tpcds_raw/income_band.parquet');
CREATE OR REPLACE TABLE inventory AS SELECT * FROM read_parquet('../data/tpcds_raw/inventory.parquet');
CREATE OR REPLACE TABLE item AS SELECT * FROM read_parquet('../data/tpcds_raw/item.parquet');
CREATE OR REPLACE TABLE promotion AS SELECT * FROM read_parquet('../data/tpcds_raw/promotion.parquet');
CREATE OR REPLACE TABLE reason AS SELECT * FROM read_parquet('../data/tpcds_raw/reason.parquet');
CREATE OR REPLACE TABLE ship_mode AS SELECT * FROM read_parquet('../data/tpcds_raw/ship_mode.parquet');
CREATE OR REPLACE TABLE store AS SELECT * FROM read_parquet('../data/tpcds_raw/store.parquet');
CREATE OR REPLACE TABLE store_returns AS SELECT * FROM read_parquet('../data/tpcds_raw/store_returns.parquet');
CREATE OR REPLACE TABLE store_sales AS SELECT * FROM read_parquet('../data/tpcds_raw/store_sales.parquet');
CREATE OR REPLACE TABLE time_dim AS SELECT * FROM read_parquet('../data/tpcds_raw/time_dim.parquet');
CREATE OR REPLACE TABLE warehouse AS SELECT * FROM read_parquet('../data/tpcds_raw/warehouse.parquet');
CREATE OR REPLACE TABLE web_page AS SELECT * FROM read_parquet('../data/tpcds_raw/web_page.parquet');
CREATE OR REPLACE TABLE web_returns AS SELECT * FROM read_parquet('../data/tpcds_raw/web_returns.parquet');
CREATE OR REPLACE TABLE web_sales AS SELECT * FROM read_parquet('../data/tpcds_raw/web_sales.parquet');
CREATE OR REPLACE TABLE web_site AS SELECT * FROM read_parquet('../data/tpcds_raw/web_site.parquet');
