#!/bin/bash
set -e

DATA_DIR="${1:/mydata/data1gb}"
DB_FILE="tpcds.db"
SCHEMA_FILE="${2:TPC-DS-Tool/DSGen-software-code-4.0.0/tools/tpcds.sql}"
LOAD_FILE="load_data.sql"

# 1. remove old files
rm -f $DB_FILE $LOAD_FILE

echo "--- Generating $LOAD_FILE... ---"

# 2. detect all .dat files and generate COPY statements
for dat_file in $DATA_DIR/*.dat; do
  table_name=$(basename "$dat_file" .dat)
  echo "COPY $table_name FROM '$dat_file' (DELIMITER '|', NULL '');" >> $LOAD_FILE
done

echo "--- Generate done. ---"
echo "--- Put table format to $DB_FILE... ---"

# 3. use schema file to create tables in duckdb and load data
{
  cat $SCHEMA_FILE
  cat $LOAD_FILE
} | time duckdb $DB_FILE

echo "--- Data load complete. ---"