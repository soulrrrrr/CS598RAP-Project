#!/bin/bash
set -e

# ============================================
# TPC-DS Query Runner Script
# ============================================
# Usage:
#   ./run_queries.sh <query_directory> [output_directory]
#
# Example:
#   ./run_queries.sh ./tpcds_queries ./results
#
# Notes:
# - Make sure the database file "tpcds.db" exists.
#   If not, run: ./create_load_script.sh
# - The script will:
#     1. Execute all SQL files matching query_*.sql in the given directory.
#     2. Save each query’s output and runtime to separate .out files.
#     3. Report any errors found during execution.
# - Environment variable SCALE can be used for display purposes only.
# ============================================

DB_FILE="tpcds.db"
QUERY_DIR="${1:-}"
OUTPUT_DIR="${2:-tpcds_query_results}"

if [ ! -f "$DB_FILE" ]; then
  echo "Database file $DB_FILE not found. Please run ./create_load_script.sh first."
  exit 1
fi

mkdir -p $OUTPUT_DIR
echo "--- Running TPC-DS Queries (Scale: $SCALE) ---"
echo "Database: $DB_FILE"
echo "Query source: $QUERY_DIR"
echo "Results output to: $OUTPUT_DIR"
echo "---"

# Use ls -v for natural sorting (query_1, query_2, ... query_10)
for query_file in $(ls -v $QUERY_DIR/query_*.sql); do
  query_name=$(basename "$query_file")
  output_file="$OUTPUT_DIR/${query_name}.out"
  
  echo "Running: $query_name ..."
  
  # .timer on will make DuckDB print 'Run Time' internally
  # The 'time' command measures the total real time of the DuckDB process
  
  # Fixed version:
  # 1. Use .read "$query_file" to execute the query instead of using <
  # 2. Use 2>&1 to redirect stderr (error messages) to the .out file
  #    so if a query fails, the .out file will contain the error message instead of being empty
  time (duckdb $DB_FILE -c ".timer on" -c ".read $query_file" > "$output_file" 2>&1)
  
  # Check if the .out file contains 'Error:' to detect failed queries
  if grep -q "Error:" "$output_file"; then
    echo "!!!! Error occurred while running $query_name. Details saved to: $output_file"
  else
    echo "Result saved to: $output_file"
  fi
  
  echo "---"
done

echo "--- All queries completed ---"