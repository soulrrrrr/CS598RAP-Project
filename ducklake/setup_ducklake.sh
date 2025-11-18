#!/bin/bash
# -----------------------------------------------------------------------------
# DuckDB Ducklake Initialization Script
# 
# This script executes a series of DuckDB commands to:
# 1. Install and load the 'ducklake' extension.
# 2. Read and execute two SQL files (assuming they exist in the 'ducklake/' directory).
# 3. Attach a Ducklake database, defining its metadata and data file paths.
# 4. Copy all data from the current in-memory database ('memory') into the 
#    newly attached 'my_ducklake' database.
#
# Prerequisites: 
# - The 'duckdb' command must be available in your PATH.
# - The 'ducklake/' directory must exist and contain the specified SQL and data files/metadata.
# -----------------------------------------------------------------------------

# --- Configuration ---
DUCKDB_COMMAND="duckdb"
DB_FILE=":memory:"  # Run commands against an in-memory database first

# Check if duckdb command is available
if ! command -v $DUCKDB_COMMAND &> /dev/null
then
    echo "❌ Error: $DUCKDB_COMMAND could not be found." >&2
    echo "Please ensure DuckDB is installed and available in your system PATH." >&2
    exit 1
fi

echo "--- Starting DuckDB Ducklake Setup ---"

# Use a here-document (<<<) to pipe all commands into the duckdb client
$DUCKDB_COMMAND $DB_FILE <<EOF
-- 1. Install the ducklake extension
INSTALL ducklake;

-- 2. Load the installed extension
LOAD ducklake;

-- 3. Read and execute the schema definition (T-PCDS schema)
.read ducklake/tpcds_no_dbgen.sql

-- 4. Read and execute the data loading commands
.read ducklake/load_data.sql

-- 5. Attach the Ducklake database. This defines where the metadata file 
--    is located and where the actual data files live.
ATTACH 'ducklake:ducklake/metadata.ducklake' AS my_ducklake (DATA_PATH 'ducklake/data_files');

-- 6. Copy all tables/data from the source (in-memory) database to the attached Ducklake target.
COPY FROM DATABASE memory TO my_ducklake;

-- Show status or list tables for verification (optional)
PRAGMA database_list;
SELECT 'Successfully loaded and copied data to my_ducklake' AS Status;

EOF

# Check the exit status of the duckdb command
if [ $? -eq 0 ]; then
    echo "✅ DuckDB commands executed successfully. The Ducklake database 'my_ducklake' is ready."
else
    echo "❌ An error occurred during the execution of DuckDB commands." >&2
    exit 1
fi

# Note: After this script runs, the database 'my_ducklake' will be correctly set up 
# on disk, but the DuckDB connection will have closed. To use it in a new session, 
# you will need to re-ATTACH it.