#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status

# --- 1. Dynamic Path Configuration ---
# Get the directory where this script is located (e.g., .../ducklake)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Get the parent directory (Project Root)
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Define paths relative to the script or project root
DUCKDB_COMMAND="duckdb"
DB_FILE=":memory:"
OUTPUT_DATA_DIR="$PROJECT_ROOT/data/ducklake"
METADATA_FILE="$OUTPUT_DATA_DIR/metadata.ducklake"
DATA_FILES_DIR="$OUTPUT_DATA_DIR/data_files"

# SQL files are assumed to be in the same directory as this script
SQL_SCHEMA="$SCRIPT_DIR/tpcds_no_dbgen.sql"
SQL_LOAD="$SCRIPT_DIR/load_data.sql"

echo "---"
echo "📍 Context:"
echo "   Script Location:  $SCRIPT_DIR"
echo "   Output Data Dir:  $OUTPUT_DATA_DIR"
echo "   Schema File:      $SQL_SCHEMA"
echo "---"

# Check if duckdb command is available
if ! command -v $DUCKDB_COMMAND &> /dev/null; then
    echo "❌ Error: '$DUCKDB_COMMAND' command not found." >&2
    echo "Please install DuckDB and ensure it is in your PATH." >&2
    exit 1
fi

# Ensure output directory exists
if [ -d "$OUTPUT_DATA_DIR" ]; then
    echo "⚠️  Warning: Output directory '$OUTPUT_DATA_DIR' already exists."
else
    echo "📂 Creating output directory: $OUTPUT_DATA_DIR"
    mkdir -p "$OUTPUT_DATA_DIR"
fi

echo "--- Starting DuckDB Ducklake Setup ---"

# Execute DuckDB commands
$DUCKDB_COMMAND $DB_FILE <<EOF
-- 1. Install and Load ducklake
INSTALL ducklake;
LOAD ducklake;

-- 2. Read schema and load data (Input SQL files)
.read '$SQL_SCHEMA'
.read '$SQL_LOAD'

-- 3. Attach the Ducklake database (Output Configuration)
--    Using the path variables defined in the bash script
ATTACH 'ducklake:$METADATA_FILE' AS my_ducklake (DATA_PATH '$DATA_FILES_DIR');

-- 4. Copy data from memory to Ducklake
--    Using SELECT to log progress instead of bash 'echo'
SELECT '🚀 Copying data to Ducklake...' AS status;
COPY FROM DATABASE memory TO my_ducklake;

-- Verification
SELECT '✅ Successfully loaded ' || count(*) || ' tables into my_ducklake' AS status 
FROM information_schema.tables 
WHERE table_catalog = 'my_ducklake';

EOF

echo "---"
echo "✅ Ducklake pipeline finished successfully!"
echo "   Metadata: $METADATA_FILE"
echo "   Data:     $DATA_FILES_DIR"