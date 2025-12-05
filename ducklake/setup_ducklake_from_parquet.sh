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

# SQL files are in the same directory as this script
SQL_SCHEMA="$SCRIPT_DIR/tpcds_no_dbgen.sql"
SQL_LOAD="$SCRIPT_DIR/load_data_from_parquet.sql"

echo "=========================================="
echo "DuckLake Setup from Parquet Files"
echo "=========================================="
echo "📍 Context:"
echo "   Script Location:  $SCRIPT_DIR"
echo "   Output Data Dir:  $OUTPUT_DATA_DIR"
echo "   Schema File:      $SQL_SCHEMA"
echo "   Load File:        $SQL_LOAD"
echo "=========================================="

# Check if duckdb command is available
if ! command -v $DUCKDB_COMMAND &> /dev/null; then
    echo "❌ Error: '$DUCKDB_COMMAND' command not found." >&2
    echo "Please install DuckDB and ensure it is in your PATH." >&2
    exit 1
fi

# Check if source Parquet files exist
if [ ! -f "$PROJECT_ROOT/data/tpcds_raw/call_center.parquet" ]; then
    echo "❌ Error: Parquet files not found in $PROJECT_ROOT/data/tpcds_raw/" >&2
    echo "Please run TPC-DS data generation first." >&2
    exit 1
fi

# Remove existing DuckLake data if it exists
if [ -d "$OUTPUT_DATA_DIR" ]; then
    echo "⚠️  Warning: Removing existing DuckLake data in '$OUTPUT_DATA_DIR'"
    rm -rf "$OUTPUT_DATA_DIR"
fi

echo "📂 Creating output directory: $OUTPUT_DATA_DIR"
mkdir -p "$OUTPUT_DATA_DIR"

echo ""
echo "🚀 Starting DuckDB DuckLake Setup..."
echo ""

# Execute DuckDB commands
$DUCKDB_COMMAND $DB_FILE <<EOF
-- 1. Install and Load ducklake extension
INSTALL ducklake;
LOAD ducklake;

-- 2. Create schema (tables structure without data)
.read '$SQL_SCHEMA'

-- 3. Load data from Parquet files into memory tables
SELECT '📥 Loading data from Parquet files...' AS status;
.read '$SQL_LOAD'

-- 4. Attach the DuckLake database (Output Configuration)
SELECT '🔗 Attaching DuckLake database...' AS status;
ATTACH 'ducklake:$METADATA_FILE' AS my_ducklake (DATA_PATH '$DATA_FILES_DIR');

-- 5. Copy data from memory to DuckLake
SELECT '🚀 Copying data to DuckLake...' AS status;
COPY FROM DATABASE memory TO my_ducklake;

-- Verification
SELECT '✅ Successfully loaded ' || count(*) || ' tables into my_ducklake' AS status
FROM information_schema.tables
WHERE table_catalog = 'my_ducklake';

-- Show table counts
SELECT '📊 Table Row Counts:' AS status;
SELECT
    table_name,
    (SELECT count(*) FROM my_ducklake.main[table_name]) as row_count
FROM information_schema.tables
WHERE table_catalog = 'my_ducklake'
  AND table_schema = 'main'
ORDER BY table_name;

EOF

echo ""
echo "=========================================="
echo "✅ DuckLake setup complete!"
echo "=========================================="
echo "   Metadata: $METADATA_FILE"
echo "   Data:     $DATA_FILES_DIR"
echo "=========================================="
