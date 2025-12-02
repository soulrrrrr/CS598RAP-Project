#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./tpcds_gen.sh [tool_dir] [scale] [data_dir]
# Example:
#   ./tpcds_gen.sh ./TPC-DS-Tool 1 /mydata/data1gb

# Defaults
TOOL_DIR="${1:-./TPC-DS-Tool}"
SCALE="${2:-1}"
DATA_DIR="${3:-/mydata/data${SCALE}gb}"

# Check if tool directory exists
if [[ ! -d "$TOOL_DIR" ]]; then
  echo "ERROR: Tool folder not found at: $TOOL_DIR"
  echo "Please clone the TPC-DS-Tool repo or point to the correct directory."
  exit 2
fi

echo "==== PARAMETERS ===="
echo "Tool dir:   $TOOL_DIR"
echo "Scale:      $SCALE"
echo "Data dir:   $DATA_DIR"
echo "===================="

# Determine if we need sudo for package installation
SUDO=""
if [ "$EUID" -ne 0 ]; then
  SUDO="sudo"
fi

echo "Installing build tools..."
$SUDO apt-get update -qq
# Try to install dependencies. 
# Note: 'duckdb' package might not be in default repos for all distros, 
# so we allow it to fail without stopping the script (|| true) just for that package if needed,
# but here we keep it strict as per original script logic.
$SUDO apt-get install -y build-essential gcc-9 make flex bison byacc git || echo "Warning: apt install had issues, continuing..."

# Find the 'tools' subdirectory inside the TPC-DS-Tool folder
TOOLS_SUBDIR=$(find "$TOOL_DIR" -type d -name tools | head -n1)
if [[ -z "$TOOLS_SUBDIR" ]]; then
  echo "ERROR: couldn't find 'tools' directory inside $TOOL_DIR"
  exit 3
fi

cd "$TOOLS_SUBDIR"
echo "Cleaning and building dsdgen and dsqgen..."
make clean || true
make CC=gcc-9 OS=LINUX

echo "Creating data directory $DATA_DIR"
$SUDO mkdir -p "$DATA_DIR"
$SUDO chown $(id -u):$(id -g) "$DATA_DIR"

# Check if data directory already has files.
if [ -d "$DATA_DIR" ] && [ "$(ls -A "$DATA_DIR")" ]; then
    echo "Data directory $DATA_DIR already exists and is not empty. Skipping dsdgen."
    echo "Existing sample files:"
    ls -lh "$DATA_DIR" | head -10
else
    echo "Running dsdgen -scale $SCALE"
    # -TERMINATE N prevents trailing pipes which mess up CSV imports
    ./dsdgen -scale "$SCALE" -dir "$DATA_DIR" -TERMINATE N

    echo "Data generation done. Sample files:"
    ls -lh "$DATA_DIR" | head -10
fi

# --- QUERY GENERATION ---

echo "Patching netezza.tpl for query generation"
# Check to avoid appending multiple times if script is re-run
if ! grep -q 'define _END = "";' ../query_templates/netezza.tpl; then
    echo 'define _END = "";' >> ../query_templates/netezza.tpl
fi

TEMPLATE_DIR="../query_templates"
OUTPUT_DIR="queries" # Relative to current dir ($TOOLS_SUBDIR)

# Function to generate a single query
generate_query()
{
    ./dsqgen \
    -DIRECTORY "$TEMPLATE_DIR" \
    -INPUT "$TEMPLATE_DIR/templates.lst" \
    -SCALE "$SCALE" \
    -OUTPUT_DIR "$OUTPUT_DIR" \
    -DIALECT netezza \
    -TEMPLATE "query${QUERY_ID}.tpl"
  
    mv "$OUTPUT_DIR/query_0.sql" "$OUTPUT_DIR/query_${QUERY_ID}.sql"
}

# Unsupported queries list
unsupported="87 17 38 8 18 22 27 21 16 32 37 40 82 92 94 12 20 36 49 44 53 63 67 70 86 89 98 1 2 4 5 11 14 23 24 30 31 33 39 47 51 54 56 57 58 59 60 64 74 75 77 78 80 81 83 95 97"

echo "Generating individual queries..."
rm -rf "$OUTPUT_DIR"
mkdir "$OUTPUT_DIR"

for i in {1..99}; do
    supported=1
    for j in $unsupported; do
        if [ $i -eq $j ]; then
            supported=0
            break
        fi
    done
    
    if [ $supported -eq 0 ]; then
        continue
    fi
    
    QUERY_ID="$i"
    generate_query
done

echo "Query generation done. Sample files:"
ls -lh "$OUTPUT_DIR" | head -10

echo "Moving queries directory to parent folder..."
# Move queries from tools/_queries to TPC-DS-Tool/_queries
rm -rf ../_queries # remove old if exists
mv "$OUTPUT_DIR" ..

echo "--> Done. Data generated in $DATA_DIR"
echo "--> Queries generated in $TOOL_DIR/_queries"