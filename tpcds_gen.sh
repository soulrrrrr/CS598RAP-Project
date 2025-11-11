#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./tpcds_gen.sh <ssh_target> <local_tool_dir> [scale] [remote_data_dir]
# Example:
#   ./tpcds_gen.sh ubuntu@203.0.113.10 ./TPC-DS-Tool 1 /mydata/data1gb
# This will generate 1GB of TPC-DS data and query on the remote server.

SSH_TARGET="${1:-}"
LOCAL_TOOL_DIR="${2:./TPC-DS-Tool}"
SCALE="${3:-1}"
REMOTE_DATA_DIR="${4:-/mydata/data${SCALE}gb}" # Data dir now uses SCALE
SSH_PORT="${SSH_PORT:-22}"

if [[ -z "$SSH_TARGET" || -z "$LOCAL_TOOL_DIR" ]]; then
  echo "Usage: $0 <ssh_target> <local_tool_dir> [scale] [remote_data_dir]"
  exit 1
fi

if [[ ! -d "$LOCAL_TOOL_DIR" ]]; then
  echo "ERROR: local tool folder not found: $LOCAL_TOOL_DIR"
  exit 2
fi

REMOTE_STAGE="tpcds_tool"
REMOTE_TOOL_DIR="$REMOTE_STAGE/$(basename "$LOCAL_TOOL_DIR")"

echo "==== PARAMETERS ===="
echo "SSH target:       $SSH_TARGET"
echo "Local tool dir:   $LOCAL_TOOL_DIR"
echo "Remote tool dir:  $REMOTE_TOOL_DIR"
echo "Scale:            $SCALE"
echo "Remote data dir:  $REMOTE_DATA_DIR"
echo "===================="

# Check if remote tool already exists
if ssh -p "$SSH_PORT" "$SSH_TARGET" "[ -d '$REMOTE_TOOL_DIR' ]"; then
  echo "Remote tool already exists, skipping upload."
else
  echo "Uploading tool to $REMOTE_TOOL_DIR..."
  ssh -p "$SSH_PORT" "$SSH_TARGET" "mkdir -p '$REMOTE_STAGE'"
  rsync -az -e "ssh -p $SSH_PORT" "$LOCAL_TOOL_DIR" "$SSH_TARGET:$REMOTE_STAGE/"
fi

# Run remote commands
ssh -p "$SSH_PORT" "$SSH_TARGET" bash <<EOF
set -euo pipefail

REMOTE_TOOL_DIR="$REMOTE_TOOL_DIR"
REMOTE_DATA_DIR="$REMOTE_DATA_DIR"
SCALE="$SCALE"

echo "Remote: installing build tools..."
sudo apt-get update -qq
sudo apt-get install -y build-essential gcc-9 make flex bison byacc git duckdb

# add duckdb to PATH
echo 'export PATH="\$HOME/.duckdb/cli/latest:\$PATH"' >> ~/.bashrc
export PATH="\$HOME/.duckdb/cli/latest:\$PATH"
source \$HOME/.bashrc

TOOLS_DIR=\$(find "\$REMOTE_TOOL_DIR" -type d -name tools | head -n1)
if [[ -z "\$TOOLS_DIR" ]]; then
  echo "ERROR: couldn't find 'tools' directory inside \$REMOTE_TOOL_DIR"
  echo "Check the structure:"; find "\$REMOTE_TOOL_DIR" -maxdepth 3 -type d
  exit 3
fi

cd "\$TOOLS_DIR"
echo "Remote: cleaning and building dsdgen and dsqgen..."
make clean || true
make CC=gcc-9 OS=LINUX

echo "Remote: creating data directory \$REMOTE_DATA_DIR"
sudo mkdir -p "\$REMOTE_DATA_DIR"
sudo chown \$(id -u):\$(id -g) "\$REMOTE_DATA_DIR"

# Check if data directory already has files.
if [ -d "\$REMOTE_DATA_DIR" ] && [ "\$(ls -A "\$REMOTE_DATA_DIR")" ]; then
    echo "Remote: Data directory \$REMOTE_DATA_DIR already exists and is not empty. Skipping dsdgen."
    echo "Remote: Existing sample files:"
    ls -lh "\$REMOTE_DATA_DIR" | head -20
else
    echo "Remote: running dsdgen -scale \$SCALE"
    # NOTE: Added -TERMINATE N to prevent trailing delimiters.
    # This makes loading into databases (like DuckDB) MUCH easier.
    ./dsdgen -scale "\$SCALE" -dir "\$REMOTE_DATA_DIR" -TERMINATE N

    echo "Remote: data generation done. Sample files:"
    ls -lh "\$REMOTE_DATA_DIR" | head -20
fi

# --- NEW QUERY GENERATION LOGIC START HERE ---

echo "Remote: patching netezza.tpl for query generation"
# This fix appends an empty _END separator, which can be required
echo 'define _END = "";' >> ../query_templates/netezza.tpl

# Set up variables for the remote shell
TEMPLATE_DIR="../query_templates"
OUTPUT_DIR="queries" # Relative to current dir (\$TOOLS_DIR)
QUERY_ID=""
# \$SCALE is already set from the local variable passed in

# Define the generation function for the remote shell
# This function will use the remote shell's variables
generate_query()
{
    ./dsqgen \
    -DIRECTORY "\$TEMPLATE_DIR" \
    -INPUT "\$TEMPLATE_DIR/templates.lst" \
    -SCALE "\$SCALE" \
    -OUTPUT_DIR "\$OUTPUT_DIR" \
    -DIALECT netezza \
    -TEMPLATE "query\$QUERY_ID.tpl"
  
    mv "\$OUTPUT_DIR/query_0.sql" "\$OUTPUT_DIR/query_\$QUERY_ID.sql"
}

# The unsupported list https://github.com/pingcap/tidb-bench/blob/master/tpcds/genquery.sh
unsupported="87 17 38 8 18 22 27 21 16 32 37 40 82 92 94 12 20 36 49 44 53 63 67 70 86 89 98 1 2 4 5 11 14 23 24 30 31 33 39 47 51 54 56 57 58 59 60 64 74 75 77 78 80 81 83 95 97"

echo "Remote: generating individual queries..."
rm -rf "\$OUTPUT_DIR"
mkdir "\$OUTPUT_DIR"

for i in {1..99}; do
    supported=1;
    for j in \$unsupported; do
        if [ \$i -eq \$j ]; then
            supported=0;
            break;
        fi
    done
    
    if [ \$supported -eq 0 ]; then
        continue;
    fi
    
    QUERY_ID="\$i"
    generate_query
done

echo "Remote: query generation done. Sample files:"
ls -lh "\$OUTPUT_DIR" | head -10

echo "Remote: Moving queries directory to parent folder..."
mv "\$OUTPUT_DIR" ..

# --- NEW QUERY GENERATION LOGIC END HERE ---

EOF

echo "--> Done. Data generated on $SSH_TARGET:$REMOTE_DATA_DIR"
echo "--> Queries generated on $SSH_TARGET:$REMOTE_TOOL_DIR/queries"