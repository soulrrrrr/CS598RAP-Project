#!/bin/bash
set -e # This makes the script exit immediately if any command fails

# --- 1. Dynamic Path Configuration ---
# Get the directory where this script is located (e.g., .../iceberg)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Get the parent directory (e.g., .../CS598project)
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Define variables
MC_PATH="$HOME/aistor-binaries/mc"
LOCAL_DATA_DIR="data/iceberg/" # This will create 'data' in the folder where you run the command

echo "---"
echo "📍 Context:"
echo "   Script Location: $SCRIPT_DIR"
echo "   Project Root:    $PROJECT_ROOT"
echo "   Output Data Dir: $(pwd)/$LOCAL_DATA_DIR"

echo "---"
echo "♻️ Ensuring a clean environment..."
echo "Stopping and removing any existing services..."
# Use SCRIPT_DIR to find docker-compose.yml regardless of where you run this script from
sudo docker compose -f "$SCRIPT_DIR/docker-compose.yml" down

echo "---"
echo "🚀 Starting Docker containers..."
sudo docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d --wait

echo "---"
echo "📂 Copying files to Spark container..."
# Ensure these paths match your actual file locations relative to the script
sudo docker cp /mydata/data1gb/. spark-iceberg:/tmp/data1gb/
sudo docker cp "$SCRIPT_DIR/setup_spark_iceberg.py" spark-iceberg:/tmp/setup_spark_iceberg.py
sudo docker cp "$PROJECT_ROOT/tpcds_schema.py" spark-iceberg:/tmp/tpcds_schema.py

echo "---"
echo "🏃 Running Spark data loading job inside the container..."
echo "This may take several minutes..."
sudo docker exec -it spark-iceberg python3 /tmp/setup_spark_iceberg.py

echo "---"
echo "⬇️ Installing MinIO Client (mc)..."
# Downloading mc binary
curl --progress-bar -L https://dl.min.io/client/mc/release/linux-amd64/mc \
  --create-dirs \
  -o "$MC_PATH"

chmod +x "$MC_PATH"

# Verify installation
"$MC_PATH" --help > /dev/null
echo "✅ mc installed successfully."

echo "---"
echo "🔌 Configuring MinIO connection..."
# We must set the alias 'localminio' for the cp command to work.
# Assuming default MinIO credentials (admin:password) and port 9000.
"$MC_PATH" alias set localminio http://localhost:9000 admin password

echo "---"
echo "📤 Copying generated Iceberg data to local filesystem..."
# Ensure local destination directory exists
mkdir -p "$LOCAL_DATA_DIR"

# Copy data recursively from MinIO bucket 'warehouse' to local dir
"$MC_PATH" cp --recursive localminio/warehouse/ "$LOCAL_DATA_DIR"

echo "---"
echo "🛑 Stopping Docker services..."
sudo docker compose -f "$SCRIPT_DIR/docker-compose.yml" down

echo "---"
echo "✅ All operations complete!"
echo "Iceberg data has been saved to: $(pwd)/$LOCAL_DATA_DIR"