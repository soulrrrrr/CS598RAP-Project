#!/bin/bash
set -e # This makes the script exit immediately if any command fails

echo "---"
echo "♻️ Ensuring a clean environment..."
echo "Stopping and removing any existing services (docker compose down)..."
# This is the new command:
sudo docker compose down

echo "---"
echo "🚀 Starting Docker containers (docker compose up -d)..."
# We add --wait to ensure services are healthy before proceeding
sudo docker compose up -d --wait

echo "---"
echo "Copying files to Spark container..."
# Make sure these paths are correct on your local machine
sudo docker cp /mydata/data1gb/. spark-iceberg:/tmp/data1gb/
sudo docker cp $HOME/CS598project/iceberg/setup_spark_iceberg.py spark-iceberg:/tmp/setup_spark_iceberg.py
sudo docker cp $HOME/CS598project/tpcds_schema.py spark-iceberg:/tmp/tpcds_schema.py

echo "---"
echo "🏃 Running Spark data loading job inside the container..."
echo "This may take several minutes..."
echo "---"
sudo docker exec -it spark-iceberg python3 /tmp/setup_spark_iceberg.py

echo "---"
echo "✅ Data loading complete!"
echo "You can now run your DuckDB query script."