#!/bin/bash
set -e

CONTAINER_NAME="spark-delta"
IMAGE="apache/spark:3.4.1-scala2.12-java11-python3-ubuntu"

echo "---"
echo "♻️ Removing any existing container..."
docker rm -f $CONTAINER_NAME || true

echo "---"
echo "🚀 Starting Spark container..."

# Here we are mounting the current directory's data folder to /home/spark/data in the container
# This allows us to easily share data between host and container
docker run -d \
  --name $CONTAINER_NAME \
  -p 4040:4040 \
  -v $(pwd)/data:/home/spark/data \
  --user root \
  $IMAGE sleep infinity

echo "---"
echo "📦 Installing Python dependencies inside container as root..."
# Use sudo/root to avoid permission issues
docker exec -it $CONTAINER_NAME bash -c "apt-get update && apt-get install -y python3-pip"
docker exec -it $CONTAINER_NAME bash -c "pip3 install --upgrade pip --user || pip3 install --upgrade pip"
docker exec -it $CONTAINER_NAME bash -c "pip3 install --user pyspark==3.4.1 delta-spark==2.4.0 pandas || pip3 install pyspark==3.4.1 delta-spark==2.4.0 pandas"

echo "---"
echo "Copying files to Spark container..."

# TODO: Make sure the source is correct
docker cp ./delta_generate.py spark-delta:/home/spark/delta_generate.py
docker cp ./tpcds_schema.py spark-delta:/home/spark/tpcds_schema.py
docker cp ./data1gb/. spark-delta:/home/spark/data1gb/

echo "---"
echo "🏃 Running PySpark Delta job inside container..."
docker exec -it spark-delta bash -c "python3 /home/spark/delta_generate.py"


echo "Copying data back to host"
docker cp spark-delta:/home/spark/data "$LOCAL_EXPORT_DIR"

echo "---"
echo "📁 Data copied to: $LOCAL_EXPORT_DIR"

echo "---"
echo "✅ Done! Delta table created at ./data/delta_table"

