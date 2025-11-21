#!/bin/bash
# Setup script for TPC-DS Lakehouse Benchmark

set -e  # Exit on error

echo "========================================="
echo "TPC-DS Lakehouse Benchmark Setup"
echo "========================================="

# Check Python version
echo ""
echo "Checking Python version..."
if ! command -v python3 &> /dev/null; then
    echo "❌ Error: python3 not found. Please install Python 3.8 or higher."
    exit 1
fi

PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
echo "✓ Found Python $PYTHON_VERSION"

# Check Java (needed for PySpark)
echo ""
echo "Checking Java installation..."
if ! command -v java &> /dev/null; then
    echo "⚠️  Warning: Java not found. PySpark requires Java 11 or higher."
    echo "   You can install Java later if needed for Iceberg/Delta conversions."
else
    JAVA_VERSION=$(java -version 2>&1 | head -n 1)
    echo "✓ Found Java: $JAVA_VERSION"
fi

# Create virtual environment
echo ""
echo "Creating virtual environment..."
if [ -d "venv" ]; then
    echo "⚠️  Virtual environment already exists. Skipping creation."
else
    python3 -m venv venv
    echo "✓ Virtual environment created"
fi

# Activate virtual environment
echo ""
echo "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo ""
echo "Upgrading pip..."
pip install --upgrade pip > /dev/null

# Install dependencies
echo ""
echo "Installing Python dependencies..."
echo "(This may take a few minutes...)"
pip install -r requirements.txt

echo ""
echo "✓ Dependencies installed"

# Create necessary directories
echo ""
echo "Creating directory structure..."
mkdir -p data/{tpcds_raw,iceberg,delta,parquet}
mkdir -p results/{raw,processed,visualizations}
mkdir -p logs
echo "✓ Directories created"

# Check if queries exist
echo ""
echo "Checking TPC-DS queries..."
if [ -d "_queries" ] && [ "$(ls -A _queries/*.sql 2>/dev/null)" ]; then
    QUERY_COUNT=$(ls -1 _queries/*.sql 2>/dev/null | wc -l)
    echo "✓ Found $QUERY_COUNT query files in _queries/"
else
    echo "⚠️  Warning: No query files found in _queries/"
    echo "   Make sure TPC-DS query SQL files are in the _queries/ directory"
fi

# Test DuckDB installation
echo ""
echo "Testing DuckDB installation..."
python3 -c "import duckdb; print('✓ DuckDB version:', duckdb.__version__)"

# Summary
echo ""
echo "========================================="
echo "Setup Complete!"
echo "========================================="
echo ""
echo "Next steps:"
echo ""
echo "1. Activate the virtual environment:"
echo "   source venv/bin/activate"
echo ""
echo "2. Review configuration:"
echo "   cat config/benchmark_config.yaml"
echo ""
echo "3. Run the benchmark:"
echo "   python run_benchmark.py"
echo ""
echo "4. Or run data preparation only:"
echo "   python run_benchmark.py --data-prep-only"
echo ""
echo "For more information, see BENCHMARK_README.md"
echo ""
