#!/bin/bash
# Quick start script - runs the complete benchmark

set -e

echo "========================================="
echo "TPC-DS Lakehouse Benchmark Quick Start"
echo "========================================="

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo ""
    echo "Virtual environment not found. Running setup..."
    ./setup.sh
fi

# Activate virtual environment
echo ""
echo "Activating virtual environment..."
source venv/bin/activate

# Check configuration
echo ""
echo "Using configuration from config/benchmark_config.yaml"
echo ""

# Ask user what to run
echo "What would you like to do?"
echo ""
echo "1) Run full benchmark (data prep + queries)"
echo "2) Run data preparation only"
echo "3) Run queries only (skip data prep)"
echo "4) Generate visualizations from existing results"
echo ""
read -p "Enter choice [1-4]: " choice

case $choice in
    1)
        echo ""
        echo "Running full benchmark..."
        python run_benchmark.py
        ;;
    2)
        echo ""
        echo "Running data preparation..."
        python run_benchmark.py --data-prep-only
        ;;
    3)
        echo ""
        echo "Running queries only..."
        python run_benchmark.py --skip-data-prep
        ;;
    4)
        echo ""
        echo "Available results files:"
        ls -1 results/metrics_*.json 2>/dev/null || echo "No results found"
        echo ""
        read -p "Enter metrics file name: " metrics_file
        if [ -f "results/$metrics_file" ]; then
            python benchmark/metrics/visualizer.py "results/$metrics_file"
        else
            echo "File not found: results/$metrics_file"
            exit 1
        fi
        ;;
    *)
        echo "Invalid choice"
        exit 1
        ;;
esac

echo ""
echo "========================================="
echo "Done!"
echo "========================================="
echo ""
echo "Results are in the 'results/' directory"
echo ""
