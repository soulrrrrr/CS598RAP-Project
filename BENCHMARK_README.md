# TPC-DS Lakehouse Benchmark

A comprehensive benchmarking framework to compare the performance of Apache Iceberg, Delta Lake, and native Parquet formats using DuckDB as the query engine.

## Quick Start

### 1. Setup Environment

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On macOS/Linux
# or: venv\Scripts\activate  # On Windows

# Install dependencies
pip install -r requirements.txt
```

### 2. Run Complete Benchmark

```bash
# Run full benchmark (data generation + conversion + queries)
python run_benchmark.py
```

This will:
1. Generate 1GB TPC-DS dataset using DuckDB's built-in extension
2. Convert data to Iceberg and Delta formats (using PySpark)
3. Run benchmark queries on all three formats (native Parquet, Iceberg, Delta)
4. Save results to `results/` directory
5. Print summary statistics

### 3. View Results

Results are saved in the `results/` directory:
- `metrics_TIMESTAMP.json` - Complete metrics in JSON format
- `query_results.csv` - Query results in CSV format

Create visualizations:
```bash
python benchmark/metrics/visualizer.py results/metrics_TIMESTAMP.json
```

This generates:
- `query_comparison.png` - Bar chart comparing query times
- `format_summary.png` - Summary statistics per format
- `query_details.png` - Cold vs warm run comparison

## Advanced Usage

### Run Only Data Preparation

```bash
# Generate and convert data, but don't run benchmarks
python run_benchmark.py --data-prep-only
```

### Skip Data Preparation

```bash
# Use existing data, only run benchmarks
python run_benchmark.py --skip-data-prep
```

### Custom Configuration

Edit `config/benchmark_config.yaml` to customize:
- Scale factor (dataset size)
- Formats to test
- Queries to run
- DuckDB settings (memory, threads)

Example:
```yaml
benchmark:
  scale_factor: 10  # Change to 10GB dataset
  queries:
    - 3
    - 7
    - 42  # Add more queries
duckdb:
  memory_limit: "16GB"  # Increase memory
  threads: 8  # Use more threads
```

## Project Structure

```
.
├── benchmark/
│   ├── core/
│   │   └── query_executor.py      # Query execution engine
│   ├── formats/
│   ├── metrics/
│   │   ├── collector.py           # Metrics collection
│   │   └── visualizer.py          # Visualization
│   ├── preparation/
│   │   ├── generate_tpcds.py      # TPC-DS data generation
│   │   ├── convert_to_iceberg.py  # Iceberg conversion
│   │   └── convert_to_delta.py    # Delta conversion
│   └── utils/
│       ├── config_loader.py       # Configuration management
│       └── logger.py              # Logging utilities
├── config/
│   └── benchmark_config.yaml      # Main configuration
├── data/
│   ├── tpcds_raw/                 # Raw TPC-DS Parquet files
│   ├── iceberg/                   # Iceberg tables
│   └── delta/                     # Delta tables
├── results/                       # Benchmark results
├── logs/                          # Log files
└── run_benchmark.py               # Main entry point
```

## Individual Components

### Generate TPC-DS Data Only

```bash
python benchmark/preparation/generate_tpcds.py
```

### Convert to Iceberg

```bash
python benchmark/preparation/convert_to_iceberg.py
```

### Convert to Delta

```bash
python benchmark/preparation/convert_to_delta.py
```

## Configuration Options

### Benchmark Settings

```yaml
benchmark:
  scale_factor: 1          # Dataset size in GB
  formats:                 # Formats to test
    - native
    - iceberg
    - delta
  num_query_runs: 3        # Number of warm runs per query
  queries:                 # Query IDs to run
    - 3
    - 7
    - 19
    # ... add more
```

### DuckDB Settings

```yaml
duckdb:
  memory_limit: "8GB"              # Memory limit for DuckDB
  threads: 4                       # Number of threads
  enable_object_cache: true        # Enable object cache
  preserve_insertion_order: false  # Optimize for performance
```

### Data Paths

```yaml
paths:
  tpcds_raw: "data/tpcds_raw"      # Raw Parquet files
  iceberg: "data/iceberg"           # Iceberg tables
  delta: "data/delta"               # Delta tables
  results: "results"                # Output directory
  logs: "logs"                      # Log directory
  queries: "_queries"               # TPC-DS query files
```

## Understanding Results

### Metrics Collected

For each query:
- **Cold Run Time**: First execution (includes compilation)
- **Warm Run Times**: Subsequent executions (3 runs by default)
- **Median Time**: Median of warm runs (reported metric)
- **Rows Returned**: Number of result rows
- **Status**: Success or failure with error details

### Comparison Metrics

The benchmark calculates:
- Average execution time per format
- Query-by-query comparison
- Success/failure rates
- Speedup ratios (relative to baseline)

### Output Files

1. **JSON File**: Complete metrics with all details
   - Data generation metrics
   - Format conversion metrics
   - Query execution results
   - Timestamps and metadata

2. **CSV File**: Flattened query results for analysis
   - One row per query execution
   - All timing metrics
   - Easy to import into spreadsheets

3. **Visualizations**: PNG charts for presentations
   - Query comparison charts
   - Format summaries
   - Cold vs warm analysis

## Troubleshooting

### PySpark Issues

If Spark fails to start:
```bash
# Check Java installation
java -version  # Should be Java 11 or higher

# Set JAVA_HOME if needed
export JAVA_HOME=/path/to/java
```

### Memory Issues

If you encounter out-of-memory errors:
1. Reduce scale factor in config
2. Increase DuckDB memory limit
3. Reduce number of queries
4. Increase system memory or use smaller dataset

### Delta Extension Issues

Delta extension is experimental. If it fails:
1. Check platform compatibility (Linux x86_64, macOS, Windows AMD64)
2. Update DuckDB to latest version
3. Try running only native and Iceberg formats

### Query Failures

If specific queries fail:
- Check query SQL file syntax
- Verify all tables were created successfully
- Check logs in `logs/` directory for detailed errors
- Some complex queries may timeout on smaller systems

## Performance Tips

### For Faster Benchmarks

1. **Use SSD storage** for data files
2. **Increase thread count** in config (match CPU cores)
3. **Allocate more memory** to DuckDB
4. **Reduce number of warm runs** (2 instead of 3)
5. **Run subset of queries** initially

### For Accurate Comparisons

1. **Close other applications** during benchmarking
2. **Use consistent hardware** for all formats
3. **Run multiple times** and average results
4. **Clear system caches** between runs
5. **Use same query files** for all formats

## Extending the Benchmark

### Add New Queries

1. Create SQL file: `_queries/query_XX.sql`
2. Add query ID to config: `benchmark.queries`
3. Run benchmark

### Add New Metrics

Edit `benchmark/core/query_executor.py`:
```python
def execute_query(self, ...):
    # Add custom metrics
    metrics['custom_metric'] = ...
```

### Custom Visualizations

Edit `benchmark/metrics/visualizer.py` or create new visualization scripts using the JSON results.

## Known Limitations

1. **Read-Only Benchmarks**: This framework focuses on query (read) performance. Write/update operations must be performed outside DuckDB using Spark.

2. **Experimental Delta Support**: Delta Lake extension in DuckDB is experimental and platform-specific.

3. **No Merge Operations**: DuckDB cannot perform MERGE/UPDATE on Iceberg or Delta tables. These operations would need to be done via Spark.

4. **Scale Limitations**: Very large datasets (>100GB) may require more memory and time. Start with smaller scale factors.

## References

- [DuckDB Documentation](https://duckdb.org/docs/)
- [DuckDB Iceberg Extension](https://duckdb.org/docs/extensions/iceberg)
- [DuckDB Delta Extension](https://duckdb.org/docs/extensions/delta)
- [TPC-DS Benchmark Specification](http://www.tpc.org/tpcds/)
- [CIDR 2023 Paper](https://www.cidrdb.org/cidr2023/papers/p92-jain.pdf)

## License

This project is for academic and research purposes.
