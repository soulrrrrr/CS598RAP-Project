# Benchmark Visualization Guide

This guide explains all the generated visualizations from the comprehensive TPC-DS benchmark comparing DuckLake, Apache Iceberg, and Delta Lake table formats.

## Generated Visualizations

### 1. **detailed_benchmark_analysis.png** (1.0 MB) - Main Analysis Dashboard
A comprehensive 4-panel visualization showing:

**Panel A: Cold vs Warm Performance (All 42 Queries)**
- Compares cold run (first execution) vs warm run (median of 3 runs) for all queries
- Hatched bars = Cold runs
- Solid bars = Warm runs
- Shows query execution overhead from cold start
- All three formats (DuckLake, Iceberg, Delta) side-by-side for each query

**Panel B: Iceberg vs DuckLake Direct Comparison**
- Head-to-head comparison of warm run performance
- Blue bars = DuckLake (baseline)
- Green bars = Iceberg wins (faster than DuckLake)
- Orange bars = Iceberg loses (slower than DuckLake)
- Green percentage labels show how much faster Iceberg is when it wins

**Panel C: Relative Performance Chart**
- Shows percentage speedup/slowdown vs DuckLake baseline
- Positive bars (green) = Faster than DuckLake
- Negative bars (red) = Slower than DuckLake
- Zero line represents DuckLake performance
- Compare both Iceberg and Delta against baseline

**Panel D: Summary Statistics**
- Average query times across all formats
- Head-to-head win/loss counts
- Best and worst performance differences
- Cold run overhead multipliers

### 2. **iceberg_wins.png** (137 KB) - Iceberg Victory Analysis
- Shows only the queries where Iceberg outperforms DuckLake
- Green percentage labels indicate performance improvement
- Identifies Iceberg's strengths (which query patterns it excels at)
- **Finding: Only 2 out of 40 queries (5%) show Iceberg faster than DuckLake**

### 3. **top_bottom_queries.png** (178 KB) - Fastest/Slowest Queries
Two side-by-side charts:

**Left: Top 10 Fastest Queries**
- Shortest execution times across all formats
- Shows which queries are naturally fast
- Good for identifying simple/selective queries

**Right: Top 10 Slowest Queries**
- Longest execution times across all formats
- Reveals complex/expensive queries
- Shows where optimization matters most

### 4. **query_comparison.png** (160 KB) - Original Comparison Chart
- Bar chart showing median warm run times for all queries
- All three formats side-by-side
- Easy to spot outliers and trends

### 5. **format_summary.png** (153 KB) - Format-Level Statistics
- Aggregate statistics by format
- Average times, success rates
- Overall performance comparison

### 6. **query_details.png** (183 KB) - Detailed Query Metrics
- Individual query breakdown
- Includes row counts and execution details

## Key Findings from Visualizations

### Overall Performance (Warm Runs)
- **DuckLake**: 27.0 ms average (baseline)
- **Iceberg**: 36.0 ms average (+33% overhead)
- **Delta**: 31.0 ms average (+15% overhead)

### Iceberg vs DuckLake Head-to-Head
- **DuckLake Wins**: 38 queries (95%)
- **Iceberg Wins**: 2 queries (5%)
  - Q41: 20% faster
  - Q61: 2% faster

### Performance Patterns

**Where Iceberg Struggles Most:**
- Q9: 43% slower (125ms vs 87ms)
- Q28: 57% slower (119ms vs 76ms)
- Q88: 22% slower (128ms vs 105ms)
- Complex joins and aggregations show significant overhead

**Where All Formats Perform Similarly:**
- Simple selective queries (Q41, Q96, Q61)
- Single-table scans with filters
- Queries with low result set sizes

### Cold Start Overhead
- **DuckLake**: 1.06x slower on cold start
- **Iceberg**: 1.08x slower on cold start
- **Delta**: 1.07x slower on cold start
- Minimal difference - all formats cache effectively

## Interpreting the Charts

### What "Faster" Means
- Positive percentages in Panel C = Format is FASTER than DuckLake
- Negative percentages = Format is SLOWER than DuckLake
- Green bars in Panel B = Iceberg wins that specific query

### Statistical Significance
- Performance differences <5% may be within noise
- Focus on patterns across multiple queries
- Outliers (>50% difference) indicate specific optimization opportunities

### Practical Implications

**For DuckLake (Baseline):**
- Fastest overall across 95% of queries
- Native format optimized for DuckDB
- Best choice for pure query performance

**For Iceberg:**
- 33% average overhead due to metadata layer
- Benefits: ACID transactions, schema evolution, time travel
- Trade performance for data management features
- Local filesystem access still has metadata interpretation cost

**For Delta:**
- 15% average overhead (better than Iceberg)
- Middle ground between performance and features
- More efficient metadata format for DuckDB

## How to Use These Visualizations

1. **For Research Papers**: Use detailed_benchmark_analysis.png for comprehensive overview
2. **For Presentations**: Use individual panels or query_comparison.png for clarity
3. **For Debugging**: Use iceberg_wins.png to identify optimization opportunities
4. **For Decision Making**: Compare top_bottom_queries.png to understand workload-specific performance

## Reproduction

To regenerate these visualizations:
```bash
# Activate virtual environment
source venv/bin/activate

# Run benchmark
python run_comparison_benchmark.py

# Generate detailed visualizations
python create_detailed_visualizations.py
```

## File Locations

All visualizations are in the `results/` directory:
- Raw data: `comparison_results.json`
- Tabular data: `comparison_query_results.csv`
- Visualizations: `*.png` files
