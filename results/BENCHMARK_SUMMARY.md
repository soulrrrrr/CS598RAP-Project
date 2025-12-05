# 1GB TPC-DS Benchmark Results

**Date:** December 1, 2025
**Dataset:** 1GB TPC-DS
**Queries:** 10 representative queries (Q3, Q7, Q19, Q25, Q34, Q42, Q52, Q55, Q73, Q79)
**Formats:** DuckLake (baseline), Apache Iceberg (local filesystem), Delta Lake

---

## Executive Summary

**Overall Performance (Average Median Query Time):**
- **DuckLake:** 23.4 ms (baseline)
- **Delta Lake:** 26.0 ms (+11% overhead)
- **Apache Iceberg:** 31.3 ms (+34% overhead)

**Key Findings:**
- DuckLake wins **100%** of queries (10/10)
- Iceberg wins **0%** of queries (0/10)
- Delta Lake shows moderate overhead (11% slower than DuckLake)
- All queries executed successfully across all formats

---

## Query-by-Query Results

| Query | DuckLake (ms) | Delta (ms) | Iceberg (ms) | Delta vs DuckLake | Iceberg vs DuckLake |
|-------|---------------|------------|--------------|-------------------|---------------------|
| Q3    | 15.9          | 17.1       | 20.8         | +7.5%             | +30.8%              |
| Q7    | 36.9          | 45.9       | 56.8         | +24.4%            | +53.9%              |
| Q19   | 26.8          | 29.8       | 35.9         | +11.2%            | +33.9%              |
| Q25   | 21.3          | 21.7       | 28.1         | +1.9%             | +31.9%              |
| Q34   | 25.9          | 28.5       | 34.2         | +10.0%            | +32.0%              |
| Q42   | 13.6          | 14.0       | 18.4         | +2.9%             | +35.3%              |
| Q52   | 13.6          | 14.1       | 18.5         | +3.7%             | +36.0%              |
| Q55   | 13.2          | 14.2       | 17.7         | +7.6%             | +34.1%              |
| Q73   | 24.3          | 27.7       | 31.6         | +14.0%            | +30.0%              |
| Q79   | 42.3          | 46.6       | 51.6         | +10.2%            | +22.0%              |

**Average:** 23.4 ms | 26.0 ms | 31.3 ms

---

## Performance Analysis

### DuckLake (Baseline)
- Fastest format across all queries
- Average query time: **23.4 ms**
- Consistent performance with minimal variance
- Native DuckDB format provides optimal performance

### Delta Lake
- Second fastest format
- Average query time: **26.0 ms** (+11% vs DuckLake)
- Overhead ranges from +1.9% (Q25) to +24.4% (Q7)
- Competitive performance for most queries

### Apache Iceberg
- Slowest format (local filesystem access)
- Average query time: **31.3 ms** (+34% vs DuckLake)
- Overhead ranges from +22.0% (Q79) to +53.9% (Q7)
- Local filesystem access eliminates REST/S3 overhead
  - Previous setup with REST catalog: ~136% overhead
  - Current local filesystem: 34% overhead (significant improvement)

---

## Cold vs Warm Run Analysis

**Average Cold Run Performance:**
- DuckLake: 31.0 ms
- Delta: 35.5 ms (+14.5%)
- Iceberg: 39.8 ms (+28.4%)

**Average Warm Run Performance (Median):**
- DuckLake: 23.4 ms
- Delta: 26.0 ms (+11%)
- Iceberg: 31.3 ms (+34%)

**Observations:**
- Cold runs are ~32% slower than warm runs on average
- All formats show similar cold-to-warm improvement ratios
- DuckDB's caching significantly improves warm run performance

---

## Query Complexity Analysis

### Fastest Queries (< 15 ms on DuckLake)
1. Q55: 13.2 ms - Simple aggregation
2. Q52: 13.6 ms - Item analysis
3. Q42: 13.6 ms - Date-based filter

### Slowest Queries (> 35 ms on DuckLake)
1. Q79: 42.3 ms - Complex customer analysis
2. Q7: 36.9 ms - Multi-table join with aggregation

### Most Format-Sensitive Queries (Highest Iceberg overhead)
1. Q7: +53.9% - Complex joins benefit more from native format
2. Q52: +36.0% - Metadata overhead visible in simple queries
3. Q42: +35.3% - Small result set amplifies overhead percentage

---

## File Format Characteristics

### Data Storage
- **DuckLake:** 4.3 MB metadata + data files
- **Iceberg:** Local filesystem with metadata versioning
- **Delta:** Transaction logs + Parquet data files

### Query Processing
- **DuckLake:** Direct native access, minimal metadata overhead
- **Iceberg:** Metadata file parsing + Parquet scans
- **Delta:** Transaction log reading + Parquet scans

---

## Configuration Details

**DuckDB Settings:**
- Memory limit: 8GB
- Threads: 4
- Object cache: Enabled

**Benchmark Settings:**
- Cold runs: 1 per query
- Warm runs: 3 per query
- Median time used for comparison

**Data Generation:**
- TPC-DS scale factor: 1 (1GB)
- Generated via DuckDB's TPC-DS extension
- All 24 TPC-DS tables included

---

## Conclusions

1. **DuckLake Performance:** Native DuckDB format provides the best performance across all queries, serving as an effective baseline.

2. **Delta Lake:** Shows moderate overhead (11%) with good consistency. Suitable for scenarios requiring transaction guarantees with acceptable performance trade-off.

3. **Iceberg (Local Filesystem):** 34% overhead is a significant improvement over REST catalog setup (136%). Local filesystem access is essential for fair performance comparisons.

4. **Format Overhead Patterns:**
   - Simple queries (Q42, Q52, Q55): 3-7% overhead for Delta, 34-36% for Iceberg
   - Complex queries (Q7, Q79): 10-24% overhead for Delta, 22-54% for Iceberg
   - DuckLake's native integration eliminates metadata parsing overhead

5. **Recommendations:**
   - Use DuckLake for performance-critical analytics workloads
   - Delta Lake provides good balance of features and performance
   - Iceberg local filesystem access is mandatory for meaningful benchmarks
   - Consider query complexity when evaluating format overhead impact

---

## Generated Files

**Data Files:**
- `comparison_results.json` - Complete benchmark data
- `comparison_query_results.csv` - Query-by-query results

**Visualizations:**
- `detailed_benchmark_analysis.png` - 4-panel comprehensive analysis
  - Panel A: Cold vs Warm performance comparison
  - Panel B: Iceberg vs DuckLake head-to-head
  - Panel C: Speedup/slowdown chart
  - Panel D: Summary statistics
- `query_comparison.png` - Side-by-side query comparison
- `format_summary.png` - Format average comparison
- `query_details.png` - Cold vs warm run details
- `iceberg_wins.png` - Queries where Iceberg beats DuckLake (0 queries)
- `top_bottom_queries.png` - Fastest/slowest query analysis

---

## Next Steps

To run multi-scale benchmarks (5GB and 10GB):

```bash
source venv/bin/activate
./run_multi_scale_benchmark.sh
```

This will generate cross-scale analysis showing how format overhead changes with data size.
