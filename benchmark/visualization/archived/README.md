# Archived Visualization Scripts

These scripts have been replaced with simplified, cleaner visualizations.

## Archived Files

1. **create_detailed_visualizations.py** - Created complex 4-panel visualizations with many comparisons
2. **create_cross_scale_visualizations.py** - Created cross-scale analysis with hardcoded scales

## Why Archived?

These scripts created overly complex visualizations with unnecessary comparisons:
- Average performance metrics
- "Iceberg wins" comparisons
- Top/bottom query rankings
- Cold vs warm run comparisons
- Hardcoded scale factors (1GB, 5GB, 10GB only)

## New Simplified Scripts

**Query Execution Visualization:**
```bash
python3 benchmark/visualization/create_query_execution_charts.py
```
Creates:
- `results/query_execution_times.png` - Clean query time comparison
- `results/execution_summary.png` - Simple performance summary table

**Cross-Scale Analysis:**
```bash
python3 benchmark/visualization/create_scale_analysis.py
```
Creates:
- `results/scale_comparison.png` - Performance across dataset sizes
- `results/scaling_efficiency.png` - Scaling trend line chart
- `results/scale_summary_table.png` - Summary statistics table

Features:
- Automatically detects available scales (not hardcoded)
- Works with arbitrary dataset sizes
- Cleaner, simpler visualizations
- Focuses on core metrics only

**Insert Latency Visualization (unchanged):**
```bash
python3 benchmark/visualization/create_insert_latency_visualizations.py
```

## Migration Notes

Old visualizations saved as:
- `detailed_benchmark_analysis.png` (4 subplots) → `query_execution_times.png` + `execution_summary.png`
- `iceberg_wins.png` → Removed (unnecessary comparison)
- `top_bottom_queries.png` → Removed (unnecessary ranking)
- Hardcoded cross-scale → `create_scale_analysis.py` (auto-detects scales)

## See Also

- Main visualization scripts: `benchmark/visualization/`
- Documentation: `CLAUDE.md`
