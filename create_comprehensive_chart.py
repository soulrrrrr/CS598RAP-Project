"""Create comprehensive benchmark visualization showing all results in one chart."""

import json
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path

# Set style
sns.set_theme(style="whitegrid")
plt.rcParams['figure.figsize'] = (16, 10)
plt.rcParams['font.size'] = 11

# Load results
results_file = Path("results/comparison_results.json")
with open(results_file, 'r') as f:
    data = json.load(f)

# Extract query results
query_results = data['query_results']

# Organize data by query and format
queries = {}
for result in query_results:
    query_id = result['query_id']
    format_type = result['format']
    median_time = result['median_time_seconds']

    if query_id not in queries:
        queries[query_id] = {}
    queries[query_id][format_type] = median_time * 1000  # Convert to milliseconds

# Sort queries
sorted_queries = sorted(queries.keys())

# Prepare data for plotting
formats = ['native', 'delta', 'iceberg']
format_colors = {
    'native': '#2ecc71',      # Green
    'delta': '#3498db',       # Blue
    'iceberg': '#e74c3c'      # Red
}
format_labels = {
    'native': 'Native Parquet',
    'delta': 'Delta Lake',
    'iceberg': 'Apache Iceberg'
}

# Create the comprehensive chart
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12),
                                gridspec_kw={'height_ratios': [3, 1]})

# Main chart: Query-by-query comparison
x = np.arange(len(sorted_queries))
width = 0.25

for i, fmt in enumerate(formats):
    times = [queries[q].get(fmt, 0) for q in sorted_queries]
    offset = (i - 1) * width
    bars = ax1.bar(x + offset, times, width,
                   label=format_labels[fmt],
                   color=format_colors[fmt],
                   alpha=0.8,
                   edgecolor='black',
                   linewidth=0.5)

    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        if height > 0:
            ax1.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.1f}ms',
                    ha='center', va='bottom', fontsize=9, fontweight='bold')

ax1.set_xlabel('TPC-DS Query', fontsize=14, fontweight='bold')
ax1.set_ylabel('Execution Time (milliseconds)', fontsize=14, fontweight='bold')
ax1.set_title('TPC-DS Query Performance Comparison: Native Parquet vs Delta Lake vs Apache Iceberg\n' +
              'Dataset: 1GB | DuckDB Query Engine | Cold + 3 Warm Runs (Median)',
              fontsize=16, fontweight='bold', pad=20)
ax1.set_xticks(x)
ax1.set_xticklabels(sorted_queries, fontsize=12, fontweight='bold')
ax1.legend(fontsize=12, loc='upper left', framealpha=0.9)
ax1.grid(axis='y', alpha=0.3, linestyle='--')
ax1.set_ylim(0, max([max(queries[q].values()) for q in sorted_queries]) * 1.15)

# Summary chart: Average performance
avg_times = {}
for fmt in formats:
    times = [queries[q].get(fmt, 0) for q in sorted_queries]
    avg_times[fmt] = np.mean(times)

colors = [format_colors[fmt] for fmt in formats]
labels = [format_labels[fmt] for fmt in formats]
values = [avg_times[fmt] for fmt in formats]

bars2 = ax2.bar(labels, values, color=colors, alpha=0.8,
                edgecolor='black', linewidth=1.5, width=0.6)

# Add value labels and speedup annotations
baseline = avg_times['native']
for i, (bar, fmt) in enumerate(zip(bars2, formats)):
    height = bar.get_height()

    # Value label
    ax2.text(bar.get_x() + bar.get_width()/2., height,
            f'{height:.1f}ms',
            ha='center', va='bottom', fontsize=14, fontweight='bold')

    # Speedup annotation
    if fmt != 'native':
        speedup = height / baseline
        ax2.text(bar.get_x() + bar.get_width()/2., height/2,
                f'{speedup:.2f}x',
                ha='center', va='center', fontsize=12,
                fontweight='bold', color='white',
                bbox=dict(boxstyle='round,pad=0.3', facecolor='black', alpha=0.7))

ax2.set_ylabel('Average Time (ms)', fontsize=13, fontweight='bold')
ax2.set_title('Average Query Execution Time Across All Queries',
              fontsize=14, fontweight='bold', pad=15)
ax2.grid(axis='y', alpha=0.3, linestyle='--')
ax2.set_ylim(0, max(values) * 1.2)

plt.tight_layout()

# Save
output_path = "results/comprehensive_benchmark_chart.png"
plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
print(f"✅ Comprehensive chart saved to: {output_path}")
plt.close()

print("\n" + "="*70)
print("BENCHMARK SUMMARY")
print("="*70)
for fmt in formats:
    print(f"{format_labels[fmt]:20s}: {avg_times[fmt]:6.2f}ms average")
print("="*70)
