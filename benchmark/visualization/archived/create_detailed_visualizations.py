"""Create detailed benchmark visualizations with cold/warm performance breakdown."""

import json
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from pathlib import Path

# Load results
results_file = Path("results/comparison_results.json")
with open(results_file) as f:
    data = json.load(f)

# Organize data by query and format
queries = {}
for result in data['query_results']:
    if result['status'] != 'success':
        continue

    query_id = result['query_id']
    format_name = result['format']

    if query_id not in queries:
        queries[query_id] = {}

    queries[query_id][format_name] = {
        'cold': result['cold_run_time_seconds'],
        'warm': result['median_time_seconds'],
        'rows': result['rows_returned']
    }

# Sort queries by number
query_ids = sorted(queries.keys(), key=lambda x: int(x[1:]))

# Create figure with multiple subplots
fig = plt.figure(figsize=(20, 24))

# 1. Cold vs Warm Performance Comparison (All Queries)
print("Creating cold vs warm comparison chart...")
ax1 = plt.subplot(4, 1, 1)

formats = ['ducklake', 'iceberg', 'delta']
format_colors = {
    'ducklake': '#2E86AB',  # Blue
    'iceberg': '#F77F00',   # Orange
    'delta': '#06A77D'      # Green
}
format_labels = {
    'ducklake': 'DuckLake',
    'iceberg': 'Iceberg',
    'delta': 'Delta'
}

x = np.arange(len(query_ids))
width = 0.12
offset_multiplier = [-2.5, -1.5, -0.5, 0.5, 1.5, 2.5]

for i, fmt in enumerate(formats):
    cold_times = []
    warm_times = []

    for qid in query_ids:
        if fmt in queries[qid]:
            cold_times.append(queries[qid][fmt]['cold'] * 1000)  # Convert to ms
            warm_times.append(queries[qid][fmt]['warm'] * 1000)
        else:
            cold_times.append(0)
            warm_times.append(0)

    # Plot cold runs (hatched)
    ax1.bar(x + width * offset_multiplier[i*2], cold_times, width,
            label=f'{format_labels[fmt]} (Cold)', color=format_colors[fmt],
            alpha=0.6, hatch='///')

    # Plot warm runs (solid)
    ax1.bar(x + width * offset_multiplier[i*2+1], warm_times, width,
            label=f'{format_labels[fmt]} (Warm)', color=format_colors[fmt])

ax1.set_ylabel('Query Time (ms)', fontsize=14, fontweight='bold')
ax1.set_xlabel('Query ID', fontsize=14, fontweight='bold')
ax1.set_title('Cold vs Warm Query Performance (All 42 Queries)',
              fontsize=16, fontweight='bold', pad=20)
ax1.set_xticks(x)
ax1.set_xticklabels(query_ids, rotation=45, ha='right', fontsize=9)
ax1.legend(loc='upper left', ncol=3, fontsize=10)
ax1.grid(axis='y', alpha=0.3, linestyle='--')
ax1.set_ylim(0, max([max(cold_times), max(warm_times)]) * 1.1)

# 2. Iceberg vs DuckLake Direct Comparison
print("Creating Iceberg vs DuckLake comparison...")
ax2 = plt.subplot(4, 1, 2)

ducklake_times = []
iceberg_times = []
iceberg_wins = []

for qid in query_ids:
    if 'ducklake' in queries[qid] and 'iceberg' in queries[qid]:
        dl_time = queries[qid]['ducklake']['warm'] * 1000
        ice_time = queries[qid]['iceberg']['warm'] * 1000
        ducklake_times.append(dl_time)
        iceberg_times.append(ice_time)
        iceberg_wins.append(ice_time < dl_time)

x = np.arange(len(query_ids))
width = 0.35

# Color bars based on who wins
bar_colors = ['#06A77D' if win else '#F77F00' for win in iceberg_wins]

bars1 = ax2.bar(x - width/2, ducklake_times, width, label='DuckLake',
                color='#2E86AB', alpha=0.8)
bars2 = ax2.bar(x + width/2, iceberg_times, width, label='Iceberg',
                color=bar_colors, alpha=0.8)

# Add percentage difference labels for significant differences
for i, (dl, ice) in enumerate(zip(ducklake_times, iceberg_times)):
    if ice < dl:
        diff_pct = ((dl - ice) / dl) * 100
        if diff_pct > 5:  # Only show if >5% faster
            ax2.text(i, max(dl, ice) + 2, f'-{diff_pct:.0f}%',
                    ha='center', va='bottom', fontsize=8, color='green', fontweight='bold')

ax2.set_ylabel('Query Time (ms)', fontsize=14, fontweight='bold')
ax2.set_xlabel('Query ID', fontsize=14, fontweight='bold')
ax2.set_title('Iceberg vs DuckLake (Warm Run Performance)\nGreen = Iceberg Faster',
              fontsize=16, fontweight='bold', pad=20)
ax2.set_xticks(x)
ax2.set_xticklabels(query_ids, rotation=45, ha='right', fontsize=9)
ax2.legend(fontsize=12)
ax2.grid(axis='y', alpha=0.3, linestyle='--')

# 3. Speedup/Slowdown Chart
print("Creating speedup chart...")
ax3 = plt.subplot(4, 1, 3)

iceberg_speedup = []
delta_speedup = []

for qid in query_ids:
    if 'ducklake' in queries[qid]:
        dl_time = queries[qid]['ducklake']['warm']

        if 'iceberg' in queries[qid]:
            ice_time = queries[qid]['iceberg']['warm']
            speedup = ((dl_time - ice_time) / dl_time) * 100  # Positive = faster than DuckLake
            iceberg_speedup.append(speedup)
        else:
            iceberg_speedup.append(0)

        if 'delta' in queries[qid]:
            delta_time = queries[qid]['delta']['warm']
            speedup = ((dl_time - delta_time) / dl_time) * 100
            delta_speedup.append(speedup)
        else:
            delta_speedup.append(0)

x = np.arange(len(query_ids))
width = 0.35

# Color bars based on positive/negative
ice_colors = ['#06A77D' if s > 0 else '#E63946' for s in iceberg_speedup]
delta_colors = ['#06A77D' if s > 0 else '#E63946' for s in delta_speedup]

bars1 = ax3.bar(x - width/2, iceberg_speedup, width, label='Iceberg vs DuckLake',
                color=ice_colors, alpha=0.8)
bars2 = ax3.bar(x + width/2, delta_speedup, width, label='Delta vs DuckLake',
                color=delta_colors, alpha=0.8)

ax3.axhline(y=0, color='black', linestyle='-', linewidth=1)
ax3.set_ylabel('Performance vs DuckLake (%)', fontsize=14, fontweight='bold')
ax3.set_xlabel('Query ID', fontsize=14, fontweight='bold')
ax3.set_title('Relative Performance vs DuckLake Baseline\n(Positive = Faster, Negative = Slower)',
              fontsize=16, fontweight='bold', pad=20)
ax3.set_xticks(x)
ax3.set_xticklabels(query_ids, rotation=45, ha='right', fontsize=9)
ax3.legend(fontsize=12)
ax3.grid(axis='y', alpha=0.3, linestyle='--')

# 4. Summary Statistics
print("Creating summary statistics...")
ax4 = plt.subplot(4, 1, 4)
ax4.axis('off')

# Calculate statistics
ice_wins_count = sum(iceberg_wins)
ice_losses_count = len(iceberg_wins) - ice_wins_count

avg_ducklake = np.mean([queries[qid]['ducklake']['warm'] for qid in query_ids if 'ducklake' in queries[qid]]) * 1000
avg_iceberg = np.mean([queries[qid]['iceberg']['warm'] for qid in query_ids if 'iceberg' in queries[qid]]) * 1000
avg_delta = np.mean([queries[qid]['delta']['warm'] for qid in query_ids if 'delta' in queries[qid]]) * 1000

ice_overhead = ((avg_iceberg - avg_ducklake) / avg_ducklake) * 100
delta_overhead = ((avg_delta - avg_ducklake) / avg_ducklake) * 100

stats_text = f"""
BENCHMARK SUMMARY (42 TPC-DS Queries)

Average Query Time (Warm Runs):
  • DuckLake (Baseline): {avg_ducklake:.2f} ms
  • Iceberg:             {avg_iceberg:.2f} ms  ({ice_overhead:+.1f}% vs DuckLake)
  • Delta:               {avg_delta:.2f} ms  ({delta_overhead:+.1f}% vs DuckLake)

Iceberg vs DuckLake Head-to-Head:
  • Iceberg Faster: {ice_wins_count} queries ({ice_wins_count/len(iceberg_wins)*100:.1f}%)
  • DuckLake Faster: {ice_losses_count} queries ({ice_losses_count/len(iceberg_wins)*100:.1f}%)

Performance Distribution:
  • Best Iceberg Win: {max(iceberg_speedup):.1f}% faster (Query {query_ids[iceberg_speedup.index(max(iceberg_speedup))]})
  • Worst Iceberg Loss: {min(iceberg_speedup):.1f}% slower (Query {query_ids[iceberg_speedup.index(min(iceberg_speedup))]})

  • Best Delta Win: {max(delta_speedup):.1f}% faster (Query {query_ids[delta_speedup.index(max(delta_speedup))]})
  • Worst Delta Loss: {min(delta_speedup):.1f}% slower (Query {query_ids[delta_speedup.index(min(delta_speedup))]})

Cold Run Overhead:
  • DuckLake: {np.mean([queries[qid]['ducklake']['cold']/queries[qid]['ducklake']['warm'] for qid in query_ids if 'ducklake' in queries[qid]]):.2f}x slower
  • Iceberg:  {np.mean([queries[qid]['iceberg']['cold']/queries[qid]['iceberg']['warm'] for qid in query_ids if 'iceberg' in queries[qid]]):.2f}x slower
  • Delta:    {np.mean([queries[qid]['delta']['cold']/queries[qid]['delta']['warm'] for qid in query_ids if 'delta' in queries[qid]]):.2f}x slower
"""

ax4.text(0.1, 0.9, stats_text, transform=ax4.transAxes, fontsize=13,
         verticalalignment='top', fontfamily='monospace',
         bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))

plt.tight_layout()
plt.savefig('results/detailed_benchmark_analysis.png', dpi=300, bbox_inches='tight')
print(f"\n✓ Saved: results/detailed_benchmark_analysis.png")

# Create separate chart for queries where Iceberg wins
print("\nCreating Iceberg wins chart...")
fig2, ax = plt.subplots(figsize=(16, 8))

ice_win_queries = [qid for i, qid in enumerate(query_ids) if iceberg_wins[i]]
ice_win_dl = [ducklake_times[i] for i in range(len(query_ids)) if iceberg_wins[i]]
ice_win_ice = [iceberg_times[i] for i in range(len(query_ids)) if iceberg_wins[i]]
ice_win_speedup = [iceberg_speedup[i] for i in range(len(query_ids)) if iceberg_wins[i]]

x = np.arange(len(ice_win_queries))
width = 0.35

bars1 = ax.bar(x - width/2, ice_win_dl, width, label='DuckLake', color='#2E86AB', alpha=0.8)
bars2 = ax.bar(x + width/2, ice_win_ice, width, label='Iceberg', color='#06A77D', alpha=0.8)

# Add speedup percentage labels
for i, speedup in enumerate(ice_win_speedup):
    ax.text(i, max(ice_win_dl[i], ice_win_ice[i]) + 1,
            f'+{speedup:.1f}%', ha='center', va='bottom',
            fontsize=10, color='green', fontweight='bold')

ax.set_ylabel('Query Time (ms)', fontsize=14, fontweight='bold')
ax.set_xlabel('Query ID', fontsize=14, fontweight='bold')
ax.set_title(f'Queries Where Iceberg Outperforms DuckLake ({len(ice_win_queries)} queries)',
             fontsize=16, fontweight='bold', pad=20)
ax.set_xticks(x)
ax.set_xticklabels(ice_win_queries, fontsize=11)
ax.legend(fontsize=12)
ax.grid(axis='y', alpha=0.3, linestyle='--')

plt.tight_layout()
plt.savefig('results/iceberg_wins.png', dpi=300, bbox_inches='tight')
print(f"✓ Saved: results/iceberg_wins.png")

# Create chart showing top 10 fastest and slowest queries
print("\nCreating top/bottom performance chart...")
fig3, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8))

# Sort queries by DuckLake time
sorted_indices = sorted(range(len(query_ids)),
                       key=lambda i: queries[query_ids[i]].get('ducklake', {}).get('warm', float('inf')))

# Top 10 fastest
fastest_10_idx = sorted_indices[:10]
fastest_queries = [query_ids[i] for i in fastest_10_idx]
fastest_dl = [queries[query_ids[i]]['ducklake']['warm'] * 1000 for i in fastest_10_idx]
fastest_ice = [queries[query_ids[i]]['iceberg']['warm'] * 1000 for i in fastest_10_idx]
fastest_delta = [queries[query_ids[i]]['delta']['warm'] * 1000 for i in fastest_10_idx]

x = np.arange(len(fastest_queries))
width = 0.25

ax1.bar(x - width, fastest_dl, width, label='DuckLake', color='#2E86AB', alpha=0.8)
ax1.bar(x, fastest_ice, width, label='Iceberg', color='#F77F00', alpha=0.8)
ax1.bar(x + width, fastest_delta, width, label='Delta', color='#06A77D', alpha=0.8)

ax1.set_ylabel('Query Time (ms)', fontsize=14, fontweight='bold')
ax1.set_xlabel('Query ID', fontsize=14, fontweight='bold')
ax1.set_title('Top 10 Fastest Queries', fontsize=16, fontweight='bold', pad=20)
ax1.set_xticks(x)
ax1.set_xticklabels(fastest_queries, fontsize=11)
ax1.legend(fontsize=11)
ax1.grid(axis='y', alpha=0.3, linestyle='--')

# Top 10 slowest
slowest_10_idx = sorted_indices[-10:]
slowest_queries = [query_ids[i] for i in slowest_10_idx]
slowest_dl = [queries[query_ids[i]]['ducklake']['warm'] * 1000 for i in slowest_10_idx]
slowest_ice = [queries[query_ids[i]]['iceberg']['warm'] * 1000 for i in slowest_10_idx]
slowest_delta = [queries[query_ids[i]]['delta']['warm'] * 1000 for i in slowest_10_idx]

x = np.arange(len(slowest_queries))

ax2.bar(x - width, slowest_dl, width, label='DuckLake', color='#2E86AB', alpha=0.8)
ax2.bar(x, slowest_ice, width, label='Iceberg', color='#F77F00', alpha=0.8)
ax2.bar(x + width, slowest_delta, width, label='Delta', color='#06A77D', alpha=0.8)

ax2.set_ylabel('Query Time (ms)', fontsize=14, fontweight='bold')
ax2.set_xlabel('Query ID', fontsize=14, fontweight='bold')
ax2.set_title('Top 10 Slowest Queries', fontsize=16, fontweight='bold', pad=20)
ax2.set_xticks(x)
ax2.set_xticklabels(slowest_queries, fontsize=11)
ax2.legend(fontsize=11)
ax2.grid(axis='y', alpha=0.3, linestyle='--')

plt.tight_layout()
plt.savefig('results/top_bottom_queries.png', dpi=300, bbox_inches='tight')
print(f"✓ Saved: results/top_bottom_queries.png")

print("\n" + "="*60)
print("All detailed visualizations created successfully!")
print("="*60)
print("\nGenerated files:")
print("  • results/detailed_benchmark_analysis.png - Comprehensive 4-panel analysis")
print(f"  • results/iceberg_wins.png - {len(ice_win_queries)} queries where Iceberg beats DuckLake")
print("  • results/top_bottom_queries.png - Top 10 fastest/slowest queries")
