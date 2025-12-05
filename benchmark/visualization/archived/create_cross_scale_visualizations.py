"""Create cross-scale performance visualizations comparing 1GB, 5GB, 10GB datasets."""

import json
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

def load_scale_results(scale_gb):
    """Load results for a specific scale."""
    results_file = Path(f"results/multi_scale/{scale_gb}gb/comparison_results.json")
    if not results_file.exists():
        return None

    with open(results_file) as f:
        data = json.load(f)

    # Organize by query and format
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

    return queries

def main():
    print("Loading results from all scales...")

    # Load results for each scale
    scales = [1, 5, 10]
    all_results = {}

    for scale in scales:
        results = load_scale_results(scale)
        if results:
            all_results[scale] = results
            print(f"  ✓ Loaded {scale}GB results ({len(results)} queries)")
        else:
            print(f"  ✗ No results found for {scale}GB")

    if not all_results:
        print("\nError: No results found. Run benchmarks first.")
        return

    # Get common queries across all scales
    common_queries = set(all_results[scales[0]].keys())
    for scale in scales[1:]:
        common_queries &= set(all_results[scale].keys())

    common_queries = sorted(common_queries, key=lambda x: int(x[1:]))
    print(f"\nCommon queries across all scales: {len(common_queries)}")

    # Create visualizations
    fig = plt.figure(figsize=(20, 16))

    # 1. Average Performance Scaling by Format
    print("\nCreating average performance scaling chart...")
    ax1 = plt.subplot(2, 2, 1)

    formats = ['ducklake', 'iceberg', 'delta']
    format_colors = {
        'ducklake': '#2E86AB',
        'iceberg': '#F77F00',
        'delta': '#06A77D'
    }
    format_labels = {
        'ducklake': 'DuckLake',
        'iceberg': 'Iceberg',
        'delta': 'Delta'
    }

    for fmt in formats:
        avg_times = []
        for scale in scales:
            times = []
            for qid in common_queries:
                if fmt in all_results[scale][qid]:
                    times.append(all_results[scale][qid][fmt]['warm'] * 1000)  # ms
            if times:
                avg_times.append(np.mean(times))
            else:
                avg_times.append(0)

        ax1.plot(scales, avg_times, marker='o', linewidth=2, markersize=8,
                label=format_labels[fmt], color=format_colors[fmt])

    ax1.set_xlabel('Dataset Size (GB)', fontsize=14, fontweight='bold')
    ax1.set_ylabel('Average Query Time (ms)', fontsize=14, fontweight='bold')
    ax1.set_title('Average Query Performance Scaling', fontsize=16, fontweight='bold', pad=20)
    ax1.legend(fontsize=12)
    ax1.grid(True, alpha=0.3, linestyle='--')
    ax1.set_xticks(scales)

    # 2. Performance Overhead vs DuckLake by Scale
    print("Creating overhead scaling chart...")
    ax2 = plt.subplot(2, 2, 2)

    x = np.arange(len(scales))
    width = 0.35

    iceberg_overhead = []
    delta_overhead = []

    for scale in scales:
        ducklake_avg = np.mean([all_results[scale][q]['ducklake']['warm']
                                for q in common_queries if 'ducklake' in all_results[scale][q]])

        iceberg_avg = np.mean([all_results[scale][q]['iceberg']['warm']
                               for q in common_queries if 'iceberg' in all_results[scale][q]])
        iceberg_overhead.append(((iceberg_avg - ducklake_avg) / ducklake_avg) * 100)

        delta_avg = np.mean([all_results[scale][q]['delta']['warm']
                            for q in common_queries if 'delta' in all_results[scale][q]])
        delta_overhead.append(((delta_avg - ducklake_avg) / ducklake_avg) * 100)

    bars1 = ax2.bar(x - width/2, iceberg_overhead, width, label='Iceberg',
                    color='#F77F00', alpha=0.8)
    bars2 = ax2.bar(x + width/2, delta_overhead, width, label='Delta',
                    color='#06A77D', alpha=0.8)

    # Add value labels
    for i, (ice, dlt) in enumerate(zip(iceberg_overhead, delta_overhead)):
        ax2.text(i - width/2, ice + 1, f'+{ice:.1f}%', ha='center', va='bottom',
                fontsize=10, fontweight='bold')
        ax2.text(i + width/2, dlt + 1, f'+{dlt:.1f}%', ha='center', va='bottom',
                fontsize=10, fontweight='bold')

    ax2.set_xlabel('Dataset Size (GB)', fontsize=14, fontweight='bold')
    ax2.set_ylabel('Overhead vs DuckLake (%)', fontsize=14, fontweight='bold')
    ax2.set_title('Format Overhead Scaling', fontsize=16, fontweight='bold', pad=20)
    ax2.set_xticks(x)
    ax2.set_xticklabels([f'{s}GB' for s in scales])
    ax2.legend(fontsize=12)
    ax2.grid(axis='y', alpha=0.3, linestyle='--')
    ax2.axhline(y=0, color='black', linestyle='-', linewidth=1)

    # 3. Query Scaling Pattern (Selected Queries)
    print("Creating query scaling pattern chart...")
    ax3 = plt.subplot(2, 2, 3)

    # Select diverse queries (fastest, slowest, medium)
    ducklake_1gb_times = {q: all_results[1][q]['ducklake']['warm']
                          for q in common_queries if 'ducklake' in all_results[1][q]}
    sorted_queries = sorted(ducklake_1gb_times.items(), key=lambda x: x[1])

    selected_queries = [
        sorted_queries[0][0],  # Fastest
        sorted_queries[len(sorted_queries)//4][0],  # 25th percentile
        sorted_queries[len(sorted_queries)//2][0],  # Median
        sorted_queries[3*len(sorted_queries)//4][0],  # 75th percentile
        sorted_queries[-1][0]  # Slowest
    ]

    for qid in selected_queries:
        times = []
        for scale in scales:
            if 'ducklake' in all_results[scale][qid]:
                times.append(all_results[scale][qid]['ducklake']['warm'] * 1000)
            else:
                times.append(0)
        ax3.plot(scales, times, marker='o', linewidth=2, markersize=8, label=qid)

    ax3.set_xlabel('Dataset Size (GB)', fontsize=14, fontweight='bold')
    ax3.set_ylabel('Query Time (ms)', fontsize=14, fontweight='bold')
    ax3.set_title('Query Scaling Patterns (DuckLake)', fontsize=16, fontweight='bold', pad=20)
    ax3.legend(fontsize=11, title='Query ID')
    ax3.grid(True, alpha=0.3, linestyle='--')
    ax3.set_xticks(scales)

    # 4. Summary Statistics Table
    print("Creating summary statistics table...")
    ax4 = plt.subplot(2, 2, 4)
    ax4.axis('off')

    stats_text = "CROSS-SCALE BENCHMARK SUMMARY\n\n"

    for scale in scales:
        stats_text += f"{scale}GB Dataset:\n"
        for fmt in formats:
            times = [all_results[scale][q][fmt]['warm'] * 1000
                    for q in common_queries if fmt in all_results[scale][q]]
            if times:
                avg_time = np.mean(times)
                min_time = np.min(times)
                max_time = np.max(times)
                stats_text += f"  • {format_labels[fmt]:8s}: avg={avg_time:6.2f}ms  "
                stats_text += f"min={min_time:6.2f}ms  max={max_time:6.2f}ms\n"
        stats_text += "\n"

    stats_text += "Scaling Analysis:\n"
    for fmt in formats:
        time_1gb = np.mean([all_results[1][q][fmt]['warm']
                           for q in common_queries if fmt in all_results[1][q]])
        time_5gb = np.mean([all_results[5][q][fmt]['warm']
                           for q in common_queries if fmt in all_results[5][q]])
        time_10gb = np.mean([all_results[10][q][fmt]['warm']
                            for q in common_queries if fmt in all_results[10][q]])

        scaling_5gb = time_5gb / time_1gb
        scaling_10gb = time_10gb / time_1gb

        stats_text += f"  • {format_labels[fmt]:8s}: 5GB={scaling_5gb:.2f}x  10GB={scaling_10gb:.2f}x\n"

    ax4.text(0.1, 0.9, stats_text, transform=ax4.transAxes, fontsize=12,
            verticalalignment='top', fontfamily='monospace',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))

    plt.tight_layout()
    output_file = 'results/multi_scale/cross_scale_analysis.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\n✓ Saved: {output_file}")

    # Create detailed scaling breakdown
    print("\nCreating detailed scaling breakdown...")
    fig2, axes = plt.subplots(3, 1, figsize=(18, 16))

    for idx, fmt in enumerate(formats):
        ax = axes[idx]

        # Plot all queries for this format
        query_labels = []
        for qid in common_queries[:15]:  # Show first 15 queries
            times = []
            for scale in scales:
                if fmt in all_results[scale][qid]:
                    times.append(all_results[scale][qid][fmt]['warm'] * 1000)
                else:
                    times.append(0)

            x_pos = np.arange(len(scales))
            ax.plot(scales, times, marker='o', linewidth=1.5, markersize=6,
                   label=qid, alpha=0.7)

        ax.set_xlabel('Dataset Size (GB)', fontsize=12, fontweight='bold')
        ax.set_ylabel('Query Time (ms)', fontsize=12, fontweight='bold')
        ax.set_title(f'{format_labels[fmt]} Query Scaling (First 15 Queries)',
                    fontsize=14, fontweight='bold', pad=15)
        ax.legend(ncol=5, fontsize=9, loc='upper left')
        ax.grid(True, alpha=0.3, linestyle='--')
        ax.set_xticks(scales)

    plt.tight_layout()
    output_file2 = 'results/multi_scale/detailed_scaling.png'
    plt.savefig(output_file2, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file2}")

    print("\n" + "="*60)
    print("Cross-Scale Analysis Complete!")
    print("="*60)
    print("\nGenerated files:")
    print(f"  • {output_file} - Cross-scale comparison")
    print(f"  • {output_file2} - Detailed query scaling")

    # Print summary to console
    print("\n" + "="*60)
    print("PERFORMANCE SUMMARY")
    print("="*60)

    for scale in scales:
        print(f"\n{scale}GB Dataset:")
        for fmt in formats:
            times = [all_results[scale][q][fmt]['warm'] * 1000
                    for q in common_queries if fmt in all_results[scale][q]]
            if times:
                print(f"  {format_labels[fmt]:8s}: {np.mean(times):6.2f}ms avg")

    print("\n" + "="*60)
    print("SCALING FACTORS (vs 1GB baseline)")
    print("="*60)

    for fmt in formats:
        time_1gb = np.mean([all_results[1][q][fmt]['warm']
                           for q in common_queries if fmt in all_results[1][q]])
        time_5gb = np.mean([all_results[5][q][fmt]['warm']
                           for q in common_queries if fmt in all_results[5][q]])
        time_10gb = np.mean([all_results[10][q][fmt]['warm']
                            for q in common_queries if fmt in all_results[10][q]])

        print(f"\n{format_labels[fmt]}:")
        print(f"  1GB:  {time_1gb*1000:6.2f}ms (1.00x)")
        print(f"  5GB:  {time_5gb*1000:6.2f}ms ({time_5gb/time_1gb:.2f}x)")
        print(f"  10GB: {time_10gb*1000:6.2f}ms ({time_10gb/time_1gb:.2f}x)")

if __name__ == "__main__":
    main()
