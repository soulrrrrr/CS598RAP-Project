#!/usr/bin/env python3
"""
Create clean query execution time visualizations.

Focuses only on query execution times across formats without extra analysis.
"""

import json
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import sys


def load_results(results_file):
    """Load benchmark results from JSON file."""
    with open(results_file) as f:
        return json.load(f)


def organize_data(data):
    """Organize results by query and format."""
    queries = {}

    for result in data['query_results']:
        if result['status'] != 'success':
            continue

        query_id = result['query_id']
        format_name = result['format']

        if query_id not in queries:
            queries[query_id] = {}

        queries[query_id][format_name] = {
            'median_time': result['median_time_seconds'],
            'rows': result['rows_returned']
        }

    return queries


def create_query_execution_chart(queries, output_file):
    """
    Create a clean query execution time comparison chart.

    Shows median query execution time for each format across all queries.
    """
    # Sort queries by number
    query_ids = sorted(queries.keys(), key=lambda x: int(x[1:]))

    # Format configuration
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

    # Create figure
    fig, ax = plt.subplots(figsize=(16, 8))

    # Prepare data
    x = np.arange(len(query_ids))
    width = 0.25

    # Plot bars for each format
    for i, fmt in enumerate(formats):
        times = []

        for qid in query_ids:
            if fmt in queries[qid]:
                # Convert to milliseconds
                times.append(queries[qid][fmt]['median_time'] * 1000)
            else:
                times.append(0)

        offset = (i - 1) * width
        ax.bar(x + offset, times, width,
               label=format_labels[fmt],
               color=format_colors[fmt],
               alpha=0.8)

    # Formatting
    ax.set_ylabel('Query Execution Time (ms)', fontsize=14, fontweight='bold')
    ax.set_xlabel('Query ID', fontsize=14, fontweight='bold')
    ax.set_title('Query Execution Time by Format (Median of 3 Warm Runs)',
                 fontsize=16, fontweight='bold', pad=20)
    ax.set_xticks(x)
    ax.set_xticklabels(query_ids, rotation=45, ha='right', fontsize=11)
    ax.legend(loc='upper left', fontsize=12)
    ax.grid(axis='y', alpha=0.3, linestyle='--')

    # Save
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()


def create_summary_table_chart(queries, output_file):
    """
    Create a summary table showing format performance.

    Simple table with average execution time and format overhead.
    """
    formats = ['ducklake', 'iceberg', 'delta']
    format_labels = {
        'ducklake': 'DuckLake',
        'iceberg': 'Iceberg',
        'delta': 'Delta'
    }

    # Calculate statistics
    stats = {}
    for fmt in formats:
        times = []
        for qid, data in queries.items():
            if fmt in data:
                times.append(data[fmt]['median_time'])

        if times:
            stats[fmt] = {
                'avg_time': np.mean(times),
                'count': len(times)
            }

    # Calculate overhead vs DuckLake (baseline)
    ducklake_avg = stats['ducklake']['avg_time']
    for fmt in formats:
        if fmt != 'ducklake':
            overhead = ((stats[fmt]['avg_time'] - ducklake_avg) / ducklake_avg) * 100
            stats[fmt]['overhead'] = overhead

    # Create figure with table
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.axis('tight')
    ax.axis('off')

    # Prepare table data
    table_data = []
    headers = ['Format', 'Avg Time (ms)', 'Overhead vs DuckLake', 'Queries']

    for fmt in formats:
        if fmt not in stats:
            continue

        row = [
            format_labels[fmt],
            f"{stats[fmt]['avg_time'] * 1000:.2f}",
            f"+{stats[fmt]['overhead']:.1f}%" if fmt != 'ducklake' else "Baseline",
            str(stats[fmt]['count'])
        ]
        table_data.append(row)

    # Create table
    table = ax.table(cellText=table_data,
                     colLabels=headers,
                     cellLoc='center',
                     loc='center',
                     colWidths=[0.2, 0.25, 0.3, 0.15])

    table.auto_set_font_size(False)
    table.set_fontsize(12)
    table.scale(1, 2.5)

    # Style header
    for i in range(len(headers)):
        table[(0, i)].set_facecolor('#2E86AB')
        table[(0, i)].set_text_props(weight='bold', color='white')

    # Style rows
    colors = ['#E8F4F8', '#FFFFFF']
    for i, row in enumerate(table_data, start=1):
        for j in range(len(headers)):
            table[(i, j)].set_facecolor(colors[i % 2])

    plt.title('Query Execution Performance Summary',
              fontsize=16, fontweight='bold', pad=20)

    # Save
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()


def main():
    """Main execution."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Create query execution visualizations"
    )
    parser.add_argument(
        '--run',
        type=str,
        help='Specific run name to visualize (default: latest results)'
    )

    args = parser.parse_args()

    # Determine results location
    if args.run:
        results_dir = Path(f"results/{args.run}")
        if not results_dir.exists():
            print(f"❌ Error: Run not found: {args.run}")
            print("\n💡 Available runs:")
            results_base = Path("results")
            exclude_dirs = {'multi_scale', 'runs'}
            if results_base.exists():
                for d in sorted(results_base.iterdir()):
                    if d.is_dir() and d.name not in exclude_dirs:
                        print(f"   • {d.name}")
            sys.exit(1)
        results_file = results_dir / "comparison_results.json"
        output_dir = results_dir
    else:
        results_file = Path("results/comparison_results.json")
        output_dir = Path("results")

    if not results_file.exists():
        print(f"❌ Error: Results file not found: {results_file}")
        print("   Run benchmark first: python3 run_benchmark.py --scale 5 --name myrun")
        sys.exit(1)

    print("=" * 70)
    print("Creating Query Execution Visualizations")
    print("=" * 70)
    print()

    # Load and organize data
    print("📊 Loading results...")
    data = load_results(results_file)
    queries = organize_data(data)
    print(f"   Loaded {len(queries)} queries")

    # Create visualizations
    print("\n📈 Creating charts...")

    create_query_execution_chart(
        queries,
        output_dir / "query_execution_times.png"
    )

    create_summary_table_chart(
        queries,
        output_dir / "execution_summary.png"
    )

    print("\n" + "=" * 70)
    print("✅ Visualizations Complete!")
    print("=" * 70)
    print("\nOutput files:")
    print(f"  • {output_dir}/query_execution_times.png - Query time comparison")
    print(f"  • {output_dir}/execution_summary.png - Performance summary table")
    print("=" * 70)


if __name__ == "__main__":
    main()
