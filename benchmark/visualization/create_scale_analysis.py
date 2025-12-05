#!/usr/bin/env python3
"""
Create cross-scale analysis for arbitrary dataset sizes.

Automatically detects available scale results in results/multi_scale/
and generates comparison visualizations.
"""

import json
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pathlib import Path
import sys


def discover_available_scales():
    """
    Discover available scale results automatically.

    Returns:
        list: List of (scale_gb, results_file) tuples
    """
    multi_scale_dir = Path("results/multi_scale")

    if not multi_scale_dir.exists():
        return []

    scales = []

    for scale_dir in multi_scale_dir.iterdir():
        if not scale_dir.is_dir():
            continue

        # Extract scale from directory name (e.g., "1gb", "5gb", "10gb")
        dir_name = scale_dir.name.lower()
        if dir_name.endswith('gb'):
            try:
                scale = int(dir_name[:-2])
                results_file = scale_dir / "comparison_results.json"

                if results_file.exists():
                    scales.append((scale, results_file))
            except ValueError:
                continue

    # Sort by scale
    scales.sort(key=lambda x: x[0])
    return scales


def load_scale_results(results_file):
    """Load results for a specific scale."""
    with open(results_file) as f:
        data = json.load(f)

    # Extract timing data
    records = []
    for result in data['query_results']:
        if result['status'] == 'success':
            records.append({
                'query': result['query_id'],
                'format': result['format'],
                'time': result['median_time_seconds'],
                'rows': result.get('rows_returned', 0)
            })

    return pd.DataFrame(records)


def create_scale_comparison_chart(scale_data, output_file):
    """
    Create chart comparing performance across different scales.

    Args:
        scale_data: dict mapping scale -> DataFrame
        output_file: Output file path
    """
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

    # Calculate average time for each scale and format
    scales = sorted(scale_data.keys())
    scale_labels = [f"{s}GB" for s in scales]

    fig, ax = plt.subplots(figsize=(12, 7))

    x = np.arange(len(scales))
    width = 0.25

    for i, fmt in enumerate(formats):
        avg_times = []

        for scale in scales:
            df = scale_data[scale]
            format_data = df[df['format'] == fmt]

            if len(format_data) > 0:
                avg_time = format_data['time'].mean() * 1000  # Convert to ms
                avg_times.append(avg_time)
            else:
                avg_times.append(0)

        offset = (i - 1) * width
        ax.bar(x + offset, avg_times, width,
               label=format_labels[fmt],
               color=format_colors[fmt],
               alpha=0.8)

        # Add value labels on bars
        for j, v in enumerate(avg_times):
            if v > 0:
                ax.text(j + offset, v, f'{v:.1f}',
                       ha='center', va='bottom', fontsize=9)

    ax.set_ylabel('Average Query Time (ms)', fontsize=14, fontweight='bold')
    ax.set_xlabel('Dataset Size', fontsize=14, fontweight='bold')
    ax.set_title('Query Performance Across Dataset Scales',
                 fontsize=16, fontweight='bold', pad=20)
    ax.set_xticks(x)
    ax.set_xticklabels(scale_labels, fontsize=12)
    ax.legend(fontsize=12)
    ax.grid(axis='y', alpha=0.3, linestyle='--')

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()


def create_scaling_efficiency_chart(scale_data, output_file):
    """
    Create chart showing scaling efficiency (how performance changes with scale).

    Args:
        scale_data: dict mapping scale -> DataFrame
        output_file: Output file path
    """
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

    scales = sorted(scale_data.keys())
    scale_labels = [f"{s}GB" for s in scales]

    fig, ax = plt.subplots(figsize=(12, 7))

    for fmt in formats:
        avg_times = []

        for scale in scales:
            df = scale_data[scale]
            format_data = df[df['format'] == fmt]

            if len(format_data) > 0:
                avg_time = format_data['time'].mean() * 1000
                avg_times.append(avg_time)
            else:
                avg_times.append(None)

        # Plot line
        ax.plot(scales, avg_times, marker='o', linewidth=2.5,
                markersize=10, label=format_labels[fmt],
                color=format_colors[fmt])

        # Add value labels
        for scale, time in zip(scales, avg_times):
            if time is not None:
                ax.text(scale, time, f'{time:.1f}ms',
                       ha='center', va='bottom', fontsize=9)

    ax.set_ylabel('Average Query Time (ms)', fontsize=14, fontweight='bold')
    ax.set_xlabel('Dataset Size (GB)', fontsize=14, fontweight='bold')
    ax.set_title('Scaling Efficiency: Query Time vs Dataset Size',
                 fontsize=16, fontweight='bold', pad=20)
    ax.legend(fontsize=12)
    ax.grid(alpha=0.3, linestyle='--')

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()


def create_summary_table(scale_data, output_file):
    """
    Create summary table with performance statistics.

    Args:
        scale_data: dict mapping scale -> DataFrame
        output_file: Output file path
    """
    formats = ['ducklake', 'iceberg', 'delta']
    format_labels = {
        'ducklake': 'DuckLake',
        'iceberg': 'Iceberg',
        'delta': 'Delta'
    }

    scales = sorted(scale_data.keys())

    # Create table data
    fig, ax = plt.subplots(figsize=(14, len(formats) + 2))
    ax.axis('tight')
    ax.axis('off')

    # Headers
    headers = ['Format'] + [f'{s}GB' for s in scales]

    table_data = []
    for fmt in formats:
        row = [format_labels[fmt]]

        for scale in scales:
            df = scale_data[scale]
            format_data = df[df['format'] == fmt]

            if len(format_data) > 0:
                avg_time = format_data['time'].mean() * 1000
                row.append(f'{avg_time:.2f} ms')
            else:
                row.append('N/A')

        table_data.append(row)

    # Create table
    table = ax.table(cellText=table_data,
                     colLabels=headers,
                     cellLoc='center',
                     loc='center')

    table.auto_set_font_size(False)
    table.set_fontsize(12)
    table.scale(1, 2.5)

    # Style header
    for i in range(len(headers)):
        table[(0, i)].set_facecolor('#2E86AB')
        table[(0, i)].set_text_props(weight='bold', color='white')

    # Style rows
    colors = ['#E8F4F8', '#FFFFFF']
    for i in range(len(table_data)):
        for j in range(len(headers)):
            table[(i+1, j)].set_facecolor(colors[i % 2])

    plt.title('Average Query Time by Format and Scale',
              fontsize=16, fontweight='bold', pad=20)

    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_file}")
    plt.close()


def main():
    """Main execution."""
    print("=" * 70)
    print("Creating Cross-Scale Analysis")
    print("=" * 70)
    print()

    # Discover available scales
    print("🔍 Discovering available scale results...")
    available_scales = discover_available_scales()

    if not available_scales:
        print("❌ No scale results found in results/multi_scale/")
        print("\n💡 Run multi-scale benchmark first:")
        print("   ./run_multi_scale_benchmark.sh")
        print("\n   Or run individual benchmarks:")
        print("   python3 run_benchmark.py --scale 1 --save-to-multi-scale")
        print("   python3 run_benchmark.py --scale 5 --save-to-multi-scale")
        sys.exit(1)

    print(f"   Found {len(available_scales)} scales:")
    for scale, path in available_scales:
        print(f"   • {scale}GB - {path}")

    # Load data for each scale
    print("\n📊 Loading scale data...")
    scale_data = {}
    for scale, results_file in available_scales:
        df = load_scale_results(results_file)
        scale_data[scale] = df
        print(f"   • {scale}GB: {len(df)} query results")

    # Create visualizations
    print("\n📈 Creating charts...")

    output_dir = Path("results")
    output_dir.mkdir(exist_ok=True)

    if len(available_scales) >= 2:
        # Only create comparison charts if we have 2+ scales
        create_scale_comparison_chart(
            scale_data,
            output_dir / "scale_comparison.png"
        )

        create_scaling_efficiency_chart(
            scale_data,
            output_dir / "scaling_efficiency.png"
        )

    create_summary_table(
        scale_data,
        output_dir / "scale_summary_table.png"
    )

    print("\n" + "=" * 70)
    print("✅ Cross-Scale Analysis Complete!")
    print("=" * 70)
    print("\nOutput files:")
    if len(available_scales) >= 2:
        print("  • results/scale_comparison.png - Bar chart comparison")
        print("  • results/scaling_efficiency.png - Line chart showing scaling")
    print("  • results/scale_summary_table.png - Summary table")
    print("=" * 70)


if __name__ == "__main__":
    main()
