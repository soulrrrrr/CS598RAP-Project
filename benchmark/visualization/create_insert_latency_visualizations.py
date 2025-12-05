"""
Create visualizations for insert latency benchmark results.

Generates comprehensive charts comparing DuckLake vs Iceberg REST insert performance.
"""

import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import sys
from datetime import datetime

sys.path.append(str(Path(__file__).parent.parent.parent))
from benchmark.utils.logger import setup_logger


class InsertLatencyVisualizer:
    """Create visualizations for insert latency benchmark."""

    def __init__(self, results_file: str):
        """Initialize visualizer."""
        self.logger = setup_logger("insert_latency_visualizer")
        self.results_file = Path(results_file)

        # Load results
        self.load_results()

        # Set style
        sns.set_style("whitegrid")
        plt.rcParams['figure.dpi'] = 100
        plt.rcParams['savefig.dpi'] = 300
        plt.rcParams['font.size'] = 10

    def load_results(self):
        """Load benchmark results from JSON file."""
        self.logger.info(f"Loading results from {self.results_file}")

        with open(self.results_file, 'r') as f:
            data = json.load(f)

        self.metadata = data['metadata']
        self.results_df = pd.DataFrame([r for r in data['results'] if r['status'] == 'success'])

        self.logger.info(f"Loaded {len(self.results_df)} successful results")

    def create_comprehensive_visualization(self):
        """Create comprehensive 4-panel visualization."""
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

        # Panel A: Insert time comparison by batch size
        self._plot_insert_time_comparison(ax1)

        # Panel B: Throughput comparison (rows/second)
        self._plot_throughput_comparison(ax2)

        # Panel C: Overhead percentage by batch size
        self._plot_overhead_percentage(ax3)

        # Panel D: Summary statistics table
        self._plot_summary_table(ax4)

        plt.tight_layout()

        # Save figure
        output_dir = self.results_file.parent
        output_file = output_dir / "insert_latency_analysis.png"
        plt.savefig(output_file, bbox_inches='tight')
        self.logger.info(f"Saved comprehensive visualization to {output_file}")

        plt.close()

    def _plot_insert_time_comparison(self, ax):
        """Plot insert time comparison."""
        # Pivot data for plotting
        pivot_df = self.results_df.pivot_table(
            index='batch_size',
            columns='format',
            values='insert_time_seconds',
            aggfunc='mean'
        )

        x = range(len(pivot_df.index))
        width = 0.35

        ax.bar([i - width/2 for i in x], pivot_df['ducklake'], width,
               label='DuckLake', color='#2ecc71', alpha=0.8)
        ax.bar([i + width/2 for i in x], pivot_df['iceberg_rest'], width,
               label='Iceberg REST', color='#e74c3c', alpha=0.8)

        ax.set_xlabel('Batch Size (rows)', fontweight='bold')
        ax.set_ylabel('Insert Time (seconds)', fontweight='bold')
        ax.set_title('A. Insert Time Comparison', fontweight='bold', fontsize=12)
        ax.set_xticks(x)
        ax.set_xticklabels([f'{int(bs):,}' for bs in pivot_df.index])
        ax.legend()
        ax.grid(axis='y', alpha=0.3)

    def _plot_throughput_comparison(self, ax):
        """Plot throughput comparison."""
        # Pivot data for plotting
        pivot_df = self.results_df.pivot_table(
            index='batch_size',
            columns='format',
            values='rows_per_second',
            aggfunc='mean'
        )

        x = range(len(pivot_df.index))
        width = 0.35

        ax.bar([i - width/2 for i in x], pivot_df['ducklake'], width,
               label='DuckLake', color='#2ecc71', alpha=0.8)
        ax.bar([i + width/2 for i in x], pivot_df['iceberg_rest'], width,
               label='Iceberg REST', color='#e74c3c', alpha=0.8)

        ax.set_xlabel('Batch Size (rows)', fontweight='bold')
        ax.set_ylabel('Throughput (rows/second)', fontweight='bold')
        ax.set_title('B. Throughput Comparison', fontweight='bold', fontsize=12)
        ax.set_xticks(x)
        ax.set_xticklabels([f'{int(bs):,}' for bs in pivot_df.index])
        ax.legend()
        ax.grid(axis='y', alpha=0.3)

        # Format y-axis with thousands separator
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))

    def _plot_overhead_percentage(self, ax):
        """Plot overhead percentage."""
        # Calculate overhead for each batch size
        overhead_data = []

        for batch_size in self.results_df['batch_size'].unique():
            batch_df = self.results_df[self.results_df['batch_size'] == batch_size]

            ducklake = batch_df[batch_df['format'] == 'ducklake']
            iceberg = batch_df[batch_df['format'] == 'iceberg_rest']

            if not ducklake.empty and not iceberg.empty:
                ducklake_time = ducklake['insert_time_seconds'].mean()
                iceberg_time = iceberg['insert_time_seconds'].mean()
                overhead = (iceberg_time - ducklake_time) / ducklake_time * 100

                overhead_data.append({
                    'batch_size': batch_size,
                    'overhead_pct': overhead
                })

        overhead_df = pd.DataFrame(overhead_data)

        # Plot
        ax.plot(range(len(overhead_df)), overhead_df['overhead_pct'],
                marker='o', linewidth=2, markersize=8, color='#e74c3c')

        ax.set_xlabel('Batch Size (rows)', fontweight='bold')
        ax.set_ylabel('Overhead (%)', fontweight='bold')
        ax.set_title('C. Iceberg REST Overhead vs DuckLake', fontweight='bold', fontsize=12)
        ax.set_xticks(range(len(overhead_df)))
        ax.set_xticklabels([f'{int(bs):,}' for bs in overhead_df['batch_size']])
        ax.grid(alpha=0.3)

        # Add horizontal line at 0%
        ax.axhline(y=0, color='black', linestyle='--', alpha=0.3)

    def _plot_summary_table(self, ax):
        """Plot summary statistics table."""
        ax.axis('off')

        # Calculate summary stats
        summary_data = []

        for batch_size in sorted(self.results_df['batch_size'].unique()):
            batch_df = self.results_df[self.results_df['batch_size'] == batch_size]

            ducklake = batch_df[batch_df['format'] == 'ducklake']
            iceberg = batch_df[batch_df['format'] == 'iceberg_rest']

            if not ducklake.empty and not iceberg.empty:
                ducklake_time = ducklake['insert_time_seconds'].mean()
                iceberg_time = iceberg['insert_time_seconds'].mean()
                overhead = (iceberg_time - ducklake_time) / ducklake_time * 100

                summary_data.append([
                    f'{int(batch_size):,}',
                    f'{ducklake_time:.3f}s',
                    f'{iceberg_time:.3f}s',
                    f'+{overhead:.1f}%'
                ])

        # Create table
        table = ax.table(
            cellText=summary_data,
            colLabels=['Batch Size', 'DuckLake', 'Iceberg REST', 'Overhead'],
            loc='center',
            cellLoc='center'
        )

        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 2)

        # Style header
        for i in range(4):
            table[(0, i)].set_facecolor('#3498db')
            table[(0, i)].set_text_props(weight='bold', color='white')

        # Alternate row colors
        for i in range(1, len(summary_data) + 1):
            for j in range(4):
                if i % 2 == 0:
                    table[(i, j)].set_facecolor('#ecf0f1')

        ax.set_title('D. Summary Statistics', fontweight='bold', fontsize=12, y=0.95)

    def create_batch_size_scaling_plot(self):
        """Create plot showing how performance scales with batch size."""
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Plot 1: Insert time scaling
        for format_type in ['ducklake', 'iceberg_rest']:
            format_df = self.results_df[self.results_df['format'] == format_type]

            grouped = format_df.groupby('batch_size')['insert_time_seconds'].mean()

            color = '#2ecc71' if format_type == 'ducklake' else '#e74c3c'
            label = 'DuckLake' if format_type == 'ducklake' else 'Iceberg REST'

            ax1.plot(grouped.index, grouped.values,
                    marker='o', linewidth=2, markersize=8,
                    color=color, label=label)

        ax1.set_xlabel('Batch Size (rows)', fontweight='bold')
        ax1.set_ylabel('Insert Time (seconds)', fontweight='bold')
        ax1.set_title('Insert Time Scaling', fontweight='bold', fontsize=12)
        ax1.set_xscale('log')
        ax1.set_yscale('log')
        ax1.legend()
        ax1.grid(alpha=0.3)

        # Plot 2: Throughput scaling
        for format_type in ['ducklake', 'iceberg_rest']:
            format_df = self.results_df[self.results_df['format'] == format_type]

            grouped = format_df.groupby('batch_size')['rows_per_second'].mean()

            color = '#2ecc71' if format_type == 'ducklake' else '#e74c3c'
            label = 'DuckLake' if format_type == 'ducklake' else 'Iceberg REST'

            ax2.plot(grouped.index, grouped.values,
                    marker='o', linewidth=2, markersize=8,
                    color=color, label=label)

        ax2.set_xlabel('Batch Size (rows)', fontweight='bold')
        ax2.set_ylabel('Throughput (rows/second)', fontweight='bold')
        ax2.set_title('Throughput Scaling', fontweight='bold', fontsize=12)
        ax2.set_xscale('log')
        ax2.legend()
        ax2.grid(alpha=0.3)
        ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))

        plt.tight_layout()

        # Save figure
        output_dir = self.results_file.parent
        output_file = output_dir / "insert_latency_scaling.png"
        plt.savefig(output_file, bbox_inches='tight')
        self.logger.info(f"Saved scaling plot to {output_file}")

        plt.close()

    def create_all_visualizations(self):
        """Create all visualizations."""
        self.logger.info("Creating all visualizations...")

        self.create_comprehensive_visualization()
        self.create_batch_size_scaling_plot()

        self.logger.info("All visualizations created successfully")


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description='Create insert latency visualizations')
    parser.add_argument('results_file', nargs='?',
                       help='Path to results JSON file')
    args = parser.parse_args()

    # Find most recent results file if not specified
    if args.results_file:
        results_file = args.results_file
    else:
        results_dir = Path('results')
        json_files = list(results_dir.glob('insert_latency_results_*.json'))

        if not json_files:
            print("No insert latency results files found in results/")
            return

        results_file = max(json_files, key=lambda p: p.stat().st_mtime)
        print(f"Using most recent results file: {results_file}")

    # Create visualizations
    visualizer = InsertLatencyVisualizer(results_file)
    visualizer.create_all_visualizations()


if __name__ == '__main__':
    main()
