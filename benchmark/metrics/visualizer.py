"""Visualization module for benchmark results."""

import json
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import Dict, Any, List
import pandas as pd


class BenchmarkVisualizer:
    """Create visualizations from benchmark results."""

    def __init__(self, metrics_file: str, output_dir: str = None):
        """
        Initialize visualizer.

        Args:
            metrics_file: Path to metrics JSON file
            output_dir: Directory to save visualizations (default: same as metrics)
        """
        self.metrics_file = Path(metrics_file)
        self.output_dir = Path(output_dir) if output_dir else self.metrics_file.parent

        # Load metrics
        with open(self.metrics_file, 'r') as f:
            self.metrics = json.load(f)

        # Set style
        sns.set_theme(style="whitegrid")
        self.colors = sns.color_palette("husl", 3)

    def create_all_visualizations(self):
        """Create all available visualizations."""
        print("Creating visualizations...")

        self.plot_query_comparison()
        self.plot_format_summary()
        self.plot_query_details()

        print(f"✓ Visualizations saved to {self.output_dir}")

    def plot_query_comparison(self):
        """Create bar chart comparing query times across formats."""
        query_results = self.metrics.get('query_results', [])

        if not query_results:
            print("No query results to visualize")
            return

        # Prepare data
        data = []
        for result in query_results:
            if result.get('status') == 'success':
                data.append({
                    'Query': result['query_id'],
                    'Format': result['format'],
                    'Time (s)': result.get('median_time_seconds', 0)
                })

        df = pd.DataFrame(data)

        if df.empty:
            print("No successful queries to visualize")
            return

        # Create plot
        plt.figure(figsize=(12, 6))

        # Pivot data for grouped bar chart
        pivot_df = df.pivot(index='Query', columns='Format', values='Time (s)')

        ax = pivot_df.plot(kind='bar', width=0.8, figsize=(14, 6))
        ax.set_xlabel('Query', fontsize=12, fontweight='bold')
        ax.set_ylabel('Execution Time (seconds)', fontsize=12, fontweight='bold')
        ax.set_title('Query Execution Time by Format', fontsize=14, fontweight='bold')
        ax.legend(title='Format', fontsize=10)
        ax.grid(axis='y', alpha=0.3)

        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()

        output_path = self.output_dir / 'query_comparison.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()

        print(f"  ✓ Query comparison: {output_path}")

    def plot_format_summary(self):
        """Create summary chart showing average times per format."""
        query_results = self.metrics.get('query_results', [])

        # Calculate averages per format
        format_stats = {}
        for result in query_results:
            if result.get('status') == 'success':
                fmt = result['format']
                time = result.get('median_time_seconds', 0)

                if fmt not in format_stats:
                    format_stats[fmt] = []

                format_stats[fmt].append(time)

        if not format_stats:
            print("No successful queries for format summary")
            return

        # Calculate statistics
        formats = []
        avg_times = []
        min_times = []
        max_times = []

        for fmt, times in format_stats.items():
            formats.append(fmt.capitalize())
            avg_times.append(sum(times) / len(times))
            min_times.append(min(times))
            max_times.append(max(times))

        # Create plot
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

        # Average time bar chart
        bars = ax1.bar(formats, avg_times, color=self.colors[:len(formats)])
        ax1.set_ylabel('Average Time (seconds)', fontsize=12, fontweight='bold')
        ax1.set_title('Average Query Execution Time', fontsize=14, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)

        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.3f}s',
                    ha='center', va='bottom', fontsize=10)

        # Query count bar chart
        query_counts = [len(times) for times in format_stats.values()]
        bars2 = ax2.bar(formats, query_counts, color=self.colors[:len(formats)])
        ax2.set_ylabel('Number of Queries', fontsize=12, fontweight='bold')
        ax2.set_title('Successful Queries per Format', fontsize=14, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        # Add value labels
        for bar in bars2:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(height)}',
                    ha='center', va='bottom', fontsize=10)

        plt.tight_layout()

        output_path = self.output_dir / 'format_summary.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()

        print(f"  ✓ Format summary: {output_path}")

    def plot_query_details(self):
        """Create detailed plot showing cold vs warm times."""
        query_results = self.metrics.get('query_results', [])

        data = []
        for result in query_results:
            if result.get('status') == 'success':
                query_id = result['query_id']
                fmt = result['format']
                cold_time = result.get('cold_run_time_seconds', 0)
                warm_time = result.get('median_time_seconds', 0)

                data.append({
                    'Query': query_id,
                    'Format': fmt,
                    'Cold': cold_time,
                    'Warm': warm_time
                })

        if not data:
            return

        df = pd.DataFrame(data)

        # Create grouped bar chart
        fig, ax = plt.subplots(figsize=(14, 6))

        # Group by query and format
        queries = df['Query'].unique()
        x = range(len(queries))
        width = 0.15

        formats = df['Format'].unique()
        for i, fmt in enumerate(formats):
            fmt_data = df[df['Format'] == fmt]
            cold_times = [fmt_data[fmt_data['Query'] == q]['Cold'].values[0]
                         if len(fmt_data[fmt_data['Query'] == q]) > 0 else 0
                         for q in queries]
            warm_times = [fmt_data[fmt_data['Query'] == q]['Warm'].values[0]
                         if len(fmt_data[fmt_data['Query'] == q]) > 0 else 0
                         for q in queries]

            offset = (i - len(formats)/2 + 0.5) * width
            ax.bar([xi + offset for xi in x], cold_times,
                  width, label=f'{fmt} (cold)', alpha=0.5)
            ax.bar([xi + offset for xi in x], warm_times,
                  width, label=f'{fmt} (warm)')

        ax.set_xlabel('Query', fontsize=12, fontweight='bold')
        ax.set_ylabel('Time (seconds)', fontsize=12, fontweight='bold')
        ax.set_title('Cold vs Warm Run Times', fontsize=14, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(queries, rotation=45, ha='right')
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        ax.grid(axis='y', alpha=0.3)

        plt.tight_layout()

        output_path = self.output_dir / 'query_details.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()

        print(f"  ✓ Query details: {output_path}")


def main():
    """Main entry point for visualization."""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python visualizer.py <metrics_file.json>")
        sys.exit(1)

    metrics_file = sys.argv[1]
    visualizer = BenchmarkVisualizer(metrics_file)
    visualizer.create_all_visualizations()


if __name__ == "__main__":
    main()
