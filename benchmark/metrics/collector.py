"""Metrics collection and storage for benchmark results."""

import json
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))


class MetricsCollector:
    """Collect and store benchmark metrics."""

    def __init__(self, output_dir: str):
        """
        Initialize metrics collector.

        Args:
            output_dir: Directory to store metrics
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.metrics = {
            'timestamp': datetime.now().isoformat(),
            'benchmark_version': '1.0.0',
            'data_generation': {},
            'format_conversions': {},
            'query_results': [],
            'metadata': {}
        }

    def add_data_generation_metrics(self, metrics: Dict[str, Any]):
        """Add data generation metrics."""
        self.metrics['data_generation'] = metrics

    def add_format_conversion_metrics(self, format_name: str, metrics: Dict[str, Any]):
        """Add format conversion metrics."""
        self.metrics['format_conversions'][format_name] = metrics

    def add_query_results(self, results: List[Dict[str, Any]]):
        """Add query execution results."""
        self.metrics['query_results'].extend(results)

    def add_metadata(self, key: str, value: Any):
        """Add metadata."""
        self.metrics['metadata'][key] = value

    def save_json(self, filename: str = None) -> str:
        """
        Save metrics to JSON file.

        Args:
            filename: Optional filename (default: metrics_TIMESTAMP.json)

        Returns:
            Path to saved file
        """
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"metrics_{timestamp}.json"

        filepath = self.output_dir / filename

        with open(filepath, 'w') as f:
            json.dump(self.metrics, f, indent=2, default=str)

        return str(filepath)

    def save_query_results_csv(self, filename: str = "query_results.csv") -> str:
        """
        Save query results to CSV file.

        Args:
            filename: CSV filename

        Returns:
            Path to saved file
        """
        filepath = self.output_dir / filename

        if not self.metrics['query_results']:
            return str(filepath)

        # Flatten nested dictionaries
        flat_results = []
        for result in self.metrics['query_results']:
            flat_result = self._flatten_dict(result)
            flat_results.append(flat_result)

        # Write to CSV
        if flat_results:
            # Collect all unique keys from all results (including error fields)
            all_keys = set()
            for result in flat_results:
                all_keys.update(result.keys())
            fieldnames = sorted(all_keys)  # Sort for consistent column order

            with open(filepath, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(flat_results)

        return str(filepath)

    def _flatten_dict(self, d: Dict, parent_key: str = '', sep: str = '_') -> Dict:
        """Flatten nested dictionary."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k

            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list) and v and isinstance(v[0], (int, float)):
                # For lists of numbers, calculate statistics
                items.append((f"{new_key}_mean", sum(v) / len(v)))
                items.append((f"{new_key}_min", min(v)))
                items.append((f"{new_key}_max", max(v)))
            elif isinstance(v, list):
                # For other lists, convert to JSON string
                items.append((new_key, json.dumps(v)))
            else:
                items.append((new_key, v))

        return dict(items)

    def get_summary_statistics(self) -> Dict[str, Any]:
        """Calculate summary statistics from collected metrics."""
        summary = {
            'total_queries': len(self.metrics['query_results']),
            'successful_queries': 0,
            'failed_queries': 0,
            'by_format': {}
        }

        # Count successes and failures
        for result in self.metrics['query_results']:
            if result.get('status') == 'success':
                summary['successful_queries'] += 1
            else:
                summary['failed_queries'] += 1

            # Aggregate by format
            fmt = result.get('format', 'unknown')
            if fmt not in summary['by_format']:
                summary['by_format'][fmt] = {
                    'total': 0,
                    'successful': 0,
                    'failed': 0,
                    'total_time': 0,
                    'queries': []
                }

            summary['by_format'][fmt]['total'] += 1

            if result.get('status') == 'success':
                summary['by_format'][fmt]['successful'] += 1
                median_time = result.get('median_time_seconds', 0)
                summary['by_format'][fmt]['total_time'] += median_time
                summary['by_format'][fmt]['queries'].append({
                    'query_id': result.get('query_id'),
                    'time': median_time
                })
            else:
                summary['by_format'][fmt]['failed'] += 1

        # Calculate averages
        for fmt, stats in summary['by_format'].items():
            if stats['successful'] > 0:
                stats['avg_time'] = stats['total_time'] / stats['successful']

        return summary

    def get_format_comparison(self) -> Dict[str, Any]:
        """Get side-by-side comparison of formats for each query."""
        comparison = {}

        for result in self.metrics['query_results']:
            query_id = result.get('query_id')
            fmt = result.get('format')

            if query_id not in comparison:
                comparison[query_id] = {}

            if result.get('status') == 'success':
                comparison[query_id][fmt] = {
                    'time': result.get('median_time_seconds', 0),
                    'rows': result.get('rows_returned', 0)
                }
            else:
                comparison[query_id][fmt] = {
                    'error': result.get('error', 'Unknown error')
                }

        # Calculate speedups (using ducklake as baseline)
        for query_id, formats in comparison.items():
            times = {f: formats[f]['time'] for f in formats if 'time' in formats.get(f, {})}
            if times:
                # Use ducklake as baseline, fallback to fastest
                baseline = times.get('ducklake', min(times.values()))
                for fmt in times:
                    if baseline > 0:
                        formats[fmt]['speedup'] = baseline / times[fmt]

        return comparison

    def print_summary(self):
        """Print a summary of collected metrics."""
        print("\n" + "=" * 70)
        print("BENCHMARK SUMMARY")
        print("=" * 70)

        summary = self.get_summary_statistics()

        print(f"\nTotal Queries: {summary['total_queries']}")
        print(f"  Successful: {summary['successful_queries']}")
        print(f"  Failed: {summary['failed_queries']}")

        print("\nBy Format:")
        for fmt, stats in summary['by_format'].items():
            print(f"\n  {fmt.upper()}:")
            print(f"    Total: {stats['total']}")
            print(f"    Successful: {stats['successful']}")
            print(f"    Failed: {stats['failed']}")
            if stats.get('avg_time'):
                print(f"    Average Time: {stats['avg_time']:.3f}s")

        # Print comparison table
        comparison = self.get_format_comparison()
        if comparison:
            print("\n" + "-" * 70)
            print("QUERY COMPARISON")
            print("-" * 70)
            print(f"{'Query':<10} {'DuckLake':<12} {'Iceberg':<12} {'Delta':<12}")
            print("-" * 70)

            for query_id in sorted(comparison.keys(), key=lambda x: str(x)):
                formats = comparison[query_id]

                ducklake_time = formats.get('ducklake', {}).get('time', 'N/A')
                iceberg_time = formats.get('iceberg', {}).get('time', 'N/A')
                delta_time = formats.get('delta', {}).get('time', 'N/A')

                ducklake_str = f"{ducklake_time:.3f}s" if isinstance(ducklake_time, (int, float)) else ducklake_time
                iceberg_str = f"{iceberg_time:.3f}s" if isinstance(iceberg_time, (int, float)) else iceberg_time
                delta_str = f"{delta_time:.3f}s" if isinstance(delta_time, (int, float)) else delta_time

                print(f"{query_id:<10} {ducklake_str:<12} {iceberg_str:<12} {delta_str:<12}")

        print("=" * 70)
