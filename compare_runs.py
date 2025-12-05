#!/usr/bin/env python3
"""
Compare results from two benchmark runs.

Usage:
    python3 compare_runs.py baseline optimized
    python3 compare_runs.py 20250104_120000 20250104_150000
"""

import argparse
import json
import sys
from pathlib import Path
import pandas as pd


def load_run_results(run_name):
    """Load results for a specific run."""
    run_dir = Path(f"results/{run_name}")

    if not run_dir.exists():
        print(f"❌ Run not found: {run_name}")
        print(f"\n💡 Available runs:")
        results_dir = Path("results")
        exclude_dirs = {'multi_scale', 'runs'}
        if results_dir.exists():
            for d in sorted(results_dir.iterdir()):
                if d.is_dir() and d.name not in exclude_dirs:
                    print(f"   • {d.name}")
        sys.exit(1)

    results_file = run_dir / "comparison_results.json"
    if not results_file.exists():
        print(f"❌ No results found for run: {run_name}")
        sys.exit(1)

    with open(results_file) as f:
        data = json.load(f)

    # Load metadata
    metadata_file = run_dir / "run_metadata.json"
    metadata = {}
    if metadata_file.exists():
        with open(metadata_file) as f:
            metadata = json.load(f)

    return data, metadata


def extract_query_times(data):
    """Extract query times by format."""
    times = {}

    for result in data['query_results']:
        if result['status'] != 'success':
            continue

        query_id = result['query_id']
        format_name = result['format']
        time = result['median_time_seconds']

        if query_id not in times:
            times[query_id] = {}

        times[query_id][format_name] = time

    return times


def compare_runs(run1_name, run2_name):
    """Compare two benchmark runs."""
    print("=" * 80)
    print("Benchmark Run Comparison")
    print("=" * 80)
    print()

    # Load results
    print(f"Loading run 1: {run1_name}")
    data1, meta1 = load_run_results(run1_name)
    times1 = extract_query_times(data1)

    print(f"Loading run 2: {run2_name}")
    data2, meta2 = load_run_results(run2_name)
    times2 = extract_query_times(data2)

    # Show metadata
    print("\n" + "=" * 80)
    print("Run Metadata")
    print("=" * 80)
    print(f"\nRun 1: {run1_name}")
    if meta1:
        print(f"  Scale: {meta1.get('scale_gb', 'Unknown')}GB")
        print(f"  Time:  {meta1.get('timestamp', 'Unknown')}")

    print(f"\nRun 2: {run2_name}")
    if meta2:
        print(f"  Scale: {meta2.get('scale_gb', 'Unknown')}GB")
        print(f"  Time:  {meta2.get('timestamp', 'Unknown')}")

    # Compare by format
    formats = ['ducklake', 'iceberg', 'delta']
    format_labels = {
        'ducklake': 'DuckLake',
        'iceberg': 'Iceberg',
        'delta': 'Delta'
    }

    print("\n" + "=" * 80)
    print("Performance Comparison by Format")
    print("=" * 80)

    for fmt in formats:
        print(f"\n{format_labels[fmt]}:")

        # Calculate average times
        run1_times = []
        run2_times = []

        for query_id in sorted(times1.keys()):
            if fmt in times1.get(query_id, {}) and fmt in times2.get(query_id, {}):
                run1_times.append(times1[query_id][fmt] * 1000)  # Convert to ms
                run2_times.append(times2[query_id][fmt] * 1000)

        if run1_times and run2_times:
            avg1 = sum(run1_times) / len(run1_times)
            avg2 = sum(run2_times) / len(run2_times)
            diff = avg2 - avg1
            pct_change = (diff / avg1) * 100

            print(f"  {run1_name}: {avg1:.2f} ms")
            print(f"  {run2_name}: {avg2:.2f} ms")
            print(f"  Difference: {diff:+.2f} ms ({pct_change:+.1f}%)")

            if pct_change < -1:
                print(f"  ✅ {run2_name} is faster!")
            elif pct_change > 1:
                print(f"  ⚠️  {run2_name} is slower")
            else:
                print(f"  ≈  Similar performance")
        else:
            print(f"  ❌ No comparable data")

    # Query-by-query comparison
    print("\n" + "=" * 80)
    print("Query-by-Query Comparison (DuckLake)")
    print("=" * 80)
    print()

    print(f"{'Query':<8} {run1_name:<15} {run2_name:<15} {'Diff (ms)':<12} {'Change %':<10}")
    print("-" * 80)

    for query_id in sorted(times1.keys()):
        if 'ducklake' in times1.get(query_id, {}) and 'ducklake' in times2.get(query_id, {}):
            time1 = times1[query_id]['ducklake'] * 1000
            time2 = times2[query_id]['ducklake'] * 1000
            diff = time2 - time1
            pct_change = (diff / time1) * 100

            marker = "🔴" if pct_change > 5 else "🟢" if pct_change < -5 else "⚪"

            print(f"{query_id:<8} {time1:<15.2f} {time2:<15.2f} {diff:<+12.2f} {pct_change:<+10.1f} {marker}")

    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)

    # Overall comparison
    all_queries = set(times1.keys()) & set(times2.keys())
    if all_queries:
        improvements = 0
        regressions = 0

        for query_id in all_queries:
            if 'ducklake' in times1.get(query_id, {}) and 'ducklake' in times2.get(query_id, {}):
                time1 = times1[query_id]['ducklake']
                time2 = times2[query_id]['ducklake']

                if time2 < time1 * 0.95:  # 5% faster
                    improvements += 1
                elif time2 > time1 * 1.05:  # 5% slower
                    regressions += 1

        print(f"\nQueries improved: {improvements}")
        print(f"Queries regressed: {regressions}")
        print(f"Queries similar: {len(all_queries) - improvements - regressions}")

    print("\n" + "=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description="Compare two benchmark runs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 compare_runs.py baseline optimized
  python3 compare_runs.py run1 run2

View available runs:
  python3 list_runs.py
        """
    )
    parser.add_argument(
        'run1',
        help='First run name'
    )
    parser.add_argument(
        'run2',
        help='Second run name'
    )

    args = parser.parse_args()
    compare_runs(args.run1, args.run2)


if __name__ == "__main__":
    main()
