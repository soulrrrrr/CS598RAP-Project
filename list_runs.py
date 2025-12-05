#!/usr/bin/env python3
"""
List all benchmark runs and their metadata.

Usage:
    python3 list_runs.py
    python3 list_runs.py --detailed
"""

import argparse
import json
from pathlib import Path
from datetime import datetime


def list_runs(detailed=False):
    """List all available benchmark runs."""
    results_dir = Path("results")

    if not results_dir.exists():
        print("No results directory found.")
        return

    # Get all subdirectories except special ones
    exclude_dirs = {'multi_scale', 'runs'}  # Exclude legacy directories
    run_dirs = sorted([
        d for d in results_dir.iterdir()
        if d.is_dir() and d.name not in exclude_dirs
    ])

    if not run_dirs:
        print("No runs found in results/")
        print("\n💡 Run a benchmark first:")
        print("  python3 run_benchmark.py --scale 5 --name my_run")
        return

    print("=" * 80)
    print(f"Available Benchmark Runs ({len(run_dirs)} total)")
    print("=" * 80)
    print()

    for run_dir in run_dirs:
        run_name = run_dir.name
        metadata_file = run_dir / "run_metadata.json"

        # Load metadata if available
        if metadata_file.exists():
            with open(metadata_file) as f:
                metadata = json.load(f)

            scale = metadata.get('scale_gb', 'Unknown')
            timestamp = metadata.get('timestamp', 'Unknown')

            # Parse timestamp
            try:
                dt = datetime.fromisoformat(timestamp)
                time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            except:
                time_str = timestamp

            print(f"📁 {run_name}")
            print(f"   Scale: {scale}GB")
            print(f"   Time:  {time_str}")

            if detailed:
                command = metadata.get('command', 'N/A')
                print(f"   Command: {command}")

                # Check for results
                results_file = run_dir / "comparison_results.json"
                if results_file.exists():
                    with open(results_file) as f:
                        results = json.load(f)
                        num_queries = len(results.get('query_results', []))
                        print(f"   Queries: {num_queries}")

                # List files
                files = list(run_dir.glob("*"))
                print(f"   Files: {len(files)}")
                if detailed:
                    for f in sorted(files):
                        print(f"     - {f.name}")

            print()

        else:
            # No metadata, just show directory
            print(f"📁 {run_name}")
            print(f"   (No metadata found)")
            print()

    print("=" * 80)
    print("Commands:")
    print("  • Compare runs:    python3 compare_runs.py <run1> <run2>")
    print("  • View run:        cd results/<run_name>")
    print("  • Delete run:      rm -rf results/<run_name>")
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description="List all benchmark runs"
    )
    parser.add_argument(
        '--detailed',
        action='store_true',
        help='Show detailed information for each run'
    )

    args = parser.parse_args()
    list_runs(detailed=args.detailed)


if __name__ == "__main__":
    main()
