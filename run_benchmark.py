#!/usr/bin/env python3
"""
Run benchmark for a specific scale factor with optional run naming.

This script:
1. Updates the config to the specified scale
2. Runs the comparison benchmark
3. Saves results to a named directory

Usage:
    python3 run_benchmark.py --scale 1 --name baseline
    python3 run_benchmark.py --scale 5 --name optimized
    python3 run_benchmark.py --scale 10
"""

import argparse
import subprocess
import sys
from pathlib import Path
from datetime import datetime


def main():
    parser = argparse.ArgumentParser(
        description="Run benchmark for a specific scale factor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run benchmark with custom name
  python3 run_benchmark.py --scale 5 --name baseline

  # Run without name (uses timestamp)
  python3 run_benchmark.py --scale 5

  # Save to multi-scale directory (backward compatibility)
  python3 run_benchmark.py --scale 5 --save-to-multi-scale
        """
    )
    parser.add_argument(
        '--scale',
        type=int,
        required=True,
        choices=[1, 5, 10],
        help='Scale factor (1 = 1GB, 5 = 5GB, 10 = 10GB)'
    )
    parser.add_argument(
        '--name',
        type=str,
        help='Name for this benchmark run (default: timestamp)'
    )
    parser.add_argument(
        '--save-to-multi-scale',
        action='store_true',
        help='DEPRECATED: Save to results/multi_scale/{scale}gb/ (for backward compatibility)'
    )

    args = parser.parse_args()

    # Determine run name
    if args.name:
        run_name = args.name
    else:
        # Use timestamp if no name provided
        run_name = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 70)
    print(f"Running Benchmark for {args.scale}GB Dataset")
    print(f"Run Name: {run_name}")
    print("=" * 70)

    # Step 1: Update config
    print(f"\n📝 Updating config scale_factor to {args.scale}...")
    result = subprocess.run(
        ['python3', 'update_config_scale.py', str(args.scale)],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(f"❌ Failed to update config: {result.stderr}")
        sys.exit(1)

    print(result.stdout.strip())

    # Step 2: Run benchmark
    print(f"\n🚀 Running comparison benchmark...")
    result = subprocess.run(
        ['python3', 'benchmark/execution/run_comparison_benchmark.py'],
        capture_output=False,
        text=True
    )

    if result.returncode != 0:
        print(f"\n❌ Benchmark failed with exit code {result.returncode}")
        sys.exit(1)

    # Step 3: Save results to named directory
    import shutil
    from datetime import datetime as dt

    # Create run directory
    run_dir = Path(f"results/{run_name}")
    run_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n💾 Saving results to: {run_dir}")

    results_dir = Path("results")

    # Copy result files
    files_to_copy = [
        "comparison_results.json",
        "comparison_query_results.csv"
    ]

    for filename in files_to_copy:
        src = results_dir / filename
        if src.exists():
            dst = run_dir / filename
            shutil.copy2(src, dst)
            print(f"  ✓ Copied {filename}")

    # Copy PNG files
    for png_file in results_dir.glob("*.png"):
        dst = run_dir / png_file.name
        shutil.copy2(png_file, dst)
        print(f"  ✓ Copied {png_file.name}")

    # Save metadata
    metadata = {
        'run_name': run_name,
        'scale_gb': args.scale,
        'timestamp': dt.now().isoformat(),
        'command': ' '.join(sys.argv)
    }

    import json
    with open(run_dir / 'run_metadata.json', 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"  ✓ Saved run metadata")

    # Backward compatibility: also save to multi_scale if requested
    if args.save_to_multi_scale:
        print(f"\n💾 Also saving to multi_scale directory (backward compatibility)...")

        multi_scale_dir = Path(f"results/multi_scale/{args.scale}gb")
        multi_scale_dir.mkdir(parents=True, exist_ok=True)

        for filename in files_to_copy:
            src = results_dir / filename
            if src.exists():
                dst = multi_scale_dir / filename
                shutil.copy2(src, dst)

        for png_file in results_dir.glob("*.png"):
            dst = multi_scale_dir / png_file.name
            shutil.copy2(png_file, dst)

        print(f"  ✓ Saved to: {multi_scale_dir}")

    print("\n" + "=" * 70)
    print("✅ Benchmark Complete!")
    print("=" * 70)
    print(f"\nResults saved to:")
    print(f"  📁 {run_dir}/")
    print(f"     • comparison_results.json")
    print(f"     • comparison_query_results.csv")
    print(f"     • *.png (visualizations)")
    print(f"     • run_metadata.json")
    if args.save_to_multi_scale:
        print(f"\n  📁 {multi_scale_dir}/ (backward compatibility)")
    print("\n💡 View all runs: python3 list_runs.py")
    print("=" * 70)


if __name__ == "__main__":
    main()
