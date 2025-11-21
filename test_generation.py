#!/usr/bin/env python3
"""Quick test of TPC-DS data generation."""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

from benchmark.utils.config_loader import ConfigLoader
from benchmark.preparation.generate_tpcds import TPCDSGenerator

print("=" * 60)
print("Testing TPC-DS Data Generation")
print("=" * 60)

try:
    # Load config
    print("\n1. Loading configuration...")
    config = ConfigLoader()
    print(f"   ✓ Scale factor: {config.get('benchmark.scale_factor')}GB")
    print(f"   ✓ Output path: {config.get('paths.tpcds_raw')}")

    # Generate data
    print("\n2. Generating TPC-DS data...")
    generator = TPCDSGenerator(config)
    metrics = generator.generate_data()

    # Print results
    print("\n" + "=" * 60)
    print("SUCCESS!")
    print("=" * 60)
    print(f"Tables generated: {metrics['num_tables']}")
    print(f"Total rows: {metrics['total_rows']:,}")
    print(f"Total size: {metrics['total_size_gb']:.2f} GB")
    print(f"Time taken: {metrics['total_time_seconds']:.2f}s")
    print(f"Output: {metrics['output_path']}")
    print("=" * 60)

except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
