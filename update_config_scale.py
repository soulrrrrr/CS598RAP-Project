#!/usr/bin/env python3
"""
Helper script to update the scale_factor in benchmark_config.yaml

Usage:
    python3 update_config_scale.py 1
    python3 update_config_scale.py 5
    python3 update_config_scale.py 10
"""

import sys
import yaml
from pathlib import Path


def update_scale_factor(scale: int):
    """
    Update the scale_factor in benchmark_config.yaml

    Args:
        scale: Scale factor (1, 5, or 10)
    """
    if scale not in [1, 5, 10]:
        print(f"❌ Error: Scale must be 1, 5, or 10 (got {scale})")
        sys.exit(1)

    config_path = Path(__file__).parent / "config" / "benchmark_config.yaml"

    if not config_path.exists():
        print(f"❌ Error: Config file not found: {config_path}")
        sys.exit(1)

    # Read config
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    # Update scale factor
    old_scale = config.get('benchmark', {}).get('scale_factor', 'unknown')
    config['benchmark']['scale_factor'] = scale

    # Write back
    with open(config_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    print(f"✅ Updated scale_factor: {old_scale} → {scale}")
    print(f"   Config: {config_path}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 update_config_scale.py <scale>")
        print("  scale: 1, 5, or 10")
        sys.exit(1)

    try:
        scale = int(sys.argv[1])
        update_scale_factor(scale)
    except ValueError:
        print(f"❌ Error: Scale must be an integer (got '{sys.argv[1]}')")
        sys.exit(1)
