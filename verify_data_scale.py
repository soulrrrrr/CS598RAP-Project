#!/usr/bin/env python3
"""
Verify that data at different scales has the correct row counts.

This script checks the store_sales table row count for each format
and compares it to expected values for each scale factor.

Usage:
    python3 verify_data_scale.py
"""

import duckdb
from pathlib import Path


# Expected row counts for store_sales at each scale
EXPECTED_COUNTS = {
    1: 2_880_052,
    5: 14_400_265,
    10: 28_800_530
}


def check_ducklake():
    """Check DuckLake store_sales row count."""
    try:
        metadata_path = Path("data/ducklake/metadata.ducklake")
        data_path = Path("data/ducklake/data_files")

        if not metadata_path.exists():
            return None, "Metadata file not found"

        conn = duckdb.connect()
        conn.execute("INSTALL ducklake")
        conn.execute("LOAD ducklake")

        attach_sql = f"""
            ATTACH 'ducklake:{metadata_path}' AS db
            (DATA_PATH '{data_path}', OVERRIDE_DATA_PATH true)
        """
        conn.execute(attach_sql)

        count = conn.execute("SELECT COUNT(*) FROM db.store_sales").fetchone()[0]
        conn.close()

        return count, None

    except Exception as e:
        return None, str(e)


def check_delta():
    """Check Delta store_sales row count."""
    try:
        delta_path = Path("data/delta/store_sales")

        if not delta_path.exists():
            return None, "Delta directory not found"

        conn = duckdb.connect()
        conn.execute("INSTALL delta")
        conn.execute("LOAD delta")

        count = conn.execute(
            f"SELECT COUNT(*) FROM delta_scan('{delta_path}')"
        ).fetchone()[0]
        conn.close()

        return count, None

    except Exception as e:
        return None, str(e)


def check_iceberg():
    """Check Iceberg store_sales row count."""
    try:
        iceberg_path = Path("data/iceberg_local/store_sales")

        if not iceberg_path.exists():
            return None, "Iceberg directory not found"

        conn = duckdb.connect()
        conn.execute("INSTALL iceberg")
        conn.execute("LOAD iceberg")
        conn.execute("SET unsafe_enable_version_guessing=true")

        count = conn.execute(
            f"SELECT COUNT(*) FROM iceberg_scan('{iceberg_path}', allow_moved_paths=true)"
        ).fetchone()[0]
        conn.close()

        return count, None

    except Exception as e:
        return None, str(e)


def determine_scale(count):
    """Determine scale factor based on row count."""
    if count is None:
        return None

    # Find closest scale
    min_diff = float('inf')
    detected_scale = None

    for scale, expected in EXPECTED_COUNTS.items():
        diff = abs(count - expected)
        if diff < min_diff:
            min_diff = diff
            detected_scale = scale

    # Allow 1% tolerance
    expected_for_scale = EXPECTED_COUNTS[detected_scale]
    tolerance = expected_for_scale * 0.01

    if min_diff <= tolerance:
        return detected_scale
    else:
        return None


def main():
    print("=" * 70)
    print("Data Scale Verification")
    print("=" * 70)
    print("\nChecking store_sales row counts...\n")

    results = {}

    # Check DuckLake
    print("🔍 DuckLake...")
    count, error = check_ducklake()
    if error:
        print(f"   ⚠️  {error}")
    else:
        scale = determine_scale(count)
        results['ducklake'] = (count, scale)
        print(f"   ✓ {count:,} rows (Scale: {scale}GB)")

    # Check Delta
    print("\n🔍 Delta...")
    count, error = check_delta()
    if error:
        print(f"   ⚠️  {error}")
    else:
        scale = determine_scale(count)
        results['delta'] = (count, scale)
        print(f"   ✓ {count:,} rows (Scale: {scale}GB)")

    # Check Iceberg
    print("\n🔍 Iceberg...")
    count, error = check_iceberg()
    if error:
        print(f"   ⚠️  {error}")
    else:
        scale = determine_scale(count)
        results['iceberg'] = (count, scale)
        print(f"   ✓ {count:,} rows (Scale: {scale}GB)")

    # Analysis
    print("\n" + "=" * 70)
    print("Analysis")
    print("=" * 70)

    if not results:
        print("\n❌ No data found! Please generate data first:")
        print("   python3 generate_data.py --scale 1 --format all")
        return 1

    # Check if all formats have the same scale
    scales = [scale for _, scale in results.values() if scale is not None]

    if not scales:
        print("\n❌ Could not determine scale for any format")
        print("   Row counts don't match expected values")
        return 1

    if len(set(scales)) == 1:
        detected_scale = scales[0]
        print(f"\n✅ All formats consistent: {detected_scale}GB dataset")

        # Check config
        import yaml
        config_path = Path("config/benchmark_config.yaml")
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            config_scale = config.get('benchmark', {}).get('scale_factor')

        if config_scale == detected_scale:
            print(f"✅ Config scale_factor matches: {config_scale}GB")
        else:
            print(f"\n⚠️  Config scale_factor mismatch!")
            print(f"   Config: {config_scale}GB")
            print(f"   Data:   {detected_scale}GB")
            print(f"\n💡 To fix, run:")
            print(f"   python3 update_config_scale.py {detected_scale}")

        print("\n📊 Expected row counts for reference:")
        for scale, expected in EXPECTED_COUNTS.items():
            marker = "👉" if scale == detected_scale else "  "
            print(f"   {marker} {scale}GB:  {expected:,} rows")

        return 0

    else:
        print("\n❌ INCONSISTENCY DETECTED!")
        print("   Different formats have different scale data:")
        for format_name, (count, scale) in results.items():
            print(f"   • {format_name}: {scale}GB ({count:,} rows)")

        print("\n💡 To fix: Regenerate all formats from same Parquet source")
        print("   1. Delete existing data:")
        print("      rm -rf data/ducklake data/delta data/iceberg_local")
        print("   2. Generate all formats:")
        print("      python3 generate_data.py --scale <desired_scale> --format all")

        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
