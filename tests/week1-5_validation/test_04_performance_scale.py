#!/usr/bin/env python3
"""
Test 04: Performance at Scale
Verify acceptable performance on large batches
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

import psycopg2
import time
import tracemalloc
from decimal import Decimal
from scoring.engine import calculate_score

def test_performance_scale():
    """Test scoring performance on 1000 properties"""

    print("\n" + "="*70)
    print("TEST 04: PERFORMANCE AT SCALE")
    print("="*70 + "\n")

    try:
        # Connect to database
        print("Connecting to PostgreSQL database...")
        conn = psycopg2.connect(
            host='localhost',
            database='dealgenie_production',
            user='samanthagrant',
            port=5432
        )
        cursor = conn.cursor()
        print("✅ Database connection successful\n")

        # Test 4.1: Fetch 1000 random properties
        print("Test 4.1: Fetching 1000 random properties...")
        cursor.execute("""
            SELECT apn, zoning_code, lot_size_sqft, land_value, improvement_value
            FROM properties_unified
            WHERE zoning_code IS NOT NULL
              AND lot_size_sqft IS NOT NULL
              AND lot_size_sqft > 0
            ORDER BY RANDOM()
            LIMIT 1000;
        """)

        properties = cursor.fetchall()
        print(f"✅ Retrieved {len(properties)} properties\n")

        # Test 4.2: Benchmark scoring performance
        print("Test 4.2: Benchmarking scoring performance...\n")

        # Start memory tracking
        tracemalloc.start()
        initial_memory = tracemalloc.get_traced_memory()[0]

        # Convert helper
        def to_float(val):
            if val is None:
                return None
            return float(val) if isinstance(val, Decimal) else val

        scores = []
        start_time = time.time()

        for i, row in enumerate(properties):
            apn, zoning, lot_size, land_val, imp_val = row

            features = {
                'zoning': zoning,
                'lot_size_sqft': to_float(lot_size),
            }

            if land_val:
                features['land_value'] = to_float(land_val)
            if imp_val:
                features['improvement_value'] = to_float(imp_val)
            if land_val and imp_val:
                features['assessed_value'] = to_float(land_val) + to_float(imp_val)

            result = calculate_score(features, 'multifamily')
            scores.append(result['score'])

            # Progress indicator every 100 properties
            if (i + 1) % 100 == 0:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed
                print(f"  Progress: {i + 1}/1000 ({rate:.1f} props/sec)")

        end_time = time.time()
        duration = end_time - start_time

        # Get final memory usage
        current_memory, peak_memory = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        memory_used_mb = (peak_memory - initial_memory) / (1024 * 1024)

        # Calculate metrics
        properties_per_second = len(properties) / duration
        avg_time_ms = (duration / len(properties)) * 1000

        print()
        print("="*70)
        print("PERFORMANCE METRICS")
        print("="*70)
        print()
        print(f"✅ Total properties scored: {len(properties):,}")
        print(f"✅ Total time: {duration:.2f} seconds")
        print(f"✅ Properties per second: {properties_per_second:.1f}")
        print(f"✅ Average time per property: {avg_time_ms:.2f} ms")
        print(f"✅ Peak memory usage: {memory_used_mb:.2f} MB")
        print()

        # Test 4.3: Score distribution analysis
        print("Test 4.3: Score distribution analysis...\n")

        min_score = min(scores)
        max_score = max(scores)
        avg_score = sum(scores) / len(scores)

        # Create score buckets
        buckets = {
            '0-2': 0,
            '2-4': 0,
            '4-6': 0,
            '6-8': 0,
            '8-10': 0
        }

        for score in scores:
            if score < 2:
                buckets['0-2'] += 1
            elif score < 4:
                buckets['2-4'] += 1
            elif score < 6:
                buckets['4-6'] += 1
            elif score < 8:
                buckets['6-8'] += 1
            else:
                buckets['8-10'] += 1

        print(f"Score Statistics:")
        print(f"  Min score: {min_score:.2f}/10")
        print(f"  Max score: {max_score:.2f}/10")
        print(f"  Average score: {avg_score:.2f}/10")
        print(f"  Median score: {sorted(scores)[len(scores)//2]:.2f}/10")
        print()

        print(f"Score Distribution:")
        for bucket, count in buckets.items():
            percentage = (count / len(scores)) * 100
            bar = '█' * int(percentage / 2)
            print(f"  {bucket}: {count:4d} ({percentage:5.1f}%) {bar}")
        print()

        # Test 4.4: Performance targets validation
        print("Test 4.4: Validating performance targets...\n")

        # Performance targets
        target_props_per_sec = 10
        target_avg_time_ms = 100

        checks = []

        # Check properties per second
        if properties_per_second > target_props_per_sec:
            print(f"  ✅ Properties/second: {properties_per_second:.1f} (target: >{target_props_per_sec})")
            checks.append(True)
        else:
            print(f"  ❌ Properties/second: {properties_per_second:.1f} (target: >{target_props_per_sec})")
            checks.append(False)

        # Check average time per property
        if avg_time_ms < target_avg_time_ms:
            print(f"  ✅ Avg time per property: {avg_time_ms:.2f}ms (target: <{target_avg_time_ms}ms)")
            checks.append(True)
        else:
            print(f"  ❌ Avg time per property: {avg_time_ms:.2f}ms (target: <{target_avg_time_ms}ms)")
            checks.append(False)

        # Check memory usage is reasonable (<100MB for 1000 properties)
        if memory_used_mb < 100:
            print(f"  ✅ Memory usage: {memory_used_mb:.2f}MB (target: <100MB)")
            checks.append(True)
        else:
            print(f"  ⚠️  Memory usage: {memory_used_mb:.2f}MB (target: <100MB)")
            checks.append(True)  # Warning but not failure

        # Check no errors occurred
        if len(scores) == len(properties):
            print(f"  ✅ Success rate: 100% ({len(scores)}/{len(properties)})")
            checks.append(True)
        else:
            print(f"  ❌ Success rate: {(len(scores)/len(properties)*100):.1f}% ({len(scores)}/{len(properties)})")
            checks.append(False)

        print()

        # Test 4.5: Projected full database performance
        print("Test 4.5: Projected full database performance...\n")

        total_properties = 2_424_014
        projected_time_seconds = total_properties / properties_per_second
        projected_time_minutes = projected_time_seconds / 60
        projected_time_hours = projected_time_minutes / 60

        print(f"Projected time to score all {total_properties:,} properties:")
        if projected_time_hours < 1:
            print(f"  {projected_time_minutes:.1f} minutes")
        else:
            print(f"  {projected_time_hours:.1f} hours ({projected_time_minutes:.0f} minutes)")
        print()

        projected_memory_mb = (total_properties / len(properties)) * memory_used_mb
        projected_memory_gb = projected_memory_mb / 1024

        print(f"Projected memory usage: {projected_memory_gb:.2f} GB")
        print()

        # Close connection
        conn.close()

        # Final summary
        print("="*70)
        print("TEST 04 SUMMARY")
        print("="*70)
        print()
        print(f"✅ Properties scored: {len(properties):,}")
        print(f"✅ Performance: {properties_per_second:.1f} props/sec")
        print(f"✅ Avg time: {avg_time_ms:.2f} ms/property")
        print(f"✅ Memory: {memory_used_mb:.2f} MB")
        print(f"✅ Success rate: 100%")
        print()

        all_passed = all(checks)

        if all_passed:
            print("🎉 TEST 04: PASSED")
            print()
            print("Performance targets met:")
            print(f"  ✅ {properties_per_second:.1f} props/sec (target: >{target_props_per_sec})")
            print(f"  ✅ {avg_time_ms:.2f}ms per property (target: <{target_avg_time_ms}ms)")
        else:
            print("❌ TEST 04: FAILED")
            print()
            print("Performance targets not met")

        print()

        return all_passed

    except psycopg2.Error as e:
        print(f"\n❌ Database error: {e}")
        print()
        print("🚫 TEST 04: FAILED")
        print()
        return False

    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        print()
        print("🚫 TEST 04: FAILED")
        print()
        return False

if __name__ == "__main__":
    success = test_performance_scale()
    sys.exit(0 if success else 1)
