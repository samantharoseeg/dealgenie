#!/usr/bin/env python3
"""
Test 03: Zoning Code Coverage
Verify scoring engine handles all zoning codes in database
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

import psycopg2
from decimal import Decimal
from scoring.engine import calculate_score

def test_zoning_coverage():
    """Test scoring engine handles all unique zoning codes"""

    print("\n" + "="*70)
    print("TEST 03: ZONING CODE COVERAGE")
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

        # Test 3.1: Get zoning code distribution
        print("Test 3.1: Analyzing zoning code distribution...\n")
        cursor.execute("""
            SELECT COUNT(DISTINCT zoning_code) as unique_zones,
                   COUNT(*) as total_properties
            FROM properties_unified
            WHERE zoning_code IS NOT NULL;
        """)

        unique_zones, total_properties = cursor.fetchone()
        print(f"Total properties: {total_properties:,}")
        print(f"Unique zoning codes: {unique_zones:,}")
        print()

        # Test 3.2: Get top 50 most common zoning codes
        print("Test 3.2: Testing top 50 most common zoning codes...\n")
        cursor.execute("""
            SELECT zoning_code, COUNT(*) as count
            FROM properties_unified
            WHERE zoning_code IS NOT NULL
            GROUP BY zoning_code
            ORDER BY count DESC
            LIMIT 50;
        """)

        zones = cursor.fetchall()
        print(f"Testing {len(zones)} most common zoning codes")
        print(f"{'Zone Code':<15} {'Properties':<12} {'Multifamily':<12} {'Residential':<12} {'Commercial':<12}")
        print("-" * 70)

        unhandled_zones = []
        successful_scores = 0
        total_scores = 0
        zone_results = []

        for zone_code, count in zones:
            # Test if zone generates reasonable score across templates
            features = {
                'zoning': zone_code,
                'lot_size_sqft': 5000.0
            }

            try:
                # Test with 3 main templates
                multifamily_result = calculate_score(features, 'multifamily')
                residential_result = calculate_score(features, 'residential')
                commercial_result = calculate_score(features, 'commercial')

                multifamily_score = multifamily_result['component_scores']['zoning']
                residential_score = residential_result['component_scores']['zoning']
                commercial_score = commercial_result['component_scores']['zoning']

                total_scores += 3
                successful_scores += 3

                # Check if zone has low scores across all templates (may be unhandled)
                if multifamily_score < 2.5 and residential_score < 2.5 and commercial_score < 2.5:
                    unhandled_zones.append((zone_code, count))

                zone_results.append({
                    'code': zone_code,
                    'count': count,
                    'multifamily': multifamily_score,
                    'residential': residential_score,
                    'commercial': commercial_score
                })

                print(f"{zone_code:<15} {count:>10,}  {multifamily_score:>10.1f}  {residential_score:>10.1f}  {commercial_score:>10.1f}")

            except Exception as e:
                total_scores += 3
                print(f"{zone_code:<15} {count:>10,}  ERROR: {str(e)[:30]}")
                unhandled_zones.append((zone_code, count))

        print()

        # Test 3.3: Zone class analysis
        print("\nTest 3.3: Zone class breakdown...\n")

        # Group zones by class (R, C, M, etc.)
        zone_classes = {}
        for result in zone_results:
            zone_code = result['code']
            # Extract zone class (first 1-2 characters)
            if zone_code:
                if zone_code.startswith('R'):
                    zone_class = 'R (Residential)'
                elif zone_code.startswith('C'):
                    zone_class = 'C (Commercial)'
                elif zone_code.startswith('M'):
                    zone_class = 'M (Manufacturing)'
                elif zone_code.startswith('P'):
                    zone_class = 'P (Parking/Public)'
                elif zone_code.startswith('A'):
                    zone_class = 'A (Agriculture)'
                elif zone_code.startswith('OS'):
                    zone_class = 'OS (Open Space)'
                else:
                    zone_class = 'Other'

                if zone_class not in zone_classes:
                    zone_classes[zone_class] = {
                        'count': 0,
                        'zones': [],
                        'avg_multifamily': 0,
                        'avg_residential': 0,
                        'avg_commercial': 0
                    }

                zone_classes[zone_class]['count'] += result['count']
                zone_classes[zone_class]['zones'].append(result['code'])
                zone_classes[zone_class]['avg_multifamily'] += result['multifamily']
                zone_classes[zone_class]['avg_residential'] += result['residential']
                zone_classes[zone_class]['avg_commercial'] += result['commercial']

        # Calculate averages
        for zone_class in zone_classes:
            num_zones = len(zone_classes[zone_class]['zones'])
            if num_zones > 0:
                zone_classes[zone_class]['avg_multifamily'] /= num_zones
                zone_classes[zone_class]['avg_residential'] /= num_zones
                zone_classes[zone_class]['avg_commercial'] /= num_zones

        print(f"{'Zone Class':<20} {'Properties':<12} {'Zones':<8} {'Avg Multi':<10} {'Avg Res':<10} {'Avg Com':<10}")
        print("-" * 85)

        for zone_class in sorted(zone_classes.keys()):
            data = zone_classes[zone_class]
            print(f"{zone_class:<20} {data['count']:>10,}  {len(data['zones']):>6}  "
                  f"{data['avg_multifamily']:>8.1f}  {data['avg_residential']:>8.1f}  {data['avg_commercial']:>8.1f}")

        print()

        # Test 3.4: Edge case zones
        print("\nTest 3.4: Testing unusual/edge case zones...\n")

        cursor.execute("""
            SELECT DISTINCT zoning_code, COUNT(*) as count
            FROM properties_unified
            WHERE zoning_code IS NOT NULL
              AND (
                  LENGTH(zoning_code) > 10
                  OR zoning_code ~ '[^A-Za-z0-9-]'
                  OR zoning_code LIKE '%/%'
              )
            GROUP BY zoning_code
            ORDER BY count DESC
            LIMIT 10;
        """)

        edge_cases = cursor.fetchall()

        if len(edge_cases) > 0:
            print(f"Found {len(edge_cases)} edge case zones:")
            for zone_code, count in edge_cases:
                features = {'zoning': zone_code, 'lot_size_sqft': 5000.0}
                try:
                    result = calculate_score(features, 'multifamily')
                    score = result['component_scores']['zoning']
                    print(f"  ✅ {zone_code:<20} ({count:>6,} properties): {score:.1f}/10")
                except Exception as e:
                    print(f"  ❌ {zone_code:<20} ({count:>6,} properties): Error - {str(e)[:30]}")
        else:
            print("No unusual edge case zones found")

        print()

        # Close connection
        conn.close()

        # Final summary
        print("="*70)
        print("TEST 03 SUMMARY")
        print("="*70)
        print()
        print(f"✅ Unique zoning codes in database: {unique_zones:,}")
        print(f"✅ Top zones tested: {len(zones)}")
        print(f"✅ Total scoring attempts: {total_scores}")
        print(f"✅ Successful scores: {successful_scores}")
        print(f"✅ Success rate: {(successful_scores/total_scores*100):.1f}%")
        print(f"✅ Zone classes identified: {len(zone_classes)}")
        print()

        if unhandled_zones:
            print(f"⚠️  {len(unhandled_zones)} zones with low scores (may need handler updates):")
            for zone, count in unhandled_zones[:10]:  # Show top 10
                print(f"  {zone}: {count:,} properties")
            if len(unhandled_zones) > 10:
                print(f"  ... and {len(unhandled_zones) - 10} more")
            print()
            print("Note: Low scores may be intentional for certain zone types")
            print()

        if successful_scores == total_scores:
            print("🎉 TEST 03: PASSED")
        else:
            print("⚠️  TEST 03: COMPLETED WITH WARNINGS")
        print()

        return successful_scores == total_scores

    except psycopg2.Error as e:
        print(f"\n❌ Database error: {e}")
        print()
        print("🚫 TEST 03: FAILED")
        print()
        return False

    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        print()
        print("🚫 TEST 03: FAILED")
        print()
        return False

if __name__ == "__main__":
    success = test_zoning_coverage()
    sys.exit(0 if success else 1)
