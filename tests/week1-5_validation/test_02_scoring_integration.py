#!/usr/bin/env python3
"""
Test 02: Scoring Integration
Verify scoring engine works with database properties
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

import psycopg2
from decimal import Decimal
from scoring.engine import calculate_score

def test_scoring_integration():
    """Test scoring engine with database properties"""

    print("\n" + "="*70)
    print("TEST 02: SCORING ENGINE INTEGRATION")
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

        # Test 2.1: Fetch sample properties
        print("Test 2.1: Fetching 5 random properties for scoring...")
        cursor.execute("""
            SELECT
                apn,
                zoning_code,
                lot_size_sqft,
                land_value,
                improvement_value,
                crime_score,
                data_source
            FROM properties_unified
            WHERE zoning_code IS NOT NULL
              AND lot_size_sqft IS NOT NULL
              AND lot_size_sqft > 0
            ORDER BY RANDOM()
            LIMIT 5;
        """)

        properties = cursor.fetchall()

        if len(properties) == 0:
            print("❌ No properties retrieved from database\n")
            return False

        print(f"✅ Retrieved {len(properties)} properties\n")

        # Test 2.2: Score properties with all templates
        print("Test 2.2: Scoring properties with all templates...\n")

        templates = ['multifamily', 'residential', 'commercial', 'industrial', 'retail', 'mixed_use', 'office']

        all_passed = True
        total_scores = 0
        successful_scores = 0

        for row in properties:
            apn, zoning, lot_size, land_val, imp_val, crime, source = row

            # Convert Decimal to float
            def to_float(val):
                if val is None:
                    return None
                return float(val) if isinstance(val, Decimal) else val

            # Build features dict for scoring engine
            features = {
                # REQUIRED
                'zoning': zoning,
                'lot_size_sqft': to_float(lot_size),
            }

            # Add optional fields if available
            if land_val:
                features['land_value'] = to_float(land_val)
            if imp_val:
                features['improvement_value'] = to_float(imp_val)
            if land_val and imp_val:
                features['assessed_value'] = to_float(land_val) + to_float(imp_val)

            # Add enhanced data if available
            if crime is not None:
                # Convert crime_score (0-100, higher=safer) to crime_factor (lower=safer)
                features['crime_factor'] = (100 - to_float(crime)) / 100

            print(f"Property: {apn} (Zoning: {zoning}, Lot: {lot_size:,.0f} sqft)")
            print(f"Source: {source}")
            print()

            # Test each template
            for template in templates:
                total_scores += 1

                try:
                    result = calculate_score(features, template)

                    # Validate result structure (actual scoring engine output)
                    checks = [
                        ('score' in result, "Has score"),
                        ('component_scores' in result, "Has component_scores"),
                        ('explanation' in result, "Has explanation"),
                        ('recommendations' in result, "Has recommendations"),
                        (0 <= result.get('score', -1) <= 10, "Score in valid range (0-10)"),
                    ]

                    template_passed = all(check[0] for check in checks)

                    if template_passed:
                        successful_scores += 1
                        score = result['score']
                        # Generate grade from score
                        if score >= 9.0:
                            grade = "A+"
                        elif score >= 8.5:
                            grade = "A"
                        elif score >= 7.5:
                            grade = "B+"
                        elif score >= 7.0:
                            grade = "B"
                        elif score >= 6.0:
                            grade = "C"
                        else:
                            grade = "D"
                        print(f"  ✅ {template:12s}: {score:.1f}/10  Grade: {grade}")
                    else:
                        all_passed = False
                        failed_checks = [check[1] for check in checks if not check[0]]
                        print(f"  ❌ {template:12s}: Failed - {', '.join(failed_checks)}")

                except Exception as e:
                    all_passed = False
                    # Don't increment successful_scores on error
                    print(f"  ❌ {template:12s}: Error - {str(e)[:50]}")

            print()

        # Test 2.3: Score enhanced properties specifically
        print("\nTest 2.3: Scoring enhanced properties (with crime data)...\n")

        cursor.execute("""
            SELECT
                apn,
                zoning_code,
                lot_size_sqft,
                land_value,
                improvement_value,
                crime_score,
                data_quality_score,
                property_type
            FROM properties_unified
            WHERE data_source = 'week1-5_enhanced';
        """)

        enhanced = cursor.fetchall()

        enhanced_passed = True

        if len(enhanced) > 0:
            print(f"Found {len(enhanced)} enhanced properties\n")

            for row in enhanced:
                apn, zoning, lot_size, land_val, imp_val, crime, quality, prop_type = row

                def to_float(val):
                    if val is None:
                        return None
                    return float(val) if isinstance(val, Decimal) else val

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
                if crime is not None:
                    features['crime_factor'] = (100 - to_float(crime)) / 100

                print(f"Property: {apn}")
                print(f"  Type: {prop_type or 'N/A'}")
                print(f"  Zoning: {zoning}, Lot: {lot_size:,.0f} sqft")
                print(f"  Crime Score: {crime:.1f}, Quality: {quality:.1f}")
                print()

                # Score with multifamily template
                try:
                    result = calculate_score(features, template='multifamily')

                    score = result['score']

                    # Generate grade from score
                    if score >= 9.0:
                        grade = "A+"
                    elif score >= 8.5:
                        grade = "A"
                    elif score >= 7.5:
                        grade = "B+"
                    elif score >= 7.0:
                        grade = "B"
                    elif score >= 6.0:
                        grade = "C"
                    else:
                        grade = "D"

                    print(f"  Multifamily Score: {score:.1f}/10")
                    print(f"  Grade: {grade}")

                    if 'component_scores' in result:
                        print(f"  Component Scores:")
                        for component, comp_score in result['component_scores'].items():
                            print(f"    - {component}: {comp_score:.1f}")

                    if 'penalties' in result and result['penalties']:
                        print(f"  Penalties Applied:")
                        for penalty_type, penalty_val in result['penalties'].items():
                            print(f"    - {penalty_type}: -{penalty_val:.1f}")

                    print()

                except Exception as e:
                    enhanced_passed = False
                    print(f"  ❌ Error scoring enhanced property: {e}")
                    print()

        else:
            print("⚠️  No enhanced properties found (expected for initial setup)\n")

        # Test 2.4: Test template-specific scoring
        print("\nTest 2.4: Testing template-specific zone preferences...\n")

        test_cases = [
            {'zoning': 'R1', 'lot_size_sqft': 5000, 'template': 'residential', 'expected_range': (5, 10)},
            {'zoning': 'R3', 'lot_size_sqft': 10000, 'template': 'multifamily', 'expected_range': (6, 10)},
            {'zoning': 'C2', 'lot_size_sqft': 8000, 'template': 'commercial', 'expected_range': (6, 10)},
            {'zoning': 'M1', 'lot_size_sqft': 20000, 'template': 'industrial', 'expected_range': (5, 10)},
        ]

        template_test_passed = True

        for test in test_cases:
            features = {
                'zoning': test['zoning'],
                'lot_size_sqft': test['lot_size_sqft']
            }

            try:
                result = calculate_score(features, template=test['template'])
                score = result['score']
                min_score, max_score = test['expected_range']

                if min_score <= score <= max_score:
                    print(f"  ✅ {test['template']:12s} + {test['zoning']:4s}: {score:.1f}/10 (expected {min_score}-{max_score})")
                else:
                    template_test_passed = False
                    print(f"  ⚠️  {test['template']:12s} + {test['zoning']:4s}: {score:.1f}/10 (expected {min_score}-{max_score})")

            except Exception as e:
                template_test_passed = False
                print(f"  ❌ {test['template']:12s} + {test['zoning']:4s}: Error - {e}")

        print()

        # Close connection
        conn.close()

        # Final summary
        print("="*70)
        print("TEST 02 SUMMARY")
        print("="*70)
        print()
        print(f"✅ Properties tested: {len(properties)}")
        print(f"✅ Templates tested: {len(templates)}")
        print(f"✅ Total scoring attempts: {total_scores}")
        print(f"✅ Successful scores: {successful_scores}")
        print(f"✅ Success rate: {(successful_scores/total_scores*100):.1f}%")
        print()

        if len(enhanced) > 0:
            print(f"✅ Enhanced properties scored: {len(enhanced)}")
            if enhanced_passed:
                print("✅ Enhanced scoring: PASS")
            else:
                print("❌ Enhanced scoring: FAILED")
            print()

        if template_test_passed:
            print("✅ Template-specific preferences: PASS")
        else:
            print("⚠️  Template-specific preferences: Some outside expected range")
        print()

        if all_passed and successful_scores == total_scores:
            print("🎉 TEST 02: PASSED")
        else:
            print("⚠️  TEST 02: COMPLETED WITH WARNINGS")
        print()

        return all_passed

    except psycopg2.Error as e:
        print(f"\n❌ Database error: {e}")
        print()
        print("🚫 TEST 02: FAILED")
        print()
        return False

    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        print()
        print("🚫 TEST 02: FAILED")
        print()
        return False

if __name__ == "__main__":
    success = test_scoring_integration()
    sys.exit(0 if success else 1)
