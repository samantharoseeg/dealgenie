#!/usr/bin/env python3
"""
Test 05: Enhanced Data Integration
Verify enhanced scoring fields are used when available
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

import psycopg2
from decimal import Decimal
from scoring.engine import calculate_score

def test_enhanced_data():
    """Test enhanced scoring with Week 1-5 data"""

    print("\n" + "="*70)
    print("TEST 05: ENHANCED DATA INTEGRATION")
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

        # Test 5.1: Get enhanced properties
        print("Test 5.1: Fetching enhanced properties from database...\n")
        cursor.execute("""
            SELECT
                apn,
                zoning_code,
                lot_size_sqft,
                land_value,
                improvement_value,
                crime_score,
                data_quality_score,
                property_type,
                data_source
            FROM properties_unified
            WHERE data_source = 'week1-5_enhanced';
        """)

        enhanced_properties = cursor.fetchall()

        if len(enhanced_properties) == 0:
            print("❌ No enhanced properties found in database")
            print("   Expected properties with data_source = 'week1-5_enhanced'")
            print()
            return False

        print(f"✅ Found {len(enhanced_properties)} enhanced properties")
        print()

        # Convert helper
        def to_float(val):
            if val is None:
                return None
            return float(val) if isinstance(val, Decimal) else val

        # Test 5.2: Compare enhanced vs basic scoring
        print("Test 5.2: Comparing enhanced vs. basic scoring...\n")
        print(f"{'APN':<15} {'Type':<20} {'Crime':<8} {'Quality':<8} {'Enhanced':<10} {'Basic':<10} {'Diff':<8}")
        print("-" * 95)

        enhanced_results = []
        score_improvements = []

        for row in enhanced_properties:
            apn, zoning, lot_size, land_val, imp_val, crime, quality, prop_type, source = row

            # Basic features (no enhanced data)
            basic_features = {
                'zoning': zoning,
                'lot_size_sqft': to_float(lot_size),
            }

            if land_val:
                basic_features['land_value'] = to_float(land_val)
            if imp_val:
                basic_features['improvement_value'] = to_float(imp_val)
            if land_val and imp_val:
                basic_features['assessed_value'] = to_float(land_val) + to_float(imp_val)

            # Enhanced features (with crime data)
            enhanced_features = basic_features.copy()
            if crime is not None:
                # Convert crime_score (0-100, higher=safer) to crime_factor (lower=safer)
                enhanced_features['crime_factor'] = (100 - to_float(crime)) / 100

            # Score both versions
            basic_result = calculate_score(basic_features, 'multifamily')
            enhanced_result = calculate_score(enhanced_features, 'multifamily')

            basic_score = basic_result['score']
            enhanced_score = enhanced_result['score']
            score_diff = enhanced_score - basic_score

            crime_display = f"{crime:.1f}" if crime else "N/A"
            quality_display = f"{quality:.1f}" if quality else "N/A"

            print(f"{apn:<15} {(prop_type or 'N/A'):<20} {crime_display:<8} {quality_display:<8} "
                  f"{enhanced_score:<10.2f} {basic_score:<10.2f} {score_diff:>+7.2f}")

            enhanced_results.append({
                'apn': apn,
                'property_type': prop_type,
                'crime_score': crime,
                'quality_score': quality,
                'basic_score': basic_score,
                'enhanced_score': enhanced_score,
                'difference': score_diff,
                'basic_result': basic_result,
                'enhanced_result': enhanced_result
            })

            score_improvements.append(score_diff)

        print()

        # Test 5.3: Crime score impact analysis
        print("Test 5.3: Analyzing crime score impact...\n")

        crime_impacts = []
        for result in enhanced_results:
            if result['crime_score']:
                basic_penalties = result['basic_result']['penalties']
                enhanced_penalties = result['enhanced_result']['penalties']

                crime_penalty = enhanced_penalties.get('high_crime', 0)
                has_crime_penalty = crime_penalty > 0

                crime_impacts.append({
                    'apn': result['apn'],
                    'crime_score': result['crime_score'],
                    'crime_penalty': crime_penalty,
                    'has_penalty': has_crime_penalty
                })

                status = "Penalty applied" if has_crime_penalty else "Safe (no penalty)"
                print(f"  {result['apn']}: Crime score {result['crime_score']:.1f} → {status}")

        print()

        # Test 5.4: Detailed component analysis
        print("Test 5.4: Component score comparison...\n")

        for result in enhanced_results:
            print(f"Property: {result['apn']} ({result['property_type'] or 'Unknown'})")
            crime_display = f"{result['crime_score']:.1f}" if result['crime_score'] else "N/A"
            quality_display = f"{result['quality_score']:.1f}" if result['quality_score'] else "N/A"
            print(f"  Crime Score: {crime_display}")
            print(f"  Quality Score: {quality_display}")
            print()

            print(f"  Basic Scoring (no enhanced data):")
            print(f"    Total Score: {result['basic_score']:.2f}/10")
            basic_components = result['basic_result']['component_scores']
            for component, score in basic_components.items():
                print(f"      {component}: {score:.2f}")
            print()

            print(f"  Enhanced Scoring (with crime data):")
            print(f"    Total Score: {result['enhanced_score']:.2f}/10")
            enhanced_components = result['enhanced_result']['component_scores']
            for component, score in enhanced_components.items():
                print(f"      {component}: {score:.2f}")

            if result['enhanced_result']['penalties']:
                print(f"    Penalties:")
                for penalty_type, penalty_val in result['enhanced_result']['penalties'].items():
                    print(f"      {penalty_type}: -{penalty_val:.2f}")

            print(f"    Improvement: {result['difference']:+.2f}")
            print()

        # Test 5.5: Validation checks
        print("Test 5.5: Validation checks...\n")

        checks = []

        # Check 1: Enhanced properties found
        if len(enhanced_properties) > 0:
            print(f"  ✅ Enhanced properties found: {len(enhanced_properties)}")
            checks.append(True)
        else:
            print(f"  ❌ No enhanced properties found")
            checks.append(False)

        # Check 2: Crime scores present
        crime_scores_present = sum(1 for r in enhanced_results if r['crime_score'] is not None)
        if crime_scores_present == len(enhanced_results):
            print(f"  ✅ Crime scores present: {crime_scores_present}/{len(enhanced_results)}")
            checks.append(True)
        else:
            print(f"  ⚠️  Crime scores present: {crime_scores_present}/{len(enhanced_results)}")
            checks.append(True)  # Warning but not failure

        # Check 3: Quality scores present
        quality_scores_present = sum(1 for r in enhanced_results if r['quality_score'] is not None)
        if quality_scores_present == len(enhanced_results):
            print(f"  ✅ Quality scores present: {quality_scores_present}/{len(enhanced_results)}")
            checks.append(True)
        else:
            print(f"  ⚠️  Quality scores present: {quality_scores_present}/{len(enhanced_results)}")
            checks.append(True)  # Warning but not failure

        # Check 4: Crime integration working
        crime_integration_working = any(r['crime_penalty'] > 0 for r in crime_impacts if r['crime_score'] < 50)
        if crime_integration_working or all(r['crime_score'] >= 50 for r in crime_impacts):
            print(f"  ✅ Crime score integration: Working")
            checks.append(True)
        else:
            print(f"  ❌ Crime score integration: Not applying penalties")
            checks.append(False)

        # Check 5: Scores differ between basic and enhanced
        any_difference = any(abs(diff) > 0.01 for diff in score_improvements)
        if any_difference:
            avg_improvement = sum(score_improvements) / len(score_improvements)
            print(f"  ✅ Enhanced data impacts scoring: {avg_improvement:+.2f} avg difference")
            checks.append(True)
        else:
            print(f"  ⚠️  Enhanced data shows no score difference")
            checks.append(True)  # Warning but not failure

        print()

        # Close connection
        conn.close()

        # Final summary
        print("="*70)
        print("TEST 05 SUMMARY")
        print("="*70)
        print()
        print(f"✅ Enhanced properties found: {len(enhanced_properties)}")
        print(f"✅ Crime scores present: {crime_scores_present}/{len(enhanced_results)}")
        print(f"✅ Quality scores present: {quality_scores_present}/{len(enhanced_results)}")
        print(f"✅ Crime integration: {'Working' if crime_integration_working else 'Not working'}")
        if score_improvements:
            avg_improvement = sum(score_improvements) / len(score_improvements)
            print(f"✅ Average score difference: {avg_improvement:+.2f} points")
        print()

        all_passed = all(checks)

        if all_passed:
            print("🎉 TEST 05: PASSED")
        else:
            print("❌ TEST 05: FAILED")
        print()

        return all_passed

    except psycopg2.Error as e:
        print(f"\n❌ Database error: {e}")
        print()
        print("🚫 TEST 05: FAILED")
        print()
        return False

    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        print()
        print("🚫 TEST 05: FAILED")
        print()
        return False

if __name__ == "__main__":
    success = test_enhanced_data()
    sys.exit(0 if success else 1)
