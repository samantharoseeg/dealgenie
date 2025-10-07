#!/usr/bin/env python3
"""
Test 01: Database Connectivity
Verify scoring engine can read from properties_unified view
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

import psycopg2
from decimal import Decimal

def test_database_connection():
    """Test we can connect and fetch properties from properties_unified"""

    print("\n" + "="*70)
    print("TEST 01: DATABASE CONNECTIVITY")
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

        # Test 1: Verify properties_unified view exists
        print("Test 1.1: Checking properties_unified view exists...")
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.views
                WHERE table_schema = 'public'
                AND table_name = 'properties_unified'
            );
        """)
        exists = cursor.fetchone()[0]

        if exists:
            print("✅ properties_unified view exists\n")
        else:
            print("❌ properties_unified view not found\n")
            return False

        # Test 1.2: Get sample properties with required fields
        print("Test 1.2: Fetching 10 sample properties...")
        cursor.execute("""
            SELECT
                apn,
                zoning_code,
                lot_size_sqft,
                land_value,
                improvement_value,
                jurisdiction,
                data_source
            FROM properties_unified
            WHERE zoning_code IS NOT NULL
              AND lot_size_sqft IS NOT NULL
              AND lot_size_sqft > 0
            LIMIT 10;
        """)

        properties = cursor.fetchall()

        if len(properties) == 0:
            print("❌ No properties retrieved from database\n")
            return False

        print(f"✅ Retrieved {len(properties)} properties\n")

        # Test 1.3: Display sample property data
        print("Test 1.3: Sample Property Data\n")
        print(f"{'APN':<15} {'Zoning':<12} {'Lot Size':<12} {'Land Value':<15} {'Source':<20}")
        print("-" * 90)

        for row in properties:
            apn, zoning, lot_size, land_val, imp_val, jurisdiction, source = row

            # Convert Decimal to float for display
            lot_size_display = f"{float(lot_size):,.0f}" if lot_size else "N/A"
            land_val_display = f"${float(land_val):,.0f}" if land_val else "N/A"

            print(f"{apn:<15} {zoning:<12} {lot_size_display:<12} {land_val_display:<15} {source:<20}")

        print()

        # Test 1.4: Verify data types
        print("Test 1.4: Verifying data types...")
        sample = properties[0]
        apn, zoning, lot_size, land_val, imp_val, jurisdiction, source = sample

        checks = []

        # APN should be string
        checks.append(("APN is string", isinstance(apn, str)))

        # Zoning should be string
        checks.append(("Zoning is string", isinstance(zoning, str)))

        # Lot size should be numeric
        checks.append(("Lot size is numeric", isinstance(lot_size, (int, float, Decimal))))

        # Values should be numeric or None
        checks.append(("Land value is numeric or None", land_val is None or isinstance(land_val, (int, float, Decimal))))

        # Source should be string
        checks.append(("Data source is string", isinstance(source, str)))

        all_passed = True
        for check_name, result in checks:
            if result:
                print(f"  ✅ {check_name}")
            else:
                print(f"  ❌ {check_name}")
                all_passed = False

        print()

        if not all_passed:
            print("❌ Some data type checks failed\n")
            return False

        # Test 1.5: Check data source distribution
        print("Test 1.5: Data source distribution...")
        cursor.execute("""
            SELECT data_source, COUNT(*)
            FROM properties_unified
            GROUP BY data_source;
        """)

        sources = cursor.fetchall()
        print()
        for source, count in sources:
            print(f"  {source}: {count:,} properties")

        print()

        # Test 1.6: Check enhanced properties
        print("Test 1.6: Checking enhanced properties...")
        cursor.execute("""
            SELECT
                apn,
                zoning_code,
                crime_score,
                data_quality_score,
                property_type
            FROM properties_unified
            WHERE data_source = 'week1-5_enhanced';
        """)

        enhanced = cursor.fetchall()

        if len(enhanced) > 0:
            print(f"✅ Found {len(enhanced)} enhanced properties\n")
            print("Enhanced property details:")
            print(f"{'APN':<15} {'Zoning':<12} {'Crime Score':<12} {'Quality':<10} {'Type':<20}")
            print("-" * 80)
            for row in enhanced:
                apn, zoning, crime, quality, prop_type = row
                crime_display = f"{crime:.1f}" if crime else "N/A"
                quality_display = f"{quality:.1f}" if quality else "N/A"
                print(f"{apn:<15} {zoning:<12} {crime_display:<12} {quality_display:<10} {prop_type or 'N/A':<20}")
        else:
            print("⚠️  No enhanced properties found (expected for initial setup)\n")

        print()

        # Close connection
        conn.close()

        # Final summary
        print("="*70)
        print("TEST 01 SUMMARY")
        print("="*70)
        print()
        print("✅ Database connection: PASS")
        print("✅ properties_unified view: EXISTS")
        print(f"✅ Sample properties retrieved: {len(properties)}")
        print("✅ Data types verified: CORRECT")
        print(f"✅ Enhanced properties: {len(enhanced)}")
        print()
        print("🎉 TEST 01: PASSED")
        print()

        return True

    except psycopg2.Error as e:
        print(f"\n❌ Database error: {e}")
        print()
        print("🚫 TEST 01: FAILED")
        print()
        return False

    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        print()
        print("🚫 TEST 01: FAILED")
        print()
        return False

if __name__ == "__main__":
    success = test_database_connection()
    sys.exit(0 if success else 1)
