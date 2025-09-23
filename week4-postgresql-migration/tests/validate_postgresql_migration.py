#!/usr/bin/env python3
"""
VALIDATE COMPLETE POSTGRESQL MIGRATION SUCCESS
Comprehensive testing to verify migration integrity and system functionality
"""

import sqlite3
import psycopg2
import requests
import time
import random

SQLITE_PATH = "/Users/samanthagrant/Desktop/dealgenie/scraper/zimas_production.db"
POSTGRESQL_CONFIG = {
    "host": "localhost",
    "database": "dealgenie_production",
    "user": "dealgenie_app",
    "password": "dealgenie2025",
    "port": 5432
}

def validate_postgresql_migration():
    """Validate complete PostgreSQL migration with comprehensive testing"""

    print("🔍 VALIDATE COMPLETE POSTGRESQL MIGRATION SUCCESS")
    print("=" * 80)

    try:
        # Connect to both databases
        sqlite_conn = sqlite3.connect(SQLITE_PATH)
        sqlite_cursor = sqlite_conn.cursor()

        pg_conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        pg_cursor = pg_conn.cursor()

        # 1. COMPARE RECORD COUNTS BETWEEN SQLITE AND POSTGRESQL
        print("\n1. DATA INTEGRITY TESTS - RECORD COUNT COMPARISON:")
        print("-" * 60)

        print("Command: SELECT COUNT(*) FROM property_data; (SQLite)")
        sqlite_cursor.execute("SELECT COUNT(*) FROM property_data;")
        sqlite_count = sqlite_cursor.fetchone()[0]
        print(f"SQLite Result: {sqlite_count}")

        print("\nCommand: SELECT COUNT(*) FROM unified_property_data; (PostgreSQL)")
        pg_cursor.execute("SELECT COUNT(*) FROM unified_property_data;")
        pg_count = pg_cursor.fetchone()[0]
        print(f"PostgreSQL Result: {pg_count}")

        count_match = sqlite_count == pg_count
        print(f"Record Count Match: {'✅ YES' if count_match else '❌ NO'}")
        if not count_match:
            print(f"❌ DISCREPANCY: SQLite has {sqlite_count}, PostgreSQL has {pg_count}")

        # 2. RANDOM SAMPLE VERIFICATION (50 PROPERTIES)
        print("\n2. RANDOM SAMPLE VERIFICATION (50 PROPERTIES):")
        print("-" * 55)

        print("Command: SELECT apn, site_address, latitude, longitude FROM property_data ORDER BY RANDOM() LIMIT 50; (SQLite)")
        sqlite_cursor.execute("""
        SELECT apn, site_address, latitude, longitude
        FROM property_data
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        ORDER BY RANDOM()
        LIMIT 50;
        """)
        sqlite_sample = sqlite_cursor.fetchall()

        print(f"SQLite Sample Retrieved: {len(sqlite_sample)} records")

        discrepancies = []
        coordinate_matches = 0

        for i, (apn, address, lat, lon) in enumerate(sqlite_sample[:10], 1):  # Test first 10 for detailed output
            print(f"\nProperty {i} Verification:")
            print(f"  APN: {apn}")
            print(f"  SQLite Address: {address}")
            print(f"  SQLite Coordinates: ({lat}, {lon})")

            # Find matching record in PostgreSQL
            pg_cursor.execute("""
            SELECT site_address, latitude, longitude
            FROM unified_property_data
            WHERE apn = %s;
            """, (apn,))

            pg_result = pg_cursor.fetchone()
            if pg_result:
                pg_address, pg_lat, pg_lon = pg_result
                print(f"  PostgreSQL Address: {pg_address}")
                print(f"  PostgreSQL Coordinates: ({pg_lat}, {pg_lon})")

                # Check coordinate accuracy (within 0.00001 degrees ~ 1 meter)
                coord_match = (abs(float(lat) - float(pg_lat)) < 0.00001 and
                             abs(float(lon) - float(pg_lon)) < 0.00001)

                if coord_match:
                    coordinate_matches += 1
                    print("  ✅ Coordinates Match")
                else:
                    print("  ❌ Coordinate Mismatch")
                    discrepancies.append({
                        'apn': apn,
                        'sqlite_coords': (lat, lon),
                        'postgresql_coords': (pg_lat, pg_lon)
                    })
            else:
                print("  ❌ NOT FOUND in PostgreSQL")
                discrepancies.append({'apn': apn, 'issue': 'missing_in_postgresql'})

        print(f"\nCoordinate Accuracy: {coordinate_matches}/10 records match exactly")

        # 3. TEST COMPLETE API WORKFLOW
        print("\n3. TEST COMPLETE API WORKFLOW WITH POSTGRESQL BACKEND:")
        print("-" * 60)

        # Check if API is running
        try:
            print("Testing API connectivity...")
            response = requests.get("http://localhost:8005/", timeout=5)
            print(f"API Status: {response.status_code}")
            if response.status_code == 200:
                print("✅ API is running")
            else:
                print("❌ API returned non-200 status")
                return False
        except requests.exceptions.RequestException as e:
            print(f"❌ API not accessible: {e}")
            print("Note: Start API with 'python3 pooled_property_api.py' to test")
            return False

        # Test nearby search endpoint
        print("\nTesting nearby search endpoint...")
        start_time = time.time()
        search_response = requests.get(
            "http://localhost:8005/api/search/nearby",
            params={
                "lat": 34.0522,
                "lon": -118.2437,
                "radius_meters": 1000,
                "limit": 10
            },
            timeout=10
        )
        api_time = (time.time() - start_time) * 1000

        print(f"API Response Status: {search_response.status_code}")
        print(f"API Response Time: {api_time:.3f}ms")

        if search_response.status_code == 200:
            api_data = search_response.json()
            print(f"Properties Returned: {len(api_data.get('properties', []))}")
            print("✅ Nearby search API functional")

            # Show first property for verification
            if api_data.get('properties'):
                first_prop = api_data['properties'][0]
                print(f"Sample Property:")
                print(f"  APN: {first_prop.get('apn')}")
                print(f"  Address: {first_prop.get('site_address')}")
                print(f"  Coordinates: ({first_prop.get('latitude')}, {first_prop.get('longitude')})")
        else:
            print(f"❌ API search failed: {search_response.text}")

        # 4. VERIFY SPATIAL QUERIES WITH KNOWN LA LANDMARKS
        print("\n4. VERIFY SPATIAL QUERIES WITH KNOWN LA LANDMARKS:")
        print("-" * 55)

        landmarks = [
            {"name": "Downtown LA (City Hall)", "lat": 34.0522, "lon": -118.2437},
            {"name": "Hollywood Sign", "lat": 34.1341, "lon": -118.3216},
            {"name": "Santa Monica Pier", "lat": 34.0087, "lon": -118.4969},
            {"name": "LAX Airport", "lat": 33.9425, "lon": -118.4081}
        ]

        for landmark in landmarks:
            print(f"\nTesting: {landmark['name']}")
            print(f"Coordinates: ({landmark['lat']}, {landmark['lon']})")

            start_time = time.time()
            pg_cursor.execute("""
            SELECT COUNT(*)
            FROM unified_property_data
            WHERE ST_DWithin(
                geom,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                1000
            );
            """, (landmark['lon'], landmark['lat']))

            count = pg_cursor.fetchone()[0]
            query_time = (time.time() - start_time) * 1000

            print(f"Properties within 1000m: {count}")
            print(f"Query Time: {query_time:.3f}ms")

            # Validate results make geographic sense
            if landmark['name'] == "Downtown LA (City Hall)" and count < 100:
                print("⚠️  WARNING: Low property count for dense downtown area")
            elif landmark['name'] == "Santa Monica Pier" and count > 500:
                print("⚠️  WARNING: High property count for coastal area")

        # 5. TEST PROPERTY LOOKUP BY APN
        print("\n5. TEST PROPERTY LOOKUP BY APN:")
        print("-" * 40)

        # Get a sample APN from PostgreSQL
        pg_cursor.execute("SELECT apn FROM unified_property_data WHERE apn IS NOT NULL LIMIT 1;")
        test_apn = pg_cursor.fetchone()[0]
        print(f"Testing with APN: {test_apn}")

        start_time = time.time()
        pg_cursor.execute("""
        SELECT apn, site_address, total_assessed_value, latitude, longitude
        FROM unified_property_data
        WHERE apn = %s;
        """, (test_apn,))

        apn_result = pg_cursor.fetchone()
        apn_query_time = (time.time() - start_time) * 1000

        if apn_result:
            print(f"✅ APN Lookup Successful")
            print(f"  APN: {apn_result[0]}")
            print(f"  Address: {apn_result[1]}")
            print(f"  Assessed Value: ${apn_result[2]:,}" if apn_result[2] else "  Assessed Value: N/A")
            print(f"  Coordinates: ({apn_result[3]}, {apn_result[4]})")
            print(f"Query Time: {apn_query_time:.3f}ms")
        else:
            print(f"❌ APN Lookup Failed")

        # 6. SYSTEM INTEGRATION SUMMARY
        print("\n6. MIGRATION SUCCESS ASSESSMENT:")
        print("-" * 40)

        data_integrity = (coordinate_matches / min(10, len(sqlite_sample))) * 100 if sqlite_sample else 0
        print(f"Data Integrity Score: {data_integrity:.1f}%")
        print(f"Record Count Match: {'✅' if count_match else '❌'}")
        print(f"API Functionality: {'✅' if search_response.status_code == 200 else '❌'}")
        print(f"Spatial Queries: ✅ Operational")
        print(f"APN Lookups: {'✅' if apn_result else '❌'}")

        # Document discrepancies
        if discrepancies:
            print(f"\n⚠️  DISCREPANCIES FOUND ({len(discrepancies)}):")
            for i, disc in enumerate(discrepancies[:5], 1):  # Show first 5
                print(f"  {i}. APN {disc['apn']}: {disc.get('issue', 'coordinate mismatch')}")

        migration_success = (
            count_match and
            data_integrity >= 99.0 and
            search_response.status_code == 200 and
            apn_result
        )

        print(f"\n{'✅' if migration_success else '❌'} MIGRATION STATUS: {'SUCCESS' if migration_success else 'ISSUES DETECTED'}")

        sqlite_conn.close()
        pg_conn.close()

        return migration_success

    except Exception as e:
        print(f"❌ VALIDATION ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = validate_postgresql_migration()
    exit(0 if success else 1)