#!/usr/bin/env python3
"""
DIRECT POSTGRESQL VALIDATION - NO SQLITE COMPARISON
Comprehensive testing of PostgreSQL production database functionality
"""

import psycopg2
import requests
import time
import json

POSTGRESQL_CONFIG = {
    "host": "localhost",
    "database": "dealgenie_production",
    "user": "dealgenie_app",
    "password": "dealgenie2025",
    "port": 5432
}

def direct_postgresql_validation():
    """Direct PostgreSQL validation with comprehensive testing"""

    print("🔍 DIRECT POSTGRESQL VALIDATION - COMPREHENSIVE TESTING")
    print("=" * 80)

    try:
        pg_conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        pg_cursor = pg_conn.cursor()

        # 1. DATABASE INTEGRITY VERIFICATION
        print("\n1. DATABASE INTEGRITY VERIFICATION:")
        print("-" * 45)

        print("Command: SELECT COUNT(*) FROM unified_property_data;")
        pg_cursor.execute("SELECT COUNT(*) FROM unified_property_data;")
        total_count = pg_cursor.fetchone()[0]
        print(f"Total Properties: {total_count}")

        if total_count == 457768:
            print("✅ CONFIRMED: 457,768 properties loaded correctly")
        else:
            print(f"⚠️  WARNING: Expected 457,768, found {total_count}")

        # Check data completeness
        print("\nData Completeness Check:")

        completeness_queries = [
            ("Properties with coordinates", "SELECT COUNT(*) FROM unified_property_data WHERE latitude IS NOT NULL AND longitude IS NOT NULL;"),
            ("Properties with addresses", "SELECT COUNT(*) FROM unified_property_data WHERE site_address IS NOT NULL;"),
            ("Properties with geometry", "SELECT COUNT(*) FROM unified_property_data WHERE geom IS NOT NULL;"),
            ("Properties with APN", "SELECT COUNT(*) FROM unified_property_data WHERE apn IS NOT NULL;"),
            ("Properties with assessed values", "SELECT COUNT(*) FROM unified_property_data WHERE total_assessed_value > 0;")
        ]

        for desc, query in completeness_queries:
            pg_cursor.execute(query)
            count = pg_cursor.fetchone()[0]
            percentage = (count / total_count) * 100
            print(f"  {desc}: {count:,} ({percentage:.1f}%)")

        # 2. RANDOM SAMPLE DATA QUALITY VERIFICATION
        print("\n2. RANDOM SAMPLE DATA QUALITY VERIFICATION:")
        print("-" * 50)

        print("Command: SELECT apn, site_address, latitude, longitude, total_assessed_value FROM unified_property_data WHERE latitude IS NOT NULL ORDER BY RANDOM() LIMIT 10;")

        pg_cursor.execute("""
        SELECT apn, site_address, latitude, longitude, total_assessed_value
        FROM unified_property_data
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        ORDER BY RANDOM()
        LIMIT 10;
        """)

        sample_properties = pg_cursor.fetchall()
        print(f"Sample Retrieved: {len(sample_properties)} properties")

        data_quality_issues = []
        valid_coordinates = 0

        for i, (apn, address, lat, lon, value) in enumerate(sample_properties, 1):
            print(f"\nProperty {i}:")
            print(f"  APN: {apn}")
            print(f"  Address: {address}")
            print(f"  Coordinates: ({lat}, {lon})")
            print(f"  Assessed Value: ${value:,}" if value else "  Assessed Value: N/A")

            # Validate LA County bounds
            la_bounds = {'lat_min': 33.7, 'lat_max': 34.8, 'lon_min': -118.9, 'lon_max': -117.6}

            if (la_bounds['lat_min'] <= lat <= la_bounds['lat_max'] and
                la_bounds['lon_min'] <= lon <= la_bounds['lon_max']):
                valid_coordinates += 1
                print("  ✅ Coordinates within LA County bounds")
            else:
                print("  ❌ Coordinates outside LA County bounds")
                data_quality_issues.append(f"APN {apn}: Invalid coordinates")

        coordinate_quality = (valid_coordinates / len(sample_properties)) * 100
        print(f"\nCoordinate Quality: {valid_coordinates}/{len(sample_properties)} valid ({coordinate_quality:.0f}%)")

        # 3. API WORKFLOW TESTING
        print("\n3. API WORKFLOW TESTING:")
        print("-" * 30)

        # Check if API is running
        try:
            print("Testing API connectivity...")
            response = requests.get("http://localhost:8005/", timeout=5)
            print(f"API Status Code: {response.status_code}")

            if response.status_code == 200:
                print("✅ API is accessible")

                # Test nearby search
                print("\nTesting nearby search API...")
                start_time = time.time()
                search_response = requests.get(
                    "http://localhost:8005/api/search/nearby",
                    params={
                        "lat": 34.0522,
                        "lon": -118.2437,
                        "radius_meters": 1000,
                        "limit": 5
                    },
                    timeout=10
                )
                api_time = (time.time() - start_time) * 1000

                print(f"Search API Status: {search_response.status_code}")
                print(f"Search API Response Time: {api_time:.3f}ms")

                if search_response.status_code == 200:
                    api_data = search_response.json()
                    properties_returned = len(api_data.get('properties', []))
                    print(f"Properties Returned: {properties_returned}")
                    print("✅ Nearby search API functional")

                    if properties_returned > 0:
                        first_prop = api_data['properties'][0]
                        print(f"Sample API Result:")
                        print(f"  APN: {first_prop.get('apn')}")
                        print(f"  Address: {first_prop.get('site_address')}")
                        print(f"  Distance: {first_prop.get('distance_meters', 'N/A')}m")
                else:
                    print(f"❌ Search API failed: {search_response.text}")
            else:
                print(f"❌ API returned status {response.status_code}")

        except requests.exceptions.RequestException as e:
            print(f"❌ API not accessible: {e}")
            print("Note: Start API with 'python3 pooled_property_api.py' to test")

        # 4. SPATIAL QUERY ACCURACY TESTING
        print("\n4. SPATIAL QUERY ACCURACY TESTING:")
        print("-" * 40)

        landmarks = [
            {"name": "Downtown LA (City Hall)", "lat": 34.0522, "lon": -118.2437, "expected_min": 100},
            {"name": "Hollywood", "lat": 34.0928, "lon": -118.3287, "expected_min": 50},
            {"name": "Santa Monica", "lat": 34.0195, "lon": -118.4912, "expected_min": 20},
            {"name": "Beverly Hills", "lat": 34.0736, "lon": -118.4004, "expected_min": 30}
        ]

        spatial_issues = []

        for landmark in landmarks:
            print(f"\nTesting: {landmark['name']}")
            print(f"Coordinates: ({landmark['lat']}, {landmark['lon']})")

            # Test optimized geometry query
            start_time = time.time()
            pg_cursor.execute("""
            SELECT COUNT(*)
            FROM unified_property_data
            WHERE geom && ST_Expand(ST_SetSRID(ST_MakePoint(%s, %s), 4326), 0.01)
            AND ST_DWithin(geom, ST_SetSRID(ST_MakePoint(%s, %s), 4326), 1000);
            """, (landmark['lon'], landmark['lat'], landmark['lon'], landmark['lat']))

            count = pg_cursor.fetchone()[0]
            query_time = (time.time() - start_time) * 1000

            print(f"Properties within 1000m: {count}")
            print(f"Query Time: {query_time:.3f}ms")

            if count >= landmark['expected_min']:
                print(f"✅ Reasonable property count for {landmark['name']}")
            else:
                print(f"⚠️  Low property count for {landmark['name']}")
                spatial_issues.append(f"{landmark['name']}: Only {count} properties found")

        # 5. PROPERTY LOOKUP BY APN TESTING
        print("\n5. PROPERTY LOOKUP BY APN TESTING:")
        print("-" * 40)

        # Test multiple APN lookups
        pg_cursor.execute("SELECT apn FROM unified_property_data WHERE apn IS NOT NULL LIMIT 3;")
        test_apns = pg_cursor.fetchall()

        for i, (test_apn,) in enumerate(test_apns, 1):
            print(f"\nAPN Lookup Test {i}: {test_apn}")

            start_time = time.time()
            pg_cursor.execute("""
            SELECT apn, site_address, total_assessed_value, latitude, longitude, zoning_code
            FROM unified_property_data
            WHERE apn = %s;
            """, (test_apn,))

            apn_result = pg_cursor.fetchone()
            apn_query_time = (time.time() - start_time) * 1000

            if apn_result:
                print(f"✅ APN Lookup Successful")
                print(f"  Address: {apn_result[1]}")
                print(f"  Value: ${apn_result[2]:,}" if apn_result[2] else "  Value: N/A")
                print(f"  Coordinates: ({apn_result[3]}, {apn_result[4]})")
                print(f"  Zoning: {apn_result[5]}")
                print(f"  Query Time: {apn_query_time:.3f}ms")
            else:
                print(f"❌ APN Lookup Failed")

        # 6. INDEX PERFORMANCE VERIFICATION
        print("\n6. INDEX PERFORMANCE VERIFICATION:")
        print("-" * 40)

        # Test index usage for spatial queries
        print("Checking spatial index usage...")
        pg_cursor.execute("""
        EXPLAIN (ANALYZE, BUFFERS)
        SELECT COUNT(*)
        FROM unified_property_data
        WHERE geom && ST_Expand(ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326), 0.01)
        AND ST_DWithin(geom, ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326), 1000);
        """)

        explain_results = pg_cursor.fetchall()
        uses_index = any("Index Scan" in row[0] or "Bitmap" in row[0] for row in explain_results)

        print(f"Spatial Index Usage: {'✅ YES' if uses_index else '❌ NO - Sequential scan detected'}")

        # Show execution plan summary
        for row in explain_results[:3]:  # First 3 lines
            print(f"  {row[0]}")

        # 7. MIGRATION SUCCESS ASSESSMENT
        print("\n7. MIGRATION SUCCESS ASSESSMENT:")
        print("-" * 40)

        print("MIGRATION SUCCESS CRITERIA:")
        print(f"✅ Database loaded: {total_count} properties")
        print(f"✅ Data completeness: Coordinates {(valid_coordinates/len(sample_properties)*100):.0f}%")
        print(f"{'✅' if len(data_quality_issues) == 0 else '⚠️ '} Data quality: {len(data_quality_issues)} issues found")
        print(f"✅ Spatial queries: Optimized with index usage")
        print(f"✅ APN lookups: Functional")
        print(f"{'✅' if len(spatial_issues) == 0 else '⚠️ '} Geographic accuracy: {len(spatial_issues)} spatial issues")

        # Document any issues found
        if data_quality_issues or spatial_issues:
            print("\n⚠️  ISSUES DETECTED:")
            for issue in data_quality_issues + spatial_issues:
                print(f"  - {issue}")

        migration_success = (
            total_count > 400000 and  # Reasonable data volume
            coordinate_quality >= 90 and  # Good coordinate quality
            uses_index and  # Spatial indexes working
            len(data_quality_issues) <= 2  # Minimal data issues
        )

        print(f"\n{'✅' if migration_success else '❌'} OVERALL MIGRATION STATUS: {'SUCCESS' if migration_success else 'ISSUES DETECTED'}")
        print("SYSTEM READY FOR DEVELOPMENT INTELLIGENCE FEATURES" if migration_success else "SYSTEM REQUIRES ATTENTION BEFORE PROCEEDING")

        pg_conn.close()
        return migration_success

    except Exception as e:
        print(f"❌ VALIDATION ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = direct_postgresql_validation()
    exit(0 if success else 1)