#!/usr/bin/env python3
"""
INVESTIGATE SANTA MONICA GEOGRAPHIC COVERAGE GAP
Determine why Santa Monica area shows 0 properties in spatial searches
"""

import psycopg2
import time

POSTGRESQL_CONFIG = {
    "host": "localhost",
    "database": "dealgenie_production",
    "user": "dealgenie_app",
    "password": "dealgenie2025",
    "port": 5432
}

def investigate_santa_monica_gap():
    """Investigate Santa Monica geographic coverage gap"""

    print("🔍 INVESTIGATE SANTA MONICA GEOGRAPHIC COVERAGE GAP")
    print("=" * 80)

    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor()

        # 1. MANDATORY DATABASE VERIFICATION
        print("\n1. MANDATORY DATABASE VERIFICATION:")
        print("-" * 50)

        print("Command: SELECT COUNT(*) FROM unified_property_data;")
        cursor.execute("SELECT COUNT(*) FROM unified_property_data;")
        total_count = cursor.fetchone()[0]
        print(f"Result: {total_count}")

        if total_count != 457768:
            print(f"❌ ERROR: Expected 457,768, found {total_count}")
            return False
        else:
            print("✅ CONFIRMED: 457,768 properties in database")

        # 2. ANALYZE SANTA MONICA COORDINATE DISTRIBUTION
        print("\n2. ANALYZE SANTA MONICA COORDINATE DISTRIBUTION:")
        print("-" * 60)

        # Query Santa Monica bounds
        print("\nQuerying Santa Monica coordinate bounds:")
        print("Command: SELECT COUNT(*) FROM unified_property_data WHERE latitude BETWEEN 34.0 AND 34.05 AND longitude BETWEEN -118.5 AND -118.45;")

        cursor.execute("""
        SELECT COUNT(*)
        FROM unified_property_data
        WHERE latitude BETWEEN 34.0 AND 34.05
        AND longitude BETWEEN -118.5 AND -118.45;
        """)
        santa_monica_count = cursor.fetchone()[0]
        print(f"Result: {santa_monica_count} properties in Santa Monica bounds")

        # Show sample properties with "Santa Monica" in address
        print("\nSearching for properties with 'SANTA MONICA' in address:")
        print("Command: SELECT apn, site_address, latitude, longitude FROM unified_property_data WHERE site_address ILIKE '%SANTA MONICA%' LIMIT 10;")

        cursor.execute("""
        SELECT apn, site_address, latitude, longitude
        FROM unified_property_data
        WHERE site_address ILIKE '%SANTA MONICA%'
        LIMIT 10;
        """)
        santa_monica_properties = cursor.fetchall()

        print(f"Found {len(santa_monica_properties)} properties with 'SANTA MONICA' in address")
        for i, (apn, address, lat, lon) in enumerate(santa_monica_properties, 1):
            print(f"  {i}. APN: {apn}")
            print(f"     Address: {address}")
            print(f"     Coordinates: ({lat}, {lon})")

        # Check broader LA west side
        print("\nChecking broader LA west side coverage:")
        print("Command: SELECT COUNT(*) FROM unified_property_data WHERE longitude < -118.4;")

        cursor.execute("SELECT COUNT(*) FROM unified_property_data WHERE longitude < -118.4;")
        west_side_count = cursor.fetchone()[0]
        print(f"Result: {west_side_count} properties west of longitude -118.4")
        print(f"Percentage of dataset: {(west_side_count/total_count)*100:.2f}%")

        # 3. TEST SPATIAL QUERY WITH SANTA MONICA COORDINATES
        print("\n3. TEST SPATIAL QUERY WITH SANTA MONICA COORDINATES:")
        print("-" * 60)

        santa_monica_coords = {"lat": 34.0194, "lon": -118.4912}
        print(f"Testing with precise Santa Monica Pier coordinates: ({santa_monica_coords['lat']}, {santa_monica_coords['lon']})")

        # Test different radius values
        radii = [500, 1000, 2000, 5000]

        for radius in radii:
            print(f"\n--- Testing radius: {radius}m ---")

            query = f"""
            EXPLAIN (ANALYZE, BUFFERS)
            SELECT COUNT(*)
            FROM unified_property_data
            WHERE ST_DWithin(
                geom,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                %s
            );
            """

            print(f"Command: SELECT COUNT(*) WHERE ST_DWithin(geom, ST_SetSRID(ST_MakePoint({santa_monica_coords['lon']}, {santa_monica_coords['lat']}), 4326), {radius});")

            start_time = time.time()
            cursor.execute(query, (santa_monica_coords['lon'], santa_monica_coords['lat'], radius))
            explain_results = cursor.fetchall()
            query_time = (time.time() - start_time) * 1000

            # Extract count from explain output
            for row in explain_results:
                if "Aggregate" in row[0] and "rows=" in row[0]:
                    print(f"Execution plan snippet: {row[0]}")
                    break

            # Get actual count
            count_query = """
            SELECT COUNT(*)
            FROM unified_property_data
            WHERE ST_DWithin(
                geom,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                %s
            );
            """

            cursor.execute(count_query, (santa_monica_coords['lon'], santa_monica_coords['lat'], radius))
            actual_count = cursor.fetchone()[0]
            print(f"Properties found: {actual_count}")
            print(f"Query time: {query_time:.3f}ms")

        # 4. VERIFY DATA BOUNDARY COVERAGE
        print("\n4. VERIFY DATA BOUNDARY COVERAGE:")
        print("-" * 60)

        # Check coordinate extent
        print("Checking coordinate extent of dataset:")
        print("Command: SELECT MIN(latitude), MAX(latitude), MIN(longitude), MAX(longitude) FROM unified_property_data WHERE geom IS NOT NULL;")

        cursor.execute("""
        SELECT
            MIN(latitude) as min_lat,
            MAX(latitude) as max_lat,
            MIN(longitude) as min_lon,
            MAX(longitude) as max_lon
        FROM unified_property_data
        WHERE geom IS NOT NULL;
        """)

        min_lat, max_lat, min_lon, max_lon = cursor.fetchone()

        print(f"Latitude range: {min_lat} to {max_lat}")
        print(f"Longitude range: {min_lon} to {max_lon}")

        # Calculate coverage area
        lat_range = max_lat - min_lat
        lon_range = max_lon - min_lon
        print(f"Coverage span: {lat_range:.3f}° latitude × {lon_range:.3f}° longitude")

        # Check if Santa Monica is within bounds
        sm_lat, sm_lon = 34.0194, -118.4912

        print(f"\nSanta Monica Pier coordinates: ({sm_lat}, {sm_lon})")

        lat_in_bounds = min_lat <= sm_lat <= max_lat
        lon_in_bounds = min_lon <= sm_lon <= max_lon

        print(f"Latitude {sm_lat} in bounds [{min_lat}, {max_lat}]: {'YES ✅' if lat_in_bounds else 'NO ❌'}")
        print(f"Longitude {sm_lon} in bounds [{min_lon}, {max_lon}]: {'YES ✅' if lon_in_bounds else 'NO ❌'}")

        if not (lat_in_bounds and lon_in_bounds):
            print("⚠️  WARNING: Santa Monica coordinates are OUTSIDE dataset boundaries!")

        # Check distribution near boundaries
        print("\nChecking property distribution near western boundary:")

        boundary_queries = [
            ("West of -118.48", "longitude < -118.48"),
            ("West of -118.47", "longitude < -118.47"),
            ("West of -118.46", "longitude < -118.46"),
            ("West of -118.45", "longitude < -118.45"),
            ("West of -118.44", "longitude < -118.44")
        ]

        for desc, condition in boundary_queries:
            cursor.execute(f"SELECT COUNT(*) FROM unified_property_data WHERE {condition};")
            count = cursor.fetchone()[0]
            percentage = (count/total_count)*100
            print(f"  {desc}: {count:,} properties ({percentage:.2f}%)")

        # 5. DOCUMENT GEOGRAPHIC GAPS
        print("\n5. GEOGRAPHIC COVERAGE ANALYSIS SUMMARY:")
        print("-" * 50)

        # Check major LA areas
        area_queries = [
            ("Downtown LA", "latitude BETWEEN 34.04 AND 34.06 AND longitude BETWEEN -118.26 AND -118.23"),
            ("Hollywood", "latitude BETWEEN 34.09 AND 34.11 AND longitude BETWEEN -118.34 AND -118.32"),
            ("Beverly Hills", "latitude BETWEEN 34.06 AND 34.08 AND longitude BETWEEN -118.41 AND -118.39"),
            ("Santa Monica", "latitude BETWEEN 34.01 AND 34.03 AND longitude BETWEEN -118.50 AND -118.47"),
            ("Venice", "latitude BETWEEN 33.98 AND 34.00 AND longitude BETWEEN -118.48 AND -118.45"),
            ("Westwood", "latitude BETWEEN 34.05 AND 34.07 AND longitude BETWEEN -118.45 AND -118.43")
        ]

        print("Property counts by area:")
        for area_name, condition in area_queries:
            cursor.execute(f"SELECT COUNT(*) FROM unified_property_data WHERE {condition};")
            area_count = cursor.fetchone()[0]
            print(f"  {area_name}: {area_count:,} properties")

        cursor.close()
        conn.close()

        print("\n✅ INVESTIGATION COMPLETED")
        return True

    except Exception as e:
        print(f"❌ INVESTIGATION ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = investigate_santa_monica_gap()
    exit(0 if success else 1)