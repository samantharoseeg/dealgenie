#!/usr/bin/env python3
"""
DIAGNOSE AND FIX SPATIAL QUERY MALFUNCTION
Fix ST_DWithin query returning all properties instead of filtering by distance
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

def diagnose_spatial_query_bug():
    """Diagnose and fix spatial query malfunction"""

    print("🔧 DIAGNOSE AND FIX SPATIAL QUERY MALFUNCTION")
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

        # 2. ISOLATE THE SPATIAL QUERY BUG
        print("\n2. ISOLATE THE SPATIAL QUERY BUG:")
        print("-" * 60)

        # Test basic geometry validity
        print("\nTesting geometry validity:")
        print("Command: SELECT COUNT(*) FROM unified_property_data WHERE ST_IsValid(geom) = false;")
        cursor.execute("SELECT COUNT(*) FROM unified_property_data WHERE ST_IsValid(geom) = false;")
        invalid_count = cursor.fetchone()[0]
        print(f"Invalid geometries: {invalid_count}")

        # Check SRID consistency
        print("\nChecking SRID consistency:")
        print("Command: SELECT DISTINCT ST_SRID(geom) FROM unified_property_data WHERE geom IS NOT NULL;")
        cursor.execute("SELECT DISTINCT ST_SRID(geom) FROM unified_property_data WHERE geom IS NOT NULL;")
        srids = cursor.fetchall()
        print(f"Distinct SRIDs in database: {[srid[0] for srid in srids]}")

        # Test simple distance query
        print("\nTesting simple distance query (degrees):")
        print("Command: SELECT COUNT(*) FROM unified_property_data WHERE ST_Distance(geom, ST_SetSRID(ST_MakePoint(-118.4912, 34.0194), 4326)) < 0.01;")

        start_time = time.time()
        cursor.execute("""
        SELECT COUNT(*)
        FROM unified_property_data
        WHERE ST_Distance(geom, ST_SetSRID(ST_MakePoint(-118.4912, 34.0194), 4326)) < 0.01;
        """)
        distance_count = cursor.fetchone()[0]
        query_time = (time.time() - start_time) * 1000
        print(f"Properties within 0.01 degrees: {distance_count}")
        print(f"Query time: {query_time:.3f}ms")

        # 3. FIX SPATIAL QUERY SYNTAX
        print("\n3. FIX SPATIAL QUERY SYNTAX:")
        print("-" * 60)

        # Test corrected query with geography cast
        print("\n--- Method 1: Geography cast with proper SRID ---")
        print("Command: EXPLAIN ANALYZE SELECT COUNT(*) FROM unified_property_data WHERE geom IS NOT NULL AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-118.4912, 34.0194), 4326)::geography, 1000);")

        start_time = time.time()
        cursor.execute("""
        EXPLAIN (ANALYZE, BUFFERS)
        SELECT COUNT(*)
        FROM unified_property_data
        WHERE geom IS NOT NULL
        AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-118.4912, 34.0194), 4326)::geography, 1000);
        """)
        explain_results = cursor.fetchall()
        method1_time = (time.time() - start_time) * 1000

        print("Execution plan snippet:")
        for row in explain_results[:3]:
            print(f"  {row[0]}")

        # Get actual count
        cursor.execute("""
        SELECT COUNT(*)
        FROM unified_property_data
        WHERE geom IS NOT NULL
        AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-118.4912, 34.0194), 4326)::geography, 1000);
        """)
        method1_count = cursor.fetchone()[0]
        print(f"Properties found (Method 1): {method1_count}")
        print(f"Query time: {method1_time:.3f}ms")

        # Test bounding box approach
        print("\n--- Method 2: Bounding box pre-filter ---")
        print("Command: SELECT COUNT(*) FROM unified_property_data WHERE geom && ST_Expand(ST_SetSRID(ST_MakePoint(-118.4912, 34.0194), 4326), 0.01) AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-118.4912, 34.0194), 4326)::geography, 1000);")

        start_time = time.time()
        cursor.execute("""
        SELECT COUNT(*)
        FROM unified_property_data
        WHERE geom && ST_Expand(ST_SetSRID(ST_MakePoint(-118.4912, 34.0194), 4326), 0.01)
        AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-118.4912, 34.0194), 4326)::geography, 1000);
        """)
        method2_count = cursor.fetchone()[0]
        method2_time = (time.time() - start_time) * 1000
        print(f"Properties found (Method 2): {method2_count}")
        print(f"Query time: {method2_time:.3f}ms")

        # 4. VALIDATE CORRECTED SPATIAL QUERIES
        print("\n4. VALIDATE CORRECTED SPATIAL QUERIES:")
        print("-" * 60)

        # Santa Monica Pier coordinates
        sm_lat, sm_lon = 34.0194, -118.4912
        print(f"\nTesting Santa Monica Pier ({sm_lat}, {sm_lon}) with different radii:")

        # Test different radius values
        radii = [500, 1000, 2000, 5000]

        for radius in radii:
            print(f"\n--- Radius: {radius}m ---")

            # Use the corrected geography query
            query = """
            SELECT COUNT(*)
            FROM unified_property_data
            WHERE geom IS NOT NULL
            AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, %s);
            """

            start_time = time.time()
            cursor.execute(query, (sm_lon, sm_lat, radius))
            count = cursor.fetchone()[0]
            query_time = (time.time() - start_time) * 1000

            print(f"Properties within {radius}m: {count}")
            print(f"Query time: {query_time:.3f}ms")

            # Validate the count is reasonable
            if count == 0:
                print("⚠️  WARNING: No properties found")
            elif count == 455820:
                print("❌ ERROR: Query returning ALL properties")
            else:
                print("✅ Query returning filtered results")

        # Test other LA locations for comparison
        print("\n5. VALIDATE WITH OTHER LA LOCATIONS:")
        print("-" * 50)

        test_locations = [
            ("Downtown LA", 34.0522, -118.2437),
            ("Hollywood", 34.0928, -118.3287),
            ("Beverly Hills", 34.0736, -118.4004),
            ("Santa Monica", 34.0194, -118.4912)
        ]

        print("\nUsing corrected query: ST_DWithin(geom::geography, point::geography, 1000)")
        for name, lat, lon in test_locations:
            cursor.execute("""
            SELECT COUNT(*)
            FROM unified_property_data
            WHERE geom IS NOT NULL
            AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, 1000);
            """, (lon, lat))

            count = cursor.fetchone()[0]
            print(f"{name:15} ({lat}, {lon}): {count:,} properties within 1000m")

        # Document the working syntax
        print("\n6. WORKING SQL SYNTAX DOCUMENTATION:")
        print("-" * 50)
        print("\n✅ CORRECTED SPATIAL QUERY SYNTAX:")
        print("""
        SELECT COUNT(*)
        FROM unified_property_data
        WHERE geom IS NOT NULL
        AND ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography,
            radius_meters
        );
        """)

        print("\nKEY FIXES:")
        print("1. Cast geometry to geography: geom::geography")
        print("2. Cast point to geography: ST_SetSRID(ST_MakePoint(...), 4326)::geography")
        print("3. Include geom IS NOT NULL check")
        print("4. Use proper parameter order: ST_MakePoint(longitude, latitude)")

        cursor.close()
        conn.close()

        print("\n✅ SPATIAL QUERY DIAGNOSIS COMPLETED")
        return True

    except Exception as e:
        print(f"❌ DIAGNOSIS ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = diagnose_spatial_query_bug()
    exit(0 if success else 1)