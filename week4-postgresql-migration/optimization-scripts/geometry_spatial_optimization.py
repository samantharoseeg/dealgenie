#!/usr/bin/env python3
"""
IMPLEMENT GEOMETRY-BASED SPATIAL QUERIES AND BOUNDING BOX PRE-FILTERING
Replace geography-based queries with optimized geometry operations to force spatial index usage
"""

import psycopg2
import time
import math

DATABASE_CONFIG = {
    "host": "localhost",
    "database": "dealgenie_production",
    "user": "dealgenie_app",
    "password": "dealgenie2025",
    "port": 5432
}

def geometry_spatial_optimization():
    """Implement geometry-based spatial optimization"""

    print("🔧 IMPLEMENT GEOMETRY-BASED SPATIAL QUERIES AND BOUNDING BOX PRE-FILTERING")
    print("=" * 80)

    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()

        # 1. MANDATORY: Verify database first
        print("\n📊 STEP 1: MANDATORY DATABASE VERIFICATION")
        print("-" * 60)

        cursor.execute("SELECT COUNT(*) FROM unified_property_data;")
        total_count = cursor.fetchone()[0]
        print(f"Total properties in database: {total_count}")

        if total_count != 457768:
            print(f"❌ ERROR: Expected 457,768 properties, found {total_count}")
            return False
        else:
            print(f"✅ CONFIRMED: Using 457,768 property production database")

        # 2. BASELINE MEASUREMENT
        print(f"\n📋 STEP 2: BASELINE MEASUREMENT")
        print("-" * 60)

        baseline_query = """
        EXPLAIN (ANALYZE, BUFFERS)
        SELECT COUNT(*)
        FROM unified_property_data
        WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326)::geography, 1000);
        """

        print("BASELINE QUERY - Current slow geography query:")
        print("=" * 50)
        print(baseline_query.strip())
        print()

        # Run baseline 3 times
        baseline_times = []
        for i in range(3):
            print(f"BASELINE ATTEMPT {i+1}:")
            print("-" * 30)
            start_time = time.time()
            cursor.execute(baseline_query)
            results = cursor.fetchall()
            execution_time = (time.time() - start_time) * 1000
            baseline_times.append(execution_time)

            print("COMPLETE EXECUTION PLAN:")
            for row in results:
                print(row[0])
            print(f"Measured execution time: {execution_time:.3f}ms")
            print()

        print(f"BASELINE TIMING SUMMARY:")
        print(f"Attempt 1: {baseline_times[0]:.3f}ms")
        print(f"Attempt 2: {baseline_times[1]:.3f}ms")
        print(f"Attempt 3: {baseline_times[2]:.3f}ms")
        avg_baseline = sum(baseline_times) / len(baseline_times)
        print(f"Average: {avg_baseline:.3f}ms")

        # 3. IMPLEMENT BOUNDING BOX PRE-FILTERING
        print(f"\n🔧 STEP 3: IMPLEMENT BOUNDING BOX PRE-FILTERING")
        print("-" * 60)

        # Calculate coordinate bounds for 1000m radius
        test_lat = 34.0522
        test_lon = -118.2437
        radius_meters = 1000

        # Approximate degree conversion
        lat_offset = radius_meters / 111000  # ~111km per degree latitude
        lon_offset = radius_meters / (111000 * math.cos(math.radians(test_lat)))

        print(f"TEST POINT: ({test_lat}, {test_lon})")
        print(f"RADIUS: {radius_meters}m")
        print(f"CALCULATED BOUNDS:")
        print(f"  Latitude: {test_lat - lat_offset:.6f} to {test_lat + lat_offset:.6f}")
        print(f"  Longitude: {test_lon - lon_offset:.6f} to {test_lon + lon_offset:.6f}")

        bbox_query = f"""
        EXPLAIN (ANALYZE, BUFFERS)
        SELECT COUNT(*)
        FROM unified_property_data
        WHERE geom && ST_Expand(ST_SetSRID(ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326), 4326), 0.01)
        AND ST_DWithin(geom, ST_SetSRID(ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326), 4326), 1000);
        """

        print(f"\nBOUNDING BOX QUERY:")
        print("=" * 40)
        print(bbox_query.strip())
        print()

        # Run bounding box query 3 times
        bbox_times = []
        for i in range(3):
            print(f"BOUNDING BOX ATTEMPT {i+1}:")
            print("-" * 30)
            start_time = time.time()
            cursor.execute(bbox_query)
            results = cursor.fetchall()
            execution_time = (time.time() - start_time) * 1000
            bbox_times.append(execution_time)

            print("COMPLETE EXECUTION PLAN:")
            for row in results:
                print(row[0])
            print(f"Measured execution time: {execution_time:.3f}ms")
            print()

        print(f"BOUNDING BOX TIMING SUMMARY:")
        for i, time_val in enumerate(bbox_times, 1):
            print(f"Attempt {i}: {time_val:.3f}ms")
        avg_bbox = sum(bbox_times) / len(bbox_times)
        print(f"Average: {avg_bbox:.3f}ms")

        # 4. TEST GEOMETRY vs GEOGRAPHY PERFORMANCE
        print(f"\n📊 STEP 4: TEST GEOMETRY vs GEOGRAPHY PERFORMANCE")
        print("-" * 60)

        test_queries = [
            {
                "name": "Geography 500m",
                "query": """
                EXPLAIN (ANALYZE, BUFFERS)
                SELECT COUNT(*) FROM unified_property_data
                WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326)::geography, 500);
                """
            },
            {
                "name": "Geometry 500m",
                "query": """
                EXPLAIN (ANALYZE, BUFFERS)
                SELECT COUNT(*) FROM unified_property_data
                WHERE ST_DWithin(geom, ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326), 500);
                """
            },
            {
                "name": "Geography 1000m",
                "query": """
                EXPLAIN (ANALYZE, BUFFERS)
                SELECT COUNT(*) FROM unified_property_data
                WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326)::geography, 1000);
                """
            },
            {
                "name": "Geometry 1000m",
                "query": """
                EXPLAIN (ANALYZE, BUFFERS)
                SELECT COUNT(*) FROM unified_property_data
                WHERE ST_DWithin(geom, ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326), 1000);
                """
            },
            {
                "name": "Geography 2000m",
                "query": """
                EXPLAIN (ANALYZE, BUFFERS)
                SELECT COUNT(*) FROM unified_property_data
                WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326)::geography, 2000);
                """
            },
            {
                "name": "Geometry 2000m",
                "query": """
                EXPLAIN (ANALYZE, BUFFERS)
                SELECT COUNT(*) FROM unified_property_data
                WHERE ST_DWithin(geom, ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326), 2000);
                """
            },
            {
                "name": "Geography 5000m",
                "query": """
                EXPLAIN (ANALYZE, BUFFERS)
                SELECT COUNT(*) FROM unified_property_data
                WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326)::geography, 5000);
                """
            },
            {
                "name": "Geometry 5000m",
                "query": """
                EXPLAIN (ANALYZE, BUFFERS)
                SELECT COUNT(*) FROM unified_property_data
                WHERE ST_DWithin(geom, ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326), 5000);
                """
            }
        ]

        for test in test_queries:
            print(f"\n{test['name']}:")
            print("=" * 40)
            print(f"QUERY: {test['query'].strip()}")
            print()

            start_time = time.time()
            cursor.execute(test['query'])
            results = cursor.fetchall()
            execution_time = (time.time() - start_time) * 1000

            # Check execution plan for scan type
            plan_text = "\n".join([row[0] for row in results])
            uses_seq_scan = "Seq Scan" in plan_text or "Parallel Seq Scan" in plan_text
            uses_index_scan = "Index Scan" in plan_text or "Bitmap" in plan_text

            print(f"EXECUTION TIME: {execution_time:.3f}ms")
            print(f"Uses sequential scan: {'YES ❌' if uses_seq_scan else 'NO ✅'}")
            print(f"Uses index scan: {'YES ✅' if uses_index_scan else 'NO ❌'}")
            print("EXECUTION PLAN (first 5 lines):")
            for row in results[:5]:
                print(f"  {row[0]}")

        # 5. CREATE MATERIALIZED VIEW FOR COMMON SEARCHES
        print(f"\n🏗️  STEP 5: CREATE MATERIALIZED VIEW FOR DOWNTOWN LA")
        print("-" * 60)

        # Create materialized view for downtown LA area
        mv_query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS downtown_la_properties AS
        SELECT
            apn, site_address, total_assessed_value, zoning_code,
            latitude, longitude, geom
        FROM unified_property_data
        WHERE geom IS NOT NULL
        AND longitude BETWEEN -118.3 AND -118.2
        AND latitude BETWEEN 34.0 AND 34.1;
        """

        print("CREATING MATERIALIZED VIEW:")
        print("=" * 30)
        print(mv_query.strip())
        print()

        start_time = time.time()
        cursor.execute(mv_query)
        mv_creation_time = (time.time() - start_time) * 1000
        print(f"Materialized view creation time: {mv_creation_time:.3f}ms")

        # Create index on materialized view
        mv_index_query = """
        CREATE INDEX IF NOT EXISTS idx_downtown_mv_geom
        ON downtown_la_properties USING GIST(geom);
        """

        print(f"\nCREATING INDEX ON MATERIALIZED VIEW:")
        print(mv_index_query.strip())
        start_time = time.time()
        cursor.execute(mv_index_query)
        mv_index_time = (time.time() - start_time) * 1000
        print(f"Index creation time: {mv_index_time:.3f}ms")

        # Test materialized view performance
        mv_test_query = """
        EXPLAIN (ANALYZE, BUFFERS)
        SELECT COUNT(*)
        FROM downtown_la_properties
        WHERE ST_DWithin(geom, ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326), 1000);
        """

        print(f"\nTESTING MATERIALIZED VIEW PERFORMANCE:")
        print("=" * 40)
        print(mv_test_query.strip())
        print()

        start_time = time.time()
        cursor.execute(mv_test_query)
        mv_results = cursor.fetchall()
        mv_execution_time = (time.time() - start_time) * 1000

        print("MATERIALIZED VIEW EXECUTION PLAN:")
        for row in mv_results:
            print(row[0])
        print(f"Materialized view execution time: {mv_execution_time:.3f}ms")

        # Compare with main table
        main_table_query = """
        EXPLAIN (ANALYZE, BUFFERS)
        SELECT COUNT(*)
        FROM unified_property_data
        WHERE geom IS NOT NULL
        AND longitude BETWEEN -118.3 AND -118.2
        AND latitude BETWEEN 34.0 AND 34.1
        AND ST_DWithin(geom, ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326), 1000);
        """

        print(f"\nCOMPARING WITH MAIN TABLE (SAME BOUNDS):")
        print("=" * 45)
        print(main_table_query.strip())
        print()

        start_time = time.time()
        cursor.execute(main_table_query)
        main_results = cursor.fetchall()
        main_execution_time = (time.time() - start_time) * 1000

        print("MAIN TABLE EXECUTION PLAN:")
        for row in main_results:
            print(row[0])
        print(f"Main table execution time: {main_execution_time:.3f}ms")

        # 6. MEASURE ACTUAL IMPROVEMENTS
        print(f"\n📈 STEP 6: FINAL PERFORMANCE COMPARISON")
        print("-" * 60)

        print("TIMING COMPARISON SUMMARY:")
        print("=" * 30)
        print(f"Baseline geography query: {avg_baseline:.3f}ms (average)")
        print(f"Bounding box geometry query: {avg_bbox:.3f}ms (average)")
        print(f"Materialized view query: {mv_execution_time:.3f}ms")
        print(f"Main table bounded query: {main_execution_time:.3f}ms")

        if avg_bbox < avg_baseline:
            improvement = avg_baseline - avg_bbox
            ratio = avg_baseline / avg_bbox
            print(f"Bounding box improvement: {improvement:.3f}ms ({ratio:.2f}x faster)")
        else:
            degradation = avg_bbox - avg_baseline
            ratio = avg_bbox / avg_baseline
            print(f"Bounding box degradation: {degradation:.3f}ms ({ratio:.2f}x slower)")

        if mv_execution_time < avg_baseline:
            mv_improvement = avg_baseline - mv_execution_time
            mv_ratio = avg_baseline / mv_execution_time
            print(f"Materialized view improvement: {mv_improvement:.3f}ms ({mv_ratio:.2f}x faster)")
        else:
            mv_degradation = mv_execution_time - avg_baseline
            mv_ratio = mv_execution_time / avg_baseline
            print(f"Materialized view degradation: {mv_degradation:.3f}ms ({mv_ratio:.2f}x slower)")

        cursor.close()
        conn.close()

        print(f"\n✅ GEOMETRY-BASED SPATIAL OPTIMIZATION COMPLETED")
        return True

    except Exception as e:
        print(f"❌ GEOMETRY SPATIAL OPTIMIZATION ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = geometry_spatial_optimization()
    exit(0 if success else 1)