#!/usr/bin/env python3
"""
OPTIMIZE SPATIAL INDEX USAGE FOR 457K PROPERTY DATABASE
Force PostgreSQL to use spatial indexes instead of sequential scans for geographic queries
"""

import psycopg2
import time

DATABASE_CONFIG = {
    "host": "localhost",
    "database": "dealgenie_production",
    "user": "dealgenie_app",
    "password": "dealgenie2025",
    "port": 5432
}

def optimize_spatial_index_usage():
    """Optimize spatial index usage for 457K property database"""

    print("🔧 OPTIMIZE SPATIAL INDEX USAGE FOR 457K PROPERTY DATABASE")
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

        # 2. ANALYZE CURRENT INDEX USAGE
        print(f"\n📋 STEP 2: ANALYZE CURRENT INDEX USAGE")
        print("-" * 60)

        # Show existing indexes
        print("EXISTING INDEXES ON unified_property_data:")
        print("=" * 50)
        cursor.execute("""
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE tablename = 'unified_property_data'
        ORDER BY indexname;
        """)

        indexes = cursor.fetchall()
        for idx_name, idx_def in indexes:
            print(f"Index: {idx_name}")
            print(f"Definition: {idx_def}")
            print()

        # Run EXPLAIN ANALYZE on current slow query
        print("CURRENT SLOW QUERY EXECUTION PLAN (BEFORE OPTIMIZATION):")
        print("=" * 60)

        slow_query = """
        EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
        SELECT COUNT(*)
        FROM unified_property_data
        WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326)::geography, 1000);
        """

        start_time = time.time()
        cursor.execute(slow_query)
        results_before = cursor.fetchall()
        execution_time_before = (time.time() - start_time) * 1000

        print("BEFORE OPTIMIZATION - COMPLETE EXECUTION PLAN:")
        for row in results_before:
            print(row[0])
        print(f"\nActual measured execution time BEFORE: {execution_time_before:.3f}ms")

        # 3. IMPLEMENT SPATIAL INDEX OPTIMIZATIONS
        print(f"\n🔧 STEP 3: IMPLEMENT SPATIAL INDEX OPTIMIZATIONS")
        print("-" * 60)

        # Create specialized spatial indexes for common query patterns
        spatial_index_commands = [
            # Basic spatial index if it doesn't exist
            """
            CREATE INDEX IF NOT EXISTS idx_unified_geom_gist
            ON unified_property_data USING GIST(geom);
            """,

            # Partial index for non-null geometries
            """
            CREATE INDEX IF NOT EXISTS idx_unified_geom_notnull_gist
            ON unified_property_data USING GIST(geom)
            WHERE geom IS NOT NULL;
            """,

            # Skip composite index - real type not supported in GIST
            # """
            # CREATE INDEX IF NOT EXISTS idx_unified_geom_value_gist
            # ON unified_property_data USING GIST(geom, total_assessed_value)
            # WHERE geom IS NOT NULL AND total_assessed_value > 0;
            # """,

            # Specialized index for downtown LA queries
            """
            CREATE INDEX IF NOT EXISTS idx_unified_downtown_gist
            ON unified_property_data USING GIST(geom)
            WHERE geom IS NOT NULL
            AND longitude BETWEEN -118.3 AND -118.2
            AND latitude BETWEEN 34.0 AND 34.1;
            """
        ]

        print("CREATING SPATIAL INDEXES:")
        for i, cmd in enumerate(spatial_index_commands, 1):
            if cmd.strip().startswith('#'):
                continue  # Skip commented out commands
            print(f"\nIndex {i}:")
            print(cmd.strip())

            start_time = time.time()
            cursor.execute(cmd)
            creation_time = (time.time() - start_time) * 1000
            print(f"Index creation time: {creation_time:.3f}ms")

        # Update table statistics
        print(f"\nUPDATING TABLE STATISTICS:")
        start_time = time.time()
        cursor.execute("ANALYZE unified_property_data;")
        analyze_time = (time.time() - start_time) * 1000
        print(f"ANALYZE execution time: {analyze_time:.3f}ms")

        # 4. MEASURE ACTUAL IMPROVEMENTS
        print(f"\n📈 STEP 4: MEASURE ACTUAL IMPROVEMENTS")
        print("-" * 60)

        # Test the same query after optimization
        print("SAME QUERY EXECUTION PLAN (AFTER OPTIMIZATION):")
        print("=" * 60)

        start_time = time.time()
        cursor.execute(slow_query)
        results_after = cursor.fetchall()
        execution_time_after = (time.time() - start_time) * 1000

        print("AFTER OPTIMIZATION - COMPLETE EXECUTION PLAN:")
        for row in results_after:
            print(row[0])
        print(f"\nActual measured execution time AFTER: {execution_time_after:.3f}ms")

        # Calculate actual improvement
        if execution_time_before > 0:
            improvement_ms = execution_time_before - execution_time_after
            improvement_ratio = execution_time_after / execution_time_before
            print(f"\nACTUAL TIMING COMPARISON:")
            print(f"Before optimization: {execution_time_before:.3f}ms")
            print(f"After optimization: {execution_time_after:.3f}ms")
            print(f"Absolute improvement: {improvement_ms:.3f}ms")
            print(f"Performance ratio: {improvement_ratio:.3f}x")

        # Test multiple spatial query patterns
        print(f"\nTESTING MULTIPLE SPATIAL QUERY PATTERNS:")
        print("=" * 50)

        test_queries = [
            {
                "name": "Small radius (500m)",
                "query": """
                EXPLAIN (ANALYZE, BUFFERS)
                SELECT COUNT(*) FROM unified_property_data
                WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326)::geography, 500);
                """
            },
            {
                "name": "Medium radius (2000m)",
                "query": """
                EXPLAIN (ANALYZE, BUFFERS)
                SELECT COUNT(*) FROM unified_property_data
                WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326)::geography, 2000);
                """
            },
            {
                "name": "Large radius (5000m)",
                "query": """
                EXPLAIN (ANALYZE, BUFFERS)
                SELECT COUNT(*) FROM unified_property_data
                WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326)::geography, 5000);
                """
            },
            {
                "name": "Different location (Hollywood)",
                "query": """
                EXPLAIN (ANALYZE, BUFFERS)
                SELECT COUNT(*) FROM unified_property_data
                WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-118.3287, 34.0928), 4326)::geography, 1000);
                """
            }
        ]

        for test in test_queries:
            print(f"\n{test['name']}:")
            print("-" * 30)

            start_time = time.time()
            cursor.execute(test['query'])
            test_results = cursor.fetchall()
            test_time = (time.time() - start_time) * 1000

            # Check if using sequential scan
            plan_text = "\n".join([row[0] for row in test_results])
            uses_seq_scan = "Seq Scan" in plan_text
            uses_index_scan = "Index Scan" in plan_text or "Bitmap" in plan_text

            print(f"Execution time: {test_time:.3f}ms")
            print(f"Uses sequential scan: {'YES ❌' if uses_seq_scan else 'NO ✅'}")
            print(f"Uses index scan: {'YES ✅' if uses_index_scan else 'NO ❌'}")

            # Show first few lines of execution plan
            for row in test_results[:3]:
                print(f"  {row[0]}")

        # 5. VALIDATE INDEX EFFECTIVENESS
        print(f"\n✅ STEP 5: VALIDATE INDEX EFFECTIVENESS")
        print("-" * 60)

        # Check index usage statistics
        print("INDEX USAGE STATISTICS:")
        print("=" * 40)
        cursor.execute("""
        SELECT schemaname, relname, indexrelname, idx_scan, idx_tup_read, idx_tup_fetch
        FROM pg_stat_user_indexes
        WHERE relname = 'unified_property_data'
        ORDER BY idx_scan DESC;
        """)

        index_stats = cursor.fetchall()
        print("Schema | Table | Index Name | Scans | Tuples Read | Tuples Fetched")
        print("-" * 70)
        for stat in index_stats:
            print(f"{stat[0]} | {stat[1]} | {stat[2]} | {stat[3]} | {stat[4]} | {stat[5]}")

        # Check table size and index sizes
        print(f"\nTABLE AND INDEX SIZES:")
        print("=" * 30)
        cursor.execute("""
        SELECT
            schemaname,
            tablename,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
            pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size
        FROM pg_tables
        WHERE tablename = 'unified_property_data';
        """)

        size_info = cursor.fetchone()
        print(f"Table: {size_info[1]}")
        print(f"Total size (including indexes): {size_info[2]}")
        print(f"Table size (data only): {size_info[3]}")

        cursor.close()
        conn.close()

        print(f"\n✅ SPATIAL INDEX OPTIMIZATION COMPLETED")
        return True

    except Exception as e:
        print(f"❌ SPATIAL INDEX OPTIMIZATION ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = optimize_spatial_index_usage()
    exit(0 if success else 1)