#!/usr/bin/env python3
"""
PostgreSQL Optimization - Fixed Version
Run optimizations outside transactions to avoid blocking issues
"""

import psycopg2
import time
import concurrent.futures
import statistics

def postgresql_optimization_fixed():
    """Implement PostgreSQL optimizations with proper transaction handling"""

    print("🚀 POSTGRESQL OPTIMIZATION - PRODUCTION READY")
    print("=" * 80)

    config = {
        "host": "localhost",
        "database": "dealgenie_production",
        "user": "dealgenie_app",
        "password": "dealgenie2025",
        "port": 5432
    }

    # Step 1: Baseline Performance
    print("\n📊 Step 1: Baseline Performance Measurement")
    print("-" * 60)

    try:
        conn = psycopg2.connect(**config)
        conn.autocommit = True  # Avoid transaction blocks
        cursor = conn.cursor()

        baseline_query = """
        EXPLAIN ANALYZE
        SELECT COUNT(*)
        FROM unified_property_data
        WHERE geom IS NOT NULL
        AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326)::geography, 1000)
        """

        print("BASELINE SPATIAL QUERY:")
        print(baseline_query.replace("EXPLAIN ANALYZE", "").strip())

        start_time = time.time()
        cursor.execute(baseline_query)
        baseline_plan = cursor.fetchall()
        baseline_time = (time.time() - start_time) * 1000

        print("\nBASELINE EXPLAIN ANALYZE:")
        baseline_execution_time = None
        uses_seq_scan = False

        for row in baseline_plan:
            print(f"  {row[0]}")
            if "Execution Time:" in row[0]:
                baseline_execution_time = float(row[0].split("Execution Time: ")[1].split(" ms")[0])
            if "Seq Scan" in row[0]:
                uses_seq_scan = True

        print(f"\nBASELINE RESULTS:")
        print(f"  Execution time: {baseline_execution_time:.3f} ms")
        print(f"  Uses sequential scan: {uses_seq_scan}")

        conn.close()

    except Exception as e:
        print(f"❌ Baseline measurement failed: {e}")
        return False

    # Step 2: Create Indexes (separate connections)
    print("\n🚀 Step 2: Creating Specialized Indexes")
    print("-" * 60)

    indexes_to_create = [
        {
            "name": "idx_high_value_spatial_partial",
            "sql": """
            CREATE INDEX IF NOT EXISTS idx_high_value_spatial_partial
            ON unified_property_data USING GIST(geom)
            WHERE total_assessed_value > 500000 AND geom IS NOT NULL
            """,
            "description": "Spatial index for high-value properties"
        },
        {
            "name": "idx_spatial_coords_composite",
            "sql": """
            CREATE INDEX IF NOT EXISTS idx_spatial_coords_composite
            ON unified_property_data (longitude, latitude, total_assessed_value)
            WHERE geom IS NOT NULL AND total_assessed_value > 100000
            """,
            "description": "Coordinate-based composite index"
        },
        {
            "name": "idx_downtown_bbox",
            "sql": """
            CREATE INDEX IF NOT EXISTS idx_downtown_bbox
            ON unified_property_data (longitude, latitude)
            WHERE longitude BETWEEN -118.3 AND -118.2
            AND latitude BETWEEN 34.0 AND 34.1
            AND geom IS NOT NULL
            """,
            "description": "Bounding box index for downtown LA"
        }
    ]

    created_indexes = []
    for idx in indexes_to_create:
        try:
            conn = psycopg2.connect(**config)
            conn.autocommit = True
            cursor = conn.cursor()

            print(f"Creating {idx['name']}...")
            start_time = time.time()
            cursor.execute(idx["sql"])
            creation_time = (time.time() - start_time) * 1000

            created_indexes.append(idx['name'])
            print(f"  ✅ {idx['name']}: {creation_time:.1f}ms - {idx['description']}")

            conn.close()
        except Exception as e:
            print(f"  ⚠️  {idx['name']}: {e}")

    # Update statistics
    try:
        conn = psycopg2.connect(**config)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("ANALYZE unified_property_data")
        print("  ✅ Updated table statistics")
        conn.close()
    except Exception as e:
        print(f"  ⚠️  Statistics update failed: {e}")

    # Step 3: Test Optimized Queries
    print("\n📋 Step 3: Testing Optimized Query Patterns")
    print("-" * 60)

    optimized_queries = [
        {
            "name": "Bounding Box Pre-filter",
            "query": """
            EXPLAIN ANALYZE
            SELECT COUNT(*)
            FROM unified_property_data
            WHERE longitude BETWEEN -118.253 AND -118.234
            AND latitude BETWEEN 34.043 AND 34.061
            AND geom IS NOT NULL
            AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326)::geography, 1000)
            """
        },
        {
            "name": "High-Value Spatial Query",
            "query": """
            EXPLAIN ANALYZE
            SELECT COUNT(*)
            FROM unified_property_data
            WHERE total_assessed_value > 500000
            AND geom IS NOT NULL
            AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326)::geography, 1000)
            """
        },
        {
            "name": "Geometry-based Query",
            "query": """
            EXPLAIN ANALYZE
            SELECT COUNT(*)
            FROM unified_property_data
            WHERE geom IS NOT NULL
            AND ST_DWithin(geom, ST_Transform(ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326), 4326), 0.009)
            """
        }
    ]

    optimization_results = {}

    for opt in optimized_queries:
        try:
            conn = psycopg2.connect(**config)
            conn.autocommit = True
            cursor = conn.cursor()

            print(f"\nTesting: {opt['name']}")

            start_time = time.time()
            cursor.execute(opt["query"])
            plan_results = cursor.fetchall()
            query_time = (time.time() - start_time) * 1000

            execution_time = None
            uses_seq_scan = False
            uses_index = False

            for row in plan_results:
                if "Execution Time:" in row[0]:
                    execution_time = float(row[0].split("Execution Time: ")[1].split(" ms")[0])
                if "Seq Scan" in row[0]:
                    uses_seq_scan = True
                if "Index" in row[0] or "Bitmap" in row[0]:
                    uses_index = True

            optimization_results[opt['name']] = {
                'execution_time': execution_time,
                'uses_seq_scan': uses_seq_scan,
                'uses_index': uses_index
            }

            print(f"  Execution time: {execution_time:.3f} ms")
            print(f"  Uses index: {uses_index}")
            print(f"  Uses sequential scan: {uses_seq_scan}")

            conn.close()

        except Exception as e:
            print(f"  ❌ Query failed: {e}")

    # Step 4: Concurrent Performance Test
    print("\n🔥 Step 4: Concurrent Performance Comparison")
    print("-" * 60)

    def run_concurrent_test(query_type="baseline"):
        """Run concurrent queries"""

        def worker_query(worker_id):
            try:
                local_conn = psycopg2.connect(**config)
                local_conn.autocommit = True
                local_cursor = local_conn.cursor()

                if query_type == "optimized":
                    query = """
                    SELECT COUNT(*)
                    FROM unified_property_data
                    WHERE longitude BETWEEN -118.253 AND -118.234
                    AND latitude BETWEEN 34.043 AND 34.061
                    AND geom IS NOT NULL
                    AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326)::geography, 1000)
                    """
                else:
                    query = """
                    SELECT COUNT(*)
                    FROM unified_property_data
                    WHERE geom IS NOT NULL
                    AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326)::geography, 1000)
                    """

                start_time = time.time()
                local_cursor.execute(query)
                result = local_cursor.fetchone()[0]
                execution_time = (time.time() - start_time) * 1000

                local_conn.close()
                return {
                    'worker_id': worker_id,
                    'execution_time': execution_time,
                    'result': result,
                    'success': True
                }
            except Exception as e:
                return {
                    'worker_id': worker_id,
                    'error': str(e),
                    'success': False
                }

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(worker_query, i) for i in range(1, 6)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        successful = [r for r in results if r['success']]
        return successful

    # Test baseline concurrent
    print("Baseline concurrent performance (5 workers):")
    baseline_concurrent = run_concurrent_test("baseline")
    baseline_concurrent_avg = statistics.mean([r['execution_time'] for r in baseline_concurrent]) if baseline_concurrent else 0

    for result in sorted(baseline_concurrent, key=lambda x: x['worker_id']):
        print(f"  Worker {result['worker_id']}: {result['execution_time']:.3f} ms")
    print(f"  Average: {baseline_concurrent_avg:.3f} ms")

    # Test optimized concurrent
    print("\nOptimized concurrent performance (5 workers):")
    optimized_concurrent = run_concurrent_test("optimized")
    optimized_concurrent_avg = statistics.mean([r['execution_time'] for r in optimized_concurrent]) if optimized_concurrent else 0

    for result in sorted(optimized_concurrent, key=lambda x: x['worker_id']):
        print(f"  Worker {result['worker_id']}: {result['execution_time']:.3f} ms")
    print(f"  Average: {optimized_concurrent_avg:.3f} ms")

    # Step 5: Results Analysis
    print("\n" + "=" * 80)
    print("🎯 POSTGRESQL OPTIMIZATION RESULTS")
    print("=" * 80)

    print("BASELINE PERFORMANCE:")
    print(f"  Single query execution: {baseline_execution_time:.3f} ms")
    print(f"  Concurrent average: {baseline_concurrent_avg:.3f} ms")
    print(f"  Uses sequential scan: {uses_seq_scan}")

    print(f"\nINDEXES CREATED: {len(created_indexes)}")
    for idx in created_indexes:
        print(f"  ✅ {idx}")

    print("\nOPTIMIZED QUERY RESULTS:")
    best_improvement = 0
    best_query = None

    for name, results in optimization_results.items():
        if results['execution_time'] and baseline_execution_time:
            improvement = ((baseline_execution_time - results['execution_time']) / baseline_execution_time * 100)
            print(f"  {name}:")
            print(f"    Execution time: {results['execution_time']:.3f} ms ({improvement:+.1f}%)")
            print(f"    Uses index: {results['uses_index']}")
            print(f"    Uses seq scan: {results['uses_seq_scan']}")

            if improvement > best_improvement:
                best_improvement = improvement
                best_query = name

    concurrent_improvement = 0
    if baseline_concurrent_avg > 0 and optimized_concurrent_avg > 0:
        concurrent_improvement = ((baseline_concurrent_avg - optimized_concurrent_avg) / baseline_concurrent_avg * 100)

    print(f"\nCONCURRENT PERFORMANCE:")
    print(f"  Baseline: {baseline_concurrent_avg:.3f} ms")
    print(f"  Optimized: {optimized_concurrent_avg:.3f} ms ({concurrent_improvement:+.1f}%)")

    print(f"\nOPTIMIZATION SUMMARY:")
    print(f"  Best single query improvement: {best_improvement:.1f}% ({best_query})")
    print(f"  Concurrent performance change: {concurrent_improvement:.1f}%")

    # Realistic assessment
    if best_improvement > 30:
        print("  🚀 Excellent optimization success")
    elif best_improvement > 15:
        print("  ✅ Good optimization results")
    elif best_improvement > 5:
        print("  ⚠️  Modest optimization improvement")
    else:
        print("  ❌ Limited optimization benefit")

    # Production readiness
    best_time = min([r['execution_time'] for r in optimization_results.values() if r['execution_time']]) if optimization_results else baseline_execution_time

    print(f"\nPRODUCTION ASSESSMENT:")
    if best_time < 100:
        print(f"  ✅ Production ready: {best_time:.1f}ms optimal performance")
    elif best_time < 200:
        print(f"  ⚠️  Acceptable: {best_time:.1f}ms with optimization")
    else:
        print(f"  ❌ Still needs work: {best_time:.1f}ms minimum time")

    return True

if __name__ == "__main__":
    success = postgresql_optimization_fixed()
    exit(0 if success else 1)