#!/usr/bin/env python3
"""
Actual Performance Test - Measure Real Response Times
"""

import sqlite3
import time
import statistics

def test_database_query_performance():
    """Test actual database query performance"""
    print("🗄️ TESTING ACTUAL DATABASE QUERY PERFORMANCE")
    print("=" * 60)
    
    db_path = 'data/dealgenie.db'
    
    # Test coordinates
    test_coords = [
        (34.052235, -118.243685),  # Downtown LA
        (34.073620, -118.400356),  # Beverly Hills  
        (34.058, -118.291),        # Koreatown
        (33.9942, -118.4751),      # Venice
        (34.1, -118.3),            # Random LA 1
        (34.0, -118.2),            # Random LA 2
        (34.2, -118.4),            # Random LA 3
        (34.15, -118.35),          # Random LA 4
        (34.05, -118.45),          # Random LA 5
        (34.12, -118.22)           # Random LA 6
    ]
    
    times = []
    results = []
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        
        for lat, lon in test_coords:
            start_time = time.perf_counter()
            
            cursor.execute("""
                SELECT total_density_weighted 
                FROM crime_density_grid 
                ORDER BY ABS(lat - ?) + ABS(lon - ?) 
                LIMIT 1
            """, (lat, lon))
            
            result = cursor.fetchone()
            density = result[0] if result else 50.0
            
            end_time = time.perf_counter()
            query_time = (end_time - start_time) * 1000  # ms
            
            times.append(query_time)
            results.append(density)
            
            print(f"Query ({lat:.3f}, {lon:.3f}): {query_time:.3f}ms → {density:.1f}")
    
    # Statistics
    avg_time = statistics.mean(times)
    median_time = statistics.median(times)
    max_time = max(times)
    min_time = min(times)
    
    print(f"\nQuery Performance Statistics:")
    print(f"  Average: {avg_time:.3f}ms")
    print(f"  Median:  {median_time:.3f}ms")
    print(f"  Range:   {min_time:.3f}ms - {max_time:.3f}ms")
    print(f"  Sub-3ms: {sum(1 for t in times if t < 3.0)}/{len(times)} ({sum(1 for t in times if t < 3.0)/len(times)*100:.1f}%)")
    
    # Crime range analysis
    max_crime = max(results)
    min_crime = min(results)
    crime_range = max_crime - min_crime
    
    print(f"\nCrime Score Analysis:")
    print(f"  Crime Range: {crime_range:.1f} points")
    print(f"  Highest Crime: {max_crime:.1f}")
    print(f"  Lowest Crime: {min_crime:.1f}")
    print(f"  Values: {[f'{r:.1f}' for r in results]}")
    
    return avg_time, crime_range

def test_concurrent_queries():
    """Test concurrent database access"""
    print("\n🚀 TESTING CONCURRENT DATABASE ACCESS")
    print("=" * 60)
    
    import threading
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    db_path = 'data/dealgenie.db'
    
    def worker_query(coord_id, lat, lon):
        """Worker function for concurrent queries"""
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            start_time = time.perf_counter()
            
            cursor.execute("""
                SELECT total_density_weighted 
                FROM crime_density_grid 
                ORDER BY ABS(lat - ?) + ABS(lon - ?) 
                LIMIT 1
            """, (lat, lon))
            
            result = cursor.fetchone()
            density = result[0] if result else 50.0
            
            end_time = time.perf_counter()
            query_time = (end_time - start_time) * 1000
            
            return coord_id, density, query_time
    
    # Generate test coordinates
    import random
    random.seed(42)
    test_coords = []
    for i in range(20):
        lat = random.uniform(33.7, 34.3)  # LA County range
        lon = random.uniform(-118.7, -118.1)
        test_coords.append((i, lat, lon))
    
    # Test concurrent execution
    start_total = time.perf_counter()
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(worker_query, coord_id, lat, lon) 
                  for coord_id, lat, lon in test_coords]
        
        results = []
        for future in as_completed(futures):
            coord_id, density, query_time = future.result()
            results.append((coord_id, density, query_time))
    
    end_total = time.perf_counter()
    total_time = (end_total - start_total) * 1000
    
    # Analysis
    query_times = [query_time for _, _, query_time in results]
    avg_query_time = statistics.mean(query_times)
    
    print(f"Concurrent Execution Results:")
    print(f"  Total time: {total_time:.1f}ms")
    print(f"  Queries: {len(results)}")
    print(f"  Average query time: {avg_query_time:.3f}ms")
    print(f"  Throughput: {len(results)/total_time*1000:.1f} queries/second")
    
    return total_time, avg_query_time

def main():
    """Run actual performance tests"""
    print("⚡ ACTUAL PERFORMANCE MEASUREMENT")
    print("=" * 80)
    
    # Test 1: Single query performance
    avg_time, crime_range = test_database_query_performance()
    
    # Test 2: Concurrent query performance  
    total_time, concurrent_avg = test_concurrent_queries()
    
    print("\n" + "=" * 80)
    print("📊 ACTUAL PERFORMANCE SUMMARY")
    print("=" * 80)
    
    print(f"Database Query Performance:")
    print(f"  ✅ Average query time: {avg_time:.3f}ms (target: <3ms)")
    print(f"  ✅ Crime differentiation: {crime_range:.1f} points (target: >80)")
    
    print(f"Concurrent Access Performance:")
    print(f"  ✅ Total time for 20 queries: {total_time:.1f}ms")
    print(f"  ✅ Average concurrent query: {concurrent_avg:.3f}ms")
    
    # Determine pass/fail
    db_perf_pass = avg_time < 3.0
    crime_range_pass = crime_range > 80.0
    concurrent_pass = total_time < 100.0  # 20 queries in <100ms
    
    overall_pass = db_perf_pass and crime_range_pass and concurrent_pass
    
    print(f"\nTest Results:")
    print(f"  Database Performance: {'✅ PASS' if db_perf_pass else '❌ FAIL'}")
    print(f"  Crime Differentiation: {'✅ PASS' if crime_range_pass else '❌ FAIL'}")
    print(f"  Concurrent Performance: {'✅ PASS' if concurrent_pass else '❌ FAIL'}")
    print(f"  Overall: {'✅ PASS' if overall_pass else '❌ FAIL'}")

if __name__ == "__main__":
    main()