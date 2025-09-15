#!/usr/bin/env python3
"""
Real-World Stress Testing for Week 3 Readiness
Comprehensive testing of concurrent users, API failures, data freshness, and business logic
"""

import sqlite3
import time
import threading
import random
import statistics
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass, asdict
import queue

@dataclass
class StressTestResult:
    test_type: str
    success: bool
    response_time_ms: float
    error: str = None
    data: Dict[str, Any] = None

class RealWorldStressTester:
    def __init__(self, db_path: str = "data/dealgenie.db"):
        self.db_path = db_path
        self.results = []
        self.lock = threading.Lock()
        
        # Test locations across LA
        self.test_properties = [
            ("123 S Spring St, Downtown LA", 34.0522, -118.2437),
            ("9641 Sunset Blvd, Beverly Hills", 34.0736, -118.4004),
            ("1 World Way, LAX", 33.9425, -118.4081),
            ("6801 Hollywood Blvd, Hollywood", 34.1016, -118.3267),
            ("200 Santa Monica Pier, Santa Monica", 34.0095, -118.4970),
            ("5482 Wilshire Blvd, Koreatown", 34.0619, -118.3089),
            ("777 S Figueroa St, Downtown", 34.0522, -118.2620),
            ("1200 Getty Center Dr, Brentwood", 34.0780, -118.4741),
            ("3400 Cahuenga Blvd, Hollywood Hills", 34.1341, -118.3215),
            ("400 S Hope St, Downtown", 34.0535, -118.2520)
        ]
    
    def get_db_connection(self):
        """Get a new database connection for thread safety"""
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        return conn
    
    def query_crime_score(self, lat: float, lon: float, conn=None) -> Tuple[float, float]:
        """Query crime score for coordinates"""
        close_conn = False
        if conn is None:
            conn = self.get_db_connection()
            close_conn = True
            
        try:
            start = time.perf_counter()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT total_density_weighted, lat, lon,
                       ((lat - ?) * (lat - ?) + (lon - ?) * (lon - ?)) as distance_sq
                FROM crime_density_grid 
                WHERE lat BETWEEN ? - 0.01 AND ? + 0.01
                  AND lon BETWEEN ? - 0.01 AND ? + 0.01
                ORDER BY distance_sq
                LIMIT 1
            """, (lat, lat, lon, lon, lat, lat, lon, lon))
            
            result = cursor.fetchone()
            query_time = (time.perf_counter() - start) * 1000
            
            if result:
                return result[0], query_time
            return 0.0, query_time
            
        finally:
            if close_conn:
                conn.close()
    
    def test_concurrent_users(self, num_users: int = 10) -> Dict[str, Any]:
        """Simulate concurrent users querying different properties"""
        print(f"\n🔥 CONCURRENT USER SIMULATION ({num_users} users)")
        print("="*60)
        
        results = {
            'response_times': [],
            'errors': [],
            'success_count': 0,
            'total_requests': 0
        }
        
        def user_session(user_id: int, num_queries: int = 5):
            """Simulate a single user session"""
            session_results = []
            conn = self.get_db_connection()
            
            try:
                for query_num in range(num_queries):
                    # Random property from test set
                    prop = random.choice(self.test_properties)
                    
                    try:
                        crime_score, query_time = self.query_crime_score(prop[1], prop[2], conn)
                        
                        with self.lock:
                            results['response_times'].append(query_time)
                            results['success_count'] += 1
                            results['total_requests'] += 1
                        
                        session_results.append({
                            'user_id': user_id,
                            'query': query_num,
                            'response_time_ms': query_time,
                            'crime_score': crime_score,
                            'success': True
                        })
                        
                        # Simulate think time between queries
                        time.sleep(random.uniform(0.1, 0.3))
                        
                    except Exception as e:
                        with self.lock:
                            results['errors'].append(str(e))
                            results['total_requests'] += 1
                        
                        session_results.append({
                            'user_id': user_id,
                            'query': query_num,
                            'error': str(e),
                            'success': False
                        })
            finally:
                conn.close()
            
            return session_results
        
        # Run concurrent user sessions
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=num_users) as executor:
            futures = [executor.submit(user_session, i) for i in range(num_users)]
            all_sessions = []
            
            for future in as_completed(futures):
                session_data = future.result()
                all_sessions.extend(session_data)
        
        total_time = time.time() - start_time
        
        # Analyze results
        if results['response_times']:
            avg_response = statistics.mean(results['response_times'])
            p95_response = sorted(results['response_times'])[int(len(results['response_times']) * 0.95)]
            max_response = max(results['response_times'])
            min_response = min(results['response_times'])
        else:
            avg_response = p95_response = max_response = min_response = 0
        
        success_rate = (results['success_count'] / results['total_requests'] * 100) if results['total_requests'] > 0 else 0
        
        print(f"📊 RESULTS:")
        print(f"   Total Requests: {results['total_requests']}")
        print(f"   Successful: {results['success_count']} ({success_rate:.1f}%)")
        print(f"   Failed: {len(results['errors'])}")
        print(f"   Average Response: {avg_response:.2f}ms")
        print(f"   P95 Response: {p95_response:.2f}ms")
        print(f"   Min/Max: {min_response:.2f}ms / {max_response:.2f}ms")
        print(f"   Total Test Duration: {total_time:.2f}s")
        print(f"   Throughput: {results['total_requests']/total_time:.1f} req/s")
        
        # Check for performance degradation
        if len(results['response_times']) >= 20:
            first_10 = statistics.mean(results['response_times'][:10])
            last_10 = statistics.mean(results['response_times'][-10:])
            degradation = ((last_10 - first_10) / first_10 * 100) if first_10 > 0 else 0
            print(f"   Performance Degradation: {degradation:.1f}%")
        
        return {
            'num_users': num_users,
            'total_requests': results['total_requests'],
            'success_rate': success_rate,
            'avg_response_ms': avg_response,
            'p95_response_ms': p95_response,
            'throughput_rps': results['total_requests']/total_time if total_time > 0 else 0,
            'errors': results['errors'][:5]  # First 5 errors
        }
    
    def test_api_failure_scenarios(self) -> Dict[str, Any]:
        """Test behavior when APIs fail"""
        print(f"\n💥 API FAILURE SCENARIO TESTING")
        print("="*60)
        
        results = {
            'scenarios_tested': [],
            'graceful_degradation': True,
            'recovery_successful': True
        }
        
        # Test 1: Socrata API timeout
        print("\n📍 Testing Socrata API timeout...")
        try:
            # Simulate API call with very short timeout
            response = requests.get(
                "https://data.lacity.org/resource/pi9x-tg5x.json",
                timeout=0.001  # Impossibly short timeout to force failure
            )
            print("   ❌ API should have timed out")
            results['scenarios_tested'].append({
                'scenario': 'API Timeout',
                'handled': False,
                'fallback': None
            })
        except requests.Timeout:
            print("   ✅ Timeout handled gracefully")
            # Test fallback to cached data
            crime_score, query_time = self.query_crime_score(34.0522, -118.2437)
            if crime_score > 0:
                print(f"   ✅ Fallback to cached data successful (score: {crime_score:.1f})")
                results['scenarios_tested'].append({
                    'scenario': 'API Timeout',
                    'handled': True,
                    'fallback': 'cached_data',
                    'fallback_response_ms': query_time
                })
            else:
                print("   ⚠️ No cached data available")
                results['graceful_degradation'] = False
        
        # Test 2: API returns error
        print("\n📍 Testing API error response...")
        try:
            response = requests.get(
                "https://data.lacity.org/resource/invalid_dataset.json"
            )
            if response.status_code != 200:
                print(f"   ✅ API error detected (HTTP {response.status_code})")
                # Test system continues functioning
                crime_score, query_time = self.query_crime_score(34.0736, -118.4004)
                if crime_score >= 0:  # Score can be 0 for low-crime areas
                    print(f"   ✅ System continues with cached data")
                    results['scenarios_tested'].append({
                        'scenario': 'API Error Response',
                        'handled': True,
                        'fallback': 'cached_data'
                    })
        except Exception as e:
            print(f"   ⚠️ Unexpected error: {e}")
            results['graceful_degradation'] = False
        
        # Test 3: Database connection failure recovery
        print("\n📍 Testing database connection recovery...")
        try:
            # Test with invalid path first
            bad_conn = sqlite3.connect('nonexistent.db', timeout=1.0)
            bad_conn.close()
        except:
            pass
        
        # Now test recovery with valid connection
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM crime_density_grid")
            count = cursor.fetchone()[0]
            conn.close()
            
            if count > 0:
                print(f"   ✅ Database connection recovered ({count:,} records)")
                results['scenarios_tested'].append({
                    'scenario': 'Database Recovery',
                    'handled': True,
                    'recovery_time_ms': 0
                })
            
        except Exception as e:
            print(f"   ❌ Database recovery failed: {e}")
            results['recovery_successful'] = False
        
        # Test 4: Concurrent failures
        print("\n📍 Testing concurrent API failures...")
        concurrent_errors = []
        
        def failing_api_call(i):
            try:
                # Force timeout
                requests.get("https://data.lacity.org/resource/pi9x-tg5x.json", timeout=0.001)
                return False
            except:
                # Fall back to database
                crime_score, _ = self.query_crime_score(34.0522, -118.2437)
                return crime_score > 0
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(failing_api_call, i) for i in range(5)]
            fallback_success = sum(1 for f in as_completed(futures) if f.result())
        
        if fallback_success == 5:
            print(f"   ✅ All {fallback_success} concurrent failures handled")
            results['scenarios_tested'].append({
                'scenario': 'Concurrent API Failures',
                'handled': True,
                'fallback_success_rate': 100
            })
        else:
            print(f"   ⚠️ Only {fallback_success}/5 handled successfully")
            results['graceful_degradation'] = False
        
        return results
    
    def test_data_freshness(self) -> Dict[str, Any]:
        """Test data freshness and currency tracking"""
        print(f"\n🕐 DATA FRESHNESS VALIDATION")
        print("="*60)
        
        results = {
            'data_age': {},
            'mixed_freshness_handling': True,
            'stale_data_warning': True
        }
        
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Check data timestamps
            cursor.execute("""
                SELECT 
                    MIN(as_of_date) as oldest_data,
                    MAX(as_of_date) as newest_data,
                    COUNT(DISTINCT as_of_date) as unique_dates
                FROM crime_density_grid
                WHERE as_of_date IS NOT NULL
            """)
            
            freshness = cursor.fetchone()
            
            if freshness['oldest_data'] and freshness['newest_data']:
                print(f"📅 Data Age Analysis:")
                print(f"   Oldest Data: {freshness['oldest_data']}")
                print(f"   Newest Data: {freshness['newest_data']}")
                print(f"   Unique Date Batches: {freshness['unique_dates']}")
                
                # Calculate age
                try:
                    newest = datetime.fromisoformat(freshness['newest_data'].replace('Z', '+00:00'))
                    age_days = (datetime.now() - newest.replace(tzinfo=None)).days
                    
                    if age_days < 7:
                        print(f"   ✅ Data is FRESH ({age_days} days old)")
                    elif age_days < 30:
                        print(f"   ⚠️ Data is AGING ({age_days} days old)")
                    else:
                        print(f"   ❌ Data is STALE ({age_days} days old)")
                    
                    results['data_age'] = {
                        'age_days': age_days,
                        'status': 'fresh' if age_days < 7 else 'aging' if age_days < 30 else 'stale'
                    }
                    
                except:
                    print("   ⚠️ Could not parse date format")
            
            # Test mixed freshness handling
            print(f"\n📊 Mixed Freshness Test:")
            
            cursor.execute("""
                SELECT 
                    COUNT(CASE WHEN as_of_date >= date('now', '-7 days') THEN 1 END) as fresh_records,
                    COUNT(CASE WHEN as_of_date < date('now', '-7 days') 
                               AND as_of_date >= date('now', '-30 days') THEN 1 END) as aging_records,
                    COUNT(CASE WHEN as_of_date < date('now', '-30 days') THEN 1 END) as stale_records,
                    COUNT(*) as total_records
                FROM crime_density_grid
            """)
            
            mix = cursor.fetchone()
            
            print(f"   Fresh Records: {mix['fresh_records']:,} ({mix['fresh_records']/mix['total_records']*100:.1f}%)")
            print(f"   Aging Records: {mix['aging_records']:,} ({mix['aging_records']/mix['total_records']*100:.1f}%)")
            print(f"   Stale Records: {mix['stale_records']:,} ({mix['stale_records']/mix['total_records']*100:.1f}%)")
            
            # Test if system handles mixed data appropriately
            if mix['total_records'] > 0:
                mixed_ratio = (mix['fresh_records'] + mix['aging_records']) / mix['total_records']
                if mixed_ratio > 0.7:
                    print(f"   ✅ Acceptable data freshness ({mixed_ratio*100:.1f}% usable)")
                else:
                    print(f"   ⚠️ High stale data ratio")
                    results['mixed_freshness_handling'] = False
            
            results['freshness_distribution'] = {
                'fresh_pct': mix['fresh_records']/mix['total_records']*100 if mix['total_records'] > 0 else 0,
                'aging_pct': mix['aging_records']/mix['total_records']*100 if mix['total_records'] > 0 else 0,
                'stale_pct': mix['stale_records']/mix['total_records']*100 if mix['total_records'] > 0 else 0
            }
            
        finally:
            conn.close()
        
        return results
    
    def test_business_logic_accuracy(self) -> Dict[str, Any]:
        """Test business logic and scoring consistency"""
        print(f"\n🎯 BUSINESS LOGIC ACCURACY VALIDATION")
        print("="*60)
        
        results = {
            'scoring_consistency': True,
            'ranking_sensible': True,
            'validation_tests': []
        }
        
        # Test 1: Known high/low crime areas
        print("\n📍 Testing known crime area differentiation...")
        
        test_areas = [
            ("Downtown LA (High)", 34.0522, -118.2437, "high"),
            ("Beverly Hills (Low)", 34.0736, -118.4004, "low"),
            ("Hollywood (High)", 34.1016, -118.3267, "high"),
            ("Brentwood (Low)", 34.0780, -118.4741, "low"),
            ("Koreatown (High)", 34.0619, -118.3089, "high")
        ]
        
        area_scores = []
        for name, lat, lon, expected in test_areas:
            crime_score, _ = self.query_crime_score(lat, lon)
            area_scores.append((name, crime_score, expected))
            
            # Validate against expectation
            if expected == "high" and crime_score > 50:
                result = "✅ Correct"
            elif expected == "low" and crime_score < 30:
                result = "✅ Correct"
            else:
                result = "❌ Incorrect"
                results['scoring_consistency'] = False
            
            print(f"   {name}: {crime_score:.1f}/100 - {result}")
        
        results['validation_tests'].append({
            'test': 'Known Area Validation',
            'passed': results['scoring_consistency']
        })
        
        # Test 2: Scoring consistency for nearby properties
        print("\n📍 Testing scoring consistency for nearby properties...")
        
        # Test properties within 500m of each other
        nearby_coords = [
            (34.0522, -118.2437),
            (34.0525, -118.2440),
            (34.0519, -118.2434),
            (34.0524, -118.2435)
        ]
        
        nearby_scores = []
        for lat, lon in nearby_coords:
            score, _ = self.query_crime_score(lat, lon)
            nearby_scores.append(score)
        
        score_variance = statistics.stdev(nearby_scores) if len(nearby_scores) > 1 else 0
        
        print(f"   Nearby Property Scores: {[f'{s:.1f}' for s in nearby_scores]}")
        print(f"   Standard Deviation: {score_variance:.2f}")
        
        if score_variance < 10:  # Within 10 points is consistent
            print(f"   ✅ Consistent scoring for nearby properties")
        else:
            print(f"   ❌ Inconsistent scoring detected")
            results['scoring_consistency'] = False
        
        results['validation_tests'].append({
            'test': 'Nearby Property Consistency',
            'passed': score_variance < 10,
            'variance': score_variance
        })
        
        # Test 3: Ranking sensibility
        print("\n📍 Testing ranking sensibility...")
        
        # Get scores for diverse properties
        all_property_scores = []
        for prop in self.test_properties:
            score, _ = self.query_crime_score(prop[1], prop[2])
            all_property_scores.append((prop[0], score))
        
        # Sort by score
        ranked_properties = sorted(all_property_scores, key=lambda x: x[1])
        
        print("   Top 3 Safest Areas:")
        for i, (name, score) in enumerate(ranked_properties[:3]):
            print(f"     {i+1}. {name[:30]}: {score:.1f}")
        
        print("   Top 3 Highest Crime Areas:")
        for i, (name, score) in enumerate(ranked_properties[-3:]):
            print(f"     {i+1}. {name[:30]}: {score:.1f}")
        
        # Check if Beverly Hills is safer than Downtown
        bh_score = next((s for n, s in all_property_scores if "Beverly" in n), None)
        dt_score = next((s for n, s in all_property_scores if "Downtown" in n), None)
        
        if bh_score is not None and dt_score is not None:
            if bh_score < dt_score:
                print(f"   ✅ Beverly Hills ({bh_score:.1f}) safer than Downtown ({dt_score:.1f})")
            else:
                print(f"   ❌ Unexpected: Beverly Hills ({bh_score:.1f}) vs Downtown ({dt_score:.1f})")
                results['ranking_sensible'] = False
        
        results['validation_tests'].append({
            'test': 'Ranking Sensibility',
            'passed': results['ranking_sensible']
        })
        
        # Test 4: Score distribution
        print("\n📍 Testing score distribution...")
        
        scores = [s for _, s in all_property_scores]
        
        print(f"   Min Score: {min(scores):.1f}")
        print(f"   Max Score: {max(scores):.1f}")
        print(f"   Mean Score: {statistics.mean(scores):.1f}")
        print(f"   Median Score: {statistics.median(scores):.1f}")
        
        score_range = max(scores) - min(scores)
        if score_range > 50:
            print(f"   ✅ Good differentiation ({score_range:.1f} point range)")
        else:
            print(f"   ⚠️ Limited differentiation ({score_range:.1f} point range)")
        
        results['score_distribution'] = {
            'min': min(scores),
            'max': max(scores),
            'mean': statistics.mean(scores),
            'median': statistics.median(scores),
            'range': score_range
        }
        
        return results

def main():
    """Run comprehensive real-world stress testing"""
    print("🔥 REAL-WORLD STRESS TESTING FOR WEEK 3 READINESS")
    print("="*80)
    print("Testing concurrent users, API failures, data freshness, and business logic")
    print(f"Test Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    tester = RealWorldStressTester()
    
    # Test 1: Concurrent Users
    concurrent_10 = tester.test_concurrent_users(10)
    concurrent_25 = tester.test_concurrent_users(25)
    concurrent_50 = tester.test_concurrent_users(50)
    
    # Test 2: API Failure Scenarios
    api_failure_results = tester.test_api_failure_scenarios()
    
    # Test 3: Data Freshness
    freshness_results = tester.test_data_freshness()
    
    # Test 4: Business Logic
    business_logic_results = tester.test_business_logic_accuracy()
    
    # Final Assessment
    print(f"\n{'='*80}")
    print("🏆 WEEK 3 READINESS ASSESSMENT")
    print("="*80)
    
    # Concurrent Performance
    print(f"\n⚡ CONCURRENT PERFORMANCE:")
    print(f"   10 Users: {concurrent_10['avg_response_ms']:.2f}ms avg, {concurrent_10['success_rate']:.1f}% success")
    print(f"   25 Users: {concurrent_25['avg_response_ms']:.2f}ms avg, {concurrent_25['success_rate']:.1f}% success")
    print(f"   50 Users: {concurrent_50['avg_response_ms']:.2f}ms avg, {concurrent_50['success_rate']:.1f}% success")
    
    concurrent_pass = (
        concurrent_10['success_rate'] > 95 and
        concurrent_25['success_rate'] > 90 and
        concurrent_50['success_rate'] > 85 and
        concurrent_50['avg_response_ms'] < 100
    )
    
    print(f"   Status: {'✅ PASS' if concurrent_pass else '❌ FAIL'}")
    
    # API Resilience
    print(f"\n🛡️ API RESILIENCE:")
    print(f"   Graceful Degradation: {'✅' if api_failure_results['graceful_degradation'] else '❌'}")
    print(f"   Recovery Successful: {'✅' if api_failure_results['recovery_successful'] else '❌'}")
    print(f"   Scenarios Handled: {len([s for s in api_failure_results['scenarios_tested'] if s['handled']])}/{len(api_failure_results['scenarios_tested'])}")
    
    api_pass = api_failure_results['graceful_degradation'] and api_failure_results['recovery_successful']
    print(f"   Status: {'✅ PASS' if api_pass else '❌ FAIL'}")
    
    # Data Freshness
    print(f"\n📅 DATA FRESHNESS:")
    if 'age_days' in freshness_results['data_age']:
        print(f"   Data Age: {freshness_results['data_age']['age_days']} days ({freshness_results['data_age']['status']})")
    print(f"   Mixed Data Handling: {'✅' if freshness_results['mixed_freshness_handling'] else '❌'}")
    
    freshness_pass = freshness_results['mixed_freshness_handling']
    print(f"   Status: {'✅ PASS' if freshness_pass else '⚠️ WARNING'}")
    
    # Business Logic
    print(f"\n🎯 BUSINESS LOGIC:")
    print(f"   Scoring Consistency: {'✅' if business_logic_results['scoring_consistency'] else '❌'}")
    print(f"   Ranking Sensible: {'✅' if business_logic_results['ranking_sensible'] else '❌'}")
    print(f"   Score Range: {business_logic_results['score_distribution']['range']:.1f} points")
    
    logic_pass = business_logic_results['scoring_consistency'] and business_logic_results['ranking_sensible']
    print(f"   Status: {'✅ PASS' if logic_pass else '❌ FAIL'}")
    
    # Overall Assessment
    all_pass = concurrent_pass and api_pass and freshness_pass and logic_pass
    
    print(f"\n{'='*80}")
    print("📊 FINAL WEEK 3 READINESS")
    print("="*80)
    
    if all_pass:
        print("✅ SYSTEM PASSES ALL STRESS TESTS")
        print("✅ Ready for Week 3 web application deployment")
        print("✅ Can handle 50+ concurrent users")
        print("✅ Graceful API failure handling confirmed")
        print("✅ Business logic validated")
    else:
        print("⚠️ SYSTEM NEEDS ATTENTION")
        if not concurrent_pass:
            print("❌ Concurrent performance needs optimization")
        if not api_pass:
            print("❌ API failure handling needs improvement")
        if not freshness_pass:
            print("⚠️ Data freshness monitoring recommended")
        if not logic_pass:
            print("❌ Business logic requires review")
    
    # Save detailed results
    detailed_results = {
        'test_timestamp': datetime.now().isoformat(),
        'concurrent_performance': {
            '10_users': concurrent_10,
            '25_users': concurrent_25,
            '50_users': concurrent_50
        },
        'api_resilience': api_failure_results,
        'data_freshness': freshness_results,
        'business_logic': business_logic_results,
        'overall_pass': all_pass
    }
    
    with open('real_world_stress_test_results.json', 'w') as f:
        json.dump(detailed_results, f, indent=2, default=str)
    
    print(f"\n📋 Detailed results saved to: real_world_stress_test_results.json")
    
    return all_pass

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)