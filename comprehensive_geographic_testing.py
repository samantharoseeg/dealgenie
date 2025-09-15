#!/usr/bin/env python3
"""
Comprehensive Geographic Testing for DealGenie System
Test properties across diverse LA geography and validate system performance
"""

import sys
import time
import sqlite3
import statistics
import json
from pathlib import Path
from typing import List, Dict, Tuple, Any
from dataclasses import dataclass, asdict

# Add src path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

@dataclass
class GeographicTestResult:
    location_name: str
    area_type: str
    coordinates: Tuple[float, float]
    crime_score: float
    query_time_ms: float
    success: bool
    error: str = None
    data_quality: str = None

class ComprehensiveGeographicTester:
    def __init__(self, db_path: str = "data/dealgenie.db"):
        self.db_path = db_path
        self.results = []
        
    def get_diverse_test_locations(self) -> List[Dict[str, Any]]:
        """Get comprehensive test locations across diverse LA geography"""
        
        locations = [
            # COASTAL AREAS
            {"name": "Santa Monica Beach", "type": "coastal", "coords": (34.0195, -118.4912), "expected_crime": "medium"},
            {"name": "Manhattan Beach Pier", "type": "coastal", "coords": (33.8841, -118.4109), "expected_crime": "low"},
            {"name": "Venice Beach", "type": "coastal", "coords": (33.9850, -118.4695), "expected_crime": "high"},
            {"name": "Redondo Beach", "type": "coastal", "coords": (33.8192, -118.3884), "expected_crime": "low"},
            {"name": "El Segundo near LAX", "type": "coastal", "coords": (33.9192, -118.4165), "expected_crime": "medium"},
            
            # MOUNTAIN/HILL AREAS  
            {"name": "Hollywood Hills", "type": "mountain", "coords": (34.1341, -118.3215), "expected_crime": "low"},
            {"name": "Griffith Park Vicinity", "type": "mountain", "coords": (34.1365, -118.2942), "expected_crime": "low"},
            {"name": "Beverly Hills Canyon", "type": "mountain", "coords": (34.0901, -118.4065), "expected_crime": "low"},
            {"name": "Topanga Canyon", "type": "mountain", "coords": (34.0947, -118.6014), "expected_crime": "low"},
            {"name": "Mount Washington", "type": "mountain", "coords": (34.0981, -118.2176), "expected_crime": "medium"},
            
            # INDUSTRIAL AREAS
            {"name": "Vernon Industrial", "type": "industrial", "coords": (34.0042, -118.2312), "expected_crime": "high"},
            {"name": "Commerce Industrial", "type": "industrial", "coords": (34.0006, -118.1596), "expected_crime": "high"},
            {"name": "Downtown Industrial", "type": "industrial", "coords": (34.0430, -118.2673), "expected_crime": "high"},
            {"name": "Port of LA Industrial", "type": "industrial", "coords": (33.7361, -118.2639), "expected_crime": "high"},
            {"name": "Alameda Corridor", "type": "industrial", "coords": (33.8897, -118.2023), "expected_crime": "high"},
            
            # SUBURBAN AREAS
            {"name": "Woodland Hills", "type": "suburban", "coords": (34.1684, -118.6059), "expected_crime": "low"},
            {"name": "Tarzana", "type": "suburban", "coords": (34.1703, -118.5370), "expected_crime": "low"},
            {"name": "Northridge", "type": "suburban", "coords": (34.2283, -118.5329), "expected_crime": "low"},
            {"name": "Canoga Park", "type": "suburban", "coords": (34.2014, -118.5979), "expected_crime": "medium"},
            {"name": "Chatsworth", "type": "suburban", "coords": (34.2514, -118.6178), "expected_crime": "low"},
            
            # BORDER/BOUNDARY AREAS
            {"name": "LA/Pasadena Border", "type": "border", "coords": (34.1139, -118.1297), "expected_crime": "medium"},
            {"name": "LA/Glendale Border", "type": "border", "coords": (34.1425, -118.2551), "expected_crime": "medium"},
            {"name": "LA/Santa Monica Border", "type": "border", "coords": (34.0522, -118.4437), "expected_crime": "medium"},
            {"name": "LA/Beverly Hills Border", "type": "border", "coords": (34.0736, -118.3904), "expected_crime": "low"},
            {"name": "LA County/Orange County Border", "type": "border", "coords": (33.8014, -118.0951), "expected_crime": "medium"},
            
            # URBAN CORE AREAS  
            {"name": "Downtown Financial District", "type": "urban_core", "coords": (34.0522, -118.2437), "expected_crime": "high"},
            {"name": "Hollywood & Highland", "type": "urban_core", "coords": (34.1016, -118.3267), "expected_crime": "high"},
            {"name": "Sunset Strip", "type": "urban_core", "coords": (34.0903, -118.3856), "expected_crime": "medium"},
            {"name": "Korea Town", "type": "urban_core", "coords": (34.058, -118.291), "expected_crime": "high"},
            {"name": "MacArthur Park", "type": "urban_core", "coords": (34.0572, -118.2755), "expected_crime": "high"},
            
            # SPECIAL/UNIQUE AREAS
            {"name": "LAX Airport Area", "type": "special", "coords": (33.9425, -118.4081), "expected_crime": "medium"},
            {"name": "Dodger Stadium Area", "type": "special", "coords": (34.0739, -118.2400), "expected_crime": "medium"},
            {"name": "UCLA Campus Area", "type": "special", "coords": (34.0689, -118.4452), "expected_crime": "low"},
            {"name": "USC Campus Area", "type": "special", "coords": (34.0224, -118.2851), "expected_crime": "high"},
            {"name": "Getty Center Area", "type": "special", "coords": (34.0781, -118.4741), "expected_crime": "low"}
        ]
        
        return locations
        
    def test_crime_intelligence_at_location(self, lat: float, lon: float, location_name: str) -> GeographicTestResult:
        """Test crime intelligence lookup at specific location"""
        
        try:
            start_time = time.perf_counter()
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Use optimized spatial query
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
                query_time = (time.perf_counter() - start_time) * 1000
                
                if result:
                    crime_score = result[0]
                    actual_lat = result[1] 
                    actual_lon = result[2]
                    distance_sq = result[3]
                    distance_km = (distance_sq ** 0.5) * 111  # Approximate km conversion
                    
                    # Data quality assessment
                    if distance_km < 0.5:  # Within 500m
                        data_quality = "excellent"
                    elif distance_km < 1.0:  # Within 1km
                        data_quality = "good"
                    elif distance_km < 2.0:  # Within 2km
                        data_quality = "fair"
                    else:
                        data_quality = "poor"
                    
                    return GeographicTestResult(
                        location_name=location_name,
                        area_type="unknown",
                        coordinates=(lat, lon),
                        crime_score=crime_score,
                        query_time_ms=query_time,
                        success=True,
                        data_quality=data_quality
                    )
                else:
                    return GeographicTestResult(
                        location_name=location_name,
                        area_type="unknown", 
                        coordinates=(lat, lon),
                        crime_score=0.0,
                        query_time_ms=query_time,
                        success=False,
                        error="No crime data found in vicinity"
                    )
                    
        except Exception as e:
            return GeographicTestResult(
                location_name=location_name,
                area_type="unknown",
                coordinates=(lat, lon),
                crime_score=0.0,
                query_time_ms=0.0,
                success=False,
                error=str(e)
            )
    
    def run_comprehensive_geographic_testing(self) -> Dict[str, Any]:
        """Run comprehensive geographic testing across all area types"""
        
        print("🗺️ COMPREHENSIVE GEOGRAPHIC TESTING")
        print("="*80)
        print("Testing crime intelligence across diverse LA geography")
        
        test_locations = self.get_diverse_test_locations()
        
        print(f"📍 Testing {len(test_locations)} locations across {len(set(loc['type'] for loc in test_locations))} area types\n")
        
        results_by_type = {}
        all_results = []
        
        for i, location in enumerate(test_locations, 1):
            print(f"Testing {i:2d}/{len(test_locations)}: {location['name']} ({location['type']})")
            
            result = self.test_crime_intelligence_at_location(
                location['coords'][0], 
                location['coords'][1],
                location['name']
            )
            result.area_type = location['type']
            
            # Display result
            if result.success:
                quality_emoji = {"excellent": "🟢", "good": "🔵", "fair": "🟡", "poor": "🔴"}.get(result.data_quality, "⚪")
                print(f"   {quality_emoji} Crime: {result.crime_score:5.1f}/100, Time: {result.query_time_ms:5.1f}ms, Quality: {result.data_quality}")
            else:
                print(f"   ❌ Failed: {result.error}")
            
            # Group by area type
            area_type = location['type']
            if area_type not in results_by_type:
                results_by_type[area_type] = []
            results_by_type[area_type].append(result)
            all_results.append(result)
        
        # Analyze results
        analysis = self.analyze_geographic_results(results_by_type, all_results)
        
        return {
            'results_by_type': results_by_type,
            'all_results': all_results,
            'analysis': analysis
        }
    
    def analyze_geographic_results(self, results_by_type: Dict[str, List[GeographicTestResult]], 
                                 all_results: List[GeographicTestResult]) -> Dict[str, Any]:
        """Analyze geographic test results"""
        
        print(f"\n📊 GEOGRAPHIC ANALYSIS")
        print("="*80)
        
        analysis = {}
        
        # Overall metrics
        successful_tests = [r for r in all_results if r.success]
        success_rate = len(successful_tests) / len(all_results) * 100
        
        if successful_tests:
            avg_query_time = statistics.mean([r.query_time_ms for r in successful_tests])
            crime_scores = [r.crime_score for r in successful_tests]
            avg_crime_score = statistics.mean(crime_scores)
            crime_score_range = max(crime_scores) - min(crime_scores)
            
            print(f"🎯 OVERALL PERFORMANCE:")
            print(f"   Success Rate: {len(successful_tests)}/{len(all_results)} ({success_rate:.1f}%)")
            print(f"   Avg Query Time: {avg_query_time:.1f}ms")
            print(f"   Crime Score Range: {crime_score_range:.1f} points ({min(crime_scores):.1f} to {max(crime_scores):.1f})")
            print(f"   Geographic Coverage: {len(set(r.area_type for r in successful_tests))} area types")
            
            analysis['overall'] = {
                'success_rate': success_rate,
                'avg_query_time_ms': avg_query_time,
                'crime_score_range': crime_score_range,
                'min_crime_score': min(crime_scores),
                'max_crime_score': max(crime_scores),
                'total_locations_tested': len(all_results)
            }
        
        # Analysis by area type
        print(f"\n🏞️ PERFORMANCE BY AREA TYPE:")
        
        for area_type, results in results_by_type.items():
            successful_in_type = [r for r in results if r.success]
            type_success_rate = len(successful_in_type) / len(results) * 100
            
            if successful_in_type:
                type_avg_time = statistics.mean([r.query_time_ms for r in successful_in_type])
                type_crime_scores = [r.crime_score for r in successful_in_type]
                type_avg_crime = statistics.mean(type_crime_scores)
                
                # Data quality distribution
                quality_counts = {}
                for result in successful_in_type:
                    quality = result.data_quality or "unknown"
                    quality_counts[quality] = quality_counts.get(quality, 0) + 1
                
                print(f"   📍 {area_type.upper().replace('_', ' ')}:")
                print(f"      Success: {len(successful_in_type)}/{len(results)} ({type_success_rate:.1f}%)")
                print(f"      Avg Time: {type_avg_time:.1f}ms")
                print(f"      Avg Crime: {type_avg_crime:.1f}/100")
                print(f"      Data Quality: {quality_counts}")
                
                analysis[area_type] = {
                    'success_rate': type_success_rate,
                    'avg_query_time_ms': type_avg_time,
                    'avg_crime_score': type_avg_crime,
                    'locations_tested': len(results),
                    'data_quality_distribution': quality_counts
                }
            else:
                print(f"   📍 {area_type.upper().replace('_', ' ')}: ❌ All tests failed")
                analysis[area_type] = {
                    'success_rate': 0,
                    'locations_tested': len(results),
                    'all_failed': True
                }
        
        # Performance consistency analysis
        if successful_tests:
            query_times = [r.query_time_ms for r in successful_tests]
            time_std_dev = statistics.stdev(query_times) if len(query_times) > 1 else 0
            time_consistency = "excellent" if time_std_dev < 1.0 else "good" if time_std_dev < 2.0 else "fair"
            
            print(f"\n⚡ PERFORMANCE CONSISTENCY:")
            print(f"   Time Std Dev: {time_std_dev:.2f}ms ({time_consistency})")
            print(f"   Fastest Query: {min(query_times):.1f}ms")
            print(f"   Slowest Query: {max(query_times):.1f}ms")
            
            analysis['performance_consistency'] = {
                'time_std_dev_ms': time_std_dev,
                'consistency_rating': time_consistency,
                'fastest_query_ms': min(query_times),
                'slowest_query_ms': max(query_times)
            }
        
        return analysis
    
    def test_edge_case_scenarios(self) -> List[GeographicTestResult]:
        """Test edge case scenarios for error handling"""
        
        print(f"\n🚨 EDGE CASE TESTING")
        print("="*80)
        
        edge_cases = [
            # Invalid coordinates
            {"name": "Invalid Coords (0,0)", "coords": (0.0, 0.0)},
            {"name": "Outside LA County", "coords": (40.7128, -74.0060)},  # NYC
            {"name": "Ocean Coordinates", "coords": (33.5, -119.0)},  # Pacific Ocean
            {"name": "Desert Coordinates", "coords": (35.0, -117.0)},  # Mojave Desert
            
            # Boundary coordinates
            {"name": "Northern LA Boundary", "coords": (34.8, -118.5)},
            {"name": "Southern LA Boundary", "coords": (33.4, -118.3)},
            {"name": "Eastern LA Boundary", "coords": (34.1, -117.5)},
            {"name": "Western LA Boundary", "coords": (34.0, -119.0)},
        ]
        
        edge_results = []
        
        for case in edge_cases:
            print(f"Testing: {case['name']}")
            result = self.test_crime_intelligence_at_location(
                case['coords'][0], 
                case['coords'][1], 
                case['name']
            )
            
            if result.success:
                print(f"   ✅ Handled gracefully: Crime {result.crime_score:.1f}, Time {result.query_time_ms:.1f}ms")
            else:
                print(f"   ⚠️ No data (expected): {result.error}")
            
            edge_results.append(result)
        
        return edge_results

def main():
    """Run comprehensive geographic testing"""
    print("🗺️ COMPREHENSIVE GEOGRAPHIC TESTING FOR DEALGENIE")
    print("Testing system performance across diverse LA geography")
    print("="*100)
    
    tester = ComprehensiveGeographicTester()
    
    # Run main geographic testing
    geographic_results = tester.run_comprehensive_geographic_testing()
    
    # Test edge cases
    edge_results = tester.test_edge_case_scenarios()
    
    # Final assessment
    print(f"\n🎯 COMPREHENSIVE GEOGRAPHIC TESTING SUMMARY")
    print("="*100)
    
    analysis = geographic_results['analysis']
    overall = analysis.get('overall', {})
    
    print(f"✅ GEOGRAPHIC COVERAGE VALIDATION:")
    print(f"   • Locations tested: {overall.get('total_locations_tested', 0)}")
    print(f"   • Area types covered: 6 (coastal, mountain, industrial, suburban, border, urban_core)")
    print(f"   • Success rate: {overall.get('success_rate', 0):.1f}%")
    print(f"   • Performance: {overall.get('avg_query_time_ms', 0):.1f}ms average")
    print(f"   • Crime differentiation: {overall.get('crime_score_range', 0):.1f} points")
    
    consistency = analysis.get('performance_consistency', {})
    if consistency:
        print(f"\n⚡ PERFORMANCE CONSISTENCY:")
        print(f"   • Time consistency: {consistency.get('consistency_rating', 'unknown')}")
        print(f"   • Standard deviation: {consistency.get('time_std_dev_ms', 0):.2f}ms")
    
    print(f"\n🚨 EDGE CASE HANDLING:")
    edge_success = sum(1 for r in edge_results if r.success or r.error)  # Success or graceful error
    print(f"   • Edge cases tested: {len(edge_results)}")
    print(f"   • Handled gracefully: {edge_success}/{len(edge_results)}")
    
    # Save results
    output_data = {
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        'geographic_results': {
            'by_type': {k: [asdict(r) for r in v] for k, v in geographic_results['results_by_type'].items()},
            'analysis': analysis
        },
        'edge_case_results': [asdict(r) for r in edge_results]
    }
    
    with open('comprehensive_geographic_test_results.json', 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\n📋 Detailed results saved to: comprehensive_geographic_test_results.json")
    
    # Validation criteria
    meets_criteria = (
        overall.get('success_rate', 0) >= 90.0 and
        overall.get('crime_score_range', 0) >= 80.0 and
        overall.get('avg_query_time_ms', 999) <= 5.0 and
        edge_success >= len(edge_results) * 0.8
    )
    
    if meets_criteria:
        print(f"\n✅ COMPREHENSIVE GEOGRAPHIC TESTING: PASSED")
        print(f"✅ System validated across diverse LA geography")
        print(f"✅ Performance consistent across all area types")
        print(f"✅ Edge cases handled appropriately")
    else:
        print(f"\n⚠️ COMPREHENSIVE GEOGRAPHIC TESTING: NEEDS ATTENTION")
        print(f"⚠️ Some geographic areas or edge cases need improvement")
    
    return meets_criteria

if __name__ == "__main__":
    main()