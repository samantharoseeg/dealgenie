#!/usr/bin/env python3
"""
LARGE-SCALE PERMIT API TEST - INDEPENDENT VERIFICATION
Test fixed permit API with 50+ addresses and export raw results
"""

import requests
import json
import csv
import time
import os
from datetime import datetime
from typing import Dict, List, Any, Optional

class LargeScalePermitTester:
    def __init__(self):
        self.base_url = "https://data.lacity.org/resource/pi9x-tg5x.json"
        self.app_token = "lmUNVajT2wIHnzFI2x3HGEt5H"
        self.test_results = []
        self.raw_responses_dir = "permit_api_responses"
        
        # Create directory for raw API responses
        os.makedirs(self.raw_responses_dir, exist_ok=True)
    
    def generate_comprehensive_test_addresses(self) -> List[Dict[str, str]]:
        """
        REQUIREMENT 1: Large-Scale Address Testing
        Generate 50+ diverse LA addresses across neighborhoods and street types
        """
        test_addresses = [
            # Downtown LA
            {"address": "100 S Main St, Los Angeles, CA", "area": "Downtown", "street_type": "St"},
            {"address": "200 N Spring St, Los Angeles, CA", "area": "Downtown", "street_type": "St"},
            {"address": "300 E 1st St, Los Angeles, CA", "area": "Downtown", "street_type": "St"},
            {"address": "400 W 2nd St, Los Angeles, CA", "area": "Downtown", "street_type": "St"},
            {"address": "500 S Figueroa St, Los Angeles, CA", "area": "Downtown", "street_type": "St"},
            
            # Hollywood
            {"address": "6000 Hollywood Blvd, Los Angeles, CA", "area": "Hollywood", "street_type": "Blvd"},
            {"address": "7000 Sunset Blvd, Los Angeles, CA", "area": "Hollywood", "street_type": "Blvd"},
            {"address": "1500 N Vine St, Los Angeles, CA", "area": "Hollywood", "street_type": "St"},
            {"address": "1600 N Highland Ave, Los Angeles, CA", "area": "Hollywood", "street_type": "Ave"},
            {"address": "1700 N Cahuenga Blvd, Los Angeles, CA", "area": "Hollywood", "street_type": "Blvd"},
            
            # West LA
            {"address": "1000 Wilshire Blvd, Los Angeles, CA", "area": "West LA", "street_type": "Blvd"},
            {"address": "2000 Westwood Blvd, Los Angeles, CA", "area": "West LA", "street_type": "Blvd"},
            {"address": "3000 Olympic Blvd, Los Angeles, CA", "area": "West LA", "street_type": "Blvd"},
            {"address": "4000 Pico Blvd, Los Angeles, CA", "area": "West LA", "street_type": "Blvd"},
            {"address": "5000 Santa Monica Blvd, Los Angeles, CA", "area": "West LA", "street_type": "Blvd"},
            
            # South LA
            {"address": "1000 S Vermont Ave, Los Angeles, CA", "area": "South LA", "street_type": "Ave"},
            {"address": "2000 S Western Ave, Los Angeles, CA", "area": "South LA", "street_type": "Ave"},
            {"address": "3000 S Crenshaw Blvd, Los Angeles, CA", "area": "South LA", "street_type": "Blvd"},
            {"address": "4000 S Normandie Ave, Los Angeles, CA", "area": "South LA", "street_type": "Ave"},
            {"address": "5000 Martin Luther King Jr Blvd, Los Angeles, CA", "area": "South LA", "street_type": "Blvd"},
            
            # Mid-City
            {"address": "1000 Melrose Ave, Los Angeles, CA", "area": "Mid-City", "street_type": "Ave"},
            {"address": "2000 Beverly Blvd, Los Angeles, CA", "area": "Mid-City", "street_type": "Blvd"},
            {"address": "3000 3rd St, Los Angeles, CA", "area": "Mid-City", "street_type": "St"},
            {"address": "4000 6th St, Los Angeles, CA", "area": "Mid-City", "street_type": "St"},
            {"address": "5000 Fairfax Ave, Los Angeles, CA", "area": "Mid-City", "street_type": "Ave"},
            
            # East LA
            {"address": "1000 Cesar Chavez Ave, Los Angeles, CA", "area": "East LA", "street_type": "Ave"},
            {"address": "2000 E 1st St, Los Angeles, CA", "area": "East LA", "street_type": "St"},
            {"address": "3000 Whittier Blvd, Los Angeles, CA", "area": "East LA", "street_type": "Blvd"},
            {"address": "4000 E Olympic Blvd, Los Angeles, CA", "area": "East LA", "street_type": "Blvd"},
            {"address": "5000 Brooklyn Ave, Los Angeles, CA", "area": "East LA", "street_type": "Ave"},
            
            # North LA/San Fernando Valley
            {"address": "1000 Ventura Blvd, Los Angeles, CA", "area": "Valley", "street_type": "Blvd"},
            {"address": "2000 Victory Blvd, Los Angeles, CA", "area": "Valley", "street_type": "Blvd"},
            {"address": "3000 Lankershim Blvd, Los Angeles, CA", "area": "Valley", "street_type": "Blvd"},
            {"address": "4000 Laurel Canyon Blvd, Los Angeles, CA", "area": "Valley", "street_type": "Blvd"},
            {"address": "5000 Sepulveda Blvd, Los Angeles, CA", "area": "Valley", "street_type": "Blvd"},
            
            # Different street types and formats
            {"address": "1000 Maple Dr, Los Angeles, CA", "area": "Central", "street_type": "Dr"},
            {"address": "2000 Oak Way, Los Angeles, CA", "area": "Central", "street_type": "Way"},
            {"address": "3000 Pine Pl, Los Angeles, CA", "area": "Central", "street_type": "Pl"},
            {"address": "4000 Elm Ct, Los Angeles, CA", "area": "Central", "street_type": "Ct"},
            {"address": "5000 Cedar Ln, Los Angeles, CA", "area": "Central", "street_type": "Ln"},
            
            # Addresses with units and directionals
            {"address": "1000 N Main St Unit 101, Los Angeles, CA", "area": "Central", "street_type": "St"},
            {"address": "2000 S Spring St Apt 5, Los Angeles, CA", "area": "Central", "street_type": "St"},
            {"address": "3000 E Sunset Blvd #200, Los Angeles, CA", "area": "Central", "street_type": "Blvd"},
            {"address": "4000 W Olympic Blvd Suite 10, Los Angeles, CA", "area": "Central", "street_type": "Blvd"},
            {"address": "5000 NE Broadway, Los Angeles, CA", "area": "Central", "street_type": "St"},
            
            # Specific known addresses (for validation)
            {"address": "123 Main St, Los Angeles, CA", "area": "Test", "street_type": "St"},
            {"address": "456 Spring St, Los Angeles, CA", "area": "Test", "street_type": "St"},
            {"address": "789 Broadway, Los Angeles, CA", "area": "Test", "street_type": "St"},
            {"address": "101 Figueroa St, Los Angeles, CA", "area": "Test", "street_type": "St"},
            {"address": "202 Hill St, Los Angeles, CA", "area": "Test", "street_type": "St"},
            
            # Additional comprehensive coverage
            {"address": "6000 Fountain Ave, Los Angeles, CA", "area": "Hollywood", "street_type": "Ave"},
            {"address": "7000 Franklin Ave, Los Angeles, CA", "area": "Hollywood", "street_type": "Ave"},
            {"address": "8000 Romaine St, Los Angeles, CA", "area": "West Hollywood", "street_type": "St"},
            {"address": "9000 Santa Monica Blvd, Los Angeles, CA", "area": "West Hollywood", "street_type": "Blvd"},
            {"address": "10000 Washington Blvd, Los Angeles, CA", "area": "Mid-City", "street_type": "Blvd"}
        ]
        
        print(f"📊 Generated {len(test_addresses)} test addresses")
        print(f"Areas covered: {len(set(addr['area'] for addr in test_addresses))}")
        print(f"Street types: {len(set(addr['street_type'] for addr in test_addresses))}")
        
        return test_addresses
    
    def test_single_address_with_api(self, address_info: Dict[str, str], test_index: int) -> Dict[str, Any]:
        """Test a single address and save raw API response"""
        address = address_info["address"]
        area = address_info["area"]
        street_type = address_info["street_type"]
        
        print(f"🧪 Testing {test_index}: {address}")
        
        # Clean address for API query
        import re
        clean_address = re.sub(r'[^\w\s]', ' ', address)
        clean_address = ' '.join(clean_address.split())
        
        # Use corrected column name
        params = {
            '$where': f"upper(primary_address) like '%{clean_address.upper()}%'",
            '$limit': '20',
            '$order': 'issue_date DESC'
        }
        
        test_result = {
            'test_index': test_index,
            'input_address': address,
            'area': area,
            'street_type': street_type,
            'clean_address': clean_address,
            'api_params': params,
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            start_time = time.time()
            
            response = requests.get(
                self.base_url,
                params=params,
                headers={"X-App-Token": self.app_token},
                timeout=30
            )
            
            duration_ms = (time.time() - start_time) * 1000
            
            test_result.update({
                'api_status_code': response.status_code,
                'response_time_ms': duration_ms,
                'response_length': len(response.text),
                'content_type': response.headers.get('Content-Type', '')
            })
            
            # Save raw API response to file
            response_filename = f"response_{test_index:03d}_{address.replace(' ', '_').replace(',', '').replace('/', '_')[:50]}.json"
            response_filepath = os.path.join(self.raw_responses_dir, response_filename)
            
            with open(response_filepath, 'w') as f:
                f.write(response.text)
            
            test_result['response_file'] = response_filepath
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    permits_found = len(data)
                    
                    test_result.update({
                        'success': True,
                        'permits_found': permits_found,
                        'error_message': None
                    })
                    
                    # Analyze permit data quality
                    if data:
                        sample_permit = data[0]
                        test_result.update({
                            'sample_permit_nbr': sample_permit.get('permit_nbr', ''),
                            'sample_address': sample_permit.get('primary_address', ''),
                            'sample_permit_type': sample_permit.get('permit_type', ''),
                            'sample_issue_date': sample_permit.get('issue_date', ''),
                            'sample_status': sample_permit.get('status_desc', ''),
                            'has_coordinates': bool(sample_permit.get('lat') and sample_permit.get('lon')),
                            'sample_lat': sample_permit.get('lat', ''),
                            'sample_lon': sample_permit.get('lon', '')
                        })
                        
                        # Check data completeness
                        complete_fields = 0
                        total_fields = 0
                        for key, value in sample_permit.items():
                            total_fields += 1
                            if value and str(value).strip():
                                complete_fields += 1
                        
                        test_result['data_completeness_percent'] = (complete_fields / total_fields * 100) if total_fields > 0 else 0
                    
                    print(f"   ✅ Success: {permits_found} permits found")
                    
                except json.JSONDecodeError as e:
                    test_result.update({
                        'success': False,
                        'permits_found': 0,
                        'error_message': f'JSON decode error: {str(e)}'
                    })
                    print(f"   ❌ JSON decode error: {e}")
            else:
                test_result.update({
                    'success': False,
                    'permits_found': 0,
                    'error_message': f'HTTP {response.status_code}: {response.text[:200]}'
                })
                print(f"   ❌ HTTP {response.status_code}")
                
        except requests.RequestException as e:
            test_result.update({
                'success': False,
                'permits_found': 0,
                'error_message': f'Request error: {str(e)}',
                'api_status_code': None,
                'response_time_ms': (time.time() - start_time) * 1000
            })
            print(f"   ❌ Request error: {e}")
        except Exception as e:
            test_result.update({
                'success': False,
                'permits_found': 0,
                'error_message': f'Unexpected error: {str(e)}',
                'api_status_code': None,
                'response_time_ms': (time.time() - start_time) * 1000
            })
            print(f"   ❌ Unexpected error: {e}")
        
        # Rate limiting
        time.sleep(0.5)
        
        return test_result
    
    def export_raw_results_to_csv(self, results: List[Dict], filename: str):
        """
        REQUIREMENT 2: Export Raw Results for Independent Review
        Generate CSV export with all test data
        """
        print(f"\n📄 EXPORTING RAW RESULTS TO CSV")
        print("="*40)
        
        if not results:
            print("No results to export")
            return
        
        # Define CSV columns
        csv_columns = [
            'test_index', 'input_address', 'area', 'street_type', 'clean_address',
            'api_status_code', 'success', 'permits_found', 'error_message',
            'response_time_ms', 'response_length', 'content_type', 'response_file',
            'sample_permit_nbr', 'sample_address', 'sample_permit_type', 
            'sample_issue_date', 'sample_status', 'has_coordinates',
            'sample_lat', 'sample_lon', 'data_completeness_percent', 'timestamp'
        ]
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            
            for result in results:
                # Ensure all columns exist in each row
                csv_row = {col: result.get(col, '') for col in csv_columns}
                writer.writerow(csv_row)
        
        print(f"✅ Exported {len(results)} test results to: {filename}")
        print(f"📁 Raw API responses saved in: {self.raw_responses_dir}/")
        
        # Create summary stats
        total_tests = len(results)
        successful_tests = sum(1 for r in results if r.get('success', False))
        total_permits = sum(r.get('permits_found', 0) for r in results)
        
        print(f"\n📊 EXPORT SUMMARY:")
        print(f"Total tests: {total_tests}")
        print(f"Successful tests: {successful_tests}")
        print(f"Success rate: {(successful_tests/total_tests*100):.1f}%")
        print(f"Total permits found: {total_permits}")
        print(f"Average permits per successful test: {(total_permits/successful_tests):.1f}" if successful_tests > 0 else "N/A")
    
    def analyze_geographic_distribution(self, results: List[Dict]):
        """
        REQUIREMENT 3: Geographic Distribution Test
        Analyze results by area and street type
        """
        print(f"\n🗺️ GEOGRAPHIC DISTRIBUTION ANALYSIS")
        print("="*45)
        
        # Group by area
        area_stats = {}
        street_type_stats = {}
        
        for result in results:
            area = result.get('area', 'Unknown')
            street_type = result.get('street_type', 'Unknown')
            success = result.get('success', False)
            permits = result.get('permits_found', 0)
            
            # Area statistics
            if area not in area_stats:
                area_stats[area] = {'total': 0, 'successful': 0, 'permits': 0}
            area_stats[area]['total'] += 1
            if success:
                area_stats[area]['successful'] += 1
                area_stats[area]['permits'] += permits
            
            # Street type statistics
            if street_type not in street_type_stats:
                street_type_stats[street_type] = {'total': 0, 'successful': 0, 'permits': 0}
            street_type_stats[street_type]['total'] += 1
            if success:
                street_type_stats[street_type]['successful'] += 1
                street_type_stats[street_type]['permits'] += permits
        
        print(f"📍 SUCCESS RATE BY AREA:")
        for area, stats in sorted(area_stats.items()):
            success_rate = (stats['successful'] / stats['total'] * 100) if stats['total'] > 0 else 0
            avg_permits = (stats['permits'] / stats['successful']) if stats['successful'] > 0 else 0
            print(f"   {area:<15}: {stats['successful']:2d}/{stats['total']:2d} ({success_rate:5.1f}%) - Avg permits: {avg_permits:.1f}")
        
        print(f"\n🛣️ SUCCESS RATE BY STREET TYPE:")
        for street_type, stats in sorted(street_type_stats.items()):
            success_rate = (stats['successful'] / stats['total'] * 100) if stats['total'] > 0 else 0
            avg_permits = (stats['permits'] / stats['successful']) if stats['successful'] > 0 else 0
            print(f"   {street_type:<8}: {stats['successful']:2d}/{stats['total']:2d} ({success_rate:5.1f}%) - Avg permits: {avg_permits:.1f}")
        
        return area_stats, street_type_stats
    
    def assess_data_quality(self, results: List[Dict]):
        """
        REQUIREMENT 4: Data Quality Assessment
        Check completeness and currency of permit data
        """
        print(f"\n🔍 DATA QUALITY ASSESSMENT")
        print("="*35)
        
        successful_results = [r for r in results if r.get('success', False) and r.get('permits_found', 0) > 0]
        
        if not successful_results:
            print("No successful results with permits to analyze")
            return
        
        # Analyze data completeness
        completeness_scores = [r.get('data_completeness_percent', 0) for r in successful_results if 'data_completeness_percent' in r]
        avg_completeness = sum(completeness_scores) / len(completeness_scores) if completeness_scores else 0
        
        # Check for coordinates
        with_coordinates = sum(1 for r in successful_results if r.get('has_coordinates', False))
        coord_coverage = (with_coordinates / len(successful_results) * 100) if successful_results else 0
        
        # Analyze permit dates
        recent_permits = 0
        for result in successful_results:
            issue_date = result.get('sample_issue_date', '')
            if issue_date and ('2024' in issue_date or '2025' in issue_date):
                recent_permits += 1
        
        recent_data_rate = (recent_permits / len(successful_results) * 100) if successful_results else 0
        
        print(f"📊 DATA QUALITY METRICS:")
        print(f"   Records analyzed: {len(successful_results)}")
        print(f"   Average field completeness: {avg_completeness:.1f}%")
        print(f"   Records with coordinates: {with_coordinates}/{len(successful_results)} ({coord_coverage:.1f}%)")
        print(f"   Records with recent data (2024-2025): {recent_permits}/{len(successful_results)} ({recent_data_rate:.1f}%)")
        
        # Sample data quality details
        print(f"\n📋 SAMPLE PERMIT DETAILS:")
        for i, result in enumerate(successful_results[:5], 1):
            print(f"   {i}. Permit: {result.get('sample_permit_nbr', 'N/A')}")
            print(f"      Address: {result.get('sample_address', 'N/A')}")
            print(f"      Type: {result.get('sample_permit_type', 'N/A')}")
            print(f"      Date: {result.get('sample_issue_date', 'N/A')}")
            print(f"      Status: {result.get('sample_status', 'N/A')}")
            print(f"      Coordinates: ({result.get('sample_lat', 'N/A')}, {result.get('sample_lon', 'N/A')})")
            print(f"      Completeness: {result.get('data_completeness_percent', 0):.1f}%")
            if i < len(successful_results):
                print()
        
        return {
            'avg_completeness': avg_completeness,
            'coord_coverage': coord_coverage,
            'recent_data_rate': recent_data_rate,
            'records_analyzed': len(successful_results)
        }
    
    def run_large_scale_test(self):
        """Run comprehensive large-scale permit API test"""
        print(f"🚀 LARGE-SCALE PERMIT API TEST")
        print("="*45)
        print(f"Starting comprehensive test at: {datetime.now().isoformat()}")
        print()
        
        # Generate test addresses
        test_addresses = self.generate_comprehensive_test_addresses()
        
        print(f"\n🧪 TESTING {len(test_addresses)} ADDRESSES")
        print("="*50)
        
        # Test all addresses
        all_results = []
        for i, address_info in enumerate(test_addresses, 1):
            result = self.test_single_address_with_api(address_info, i)
            all_results.append(result)
            self.test_results.append(result)
        
        # Export raw results to CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"large_scale_permit_test_{timestamp}.csv"
        self.export_raw_results_to_csv(all_results, csv_filename)
        
        # Analyze geographic distribution
        area_stats, street_type_stats = self.analyze_geographic_distribution(all_results)
        
        # Assess data quality
        quality_metrics = self.assess_data_quality(all_results)
        
        # Final summary
        total_tests = len(all_results)
        successful_tests = sum(1 for r in all_results if r.get('success', False))
        total_permits = sum(r.get('permits_found', 0) for r in all_results)
        
        print(f"\n🏆 LARGE-SCALE TEST SUMMARY")
        print("="*40)
        print(f"Total addresses tested: {total_tests}")
        print(f"Successful API calls: {successful_tests}")
        print(f"Overall success rate: {(successful_tests/total_tests*100):.1f}%")
        print(f"Total permits found: {total_permits}")
        print(f"Average permits per address: {(total_permits/total_tests):.1f}")
        print(f"CSV export: {csv_filename}")
        print(f"Raw responses: {self.raw_responses_dir}/")
        
        return {
            'total_tests': total_tests,
            'successful_tests': successful_tests,
            'success_rate': (successful_tests/total_tests*100),
            'total_permits': total_permits,
            'csv_filename': csv_filename,
            'area_stats': area_stats,
            'street_type_stats': street_type_stats,
            'quality_metrics': quality_metrics,
            'raw_results': all_results
        }

def main():
    """
    LARGE-SCALE PERMIT API TEST - INDEPENDENT VERIFICATION
    """
    print(f"📊 LARGE-SCALE PERMIT API VERIFICATION")
    print("="*50)
    print("Testing fixed permit API with 50+ diverse addresses")
    print()
    
    tester = LargeScalePermitTester()
    results = tester.run_large_scale_test()
    
    # Save comprehensive results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_filename = f"large_scale_test_summary_{timestamp}.json"
    
    with open(results_filename, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n📁 DELIVERABLES CREATED:")
    print(f"   📄 CSV data: {results['csv_filename']}")
    print(f"   📁 Raw responses: {tester.raw_responses_dir}/")
    print(f"   📊 Summary: {results_filename}")
    
    success_rate = results['success_rate']
    print(f"\n🎯 VERIFICATION RESULT:")
    print(f"Success rate: {success_rate:.1f}%")
    print(f"Status: {'✅ VERIFIED' if success_rate >= 70 else '❌ NOT VERIFIED'}")
    
    return success_rate >= 70

if __name__ == "__main__":
    main()