#!/usr/bin/env python3
"""
Test Socrata API Authentication with App Token
Verify LA City APIs work with proper authentication
"""

import requests
import json
import time
from typing import Dict, Any

class SocrataAPITester:
    """Test Socrata APIs with proper authentication"""
    
    def __init__(self):
        self.app_token = "lmUNVajT2wIHnzFI2x3HGEt5H"
        self.headers = {
            'X-App-Token': self.app_token,
            'Content-Type': 'application/json'
        }
        
        self.apis = {
            'crime': {
                'name': 'LAPD Crime Data',
                'url': 'https://data.lacity.org/resource/2nrs-mtv8.json',
                'test_params': {
                    '$limit': 10,
                    '$order': 'date_occ DESC',
                    '$where': "date_occ > '2024-01-01'"
                }
            },
            'permits': {
                'name': 'LA Building Permits',
                'url': 'https://data.lacity.org/resource/yv23-pmwf.json',
                'test_params': {
                    '$limit': 10,
                    '$order': 'issue_date DESC',
                    '$where': "issue_date > '2024-01-01'"
                }
            }
        }
    
    def test_api_without_token(self, api_key: str) -> Dict[str, Any]:
        """Test API without token for comparison"""
        print(f"Testing {self.apis[api_key]['name']} WITHOUT token...")
        
        try:
            start_time = time.perf_counter()
            response = requests.get(
                self.apis[api_key]['url'],
                params=self.apis[api_key]['test_params'],
                timeout=10
            )
            end_time = time.perf_counter()
            
            response_time = (end_time - start_time) * 1000
            
            result = {
                'status_code': response.status_code,
                'response_time': response_time,
                'success': response.status_code == 200
            }
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    result['records'] = len(data) if isinstance(data, list) else 1
                    result['sample_record'] = data[0] if data else None
                    print(f"  ✅ Success: {response.status_code} ({response_time:.1f}ms) - {result['records']} records")
                except json.JSONDecodeError:
                    result['error'] = 'Invalid JSON response'
                    print(f"  ❌ Invalid JSON response")
            else:
                result['error'] = f"HTTP {response.status_code}"
                print(f"  ❌ Failed: HTTP {response.status_code} ({response_time:.1f}ms)")
            
            # Check rate limit headers
            if 'X-RateLimit-Limit' in response.headers:
                result['rate_limit'] = response.headers.get('X-RateLimit-Limit')
                result['rate_remaining'] = response.headers.get('X-RateLimit-Remaining')
                print(f"  Rate Limit: {result['rate_limit']}, Remaining: {result['rate_remaining']}")
            
            return result
            
        except Exception as e:
            print(f"  ❌ Exception: {e}")
            return {'success': False, 'error': str(e)}
    
    def test_api_with_token(self, api_key: str) -> Dict[str, Any]:
        """Test API with authentication token"""
        print(f"Testing {self.apis[api_key]['name']} WITH app token...")
        
        try:
            start_time = time.perf_counter()
            response = requests.get(
                self.apis[api_key]['url'],
                params=self.apis[api_key]['test_params'],
                headers=self.headers,
                timeout=10
            )
            end_time = time.perf_counter()
            
            response_time = (end_time - start_time) * 1000
            
            result = {
                'status_code': response.status_code,
                'response_time': response_time,
                'success': response.status_code == 200
            }
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    result['records'] = len(data) if isinstance(data, list) else 1
                    result['sample_record'] = data[0] if data else None
                    print(f"  ✅ Success: {response.status_code} ({response_time:.1f}ms) - {result['records']} records")
                    
                    # Show sample data structure
                    if result['sample_record']:
                        sample = result['sample_record']
                        if api_key == 'crime':
                            crime_desc = sample.get('crm_cd_desc', 'N/A')
                            date_occ = sample.get('date_occ', 'N/A')
                            area = sample.get('area_name', 'N/A')
                            print(f"    Sample: {crime_desc} on {date_occ} in {area}")
                        elif api_key == 'permits':
                            permit_type = sample.get('permit_type', 'N/A')
                            issue_date = sample.get('issue_date', 'N/A')
                            address = sample.get('address1', 'N/A')
                            print(f"    Sample: {permit_type} issued {issue_date} at {address}")
                            
                except json.JSONDecodeError:
                    result['error'] = 'Invalid JSON response'
                    print(f"  ❌ Invalid JSON response")
            else:
                result['error'] = f"HTTP {response.status_code}"
                print(f"  ❌ Failed: HTTP {response.status_code} ({response_time:.1f}ms)")
                
                # Try to get error details
                try:
                    error_data = response.json()
                    if 'message' in error_data:
                        print(f"    Error: {error_data['message']}")
                        result['error_message'] = error_data['message']
                except:
                    pass
            
            # Check rate limit headers
            if 'X-RateLimit-Limit' in response.headers:
                result['rate_limit'] = response.headers.get('X-RateLimit-Limit')
                result['rate_remaining'] = response.headers.get('X-RateLimit-Remaining')
                print(f"  Rate Limit: {result['rate_limit']}, Remaining: {result['rate_remaining']}")
            
            return result
            
        except Exception as e:
            print(f"  ❌ Exception: {e}")
            return {'success': False, 'error': str(e)}
    
    def test_rate_limits(self, api_key: str) -> Dict[str, Any]:
        """Test multiple requests to verify rate limits"""
        print(f"\nTesting rate limits for {self.apis[api_key]['name']}...")
        
        results = []
        
        for i in range(5):
            try:
                start_time = time.perf_counter()
                response = requests.get(
                    self.apis[api_key]['url'],
                    params={'$limit': 1},
                    headers=self.headers,
                    timeout=5
                )
                end_time = time.perf_counter()
                
                response_time = (end_time - start_time) * 1000
                
                result = {
                    'request': i + 1,
                    'status_code': response.status_code,
                    'response_time': response_time,
                    'success': response.status_code == 200
                }
                
                if 'X-RateLimit-Remaining' in response.headers:
                    result['rate_remaining'] = response.headers.get('X-RateLimit-Remaining')
                
                results.append(result)
                
                print(f"  Request {i+1}: {response.status_code} ({response_time:.1f}ms) "
                      f"Remaining: {result.get('rate_remaining', 'N/A')}")
                
                # Brief pause between requests
                time.sleep(0.2)
                
            except Exception as e:
                print(f"  Request {i+1}: Exception - {e}")
                results.append({
                    'request': i + 1,
                    'success': False,
                    'error': str(e)
                })
        
        successful_requests = sum(1 for r in results if r.get('success', False))
        
        print(f"  Rate limit test: {successful_requests}/5 requests successful")
        
        return {
            'successful_requests': successful_requests,
            'total_requests': 5,
            'results': results
        }
    
    def test_data_ingestion_integration(self) -> Dict[str, Any]:
        """Test data ingestion with proper authentication"""
        print(f"\n🔄 TESTING DATA INGESTION INTEGRATION")
        print("=" * 80)
        
        ingestion_results = {}
        
        for api_key in ['crime', 'permits']:
            print(f"\nTesting {self.apis[api_key]['name']} data ingestion...")
            
            try:
                # Get larger sample for ingestion test
                params = {
                    '$limit': 100,
                    '$order': 'date_occ DESC' if api_key == 'crime' else 'issue_date DESC'
                }
                
                start_time = time.perf_counter()
                response = requests.get(
                    self.apis[api_key]['url'],
                    params=params,
                    headers=self.headers,
                    timeout=15
                )
                end_time = time.perf_counter()
                
                response_time = (end_time - start_time) * 1000
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Analyze data quality
                    total_records = len(data)
                    
                    if api_key == 'crime':
                        # Check crime data quality
                        valid_coords = sum(1 for record in data 
                                         if record.get('lat') and record.get('lon'))
                        valid_dates = sum(1 for record in data 
                                        if record.get('date_occ'))
                        valid_crimes = sum(1 for record in data 
                                         if record.get('crm_cd_desc'))
                        
                        ingestion_results[api_key] = {
                            'success': True,
                            'total_records': total_records,
                            'valid_coordinates': valid_coords,
                            'valid_dates': valid_dates,
                            'valid_crimes': valid_crimes,
                            'response_time': response_time,
                            'coord_coverage': (valid_coords / total_records * 100) if total_records > 0 else 0
                        }
                        
                        print(f"  ✅ Crime data: {total_records} records ({response_time:.1f}ms)")
                        print(f"    Valid coordinates: {valid_coords}/{total_records} ({valid_coords/total_records*100:.1f}%)")
                        print(f"    Valid dates: {valid_dates}/{total_records} ({valid_dates/total_records*100:.1f}%)")
                        print(f"    Valid crime types: {valid_crimes}/{total_records} ({valid_crimes/total_records*100:.1f}%)")
                        
                    elif api_key == 'permits':
                        # Check permits data quality
                        valid_addresses = sum(1 for record in data 
                                            if record.get('address1'))
                        valid_dates = sum(1 for record in data 
                                        if record.get('issue_date'))
                        valid_types = sum(1 for record in data 
                                        if record.get('permit_type'))
                        
                        ingestion_results[api_key] = {
                            'success': True,
                            'total_records': total_records,
                            'valid_addresses': valid_addresses,
                            'valid_dates': valid_dates,
                            'valid_types': valid_types,
                            'response_time': response_time,
                            'address_coverage': (valid_addresses / total_records * 100) if total_records > 0 else 0
                        }
                        
                        print(f"  ✅ Permits data: {total_records} records ({response_time:.1f}ms)")
                        print(f"    Valid addresses: {valid_addresses}/{total_records} ({valid_addresses/total_records*100:.1f}%)")
                        print(f"    Valid dates: {valid_dates}/{total_records} ({valid_dates/total_records*100:.1f}%)")
                        print(f"    Valid permit types: {valid_types}/{total_records} ({valid_types/total_records*100:.1f}%)")
                
                else:
                    ingestion_results[api_key] = {
                        'success': False,
                        'error': f"HTTP {response.status_code}",
                        'response_time': response_time
                    }
                    print(f"  ❌ Failed: HTTP {response.status_code} ({response_time:.1f}ms)")
                    
            except Exception as e:
                ingestion_results[api_key] = {
                    'success': False,
                    'error': str(e)
                }
                print(f"  ❌ Exception: {e}")
        
        return ingestion_results

def main():
    """Test Socrata API authentication comprehensively"""
    print("🔑 SOCRATA API AUTHENTICATION TESTING")
    print("Testing LA City APIs with app token: lmUNVajT2wIHnzFI2x3HGEt5H")
    print("=" * 100)
    
    tester = SocrataAPITester()
    results = {}
    
    # Test each API without token first, then with token
    for api_key in ['crime', 'permits']:
        print(f"\n{'='*20} TESTING {tester.apis[api_key]['name'].upper()} {'='*20}")
        
        # Test without token
        without_token = tester.test_api_without_token(api_key)
        
        print()  # Spacing
        
        # Test with token
        with_token = tester.test_api_with_token(api_key)
        
        # Test rate limits
        rate_limit_test = tester.test_rate_limits(api_key)
        
        results[api_key] = {
            'without_token': without_token,
            'with_token': with_token,
            'rate_limit_test': rate_limit_test
        }
    
    # Test data ingestion integration
    ingestion_results = tester.test_data_ingestion_integration()
    results['ingestion'] = ingestion_results
    
    # Summary
    print(f"\n{'='*100}")
    print("🎯 SOCRATA API AUTHENTICATION SUMMARY")
    print("=" * 100)
    
    print(f"Authentication Results:")
    for api_key in ['crime', 'permits']:
        api_name = tester.apis[api_key]['name']
        without_success = results[api_key]['without_token'].get('success', False)
        with_success = results[api_key]['with_token'].get('success', False)
        
        print(f"  {api_name}:")
        print(f"    Without token: {'✅ SUCCESS' if without_success else '❌ FAILED'}")
        print(f"    With token: {'✅ SUCCESS' if with_success else '❌ FAILED'}")
        
        if with_success:
            rate_limit = results[api_key]['with_token'].get('rate_limit', 'N/A')
            print(f"    Rate limit: {rate_limit} requests/hour")
        
        # Show improvement
        if with_success and not without_success:
            print(f"    🎯 Token RESOLVED authentication issue!")
        elif with_success and without_success:
            print(f"    ℹ️ Token provides higher rate limits")
        elif not with_success:
            error = results[api_key]['with_token'].get('error', 'Unknown error')
            print(f"    ❌ Still failing: {error}")
    
    print(f"\nData Ingestion Results:")
    for api_key in ['crime', 'permits']:
        if api_key in ingestion_results:
            result = ingestion_results[api_key]
            api_name = tester.apis[api_key]['name']
            
            if result.get('success'):
                print(f"  {api_name}: ✅ {result['total_records']} records")
                if api_key == 'crime':
                    print(f"    Coordinate coverage: {result.get('coord_coverage', 0):.1f}%")
                elif api_key == 'permits':
                    print(f"    Address coverage: {result.get('address_coverage', 0):.1f}%")
            else:
                print(f"  {api_name}: ❌ {result.get('error', 'Failed')}")
    
    # Final assessment
    crime_working = results['crime']['with_token'].get('success', False)
    permits_working = results['permits']['with_token'].get('success', False)
    
    print(f"\n🏆 Final Assessment:")
    if crime_working and permits_working:
        print("✅ BOTH APIs WORKING - Full Socrata integration operational")
        print("✅ App token resolved authentication issues")
        print("✅ Ready for production data ingestion")
    elif crime_working:
        print("⚠️ CRIME API WORKING - Core functionality operational")
        print("❌ Permits API still blocked - may need different endpoint or permissions")
        print("⚠️ Can proceed with crime data, permits optional")
    else:
        print("❌ API AUTHENTICATION ISSUES PERSIST")
        print("❌ May need to verify token or check API status")

if __name__ == "__main__":
    main()