#!/usr/bin/env python3
"""
DIAGNOSE FUNDAMENTAL PERMIT API ISSUE - ROOT CAUSE ANALYSIS
Systematic diagnosis to identify the actual underlying problem
"""

import requests
import json
import time
from datetime import datetime
from typing import Dict, List, Any, Optional

class PermitAPIDiagnoser:
    def __init__(self):
        self.base_url = "https://data.lacity.org/resource/pi9x-tg5x.json"
        self.app_token = "lmUNVajT2wIHnzFI2x3HGEt5H"
        self.test_results = []
        
    def test_api_endpoint_directly(self):
        """
        REQUIREMENT 1: Test API Endpoint Directly
        Make simple HTTP GET requests and show raw responses
        """
        print(f"🌐 TESTING API ENDPOINT DIRECTLY")
        print("="*50)
        print(f"Base URL: {self.base_url}")
        print()
        
        test_cases = [
            {
                "name": "No parameters",
                "params": {},
                "description": "Basic endpoint test with no query parameters"
            },
            {
                "name": "Basic limit",
                "params": {"$limit": "10"},
                "description": "Simple limit parameter test"
            },
            {
                "name": "Limit with order",
                "params": {"$limit": "5", "$order": "issue_date DESC"},
                "description": "Limit with order by date"
            }
        ]
        
        for i, test_case in enumerate(test_cases, 1):
            print(f"🧪 TEST {i}: {test_case['name']}")
            print(f"Description: {test_case['description']}")
            print(f"Parameters: {test_case['params']}")
            print("-" * 60)
            
            try:
                start_time = time.time()
                response = requests.get(self.base_url, params=test_case['params'], timeout=30)
                duration = (time.time() - start_time) * 1000
                
                print(f"⏱️  Duration: {duration:.1f}ms")
                print(f"📈 Status Code: {response.status_code}")
                print(f"📏 Response Length: {len(response.text)} characters")
                print(f"🏷️  Content-Type: {response.headers.get('Content-Type', 'N/A')}")
                
                # Show first 500 characters of response
                response_preview = response.text[:500]
                print(f"📄 Response Preview (first 500 chars):")
                print(f"   {response_preview}")
                if len(response.text) > 500:
                    print(f"   ... ({len(response.text) - 500} more characters)")
                
                # Try to parse as JSON if possible
                if response.headers.get('Content-Type', '').startswith('application/json'):
                    try:
                        json_data = response.json()
                        print(f"📊 JSON Structure:")
                        if isinstance(json_data, list):
                            print(f"   Array with {len(json_data)} items")
                            if json_data:
                                print(f"   First item keys: {list(json_data[0].keys())}")
                        elif isinstance(json_data, dict):
                            print(f"   Object with keys: {list(json_data.keys())}")
                    except json.JSONDecodeError as e:
                        print(f"   ❌ JSON Parse Error: {e}")
                
                test_result = {
                    'test_name': test_case['name'],
                    'params': test_case['params'],
                    'status_code': response.status_code,
                    'duration_ms': duration,
                    'response_length': len(response.text),
                    'content_type': response.headers.get('Content-Type'),
                    'response_preview': response_preview,
                    'success': response.status_code == 200
                }
                
                self.test_results.append(test_result)
                
            except requests.RequestException as e:
                print(f"❌ Request Exception: {e}")
                test_result = {
                    'test_name': test_case['name'],
                    'params': test_case['params'],
                    'error': str(e),
                    'success': False
                }
                self.test_results.append(test_result)
            except Exception as e:
                print(f"❌ Unexpected Error: {e}")
                test_result = {
                    'test_name': test_case['name'],
                    'params': test_case['params'],
                    'error': str(e),
                    'success': False
                }
                self.test_results.append(test_result)
            
            print("\n" + "="*70 + "\n")
    
    def test_authentication_and_parameters(self):
        """
        REQUIREMENT 2: Identify Authentication/Parameter Issues
        Test with and without app token, various parameter combinations
        """
        print(f"🔑 TESTING AUTHENTICATION AND PARAMETERS")
        print("="*55)
        
        auth_tests = [
            {
                "name": "No app token, simple limit",
                "params": {"$limit": "5"},
                "headers": {},
                "description": "Test without authentication"
            },
            {
                "name": "With app token, simple limit",
                "params": {"$limit": "5"},
                "headers": {"X-App-Token": self.app_token},
                "description": f"Test with app token: {self.app_token}"
            },
            {
                "name": "With app token in URL params",
                "params": {"$limit": "5", "$$app_token": self.app_token},
                "headers": {},
                "description": "App token as URL parameter"
            },
            {
                "name": "Complex query without token",
                "params": {
                    "$where": "upper(project_address) like '%MAIN%'",
                    "$limit": "10"
                },
                "headers": {},
                "description": "Complex where clause without token"
            },
            {
                "name": "Complex query with token",
                "params": {
                    "$where": "upper(project_address) like '%MAIN%'",
                    "$limit": "10"
                },
                "headers": {"X-App-Token": self.app_token},
                "description": "Complex where clause with token"
            },
            {
                "name": "Address search that worked before",
                "params": {
                    "project_address": "MAIN ST",
                    "$limit": "10"
                },
                "headers": {"X-App-Token": self.app_token},
                "description": "Simple address filter that might work"
            }
        ]
        
        for i, test in enumerate(auth_tests, 1):
            print(f"🧪 AUTH TEST {i}: {test['name']}")
            print(f"Description: {test['description']}")
            print(f"Parameters: {test['params']}")
            print(f"Headers: {test['headers']}")
            print("-" * 60)
            
            try:
                start_time = time.time()
                response = requests.get(
                    self.base_url, 
                    params=test['params'], 
                    headers=test['headers'],
                    timeout=30
                )
                duration = (time.time() - start_time) * 1000
                
                print(f"⏱️  Duration: {duration:.1f}ms")
                print(f"📈 Status Code: {response.status_code}")
                print(f"📏 Response Length: {len(response.text)} characters")
                
                # Show response for different status codes
                if response.status_code == 200:
                    try:
                        data = response.json()
                        print(f"✅ SUCCESS - Data items: {len(data) if isinstance(data, list) else 'N/A'}")
                        if isinstance(data, list) and data:
                            print(f"   Sample record keys: {list(data[0].keys())}")
                    except:
                        print(f"✅ SUCCESS - Raw response: {response.text[:200]}")
                elif response.status_code == 400:
                    print(f"❌ BAD REQUEST (400)")
                    print(f"   Response: {response.text[:300]}")
                elif response.status_code == 401:
                    print(f"❌ UNAUTHORIZED (401)")
                    print(f"   Response: {response.text[:300]}")
                elif response.status_code == 403:
                    print(f"❌ FORBIDDEN (403)")
                    print(f"   Response: {response.text[:300]}")
                else:
                    print(f"❌ OTHER ERROR ({response.status_code})")
                    print(f"   Response: {response.text[:300]}")
                
                auth_result = {
                    'test_name': test['name'],
                    'params': test['params'],
                    'headers': test['headers'],
                    'status_code': response.status_code,
                    'duration_ms': duration,
                    'response_length': len(response.text),
                    'response_preview': response.text[:300],
                    'success': response.status_code == 200
                }
                
                self.test_results.append(auth_result)
                
            except Exception as e:
                print(f"❌ Exception: {e}")
                auth_result = {
                    'test_name': test['name'],
                    'params': test['params'],
                    'headers': test['headers'],
                    'error': str(e),
                    'success': False
                }
                self.test_results.append(auth_result)
            
            print("\n" + "="*70 + "\n")
    
    def check_data_availability(self):
        """
        REQUIREMENT 3: Check Data Currency and Availability
        Verify dataset exists and check for deprecation notices
        """
        print(f"📊 CHECKING DATA AVAILABILITY AND CURRENCY")
        print("="*55)
        
        # Test the dataset metadata endpoint
        metadata_url = "https://data.lacity.org/api/views/pi9x-tg5x.json"
        
        print(f"🔍 Testing dataset metadata:")
        print(f"URL: {metadata_url}")
        print("-" * 60)
        
        try:
            response = requests.get(metadata_url, timeout=30)
            
            print(f"📈 Status Code: {response.status_code}")
            print(f"📏 Response Length: {len(response.text)} characters")
            
            if response.status_code == 200:
                try:
                    metadata = response.json()
                    print(f"✅ DATASET EXISTS")
                    print(f"   Name: {metadata.get('name', 'N/A')}")
                    print(f"   Description: {metadata.get('description', 'N/A')[:100]}...")
                    print(f"   Created: {metadata.get('createdAt', 'N/A')}")
                    print(f"   Updated: {metadata.get('rowsUpdatedAt', 'N/A')}")
                    print(f"   Rows: {metadata.get('totalTimesRated', 'N/A')}")
                    print(f"   Public: {metadata.get('publicationStage', 'N/A')}")
                    
                    # Check for any flags or notices
                    if 'flags' in metadata:
                        print(f"   Flags: {metadata['flags']}")
                    
                    if 'moderationStatus' in metadata:
                        print(f"   Moderation: {metadata['moderationStatus']}")
                        
                except json.JSONDecodeError:
                    print(f"❌ Metadata not in JSON format")
                    print(f"   Response: {response.text[:300]}")
            else:
                print(f"❌ METADATA REQUEST FAILED")
                print(f"   Response: {response.text[:300]}")
                
        except Exception as e:
            print(f"❌ Metadata request exception: {e}")
        
        # Test alternative dataset formats
        print(f"\n🔄 Testing alternative access methods:")
        
        csv_url = "https://data.lacity.org/resource/pi9x-tg5x.csv?$limit=5"
        print(f"CSV endpoint: {csv_url}")
        
        try:
            csv_response = requests.get(csv_url, timeout=30)
            print(f"   CSV Status: {csv_response.status_code}")
            print(f"   CSV Length: {len(csv_response.text)} chars")
            if csv_response.status_code == 200:
                print(f"   ✅ CSV format works")
                print(f"   CSV Preview: {csv_response.text[:200]}")
            else:
                print(f"   ❌ CSV format fails")
                print(f"   CSV Response: {csv_response.text[:200]}")
        except Exception as e:
            print(f"   ❌ CSV test exception: {e}")
    
    def test_alternative_approaches(self):
        """
        REQUIREMENT 4: Test Alternative Approaches
        Try different endpoints and approaches
        """
        print(f"🔄 TESTING ALTERNATIVE APPROACHES")
        print("="*45)
        
        alternatives = [
            {
                "name": "JSON endpoint with different domain",
                "url": "https://data.lacity.org/api/views/pi9x-tg5x/rows.json",
                "params": {},
                "description": "Try the direct API rows endpoint"
            },
            {
                "name": "Simple CSV download",
                "url": "https://data.lacity.org/resource/pi9x-tg5x.csv",
                "params": {"$limit": "10"},
                "description": "CSV format with limit"
            },
            {
                "name": "GeoJSON if available",
                "url": "https://data.lacity.org/resource/pi9x-tg5x.geojson",
                "params": {"$limit": "5"},
                "description": "Try GeoJSON format"
            },
            {
                "name": "Different parameter style",
                "url": "https://data.lacity.org/resource/pi9x-tg5x.json",
                "params": {"$limit": "10", "$offset": "0"},
                "description": "Use offset pagination"
            }
        ]
        
        for i, alt in enumerate(alternatives, 1):
            print(f"🧪 ALTERNATIVE {i}: {alt['name']}")
            print(f"URL: {alt['url']}")
            print(f"Parameters: {alt['params']}")
            print(f"Description: {alt['description']}")
            print("-" * 60)
            
            try:
                start_time = time.time()
                response = requests.get(alt['url'], params=alt['params'], timeout=30)
                duration = (time.time() - start_time) * 1000
                
                print(f"⏱️  Duration: {duration:.1f}ms")
                print(f"📈 Status Code: {response.status_code}")
                print(f"📏 Response Length: {len(response.text)} characters")
                print(f"🏷️  Content-Type: {response.headers.get('Content-Type', 'N/A')}")
                
                if response.status_code == 200:
                    print(f"✅ SUCCESS")
                    print(f"   Response preview: {response.text[:200]}")
                else:
                    print(f"❌ FAILED")
                    print(f"   Error response: {response.text[:200]}")
                
                alt_result = {
                    'alternative_name': alt['name'],
                    'url': alt['url'],
                    'params': alt['params'],
                    'status_code': response.status_code,
                    'duration_ms': duration,
                    'response_length': len(response.text),
                    'success': response.status_code == 200
                }
                
                self.test_results.append(alt_result)
                
            except Exception as e:
                print(f"❌ Exception: {e}")
                alt_result = {
                    'alternative_name': alt['name'],
                    'url': alt['url'],
                    'params': alt['params'],
                    'error': str(e),
                    'success': False
                }
                self.test_results.append(alt_result)
            
            print("\n" + "="*70 + "\n")
    
    def test_consistent_reproduction(self):
        """
        REQUIREMENT 5: Document Consistent Reproduction
        Run the same test 3 times to check consistency
        """
        print(f"🔁 TESTING CONSISTENT REPRODUCTION")
        print("="*45)
        
        # Test the same failing query 3 times
        test_query = {
            "url": self.base_url,
            "params": {
                "$where": "upper(project_address) like '%MAIN ST%'",
                "$limit": "10"
            },
            "headers": {"X-App-Token": self.app_token},
            "description": "Complex query that has been failing"
        }
        
        print(f"🧪 Repeating the same test 3 times:")
        print(f"URL: {test_query['url']}")
        print(f"Parameters: {test_query['params']}")
        print(f"Headers: {test_query['headers']}")
        print(f"Description: {test_query['description']}")
        print()
        
        reproduction_results = []
        
        for attempt in range(1, 4):
            print(f"🔄 ATTEMPT {attempt}/3:")
            print("-" * 30)
            
            try:
                start_time = time.time()
                response = requests.get(
                    test_query['url'],
                    params=test_query['params'],
                    headers=test_query['headers'],
                    timeout=30
                )
                duration = (time.time() - start_time) * 1000
                
                print(f"   ⏱️  Duration: {duration:.1f}ms")
                print(f"   📈 Status Code: {response.status_code}")
                print(f"   📏 Response Length: {len(response.text)} chars")
                
                if response.status_code != 200:
                    print(f"   ❌ Error Response: {response.text[:150]}")
                else:
                    print(f"   ✅ Success Response: {response.text[:150]}")
                
                attempt_result = {
                    'attempt': attempt,
                    'status_code': response.status_code,
                    'duration_ms': duration,
                    'response_length': len(response.text),
                    'response_preview': response.text[:150],
                    'timestamp': datetime.now().isoformat(),
                    'success': response.status_code == 200
                }
                
                reproduction_results.append(attempt_result)
                
            except Exception as e:
                print(f"   ❌ Exception: {e}")
                attempt_result = {
                    'attempt': attempt,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat(),
                    'success': False
                }
                reproduction_results.append(attempt_result)
            
            if attempt < 3:
                print(f"   ⏳ Waiting 2 seconds before next attempt...")
                time.sleep(2)
            
            print()
        
        # Analyze consistency
        status_codes = [r.get('status_code') for r in reproduction_results if 'status_code' in r]
        success_count = sum(1 for r in reproduction_results if r.get('success', False))
        
        print(f"📊 REPRODUCTION ANALYSIS:")
        print(f"   Status codes: {status_codes}")
        print(f"   Success count: {success_count}/3")
        print(f"   Consistency: {'✅ CONSISTENT' if len(set(status_codes)) <= 1 else '❌ INCONSISTENT'}")
        
        if success_count == 0:
            print(f"   🚨 PERMANENT FAILURE - All attempts failed")
        elif success_count == 3:
            print(f"   ✅ FULLY WORKING - All attempts succeeded")
        else:
            print(f"   ⚠️ INTERMITTENT ISSUE - {success_count}/3 attempts succeeded")
        
        return reproduction_results
    
    def run_complete_diagnosis(self):
        """Run complete systematic diagnosis"""
        print(f"🩺 PERMIT API ROOT CAUSE DIAGNOSIS")
        print("="*50)
        print(f"Timestamp: {datetime.now().isoformat()}")
        print(f"Target API: {self.base_url}")
        print(f"App Token: {self.app_token}")
        print("\n" + "="*70 + "\n")
        
        # Run all diagnostic tests
        self.test_api_endpoint_directly()
        self.test_authentication_and_parameters()
        self.check_data_availability()
        self.test_alternative_approaches()
        reproduction_results = self.test_consistent_reproduction()
        
        # Compile final diagnosis
        print(f"🏥 FINAL DIAGNOSIS SUMMARY")
        print("="*40)
        
        total_tests = len(self.test_results)
        successful_tests = sum(1 for r in self.test_results if r.get('success', False))
        
        print(f"Total diagnostic tests: {total_tests}")
        print(f"Successful tests: {successful_tests}")
        print(f"Success rate: {(successful_tests/total_tests*100):.1f}%")
        
        # Categorize issues
        auth_issues = sum(1 for r in self.test_results if r.get('status_code') == 401)
        bad_requests = sum(1 for r in self.test_results if r.get('status_code') == 400)
        forbidden = sum(1 for r in self.test_results if r.get('status_code') == 403)
        
        print(f"\n🔍 ERROR BREAKDOWN:")
        print(f"   400 Bad Request: {bad_requests}")
        print(f"   401 Unauthorized: {auth_issues}")
        print(f"   403 Forbidden: {forbidden}")
        print(f"   200 Success: {successful_tests}")
        
        # Root cause determination
        if bad_requests > 0 and successful_tests == 0:
            diagnosis = "🚨 SYSTEMATIC PARAMETER/QUERY FORMATTING ISSUE"
        elif auth_issues > 0:
            diagnosis = "🔑 AUTHENTICATION/AUTHORIZATION PROBLEM"
        elif successful_tests > 0:
            diagnosis = "⚠️ SPECIFIC QUERY PATTERNS FAILING"
        else:
            diagnosis = "❓ UNKNOWN API ISSUE"
        
        print(f"\n🎯 ROOT CAUSE DIAGNOSIS:")
        print(f"   {diagnosis}")
        
        # Save comprehensive results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"permit_api_diagnosis_{timestamp}.json"
        
        diagnosis_data = {
            'diagnosis_timestamp': datetime.now().isoformat(),
            'api_endpoint': self.base_url,
            'app_token_used': self.app_token,
            'total_tests': total_tests,
            'successful_tests': successful_tests,
            'root_cause_diagnosis': diagnosis,
            'error_breakdown': {
                'bad_requests': bad_requests,
                'unauthorized': auth_issues,
                'forbidden': forbidden,
                'success': successful_tests
            },
            'all_test_results': self.test_results,
            'reproduction_test': reproduction_results
        }
        
        with open(filename, 'w') as f:
            json.dump(diagnosis_data, f, indent=2, default=str)
        
        print(f"\n📁 Complete diagnosis saved: {filename}")
        
        return diagnosis_data

def main():
    """
    DIAGNOSE FUNDAMENTAL PERMIT API ISSUE - ROOT CAUSE ANALYSIS
    """
    print(f"🩺 PERMIT API ROOT CAUSE DIAGNOSIS")
    print("="*50)
    print("Systematic analysis to identify the actual underlying problem")
    print()
    
    diagnoser = PermitAPIDiagnoser()
    diagnosis_data = diagnoser.run_complete_diagnosis()
    
    return diagnosis_data['successful_tests'] > 0

if __name__ == "__main__":
    main()