#!/usr/bin/env python3
"""
Test Alternative Free Building Permit Sources for LA
Comprehensive testing of multiple permit data sources with actual API calls
"""

import requests
import json
import time
from urllib.parse import urljoin
from datetime import datetime

class AlternativePermitSourceTester:
    def __init__(self):
        self.results = {}
        self.headers = {
            'User-Agent': 'DealGenie/1.0 (Property Intelligence; +https://github.com/dealgenie)',
            'Accept': 'application/json, text/csv, */*'
        }
        self.timeout = 30
        
    def log_test(self, source, endpoint, method="GET", **kwargs):
        """Log and execute test with detailed results"""
        print(f"\n🔍 TESTING: {source}")
        print(f"   Endpoint: {endpoint}")
        print(f"   Method: {method}")
        
        try:
            start_time = time.time()
            
            if method == "GET":
                response = requests.get(endpoint, headers=self.headers, timeout=self.timeout, **kwargs)
            elif method == "POST":
                response = requests.post(endpoint, headers=self.headers, timeout=self.timeout, **kwargs)
            
            end_time = time.time()
            response_time = (end_time - start_time) * 1000
            
            result = {
                'source': source,
                'endpoint': endpoint,
                'method': method,
                'status_code': response.status_code,
                'response_time_ms': response_time,
                'content_type': response.headers.get('Content-Type', 'unknown'),
                'content_length': len(response.content),
                'success': response.status_code == 200,
                'timestamp': datetime.now().isoformat()
            }
            
            # Add response headers info
            rate_limit_headers = {}
            for header in response.headers:
                if 'limit' in header.lower() or 'rate' in header.lower():
                    rate_limit_headers[header] = response.headers[header]
            
            if rate_limit_headers:
                result['rate_limiting'] = rate_limit_headers
            
            # Try to parse response
            if response.status_code == 200:
                try:
                    if 'json' in response.headers.get('Content-Type', ''):
                        data = response.json()
                        result['data_type'] = 'json'
                        result['sample_data'] = str(data)[:500] + "..." if len(str(data)) > 500 else str(data)
                        
                        # Count records if it's a list
                        if isinstance(data, list):
                            result['record_count'] = len(data)
                        elif isinstance(data, dict) and 'data' in data:
                            result['record_count'] = len(data['data']) if isinstance(data['data'], list) else 1
                            
                    elif 'csv' in response.headers.get('Content-Type', ''):
                        result['data_type'] = 'csv'
                        lines = response.text.split('\n')[:5]  # First 5 lines
                        result['sample_data'] = '\n'.join(lines)
                        result['estimated_records'] = len(response.text.split('\n')) - 1  # Minus header
                        
                    else:
                        result['data_type'] = 'text/html'
                        result['sample_data'] = response.text[:200] + "..."
                        
                except Exception as e:
                    result['parse_error'] = str(e)
                    result['raw_content'] = response.text[:200] + "..."
            else:
                result['error_message'] = response.text[:200] if response.text else "No error message"
            
            # Status reporting
            status_emoji = "✅" if result['success'] else "❌"
            print(f"   Status: {status_emoji} HTTP {response.status_code} ({response_time:.0f}ms)")
            
            if result['success']:
                print(f"   Content: {result['content_type']} ({result['content_length']:,} bytes)")
                if 'record_count' in result:
                    print(f"   Records: {result['record_count']:,}")
                elif 'estimated_records' in result:
                    print(f"   Est. Records: {result['estimated_records']:,}")
            else:
                print(f"   Error: {result['error_message']}")
            
            self.results[f"{source}_{endpoint.split('/')[-1]}"] = result
            return result
            
        except requests.RequestException as e:
            error_result = {
                'source': source,
                'endpoint': endpoint,
                'method': method,
                'error': str(e),
                'success': False,
                'timestamp': datetime.now().isoformat()
            }
            print(f"   Status: ❌ Request Failed: {str(e)}")
            self.results[f"{source}_{endpoint.split('/')[-1]}"] = error_result
            return error_result

    def test_la_county_sources(self):
        """Test LA County Building & Safety data sources"""
        print("\n" + "="*80)
        print("🏛️ LA COUNTY BUILDING & SAFETY DATA SOURCES")
        print("="*80)
        
        # LA County Open Data Portal
        self.log_test(
            "LA County Open Data",
            "https://data.lacounty.gov/api/views/metadata/v1"
        )
        
        # Search for building/permit datasets
        self.log_test(
            "LA County Dataset Search - Building",
            "https://data.lacounty.gov/api/views.json?query=building"
        )
        
        self.log_test(
            "LA County Dataset Search - Permit", 
            "https://data.lacounty.gov/api/views.json?query=permit"
        )
        
        # Check specific building safety datasets
        building_datasets = [
            "building-permits",
            "construction-permits", 
            "building-inspections",
            "code-enforcement"
        ]
        
        for dataset in building_datasets:
            self.log_test(
                f"LA County - {dataset}",
                f"https://data.lacounty.gov/resource/{dataset}.json?$limit=10"
            )
        
        # Try LA County Department of Public Works
        self.log_test(
            "LA County DPW API",
            "https://dpw.lacounty.gov/api/permits"
        )

    def test_regional_building_departments(self):
        """Test Regional Building Department APIs"""
        print("\n" + "="*80)
        print("🏢 REGIONAL BUILDING DEPARTMENTS")
        print("="*80)
        
        # West Hollywood
        west_hollywood_endpoints = [
            "https://data.weho.org/api/views/metadata/v1",
            "https://weho.org/services/building-and-safety/permits",
            "https://data.weho.org/resource/permits.json"
        ]
        
        for endpoint in west_hollywood_endpoints:
            self.log_test("West Hollywood", endpoint)
        
        # Santa Monica
        santa_monica_endpoints = [
            "https://data.smgov.net/api/views/metadata/v1",
            "https://data.smgov.net/resource/building-permits.json", 
            "https://www.smgov.net/departments/pcs/permits/"
        ]
        
        for endpoint in santa_monica_endpoints:
            self.log_test("Santa Monica", endpoint)
            
        # Beverly Hills
        beverly_hills_endpoints = [
            "https://www.beverlyhills.org/departments/communitydevelopment/buildingsafety/",
            "https://beverlyhills.org/api/permits",
            "https://www.beverlyhills.org/services/permits"
        ]
        
        for endpoint in beverly_hills_endpoints:
            self.log_test("Beverly Hills", endpoint)
            
        # Culver City
        self.log_test(
            "Culver City Open Data",
            "https://data.culvercity.org/api/views/metadata/v1"
        )

    def test_alternative_la_city_endpoints(self):
        """Test alternative LA City permit dataset endpoints"""
        print("\n" + "="*80)
        print("🌆 ALTERNATIVE LA CITY ENDPOINTS")
        print("="*80)
        
        # Base Socrata endpoint for LA
        base_url = "https://data.lacity.org"
        
        # Alternative dataset IDs mentioned in documentation
        alternative_datasets = [
            "pi9x-tg5x",  # Fallback dataset mentioned
            "yv23-pmwf",  # Original that requires auth
            "building-permits",
            "construction-permits",
            "permit-data"
        ]
        
        for dataset_id in alternative_datasets:
            # Try JSON endpoint
            self.log_test(
                f"LA City - {dataset_id} (JSON)",
                f"{base_url}/resource/{dataset_id}.json?$limit=10"
            )
            
            # Try CSV endpoint
            self.log_test(
                f"LA City - {dataset_id} (CSV)",
                f"{base_url}/resource/{dataset_id}.csv?$limit=10"
            )
        
        # Try different LA City data portals
        other_la_endpoints = [
            "https://geohub.lacity.org/datasets/building-permits/api",
            "https://planning.lacity.org/api/permits",
            "https://ladbs.lacity.org/api/permits",
            "https://data.lacity.org/api/views.json?query=building permit"
        ]
        
        for endpoint in other_la_endpoints:
            self.log_test("LA City Alternative Portal", endpoint)

    def test_public_portal_analysis(self):
        """Analyze public portal scraping feasibility"""
        print("\n" + "="*80)
        print("🌐 PUBLIC PORTAL ANALYSIS")
        print("="*80)
        
        # Check robots.txt for major permit portals
        permit_portals = [
            "https://www.lacity.org/robots.txt",
            "https://ladbs.lacity.org/robots.txt", 
            "https://data.lacounty.gov/robots.txt",
            "https://data.smgov.net/robots.txt",
            "https://www.beverlyhills.org/robots.txt"
        ]
        
        for portal in permit_portals:
            self.log_test("Robots.txt Check", portal)
        
        # Test permit search interfaces
        search_interfaces = [
            "https://ladbs.lacity.org/services/permits/permit-search",
            "https://aca-prod.accela.com/lacity/",
            "https://www.lacounty.gov/government/public-works/building-and-safety/"
        ]
        
        for interface in search_interfaces:
            self.log_test("Permit Search Interface", interface)

    def generate_summary_report(self):
        """Generate comprehensive summary of all test results"""
        print("\n" + "="*100)
        print("📊 ALTERNATIVE PERMIT SOURCES - COMPREHENSIVE TEST RESULTS")
        print("="*100)
        
        successful_sources = []
        failed_sources = []
        
        for key, result in self.results.items():
            if result.get('success', False):
                successful_sources.append(result)
            else:
                failed_sources.append(result)
        
        print(f"\n✅ SUCCESSFUL SOURCES: {len(successful_sources)}")
        print(f"❌ FAILED SOURCES: {len(failed_sources)}")
        print(f"📊 TOTAL TESTED: {len(self.results)}")
        
        if successful_sources:
            print(f"\n🎯 WORKING DATA SOURCES:")
            for source in successful_sources:
                record_info = ""
                if 'record_count' in source:
                    record_info = f" ({source['record_count']:,} records)"
                elif 'estimated_records' in source:
                    record_info = f" (~{source['estimated_records']:,} records)"
                
                print(f"  ✅ {source['source']}: HTTP {source['status_code']} - {source['data_type']}{record_info}")
                if 'rate_limiting' in source:
                    print(f"     Rate Limits: {source['rate_limiting']}")
        
        print(f"\n❌ FAILED SOURCES:")
        for source in failed_sources[:10]:  # Show first 10 failures
            error = source.get('error_message', source.get('error', 'Unknown error'))
            print(f"  ❌ {source['source']}: HTTP {source.get('status_code', 'N/A')} - {error[:100]}")
        
        # Recommendations
        print(f"\n💡 RECOMMENDATIONS:")
        
        if successful_sources:
            print("1. ✅ VIABLE ALTERNATIVES FOUND:")
            for source in successful_sources:
                if source.get('record_count', 0) > 0 or source.get('estimated_records', 0) > 100:
                    print(f"   • {source['source']} - Use for permit data collection")
        else:
            print("1. ❌ NO VIABLE API ALTERNATIVES FOUND")
            print("   • Consider web scraping with proper rate limiting")
            print("   • Request API access from data providers")
            print("   • Use cached permit data from other sources")
        
        # Export results
        with open('alternative_permit_sources_results.json', 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        
        print(f"\n📋 DETAILED RESULTS: alternative_permit_sources_results.json")

def main():
    """Run comprehensive alternative permit source testing"""
    print("🔍 TESTING ALTERNATIVE FREE BUILDING PERMIT SOURCES FOR LA")
    print("Complete API testing with actual HTTP calls and response analysis")
    print("="*100)
    
    tester = AlternativePermitSourceTester()
    
    # Update todo: starting LA County tests
    from pathlib import Path
    import sys
    current_dir = Path(__file__).parent
    sys.path.insert(0, str(current_dir))
    
    # Test all source categories
    tester.test_la_county_sources()
    tester.test_regional_building_departments() 
    tester.test_alternative_la_city_endpoints()
    tester.test_public_portal_analysis()
    
    # Generate comprehensive summary
    tester.generate_summary_report()
    
    return len([r for r in tester.results.values() if r.get('success', False)]) > 0

if __name__ == "__main__":
    main()