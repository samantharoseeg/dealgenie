#!/usr/bin/env python3
"""
Test Regional Building Department APIs
West Hollywood, Santa Monica, Beverly Hills permit sources
"""

import requests
import json
import time

def test_endpoint(name, url, timeout=10):
    """Test endpoint with detailed response analysis"""
    print(f"\n🔍 TESTING: {name}")
    print(f"   URL: {url}")
    
    headers = {
        'User-Agent': 'DealGenie/1.0 (Property Intelligence Research)',
        'Accept': 'application/json, text/csv, text/html, */*'
    }
    
    try:
        start_time = time.time()
        response = requests.get(url, headers=headers, timeout=timeout)
        response_time = (time.time() - start_time) * 1000
        
        print(f"   Status: HTTP {response.status_code} ({response_time:.0f}ms)")
        print(f"   Content-Type: {response.headers.get('Content-Type', 'unknown')}")
        print(f"   Content-Length: {len(response.content):,} bytes")
        
        if response.status_code == 200:
            content_type = response.headers.get('Content-Type', '').lower()
            
            if 'json' in content_type:
                try:
                    data = response.json()
                    if isinstance(data, list):
                        print(f"   ✅ SUCCESS: JSON array with {len(data)} items")
                    elif isinstance(data, dict):
                        print(f"   ✅ SUCCESS: JSON object with keys: {list(data.keys())[:5]}")
                    print(f"   Sample: {str(data)[:200]}...")
                except:
                    print(f"   ⚠️ JSON parse failed")
                    print(f"   Raw: {response.text[:200]}...")
                    
            elif 'csv' in content_type:
                lines = response.text.split('\n')[:3]
                print(f"   ✅ SUCCESS: CSV content")
                for i, line in enumerate(lines):
                    print(f"   Line {i+1}: {line[:100]}")
                    
            elif 'html' in content_type:
                print(f"   ✅ SUCCESS: HTML page (likely interactive portal)")
                # Check for permit-related keywords
                text_lower = response.text.lower()
                permit_keywords = ['permit', 'building', 'construction', 'api', 'data']
                found_keywords = [kw for kw in permit_keywords if kw in text_lower]
                if found_keywords:
                    print(f"   Keywords found: {found_keywords}")
                
            else:
                print(f"   ✅ SUCCESS: {content_type} content")
                
        else:
            print(f"   ❌ FAILED: HTTP {response.status_code}")
            print(f"   Error: {response.text[:150]}")
            
        return {
            'name': name,
            'url': url, 
            'status_code': response.status_code,
            'success': response.status_code == 200,
            'response_time_ms': response_time,
            'content_type': response.headers.get('Content-Type'),
            'content_length': len(response.content),
            'is_api_endpoint': 'json' in response.headers.get('Content-Type', '').lower()
        }
        
    except requests.exceptions.Timeout:
        print(f"   ❌ TIMEOUT: Exceeded {timeout}s")
        return {'name': name, 'error': 'timeout', 'success': False}
        
    except requests.exceptions.ConnectionError:
        print(f"   ❌ CONNECTION ERROR")
        return {'name': name, 'error': 'connection_error', 'success': False}
        
    except Exception as e:
        print(f"   ❌ ERROR: {e}")
        return {'name': name, 'error': str(e), 'success': False}

def main():
    print("🏢 REGIONAL BUILDING DEPARTMENTS API TEST")
    print("="*60)
    
    results = []
    
    # West Hollywood
    print(f"\n{'='*20} WEST HOLLYWOOD {'='*20}")
    weho_endpoints = [
        ("West Hollywood Open Data", "https://data.weho.org/"),
        ("West Hollywood API", "https://data.weho.org/api/views/metadata/v1"),
        ("West Hollywood Building", "https://www.weho.org/services/building-and-safety"),
        ("West Hollywood Permits Search", "https://data.weho.org/browse?q=permit"),
        ("West Hollywood Socrata", "https://data.weho.org/resource/permits.json")
    ]
    
    for name, url in weho_endpoints:
        results.append(test_endpoint(name, url))
        time.sleep(0.5)
    
    # Santa Monica
    print(f"\n{'='*20} SANTA MONICA {'='*20}")
    sm_endpoints = [
        ("Santa Monica Open Data", "https://data.smgov.net/"),
        ("Santa Monica API", "https://data.smgov.net/api/views/metadata/v1"),
        ("Santa Monica Building Permits", "https://data.smgov.net/browse?q=building permit"),
        ("Santa Monica Development", "https://www.smgov.net/departments/pcs/"),
        ("Santa Monica Socrata", "https://data.smgov.net/resource/building-permits.json")
    ]
    
    for name, url in sm_endpoints:
        results.append(test_endpoint(name, url))
        time.sleep(0.5)
    
    # Beverly Hills  
    print(f"\n{'='*20} BEVERLY HILLS {'='*20}")
    bh_endpoints = [
        ("Beverly Hills Main", "https://www.beverlyhills.org/"),
        ("Beverly Hills Building", "https://www.beverlyhills.org/departments/communitydevelopment/buildingsafety/"),
        ("Beverly Hills Permits", "https://www.beverlyhills.org/services/permits"),
        ("Beverly Hills API Test", "https://beverlyhills.org/api/permits"),
        ("Beverly Hills Open Data", "https://data.beverlyhills.org/")
    ]
    
    for name, url in bh_endpoints:
        results.append(test_endpoint(name, url))
        time.sleep(0.5)
    
    # Culver City
    print(f"\n{'='*20} CULVER CITY {'='*20}")
    cc_endpoints = [
        ("Culver City Open Data", "https://data.culvercity.org/"),
        ("Culver City API", "https://data.culvercity.org/api/views/metadata/v1"),
        ("Culver City Building", "https://www.culvercity.org/services/building-safety")
    ]
    
    for name, url in cc_endpoints:
        results.append(test_endpoint(name, url))
        time.sleep(0.5)
    
    # Summary Analysis
    print(f"\n{'='*60}")
    print("📊 REGIONAL BUILDING DEPARTMENTS SUMMARY")
    print("="*60)
    
    successful = [r for r in results if r.get('success', False)]
    api_endpoints = [r for r in successful if r.get('is_api_endpoint', False)]
    html_portals = [r for r in successful if not r.get('is_api_endpoint', False)]
    failed = [r for r in results if not r.get('success', False)]
    
    print(f"✅ TOTAL SUCCESSFUL: {len(successful)}/{len(results)}")
    print(f"🔌 API ENDPOINTS: {len(api_endpoints)}")
    print(f"🌐 HTML PORTALS: {len(html_portals)}")
    print(f"❌ FAILED: {len(failed)}")
    
    if api_endpoints:
        print(f"\n🔌 WORKING API ENDPOINTS:")
        for result in api_endpoints:
            print(f"  ✅ {result['name']}: {result['content_type']}")
    else:
        print(f"\n❌ NO WORKING API ENDPOINTS FOUND")
    
    if html_portals:
        print(f"\n🌐 ACCESSIBLE PORTALS (Potential for Scraping):")
        for result in html_portals:
            print(f"  🌐 {result['name']}")
    
    if failed:
        print(f"\n❌ FAILED ENDPOINTS:")
        for result in failed[:5]:  # Show first 5 failures
            error = result.get('error', f"HTTP {result.get('status_code')}")
            print(f"  ❌ {result['name']}: {error}")
    
    # Save results
    with open('regional_permits_test_results.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n📋 Results saved to: regional_permits_test_results.json")
    
    return len(api_endpoints) > 0

if __name__ == "__main__":
    main()