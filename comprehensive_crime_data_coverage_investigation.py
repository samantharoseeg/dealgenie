#!/usr/bin/env python3
"""
COMPREHENSIVE CRIME DATA COVERAGE INVESTIGATION
Test all LAPD endpoints and external sources for complete crime data
"""

import requests
import json
from datetime import datetime, timedelta
from collections import defaultdict
import time

def comprehensive_crime_coverage_investigation():
    """Test all available crime data sources for coverage completeness"""
    print("🚨 COMPREHENSIVE CRIME DATA COVERAGE INVESTIGATION")
    print("=" * 80)
    print(f"Investigation Time: {datetime.now()}")
    print("PROBLEM: Current API shows only 1-2 crimes/day vs expected 150+ for LA")
    print()
    
    # 1. Test Multiple LAPD API Endpoints
    print("1. TESTING MULTIPLE LAPD API ENDPOINTS:")
    print("=" * 60)
    
    lapd_endpoints = [
        {
            'name': 'Crime Data 2020-Present (Current)',
            'url': 'https://data.lacity.org/resource/2nrs-mtv8.json',
            'description': 'Primary crime dataset (currently used)',
            'date_field': 'date_occ'
        },
        {
            'name': 'Crime Data 2010-2019 (Historical)',
            'url': 'https://data.lacity.org/resource/63jg-8b9z.json', 
            'description': 'Historical crime data',
            'date_field': 'date_occ'
        },
        {
            'name': 'LAPD Arrest Data',
            'url': 'https://data.lacity.org/resource/yru6-6re4.json',
            'description': 'Arrest records',
            'date_field': 'arst_date'
        },
        {
            'name': 'Traffic Collision Data',
            'url': 'https://data.lacity.org/resource/d5tf-ez2w.json',
            'description': 'Vehicle accidents and collisions',
            'date_field': 'date_occurred'
        },
        {
            'name': 'LAPD Calls for Service',
            'url': 'https://data.lacity.org/resource/84iq-i2r6.json',
            'description': 'All service calls (highest volume)',
            'date_field': 'incident_date'
        },
        {
            'name': 'LAPD Incident Reports',
            'url': 'https://data.lacity.org/resource/a6tr-bi9c.json',
            'description': 'Detailed incident reports',
            'date_field': 'date_occurred'
        }
    ]
    
    endpoint_results = []
    
    for endpoint in lapd_endpoints:
        print(f"\n📍 TESTING: {endpoint['name']}")
        print(f"   URL: {endpoint['url']}")
        print(f"   Description: {endpoint['description']}")
        
        try:
            # Test 1: Basic connectivity and recent data volume
            params = {
                '$limit': 1000,
                '$order': f"{endpoint['date_field']} DESC"
            }
            
            response = requests.get(endpoint['url'], params=params, timeout=30)
            print(f"   Response Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print(f"   Records Retrieved: {len(data)}")
                
                if data:
                    # Analyze recent data
                    dates = [item.get(endpoint['date_field'], '') for item in data]
                    dates = [d for d in dates if d and len(d) >= 10]
                    
                    if dates:
                        dates.sort(reverse=True)
                        most_recent = dates[0][:10]  # Just date part
                        oldest_in_sample = dates[-1][:10]
                        
                        print(f"   Most Recent: {most_recent}")
                        print(f"   Oldest in Sample: {oldest_in_sample}")
                        
                        # Count recent dates for daily volume estimate
                        recent_dates = [d[:10] for d in dates if d.startswith('2025')]
                        if not recent_dates:
                            recent_dates = [d[:10] for d in dates if d.startswith('2024')]
                            year_tested = '2024'
                        else:
                            year_tested = '2025'
                        
                        if recent_dates:
                            date_counts = defaultdict(int)
                            for date in recent_dates:
                                date_counts[date] += 1
                            
                            daily_counts = list(date_counts.values())
                            avg_daily = sum(daily_counts) / len(daily_counts) if daily_counts else 0
                            
                            print(f"   {year_tested} Sample Days: {len(date_counts)}")
                            print(f"   Average Daily Volume: {avg_daily:.1f}")
                            
                            # Show sample daily counts
                            print(f"   Sample Daily Counts:")
                            for date, count in sorted(date_counts.items(), reverse=True)[:5]:
                                print(f"     {date}: {count} incidents")
                            
                            endpoint_results.append({
                                'name': endpoint['name'],
                                'url': endpoint['url'],
                                'status': 'SUCCESS',
                                'records': len(data),
                                'most_recent': most_recent,
                                'avg_daily': avg_daily,
                                'sample_days': len(date_counts),
                                'year_tested': year_tested
                            })
                        else:
                            print(f"   ⚠️ No recent data found")
                            endpoint_results.append({
                                'name': endpoint['name'],
                                'url': endpoint['url'],
                                'status': 'NO_RECENT_DATA',
                                'records': len(data),
                                'most_recent': most_recent if dates else None
                            })
                    else:
                        print(f"   ⚠️ No valid dates in response")
                        endpoint_results.append({
                            'name': endpoint['name'],
                            'url': endpoint['url'],
                            'status': 'NO_DATES',
                            'records': len(data)
                        })
                else:
                    print(f"   ⚠️ Empty response")
                    endpoint_results.append({
                        'name': endpoint['name'],
                        'url': endpoint['url'],
                        'status': 'EMPTY_RESPONSE',
                        'records': 0
                    })
            else:
                print(f"   ❌ HTTP Error: {response.status_code}")
                if response.text:
                    print(f"   Error Details: {response.text[:200]}...")
                endpoint_results.append({
                    'name': endpoint['name'],
                    'url': endpoint['url'],
                    'status': f'HTTP_ERROR_{response.status_code}',
                    'records': 0
                })
                
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Connection Error: {str(e)}")
            endpoint_results.append({
                'name': endpoint['name'],
                'url': endpoint['url'],
                'status': 'CONNECTION_ERROR',
                'error': str(e),
                'records': 0
            })
        except Exception as e:
            print(f"   ❌ Processing Error: {str(e)}")
            endpoint_results.append({
                'name': endpoint['name'],
                'url': endpoint['url'],
                'status': 'PROCESSING_ERROR', 
                'error': str(e),
                'records': 0
            })
        
        time.sleep(1)  # Rate limiting
    
    print("\n" + "=" * 60)
    print("LAPD ENDPOINT SUMMARY:")
    print("=" * 60)
    
    working_endpoints = [r for r in endpoint_results if r.get('avg_daily', 0) > 0]
    if working_endpoints:
        # Sort by daily volume
        working_endpoints.sort(key=lambda x: x.get('avg_daily', 0), reverse=True)
        
        print("📊 ENDPOINTS WITH DAILY VOLUME DATA:")
        for endpoint in working_endpoints:
            print(f"   {endpoint['name']}")
            print(f"     Daily Volume: {endpoint['avg_daily']:.1f} incidents/day")
            print(f"     Sample Size: {endpoint['sample_days']} days ({endpoint['year_tested']})")
            print(f"     Most Recent: {endpoint['most_recent']}")
            print()
        
        # Identify best source
        best_endpoint = working_endpoints[0]
        print(f"🏆 HIGHEST VOLUME SOURCE: {best_endpoint['name']}")
        print(f"   Volume: {best_endpoint['avg_daily']:.1f} incidents/day")
        print(f"   Status: {'✅ VIABLE' if best_endpoint['avg_daily'] > 50 else '⚠️ STILL LOW'}")
        
    else:
        print("❌ NO ENDPOINTS WITH MEASURABLE DAILY VOLUMES")
    
    print()
    
    # 2. Test Alternative Query Parameters
    print("2. TESTING ALTERNATIVE QUERY PARAMETERS:")
    print("=" * 60)
    
    # Test primary endpoint with different parameters
    primary_url = "https://data.lacity.org/resource/2nrs-mtv8.json"
    
    test_scenarios = [
        {
            'name': 'No Date Filter (All Available)',
            'params': {'$limit': 5000, '$order': 'date_occ DESC'}
        },
        {
            'name': 'High Limit Recent Data',
            'params': {
                '$limit': 10000,
                '$where': "date_occ >= '2024-01-01T00:00:00'",
                '$order': 'date_occ DESC'
            }
        },
        {
            'name': 'Specific Area Code Test',
            'params': {
                '$limit': 1000,
                '$where': "area = '01'",  # Central area
                '$order': 'date_occ DESC'
            }
        },
        {
            'name': 'All 2025 Data Available',
            'params': {
                '$limit': 50000,
                '$where': "date_occ >= '2025-01-01T00:00:00'",
                '$order': 'date_occ DESC'
            }
        }
    ]
    
    for scenario in test_scenarios:
        print(f"\n📍 TESTING: {scenario['name']}")
        print(f"   Parameters: {scenario['params']}")
        
        try:
            response = requests.get(primary_url, params=scenario['params'], timeout=45)
            
            if response.status_code == 200:
                data = response.json()
                print(f"   Records Retrieved: {len(data)}")
                
                if data:
                    # Analyze the data
                    dates = [item.get('date_occ', '') for item in data if item.get('date_occ')]
                    dates = [d for d in dates if d and len(d) >= 10]
                    
                    if dates:
                        dates.sort(reverse=True)
                        print(f"   Date Range: {dates[-1][:10]} to {dates[0][:10]}")
                        
                        # Daily volume calculation
                        date_counts = defaultdict(int)
                        for date in dates:
                            date_counts[date[:10]] += 1
                        
                        if len(date_counts) > 1:
                            avg_daily = len(data) / len(date_counts)
                            print(f"   Days Covered: {len(date_counts)}")
                            print(f"   Average Daily: {avg_daily:.1f}")
                            
                            # Show distribution
                            daily_values = list(date_counts.values())
                            print(f"   Daily Range: {min(daily_values)} to {max(daily_values)}")
                            
                            if avg_daily > 100:
                                print("   ✅ HIGH VOLUME - Realistic daily counts")
                            elif avg_daily > 50:
                                print("   ⚠️ MODERATE VOLUME - May be filtered")
                            else:
                                print("   ❌ LOW VOLUME - Likely incomplete")
            else:
                print(f"   ❌ HTTP Error: {response.status_code}")
                
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
        
        time.sleep(2)  # Rate limiting for large requests
    
    print()
    
    # 3. External Source Validation
    print("3. EXTERNAL SOURCE VALIDATION:")
    print("=" * 60)
    
    print("📍 CHECKING EXTERNAL CRIME DATA SOURCES:")
    
    external_sources = [
        {
            'name': 'LAPD CompStat Data',
            'url': 'https://www.lapdonline.org/crime-mapping-and-compstat/',
            'description': 'Official LAPD crime statistics'
        },
        {
            'name': 'LA City Crime Statistics',
            'url': 'https://data.lacity.org/browse?category=Public+Safety',
            'description': 'All LA City safety datasets'
        },
        {
            'name': 'California DOJ Crime Stats',
            'url': 'https://data-openjustice.doj.ca.gov/sites/default/files/dataset/',
            'description': 'State-level crime reporting'
        }
    ]
    
    # Test LA City data catalog for additional datasets
    print(f"\n📍 SEARCHING LA CITY DATA CATALOG:")
    try:
        catalog_url = "https://data.lacity.org/api/catalog/v1"
        response = requests.get(catalog_url, params={'q': 'crime', 'limit': 20}, timeout=15)
        
        if response.status_code == 200:
            catalog_data = response.json()
            datasets = catalog_data.get('results', [])
            
            print(f"   Found {len(datasets)} crime-related datasets:")
            for i, dataset in enumerate(datasets[:10]):  # Show top 10
                name = dataset.get('resource', {}).get('name', 'Unknown')
                description = dataset.get('resource', {}).get('description', 'No description')
                resource_id = dataset.get('resource', {}).get('id', '')
                
                print(f"   {i+1:2d}. {name}")
                print(f"       ID: {resource_id}")
                print(f"       Description: {description[:100]}{'...' if len(description) > 100 else ''}")
                
                # If this looks like a crime dataset, test it
                if resource_id and ('crime' in name.lower() or 'incident' in name.lower()):
                    try:
                        test_url = f"https://data.lacity.org/resource/{resource_id}.json"
                        test_response = requests.get(test_url, params={'$limit': 10}, timeout=10)
                        if test_response.status_code == 200:
                            test_data = test_response.json()
                            print(f"       ✅ Accessible: {len(test_data)} sample records")
                        else:
                            print(f"       ❌ Not accessible: HTTP {test_response.status_code}")
                    except:
                        print(f"       ⚠️ Could not test accessibility")
                print()
        else:
            print(f"   ❌ Catalog Error: {response.status_code}")
    except Exception as e:
        print(f"   ❌ Catalog Search Error: {e}")
    
    print()
    
    # 4. Coverage Impact Assessment
    print("4. COVERAGE IMPACT ASSESSMENT:")
    print("=" * 60)
    
    print("📊 CURRENT SYSTEM ANALYSIS:")
    print("   Current API Daily Volume: 1-2 incidents/day")
    print("   Expected LA Daily Volume: 150+ incidents/day") 
    print("   Coverage Gap: 99%+ missing data")
    print()
    
    print("🎯 RELIABILITY ASSESSMENT:")
    print("   Current Data Density: EXTREMELY LOW")
    print("   Geographic Coverage: SPARSE (most areas no data)")
    print("   Statistical Significance: INSUFFICIENT")
    print("   Investment-Grade Analysis: ❌ NOT SUPPORTED")
    print()
    
    print("💡 RECOMMENDATIONS:")
    
    # Find the best endpoint from our testing
    if working_endpoints:
        best = working_endpoints[0]
        print(f"   1. SWITCH TO: {best['name']}")
        print(f"      Volume: {best['avg_daily']:.1f} incidents/day")
        print(f"      Status: {'Viable' if best['avg_daily'] > 50 else 'Still low but better'}")
    else:
        print("   1. NO VIABLE SINGLE ENDPOINT FOUND")
    
    print("   2. MULTI-SOURCE STRATEGY:")
    print("      - Combine multiple LAPD endpoints")
    print("      - Add LA County Sheriff data")
    print("      - Include CHP traffic incidents")
    print("      - Aggregate all incident types")
    
    print("   3. DATA FRESHNESS STRATEGY:")
    print("      - Implement daily batch updates")
    print("      - Monitor data availability")
    print("      - Alert on volume anomalies")
    
    print("   4. USER DISCLOSURE:")
    print("      - Clearly state data limitations")
    print("      - Show last update timestamp")
    print("      - Indicate coverage completeness")
    
    print()
    print(f"🏁 Investigation Complete: {datetime.now()}")
    print("=" * 80)
    
    return endpoint_results, working_endpoints

if __name__ == "__main__":
    comprehensive_crime_coverage_investigation()