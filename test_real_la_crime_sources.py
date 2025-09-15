#!/usr/bin/env python3
"""
TEST REAL LA CRIME DATA SOURCES
Find authoritative sources showing realistic crime volumes (100+ incidents/day)
"""

import requests
import json
from datetime import datetime, timedelta
from collections import defaultdict
import time
import csv
from io import StringIO

def test_real_la_crime_sources():
    """Test authoritative LA crime data sources for realistic volumes"""
    print("🔍 TESTING REAL LA CRIME DATA SOURCES")
    print("=" * 80)
    print("GOAL: Find sources showing 100+ crimes/day (not the broken 1-2/day API)")
    print(f"Investigation Time: {datetime.now()}")
    print()
    
    # 1. Los Angeles Open Data Portal - Comprehensive Search
    print("1. LOS ANGELES OPEN DATA PORTAL - COMPREHENSIVE SEARCH")
    print("=" * 70)
    
    # Search for all crime-related datasets
    try:
        catalog_search_url = "https://data.lacity.org/api/catalog/v1"
        search_params = {
            'domains': 'data.lacity.org',
            'search_context': 'data.lacity.org',
            'q': 'crime police incident arrest',
            'limit': 50
        }
        
        print("🔍 Searching LA Open Data Portal...")
        response = requests.get(catalog_search_url, params=search_params, timeout=30)
        
        if response.status_code == 200:
            catalog_data = response.json()
            datasets = catalog_data.get('results', [])
            
            print(f"Found {len(datasets)} potential crime datasets")
            
            working_datasets = []
            
            for i, dataset in enumerate(datasets):
                resource = dataset.get('resource', {})
                name = resource.get('name', 'Unknown')
                resource_id = resource.get('id', '')
                description = resource.get('description', '')
                
                print(f"\n📋 Dataset {i+1}: {name}")
                print(f"   ID: {resource_id}")
                print(f"   Description: {description[:150]}...")
                
                if resource_id:
                    # Test accessibility
                    try:
                        test_url = f"https://data.lacity.org/resource/{resource_id}.json"
                        test_params = {'$limit': 100, '$order': ':id DESC'}
                        
                        test_response = requests.get(test_url, params=test_params, timeout=15)
                        
                        if test_response.status_code == 200:
                            test_data = test_response.json()
                            print(f"   ✅ Accessible: {len(test_data)} records")
                            
                            # Check for date fields and analyze
                            if test_data:
                                sample_record = test_data[0]
                                date_fields = [k for k in sample_record.keys() if 'date' in k.lower() or 'time' in k.lower()]
                                
                                print(f"   Date fields: {date_fields}")
                                
                                # If this looks like crime data, analyze volume
                                if date_fields and ('crime' in name.lower() or 'incident' in name.lower() or 'arrest' in name.lower()):
                                    # Get larger sample to analyze volume
                                    volume_params = {'$limit': 5000, '$order': f'{date_fields[0]} DESC'}
                                    volume_response = requests.get(test_url, params=volume_params, timeout=30)
                                    
                                    if volume_response.status_code == 200:
                                        volume_data = volume_response.json()
                                        
                                        # Analyze daily volume
                                        dates = [item.get(date_fields[0], '') for item in volume_data]
                                        dates = [d for d in dates if d and len(d) >= 10]
                                        
                                        if dates:
                                            # Count by date
                                            date_counts = defaultdict(int)
                                            recent_dates = 0
                                            
                                            for date_str in dates:
                                                date_part = date_str[:10]  # YYYY-MM-DD
                                                date_counts[date_part] += 1
                                                
                                                if date_str.startswith('2025') or date_str.startswith('2024'):
                                                    recent_dates += 1
                                            
                                            if len(date_counts) > 1:
                                                daily_counts = list(date_counts.values())
                                                avg_daily = sum(daily_counts) / len(daily_counts)
                                                
                                                print(f"   📊 Volume Analysis:")
                                                print(f"      Days analyzed: {len(date_counts)}")
                                                print(f"      Average daily: {avg_daily:.1f}")
                                                print(f"      Daily range: {min(daily_counts)} to {max(daily_counts)}")
                                                print(f"      Recent data: {recent_dates} records")
                                                
                                                # Show sample recent dates
                                                sorted_dates = sorted(date_counts.items(), reverse=True)
                                                print(f"      Sample recent dates:")
                                                for date, count in sorted_dates[:5]:
                                                    print(f"        {date}: {count} incidents")
                                                
                                                if avg_daily > 50:
                                                    print(f"   🏆 HIGH VOLUME SOURCE FOUND!")
                                                    working_datasets.append({
                                                        'name': name,
                                                        'id': resource_id,
                                                        'url': test_url,
                                                        'avg_daily': avg_daily,
                                                        'date_field': date_fields[0],
                                                        'sample_dates': sorted_dates[:10]
                                                    })
                        else:
                            print(f"   ❌ Not accessible: HTTP {test_response.status_code}")
                    except Exception as e:
                        print(f"   ⚠️ Test error: {str(e)[:100]}...")
                
                time.sleep(0.5)  # Rate limiting
            
            print(f"\n🏆 HIGH VOLUME DATASETS FOUND: {len(working_datasets)}")
            for dataset in working_datasets:
                print(f"   • {dataset['name']}: {dataset['avg_daily']:.1f}/day")
        
        else:
            print(f"❌ Catalog search failed: HTTP {response.status_code}")
    
    except Exception as e:
        print(f"❌ LA Open Data Portal error: {e}")
    
    print()
    
    # 2. Test specific known crime datasets
    print("2. TESTING SPECIFIC KNOWN CRIME DATASETS")
    print("=" * 70)
    
    known_datasets = [
        {
            'name': 'Crime Data from 2020 to Present',
            'id': '2nrs-mtv8',
            'test_params': {'$limit': 10000, '$where': "date_occ >= '2024-01-01'", '$order': 'date_occ DESC'}
        },
        {
            'name': 'Arrest Data from 2020 to Present', 
            'id': 'amvf-fr4v',
            'test_params': {'$limit': 5000, '$order': 'arst_date DESC'}
        },
        {
            'name': 'Traffic Collision Data',
            'id': 'd5tf-ez2w',
            'test_params': {'$limit': 5000, '$order': 'date_occ DESC'}
        },
        {
            'name': 'LAPD Service Call Data',
            'id': 'wjz9-h9np',
            'test_params': {'$limit': 5000, '$order': 'date_occ DESC'}
        }
    ]
    
    for dataset in known_datasets:
        print(f"\n📋 Testing: {dataset['name']}")
        print(f"   ID: {dataset['id']}")
        
        try:
            url = f"https://data.lacity.org/resource/{dataset['id']}.json"
            response = requests.get(url, params=dataset['test_params'], timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                print(f"   ✅ Accessible: {len(data)} records")
                
                if data:
                    # Find date fields
                    sample = data[0]
                    date_fields = [k for k in sample.keys() if 'date' in k.lower() and sample[k]]
                    
                    if date_fields:
                        date_field = date_fields[0]
                        print(f"   Date field: {date_field}")
                        
                        # Analyze volume
                        dates = [item.get(date_field, '') for item in data if item.get(date_field)]
                        if dates:
                            # Count by date
                            date_counts = defaultdict(int)
                            for date_str in dates:
                                if len(date_str) >= 10:
                                    date_counts[date_str[:10]] += 1
                            
                            if len(date_counts) > 1:
                                daily_counts = list(date_counts.values())
                                avg_daily = sum(daily_counts) / len(date_counts)
                                
                                print(f"   📊 Volume: {avg_daily:.1f} incidents/day")
                                print(f"   Range: {min(daily_counts)} to {max(daily_counts)} per day")
                                print(f"   Days: {len(date_counts)}")
                                
                                # Show most recent
                                recent_dates = sorted(date_counts.items(), reverse=True)[:5]
                                print(f"   Recent dates:")
                                for date, count in recent_dates:
                                    print(f"     {date}: {count}")
                                
                                if avg_daily > 100:
                                    print(f"   🎯 REALISTIC VOLUME FOUND!")
            else:
                print(f"   ❌ HTTP {response.status_code}")
                
        except Exception as e:
            print(f"   ❌ Error: {str(e)[:100]}...")
    
    print()
    
    # 3. California DOJ OpenJustice Portal
    print("3. CALIFORNIA DOJ OPENJUSTICE PORTAL")
    print("=" * 70)
    
    print("🔍 Testing CA DOJ OpenJustice for LA crime data...")
    
    try:
        # Test main data portal
        openjustice_urls = [
            "https://data-openjustice.doj.ca.gov/api/3/action/package_search?q=los+angeles+crime",
            "https://data-openjustice.doj.ca.gov/api/3/action/package_search?q=crime+statistics",
            "https://openjustice.doj.ca.gov/data"
        ]
        
        for url in openjustice_urls:
            print(f"\n📍 Testing: {url}")
            try:
                response = requests.get(url, timeout=15)
                print(f"   Status: {response.status_code}")
                
                if response.status_code == 200:
                    if 'json' in response.headers.get('content-type', '').lower():
                        data = response.json()
                        if 'result' in data:
                            results = data['result']
                            print(f"   Found {len(results.get('results', []))} datasets")
                            
                            # Look for crime-related datasets
                            for dataset in results.get('results', [])[:5]:
                                name = dataset.get('title', 'Unknown')
                                if 'crime' in name.lower() or 'los angeles' in name.lower():
                                    print(f"     • {name}")
                    else:
                        print(f"   Response length: {len(response.text)} chars")
                else:
                    print(f"   Error: {response.status_code}")
            except Exception as e:
                print(f"   Error: {str(e)[:100]}...")
    
    except Exception as e:
        print(f"❌ OpenJustice error: {e}")
    
    print()
    
    # 4. CrimeMapping.com Integration Test
    print("4. CRIMEMAPPING.COM INTEGRATION TEST")
    print("=" * 70)
    
    print("🔍 Testing CrimeMapping.com for LA data access...")
    
    try:
        crimemapping_urls = [
            "https://www.crimemapping.com/map/ca/losangeles",
            "https://api.crimemapping.com/v1/incidents",
            "https://www.crimemapping.com/data"
        ]
        
        for url in crimemapping_urls:
            print(f"\n📍 Testing: {url}")
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                response = requests.get(url, headers=headers, timeout=15)
                print(f"   Status: {response.status_code}")
                
                if response.status_code == 200:
                    content_type = response.headers.get('content-type', '')
                    if 'json' in content_type:
                        try:
                            data = response.json()
                            print(f"   JSON data: {len(data)} items")
                        except:
                            print(f"   JSON parse error")
                    else:
                        print(f"   Content type: {content_type}")
                        # Look for API hints in HTML
                        if 'api' in response.text.lower():
                            print("   💡 Contains API references")
            except Exception as e:
                print(f"   Error: {str(e)[:100]}...")
    
    except Exception as e:
        print(f"❌ CrimeMapping error: {e}")
    
    print()
    
    # 5. FBI Crime Data Explorer
    print("5. FBI CRIME DATA EXPLORER")
    print("=" * 70)
    
    print("🔍 Testing FBI Crime Data Explorer for LA data...")
    
    try:
        fbi_urls = [
            "https://api.usa.gov/crime/fbi/sapi/api/data/nibrs",
            "https://crime-data-explorer.app.cloud.gov/api/estimates/national",
            "https://crime-data-explorer.fr.cloud.gov/api/agencies"
        ]
        
        for url in fbi_urls:
            print(f"\n📍 Testing: {url}")
            try:
                response = requests.get(url, timeout=15)
                print(f"   Status: {response.status_code}")
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        if isinstance(data, list):
                            print(f"   Data: {len(data)} items")
                        elif isinstance(data, dict):
                            print(f"   Data keys: {list(data.keys())[:5]}")
                            
                            # Look for LA agencies
                            if 'results' in data:
                                for item in data['results'][:10]:
                                    if isinstance(item, dict):
                                        name = item.get('agency_name', item.get('name', ''))
                                        if 'los angeles' in name.lower():
                                            print(f"     🎯 Found LA agency: {name}")
                    except:
                        print(f"   Response length: {len(response.text)}")
            except Exception as e:
                print(f"   Error: {str(e)[:100]}...")
    
    except Exception as e:
        print(f"❌ FBI Crime Data Explorer error: {e}")
    
    print()
    
    # 6. LA County Sheriff Department
    print("6. LA COUNTY SHERIFF DEPARTMENT")
    print("=" * 70)
    
    print("🔍 Testing LASD for downloadable crime data...")
    
    try:
        lasd_urls = [
            "https://lasd.org/transparency/",
            "https://lasd.org/crime-statistics/",
            "https://www.lasd.org/divisions-detail/?division=Crime-Analysis-Section"
        ]
        
        for url in lasd_urls:
            print(f"\n📍 Testing: {url}")
            try:
                response = requests.get(url, timeout=15)
                print(f"   Status: {response.status_code}")
                
                if response.status_code == 200:
                    content = response.text.lower()
                    
                    # Look for data download indicators
                    if 'csv' in content or 'download' in content or 'data' in content:
                        print("   💡 Contains data download references")
                    
                    if 'crime' in content and 'statistics' in content:
                        print("   💡 Contains crime statistics references")
                        
            except Exception as e:
                print(f"   Error: {str(e)[:100]}...")
    
    except Exception as e:
        print(f"❌ LASD error: {e}")
    
    print()
    
    # Summary and recommendations
    print("🏆 INVESTIGATION SUMMARY")
    print("=" * 70)
    
    print("FINDINGS:")
    if 'working_datasets' in locals() and working_datasets:
        print("✅ HIGH VOLUME SOURCES IDENTIFIED:")
        for dataset in working_datasets:
            print(f"   • {dataset['name']}")
            print(f"     Volume: {dataset['avg_daily']:.1f} incidents/day")
            print(f"     URL: {dataset['url']}")
            print(f"     Status: {'REALISTIC' if dataset['avg_daily'] > 100 else 'IMPROVED'}")
    else:
        print("⚠️ NO HIGH VOLUME SOURCES FOUND IN SYSTEMATIC SEARCH")
        print("   Need manual investigation of specific crime reporting agencies")
    
    print("\nRECOMMENDATIONS:")
    print("1. Focus on datasets showing 50+ incidents/day minimum")
    print("2. Implement multi-source aggregation strategy")
    print("3. Add data quality disclaimers for incomplete coverage")
    print("4. Monitor data availability and update frequency")
    
    print(f"\n🏁 Investigation Complete: {datetime.now()}")

if __name__ == "__main__":
    test_real_la_crime_sources()