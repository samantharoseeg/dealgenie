#!/usr/bin/env python3
"""
TEST ARREST DATA COORDINATES
Test the 125/day arrest data source for geographic coverage
"""

import requests
from datetime import datetime

def test_arrest_data_coordinates():
    """Test arrest data for coordinate availability"""
    print("🔍 TESTING ARREST DATA FOR COORDINATE COVERAGE")
    print("=" * 80)
    print(f"Test Time: {datetime.now()}")
    print()
    
    # Test the 125/day arrest data source
    arrest_url = "https://data.lacity.org/resource/amvf-fr72.json"
    
    print(f"📍 Testing: Arrest Data from 2020 to Present")
    print(f"   URL: {arrest_url}")
    print(f"   Expected Volume: 125 incidents/day")
    
    try:
        # Get recent sample
        params = {
            '$limit': 1000,
            '$order': 'arst_date DESC'
        }
        
        response = requests.get(arrest_url, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            print(f"   ✅ Retrieved: {len(data)} records")
            
            if data:
                # Check first few records for structure
                print("\n   📋 RECORD STRUCTURE ANALYSIS:")
                sample = data[0]
                
                print("   Available Fields:")
                for key, value in sample.items():
                    print(f"      {key}: {str(value)[:50]}{'...' if len(str(value)) > 50 else ''}")
                
                # Look for coordinate fields
                coord_fields = []
                for key in sample.keys():
                    if any(geo_term in key.lower() for geo_term in ['lat', 'lon', 'coord', 'location', 'address']):
                        coord_fields.append(key)
                
                print(f"\n   📍 POTENTIAL COORDINATE FIELDS: {coord_fields}")
                
                # Test coordinate availability
                if coord_fields:
                    for field in coord_fields:
                        values = [record.get(field, '') for record in data[:100]]  # Test first 100
                        non_empty = [v for v in values if v and str(v).strip()]
                        
                        print(f"      {field}: {len(non_empty)}/{len(values)} populated ({len(non_empty)/len(values)*100:.1f}%)")
                        
                        if non_empty:
                            print(f"         Samples: {non_empty[:3]}")
                
                # Check if we can get coordinates from address
                if 'location' in sample:
                    location_data = sample['location']
                    print(f"\n   📍 LOCATION FIELD STRUCTURE:")
                    if isinstance(location_data, dict):
                        for k, v in location_data.items():
                            print(f"      {k}: {v}")
                    else:
                        print(f"      location: {location_data}")
                
                # Test volume analysis
                print(f"\n   📊 VOLUME VERIFICATION:")
                dates = [record.get('arst_date', '') for record in data]
                dates = [d[:10] for d in dates if d and len(d) >= 10]
                
                if dates:
                    from collections import defaultdict
                    date_counts = defaultdict(int)
                    for date in dates:
                        date_counts[date] += 1
                    
                    daily_volumes = list(date_counts.values())
                    avg_daily = sum(daily_volumes) / len(daily_volumes) if daily_volumes else 0
                    
                    print(f"      Sample Days: {len(date_counts)}")
                    print(f"      Average Daily: {avg_daily:.1f}")
                    
                    recent_dates = sorted(date_counts.items(), reverse=True)[:5]
                    for date, count in recent_dates:
                        print(f"         {date}: {count} arrests")
                
                print(f"\n   🎯 ASSESSMENT:")
                if coord_fields and any(len([v for v in [record.get(field, '') for record in data[:100]] if v]) > 50 for field in coord_fields):
                    print("      ✅ HAS COORDINATE DATA - Viable for geographic analysis")
                else:
                    print("      ❌ NO COORDINATE DATA - Cannot use for geographic mapping")
            
        else:
            print(f"   ❌ HTTP Error: {response.status_code}")
    
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    print()
    
    # Also test the historical arrest data
    historical_url = "https://data.lacity.org/resource/yru6-6re4.json"
    
    print(f"📍 Testing: Historical Arrest Data (2010-2019)")
    print(f"   URL: {historical_url}")
    print(f"   Expected Volume: 200 incidents/day")
    
    try:
        params = {
            '$limit': 500,
            '$order': 'arst_date DESC'
        }
        
        response = requests.get(historical_url, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            print(f"   ✅ Retrieved: {len(data)} records")
            
            if data:
                sample = data[0]
                
                # Look for coordinate fields
                coord_fields = []
                for key in sample.keys():
                    if any(geo_term in key.lower() for geo_term in ['lat', 'lon', 'coord', 'location', 'address']):
                        coord_fields.append(key)
                
                print(f"   📍 COORDINATE FIELDS: {coord_fields}")
                
                if coord_fields:
                    for field in coord_fields:
                        values = [record.get(field, '') for record in data[:100]]
                        non_empty = [v for v in values if v and str(v).strip()]
                        print(f"      {field}: {len(non_empty)}/{len(values)} populated ({len(non_empty)/len(values)*100:.1f}%)")
                
                if coord_fields and any(len([v for v in [record.get(field, '') for record in data[:100]] if v]) > 50 for field in coord_fields):
                    print("      ✅ HAS COORDINATE DATA")
                else:
                    print("      ❌ NO COORDINATE DATA")
        
        else:
            print(f"   ❌ HTTP Error: {response.status_code}")
    
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    print()
    print("🏁 COORDINATE TESTING COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    test_arrest_data_coordinates()