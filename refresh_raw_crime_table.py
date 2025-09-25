#!/usr/bin/env python3
"""
REFRESH RAW_CRIME TABLE
Update the raw_crime table (not the view) with fresh 2025 data
"""

import requests
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import time

def refresh_raw_crime_table():
    """Pull fresh crime data and update raw_crime table"""
    print("🔄 REFRESHING RAW_CRIME TABLE WITH 2025 DATA")
    print("=" * 80)
    print(f"Refresh Start: {datetime.now()}")
    print()
    
    # Check current state
    print("📊 CURRENT RAW_CRIME TABLE:")
    with sqlite3.connect('data/dealgenie.db') as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM raw_crime")
        old_count = cursor.fetchone()[0]
        cursor.execute("SELECT MAX(date_occurred), MIN(date_occurred) FROM raw_crime")
        old_latest, old_oldest = cursor.fetchone()
        print(f"   Current Records: {old_count:,}")
        print(f"   Date Range: {old_oldest} to {old_latest}")
    print()
    
    # API endpoint with current data
    api_url = "https://data.lacity.org/resource/2nrs-mtv8.json"
    
    # Pull recent data to add to existing
    recent_date = "2024-02-02"  # Start from day after our latest data
    
    print("🌐 FETCHING NEW CRIME DATA:")
    print(f"   API: {api_url}")
    print(f"   Starting from: {recent_date}")
    
    # Fetch data in batches
    batch_size = 1000
    offset = 0
    all_new_records = []
    
    while True:
        print(f"   Fetching batch {offset//batch_size + 1}...")
        
        params = {
            '$limit': batch_size,
            '$offset': offset,
            '$where': f"date_occ >= '{recent_date}T00:00:00'",
            '$order': 'date_occ DESC'
        }
        
        try:
            response = requests.get(api_url, params=params, timeout=30)
            
            if response.status_code == 200:
                batch_data = response.json()
                
                if not batch_data:
                    print(f"   No more data available")
                    break
                    
                all_new_records.extend(batch_data)
                print(f"   Retrieved: {len(batch_data)} records (Total: {len(all_new_records)})")
                
                # Get 5000 records max for this demo
                if len(all_new_records) >= 5000:
                    all_new_records = all_new_records[:5000]
                    print("   Reached 5000 record limit")
                    break
                
                # If we got less than batch size, we're done
                if len(batch_data) < batch_size:
                    print("   Final batch received")
                    break
                    
                offset += batch_size
                time.sleep(0.3)  # Rate limiting
                
            else:
                print(f"   ❌ API Error: {response.status_code}")
                break
                
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Request Error: {e}")
            break
    
    print(f"   ✅ Total New Records Retrieved: {len(all_new_records)}")
    print()
    
    if all_new_records:
        print("📊 ANALYZING NEW DATA:")
        
        # Analyze date range
        dates = [record.get('date_occ', '') for record in all_new_records if record.get('date_occ')]
        dates = [d for d in dates if d and len(d) >= 10]
        
        if dates:
            dates.sort()
            oldest_new = dates[0]
            newest_new = dates[-1]
            
            print(f"   Date Range: {oldest_new} to {newest_new}")
            
            # Count by year
            year_counts = {}
            for date in dates:
                year = date[:4]
                year_counts[year] = year_counts.get(year, 0) + 1
            
            print("   Records by Year:")
            for year in sorted(year_counts.keys(), reverse=True):
                print(f"     {year}: {year_counts[year]:,} records")
        
        print()
        
        # Prepare data for database insert
        print("💾 PREPARING DATA FOR RAW_CRIME TABLE:")
        
        clean_records = []
        for record in all_new_records:
            # Map API fields to raw_crime table structure
            clean_record = {
                'incident_id': record.get('dr_no', ''),
                'date_occurred': record.get('date_occ', ''),
                'time_occurred': record.get('time_occ', ''),
                'area_code': record.get('area', ''),
                'area_name': record.get('area_name', ''),
                'reporting_district': record.get('rpt_dist_no', ''),
                'part_1_2': record.get('part_1_2', ''),
                'crime_code': record.get('crm_cd', ''),
                'crime_description': record.get('crm_cd_desc', ''),
                'modus_operandi': record.get('mocodes', ''),
                'victim_age': record.get('vict_age', ''),
                'victim_sex': record.get('vict_sex', ''),
                'victim_descent': record.get('vict_descent', ''),
                'premise_code': record.get('premis_cd', ''),
                'premise_description': record.get('premis_desc', ''),
                'weapon_code': record.get('weapon_used_cd', ''),
                'weapon_description': record.get('weapon_desc', ''),
                'status': record.get('status_desc', ''),
                'address': record.get('location', ''),
                'cross_street': record.get('cross_street', ''),
                'latitude': record.get('lat', ''),
                'longitude': record.get('lon', '')
            }
            
            # Validate coordinates
            if clean_record['latitude'] and clean_record['longitude']:
                try:
                    lat = float(clean_record['latitude'])
                    lon = float(clean_record['longitude'])
                    
                    # Basic LA area check
                    if 33.7 <= lat <= 34.8 and -118.7 <= lon <= -117.6:
                        clean_record['latitude'] = lat
                        clean_record['longitude'] = lon
                        clean_records.append(clean_record)
                except (ValueError, TypeError):
                    continue
        
        print(f"   Clean Records with Valid Coordinates: {len(clean_records):,}")
        
        if clean_records:
            # Insert into raw_crime table
            print("   Inserting into raw_crime table...")
            
            with sqlite3.connect('data/dealgenie.db') as conn:
                cursor = conn.cursor()
                
                # Create DataFrame for bulk insert
                df = pd.DataFrame(clean_records)
                
                # Insert new records
                df.to_sql('raw_crime', conn, if_exists='append', index=False)
                conn.commit()
                
                # Verify updated state
                cursor.execute("SELECT COUNT(*) FROM raw_crime")
                new_total = cursor.fetchone()[0]
                cursor.execute("SELECT MAX(date_occurred), MIN(date_occurred) FROM raw_crime")
                new_latest, new_oldest = cursor.fetchone()
                
                print(f"   ✅ Raw Crime Table Updated Successfully")
                print()
                
                # Show results
                print("📈 UPDATE SUMMARY:")
                print(f"   Old Record Count: {old_count:,}")
                print(f"   New Record Count: {new_total:,}")
                print(f"   Records Added: {new_total - old_count:,}")
                print(f"   Old Date Range: {old_oldest} to {old_latest}")
                print(f"   New Date Range: {new_oldest} to {new_latest}")
                
                # Check freshness improvement
                if new_latest:
                    latest_dt = datetime.fromisoformat(new_latest.replace('T00:00:00.000', '').replace('T00:00:00', ''))
                    days_behind = (datetime.now() - latest_dt).days
                    print(f"   Days Behind Now: {days_behind} (was 589)")
                    print(f"   Freshness Improvement: {589 - days_behind} days")
                
                print()
                
                # Verify crime_data view still works
                print("✅ VERIFYING CRIME_DATA VIEW:")
                cursor.execute("SELECT COUNT(*) FROM crime_data")
                view_count = cursor.fetchone()[0]
                cursor.execute("SELECT MAX(date_occurred) FROM crime_data")
                view_latest = cursor.fetchone()[0]
                print(f"   View Records: {view_count:,}")
                print(f"   View Latest: {view_latest}")
                
    else:
        print("❌ No new data to add")
    
    print()
    print(f"🏁 Raw Crime Table Refresh Complete: {datetime.now()}")

if __name__ == "__main__":
    refresh_raw_crime_table()