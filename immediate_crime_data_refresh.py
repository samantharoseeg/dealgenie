#!/usr/bin/env python3
"""
IMMEDIATE CRIME DATA REFRESH
Pull current 2025 data from LAPD API and refresh database
"""

import requests
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import time

def immediate_crime_data_refresh():
    """Pull fresh crime data and update database"""
    print("🔄 IMMEDIATE CRIME DATA REFRESH")
    print("=" * 80)
    print(f"Refresh Start: {datetime.now()}")
    print()
    
    # API endpoint with current 2025 data
    api_url = "https://data.lacity.org/resource/2nrs-mtv8.json"
    
    # Pull last 6 months of data for comprehensive coverage
    six_months_ago = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
    
    print("📊 CURRENT DATABASE STATE:")
    with sqlite3.connect('data/dealgenie.db') as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM crime_data")
        old_count = cursor.fetchone()[0]
        cursor.execute("SELECT MAX(date_occurred) FROM crime_data")
        old_latest = cursor.fetchone()[0]
        print(f"   Current Records: {old_count:,}")
        print(f"   Latest Date: {old_latest}")
    print()
    
    print("🌐 FETCHING FRESH CRIME DATA:")
    print(f"   API: {api_url}")
    print(f"   Date Range: Since {six_months_ago}")
    
    # Fetch fresh data in batches
    batch_size = 2000
    offset = 0
    all_records = []
    
    while True:
        print(f"   Fetching batch {offset//batch_size + 1}...")
        
        params = {
            '$limit': batch_size,
            '$offset': offset,
            '$where': f"date_occ >= '{six_months_ago}T00:00:00'",
            '$order': 'date_occ DESC'
        }
        
        try:
            response = requests.get(api_url, params=params, timeout=30)
            
            if response.status_code == 200:
                batch_data = response.json()
                
                if not batch_data:
                    print(f"   No more data (batch {offset//batch_size + 1})")
                    break
                    
                all_records.extend(batch_data)
                print(f"   Retrieved: {len(batch_data)} records (Total: {len(all_records)})")
                
                # If we got less than batch size, we're done
                if len(batch_data) < batch_size:
                    print("   Final batch received")
                    break
                    
                offset += batch_size
                time.sleep(0.5)  # Rate limiting
                
            else:
                print(f"   ❌ API Error: {response.status_code}")
                break
                
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Request Error: {e}")
            break
    
    print(f"   ✅ Total Records Retrieved: {len(all_records)}")
    print()
    
    if all_records:
        print("📊 ANALYZING FRESH DATA:")
        
        # Analyze date range
        dates = [record.get('date_occ', '') for record in all_records if record.get('date_occ')]
        dates = [d for d in dates if d]
        
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
        
        # Prepare data for database
        print("💾 UPDATING DATABASE:")
        
        # Clean and prepare records
        clean_records = []
        for record in all_records:
            # Extract key fields
            clean_record = {
                'dr_no': record.get('dr_no', ''),
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
                'status_code': record.get('status', ''),
                'status_description': record.get('status_desc', ''),
                'crime_code_1': record.get('crm_cd_1', ''),
                'crime_code_2': record.get('crm_cd_2', ''),
                'crime_code_3': record.get('crm_cd_3', ''),
                'crime_code_4': record.get('crm_cd_4', ''),
                'address': record.get('location', ''),
                'cross_street': record.get('cross_street', ''),
                'latitude': record.get('lat', ''),
                'longitude': record.get('lon', '')
            }
            
            # Only include records with coordinates
            if clean_record['latitude'] and clean_record['longitude']:
                try:
                    clean_record['latitude'] = float(clean_record['latitude'])
                    clean_record['longitude'] = float(clean_record['longitude'])
                    clean_records.append(clean_record)
                except (ValueError, TypeError):
                    continue
        
        print(f"   Records with Valid Coordinates: {len(clean_records):,}")
        
        if clean_records:
            # Update database
            with sqlite3.connect('data/dealgenie.db') as conn:
                cursor = conn.cursor()
                
                # First, delete old data to avoid duplicates
                print("   Removing old data...")
                cursor.execute("DELETE FROM crime_data WHERE date_occurred < ?", (six_months_ago,))
                deleted_count = cursor.rowcount
                print(f"   Removed {deleted_count:,} old records")
                
                # Insert new data
                print("   Inserting fresh data...")
                
                # Create DataFrame for bulk insert
                df = pd.DataFrame(clean_records)
                df.to_sql('crime_data', conn, if_exists='append', index=False)
                
                conn.commit()
                
                # Verify new state
                cursor.execute("SELECT COUNT(*) FROM crime_data")
                new_count = cursor.fetchone()[0]
                cursor.execute("SELECT MAX(date_occurred), MIN(date_occurred) FROM crime_data")
                new_latest, new_oldest = cursor.fetchone()
                
                print(f"   ✅ Database Updated Successfully")
                print(f"   New Record Count: {new_count:,}")
                print(f"   Date Range: {new_oldest} to {new_latest}")
                print()
                
                # Show improvement
                print("📈 IMPROVEMENT SUMMARY:")
                print(f"   Old Records: {old_count:,}")
                print(f"   New Records: {new_count:,}")
                print(f"   Net Change: {new_count - old_count:+,}")
                print(f"   Old Latest: {old_latest}")
                print(f"   New Latest: {new_latest}")
                
                # Calculate freshness improvement
                if new_latest:
                    latest_dt = datetime.fromisoformat(new_latest.replace('T00:00:00', ''))
                    days_behind = (datetime.now() - latest_dt).days
                    print(f"   Days Behind: {days_behind} (vs 589 previously)")
                    print(f"   Freshness Improvement: {589 - days_behind} days")
                    
    else:
        print("❌ No fresh data retrieved - database unchanged")
    
    print()
    print(f"🏁 Refresh Complete: {datetime.now()}")

if __name__ == "__main__":
    immediate_crime_data_refresh()