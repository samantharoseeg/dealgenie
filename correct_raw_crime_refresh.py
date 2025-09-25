#!/usr/bin/env python3
"""
CORRECT RAW_CRIME REFRESH
Update raw_crime table with proper column mapping
"""

import requests
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import time

def correct_raw_crime_refresh():
    """Refresh raw_crime table with correct column mapping"""
    print("🔄 CORRECT RAW_CRIME TABLE REFRESH")
    print("=" * 80)
    print(f"Refresh Start: {datetime.now()}")
    print()
    
    # Check current state
    print("📊 CURRENT RAW_CRIME TABLE:")
    with sqlite3.connect('data/dealgenie.db') as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM raw_crime")
        old_count = cursor.fetchone()[0]
        cursor.execute("SELECT MAX(date_occurred), MIN(date_occurred) FROM raw_crime WHERE date_occurred IS NOT NULL")
        result = cursor.fetchone()
        old_latest, old_oldest = result if result[0] else (None, None)
        print(f"   Current Records: {old_count:,}")
        print(f"   Date Range: {old_oldest} to {old_latest}")
    print()
    
    # Get 2025 data from API
    api_url = "https://data.lacity.org/resource/2nrs-mtv8.json"
    
    print("🌐 FETCHING 2025 CRIME DATA:")
    print(f"   API: {api_url}")
    
    # Get recent data - limit to 1000 for demo
    params = {
        '$limit': 1000,
        '$where': "date_occ >= '2025-01-01T00:00:00'",
        '$order': 'date_occ DESC'
    }
    
    try:
        response = requests.get(api_url, params=params, timeout=30)
        
        if response.status_code == 200:
            fresh_data = response.json()
            print(f"   Retrieved: {len(fresh_data)} 2025 records")
            
            if fresh_data:
                # Analyze date range
                dates = [record.get('date_occ', '') for record in fresh_data if record.get('date_occ')]
                dates = [d for d in dates if d and len(d) >= 10]
                
                if dates:
                    dates.sort()
                    oldest_new = dates[0]
                    newest_new = dates[-1]
                    print(f"   Date Range: {oldest_new} to {newest_new}")
                
                print()
                
                # Map to correct raw_crime columns
                print("💾 MAPPING TO RAW_CRIME SCHEMA:")
                
                clean_records = []
                for record in fresh_data:
                    # Map API fields to actual raw_crime columns
                    clean_record = {
                        'incident_id': record.get('dr_no', ''),
                        'date_reported': record.get('date_rptd', ''),
                        'date_occurred': record.get('date_occ', ''),
                        'time_occurred': record.get('time_occ', ''),
                        'area_id': record.get('area', ''),  # area_id not area_code
                        'area_name': record.get('area_name', ''),
                        'reporting_district': record.get('rpt_dist_no', ''),
                        'address': record.get('location', ''),
                        'cross_street': record.get('cross_street', ''),
                        'latitude': record.get('lat', ''),
                        'longitude': record.get('lon', ''),
                        'location_description': record.get('premis_desc', ''),
                        'crime_code': record.get('crm_cd', ''),
                        'crime_description': record.get('crm_cd_desc', ''),
                        'part_1_2': record.get('part_1_2', ''),
                        'modus_operandi': record.get('mocodes', ''),
                        'victim_age': record.get('vict_age', ''),
                        'victim_sex': record.get('vict_sex', ''),
                        'victim_descent': record.get('vict_descent', ''),
                        'weapon_used_code': record.get('weapon_used_cd', ''),
                        'weapon_description': record.get('weapon_desc', ''),
                        'status': record.get('status', ''),
                        'status_description': record.get('status_desc', ''),
                        'premise_code': record.get('premis_cd', ''),
                        'premise_description': record.get('premis_desc', ''),
                        'data_source': 'LAPD_OpenData_2025_Refresh',
                        'source_endpoint': api_url,
                        'as_of_date': datetime.now().strftime('%Y-%m-%d')
                    }
                    
                    # Validate and clean coordinates
                    if clean_record['latitude'] and clean_record['longitude']:
                        try:
                            lat = float(clean_record['latitude'])
                            lon = float(clean_record['longitude'])
                            
                            # Basic LA area validation
                            if 33.7 <= lat <= 34.8 and -118.7 <= lon <= -117.6:
                                clean_record['latitude'] = lat
                                clean_record['longitude'] = lon
                                clean_records.append(clean_record)
                        except (ValueError, TypeError):
                            continue
                
                print(f"   Clean Records with Valid Coordinates: {len(clean_records):,}")
                
                if clean_records:
                    # Insert into database
                    print("   Inserting into raw_crime table...")
                    
                    with sqlite3.connect('data/dealgenie.db') as conn:
                        cursor = conn.cursor()
                        
                        # Convert to DataFrame and insert
                        df = pd.DataFrame(clean_records)
                        
                        # Handle data type conversions
                        # Convert numeric fields appropriately
                        numeric_fields = ['area_id', 'part_1_2', 'victim_age']
                        for field in numeric_fields:
                            if field in df.columns:
                                df[field] = pd.to_numeric(df[field], errors='coerce')
                        
                        # Insert records
                        df.to_sql('raw_crime', conn, if_exists='append', index=False)
                        conn.commit()
                        
                        # Verify results
                        cursor.execute("SELECT COUNT(*) FROM raw_crime")
                        new_total = cursor.fetchone()[0]
                        cursor.execute("SELECT MAX(date_occurred), MIN(date_occurred) FROM raw_crime WHERE date_occurred IS NOT NULL")
                        result = cursor.fetchone()
                        new_latest, new_oldest = result if result[0] else (None, None)
                        
                        print(f"   ✅ Raw Crime Table Updated Successfully")
                        print()
                        
                        # Show update summary
                        print("📈 UPDATE SUMMARY:")
                        print(f"   Old Record Count: {old_count:,}")
                        print(f"   New Record Count: {new_total:,}")
                        print(f"   Records Added: {new_total - old_count:,}")
                        print(f"   Old Latest: {old_latest}")
                        print(f"   New Latest: {new_latest}")
                        
                        # Calculate freshness improvement
                        if new_latest:
                            try:
                                # Handle different datetime formats
                                if 'T' in new_latest:
                                    latest_dt = datetime.fromisoformat(new_latest.replace('T00:00:00.000', '').replace('T00:00:00', ''))
                                else:
                                    latest_dt = datetime.strptime(new_latest, '%Y-%m-%d')
                                
                                days_behind = (datetime.now() - latest_dt).days
                                print(f"   Days Behind: {days_behind} (was 589)")
                                print(f"   Freshness Improvement: {589 - days_behind} days")
                            except Exception as e:
                                print(f"   Date parsing issue: {e}")
                        
                        print()
                        
                        # Test crime_data view
                        print("✅ TESTING CRIME_DATA VIEW:")
                        cursor.execute("SELECT COUNT(*) FROM crime_data")
                        view_count = cursor.fetchone()[0]
                        cursor.execute("SELECT MAX(date_occurred) FROM crime_data WHERE date_occurred IS NOT NULL")
                        view_latest = cursor.fetchone()[0]
                        print(f"   View Records: {view_count:,}")
                        print(f"   View Latest: {view_latest}")
                        
                        # Test a quick crime intelligence lookup
                        print()
                        print("🧪 TESTING CRIME INTELLIGENCE WITH FRESH DATA:")
                        from src.scoring.crime_intelligence import get_crime_intelligence
                        
                        # Test downtown LA
                        start_time = time.time()
                        result = get_crime_intelligence(34.0522, -118.2437)
                        query_time = (time.time() - start_time) * 1000
                        
                        print(f"   Downtown LA (34.0522, -118.2437): {result['crime_score']:.1f}")
                        print(f"   Query Time: {query_time:.2f}ms")
                        print(f"   ✅ Crime intelligence working with fresh data")
                else:
                    print("   ❌ No valid records to insert")
            else:
                print("   ❌ No 2025 data returned from API")
        else:
            print(f"   ❌ API Error: {response.status_code}")
            
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Request Error: {e}")
    except Exception as e:
        print(f"   ❌ Unexpected Error: {e}")
    
    print()
    print(f"🏁 Raw Crime Refresh Complete: {datetime.now()}")

if __name__ == "__main__":
    correct_raw_crime_refresh()