import sqlite3
import pandas as pd
import json

# Connect to the database
db_path = 'zimas_ajax_last_half.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("=" * 80)
print("ZIMAS PROPERTY DATA - QUICK ANALYSIS")
print("=" * 80)

# 1. Basic table info
print("\n1. DATABASE STRUCTURE:")
print("-" * 40)
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print(f"Tables: {[t[0] for t in tables]}")

# Check comprehensive_property_data structure
cursor.execute("PRAGMA table_info(comprehensive_property_data)")
columns = cursor.fetchall()
print(f"\ncomprehensive_property_data columns:")
for col in columns:
    print(f"  {col[1]} ({col[2]})")

# 2. Record count
print("\n2. RECORD COUNT:")
print("-" * 40)
cursor.execute("SELECT COUNT(*) FROM comprehensive_property_data")
total = cursor.fetchone()[0]
print(f"Total records: {total:,}")

cursor.execute("SELECT COUNT(*) FROM comprehensive_property_data WHERE extracted_fields_json IS NOT NULL")
with_data = cursor.fetchone()[0]
print(f"Records with extracted data: {with_data:,}")

# 3. Get a few sample records to understand structure
print("\n3. SAMPLE RECORDS WITH EXTRACTED DATA:")
print("-" * 40)

cursor.execute("""
    SELECT id, apn, pin, extracted_fields_json, field_count 
    FROM comprehensive_property_data 
    WHERE extracted_fields_json IS NOT NULL 
    AND field_count > 90
    LIMIT 3
""")

samples = cursor.fetchall()

for idx, (rec_id, apn, pin, json_str, field_count) in enumerate(samples, 1):
    print(f"\n{'='*60}")
    print(f"SAMPLE {idx}:")
    print(f"  ID: {rec_id}, APN: {apn}, PIN: {pin}")
    print(f"  Field Count: {field_count}")
    
    if json_str:
        try:
            data = json.loads(json_str)
            print(f"  Extracted Fields ({len(data)}):")
            
            # Show first 30 fields
            for i, (key, value) in enumerate(list(data.items())[:30]):
                if value and str(value).strip():
                    print(f"    {key}: {value}")
            
            if len(data) > 30:
                print(f"    ... and {len(data) - 30} more fields")
        except Exception as e:
            print(f"  Error parsing JSON: {e}")

# 4. Export sample to CSV
print("\n4. EXPORTING SAMPLE DATA:")
print("-" * 40)

# Get records with good data
query = """
    SELECT id, apn, pin, extracted_fields_json, field_count, data_quality
    FROM comprehensive_property_data
    WHERE extracted_fields_json IS NOT NULL
    AND field_count > 50
    LIMIT 1000
"""

df_records = []
cursor.execute(query)

for row in cursor.fetchall():
    rec_id, apn, pin, json_str, field_count, quality = row
    if json_str:
        try:
            data = json.loads(json_str)
            data['_id'] = rec_id
            data['_apn'] = apn
            data['_pin'] = pin
            data['_field_count'] = field_count
            data['_quality'] = quality
            df_records.append(data)
        except:
            pass

if df_records:
    df = pd.DataFrame(df_records)
    
    # Save to CSV
    csv_file = 'zimas_property_sample_1000.csv'
    df.to_csv(csv_file, index=False)
    print(f"Exported {len(df)} records to {csv_file}")
    print(f"Total columns: {len(df.columns)}")
    
    # Show column names
    print("\nColumn names in export:")
    cols = list(df.columns)
    # Group by category
    
    print("\nKey identification fields:")
    id_cols = [c for c in cols if any(x in c.lower() for x in ['_id', '_apn', '_pin', 'parcel', 'assessor'])]
    print(f"  {id_cols[:10]}")
    
    print("\nAddress fields:")
    addr_cols = [c for c in cols if any(x in c.lower() for x in ['address', 'street', 'site'])]
    print(f"  {addr_cols[:10]}")
    
    print("\nZoning fields:")
    zone_cols = [c for c in cols if any(x in c.lower() for x in ['zone', 'zoning', 'plan', 'overlay'])]
    print(f"  {zone_cols[:10]}")
    
    print("\nSize/Area fields:")
    size_cols = [c for c in cols if any(x in c.lower() for x in ['area', 'size', 'square', 'feet', 'lot'])]
    print(f"  {size_cols[:10]}")
    
    print("\nAdministrative fields:")
    admin_cols = [c for c in cols if any(x in c.lower() for x in ['council', 'district', 'community', 'neighborhood'])]
    print(f"  {admin_cols[:10]}")

print("\n" + "=" * 80)
print("QUICK ANALYSIS COMPLETE")
print("=" * 80)

conn.close()