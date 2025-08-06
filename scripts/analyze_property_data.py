import sqlite3
import pandas as pd
import json
from collections import Counter

# Connect to the database
db_path = 'zimas_ajax_last_half.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("=" * 80)
print("COMPREHENSIVE PROPERTY DATA ANALYSIS")
print("=" * 80)

# Focus on the comprehensive_property_data table
table_name = 'comprehensive_property_data'

# 1. Get table schema
print("\n1. TABLE SCHEMA:")
print("-" * 40)
cursor.execute(f"PRAGMA table_info({table_name})")
columns = cursor.fetchall()
print(f"\nTable: {table_name}")
print(f"Columns ({len(columns)}):")
for col in columns:
    print(f"  - {col[1]} ({col[2]})")

# 2. Count total records
print("\n2. RECORD COUNT:")
print("-" * 40)
cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
total_records = cursor.fetchone()[0]
print(f"Total records: {total_records:,}")

# 3. Analyze extracted fields from JSON
print("\n3. ANALYZING EXTRACTED FIELDS:")
print("-" * 40)

# Get sample of records with extracted_fields_json
cursor.execute(f"""
    SELECT id, apn, pin, extracted_fields_json, field_count 
    FROM {table_name} 
    WHERE extracted_fields_json IS NOT NULL 
    LIMIT 100
""")
sample_records = cursor.fetchall()

print(f"Found {len(sample_records)} records with extracted fields")

# Analyze field structure
all_field_names = set()
field_frequency = Counter()
sample_data_for_export = []

for record in sample_records:
    record_id, apn, pin, json_str, field_count = record
    if json_str:
        try:
            data = json.loads(json_str)
            all_field_names.update(data.keys())
            field_frequency.update(data.keys())
            
            # Add to sample data
            data['record_id'] = record_id
            data['apn'] = apn
            data['pin'] = pin
            sample_data_for_export.append(data)
        except json.JSONDecodeError:
            print(f"Error parsing JSON for record {record_id}")

print(f"\nTotal unique fields found: {len(all_field_names)}")

# Group fields by category
print("\n4. FIELD CATEGORIES:")
print("-" * 40)

categories = {
    'ADDRESS': [],
    'PARCEL': [],
    'ZONING': [],
    'SIZE/AREA': [],
    'BUILDING': [],
    'ADMINISTRATIVE': [],
    'LEGAL': [],
    'PLANNING': [],
    'OTHER': []
}

for field in sorted(all_field_names):
    field_lower = field.lower()
    categorized = False
    
    if any(x in field_lower for x in ['address', 'street', 'site']):
        categories['ADDRESS'].append(field)
        categorized = True
    elif any(x in field_lower for x in ['apn', 'parcel', 'pin', 'assessor']):
        categories['PARCEL'].append(field)
        categorized = True
    elif any(x in field_lower for x in ['zone', 'zoning']):
        categories['ZONING'].append(field)
        categorized = True
    elif any(x in field_lower for x in ['size', 'area', 'square', 'feet', 'acres', 'lot']):
        categories['SIZE/AREA'].append(field)
        categorized = True
    elif any(x in field_lower for x in ['building', 'built', 'year', 'stories', 'height', 'units']):
        categories['BUILDING'].append(field)
        categorized = True
    elif any(x in field_lower for x in ['council', 'district', 'cd', 'community', 'neighborhood']):
        categories['ADMINISTRATIVE'].append(field)
        categorized = True
    elif any(x in field_lower for x in ['legal', 'tract', 'block', 'lot', 'map']):
        categories['LEGAL'].append(field)
        categorized = True
    elif any(x in field_lower for x in ['plan', 'general', 'specific', 'overlay']):
        categories['PLANNING'].append(field)
        categorized = True
    
    if not categorized:
        categories['OTHER'].append(field)

for category, fields in categories.items():
    if fields:
        print(f"\n{category}: ({len(fields)} fields)")
        for field in fields[:10]:  # Show first 10
            count = field_frequency[field]
            pct = (count / len(sample_records)) * 100
            print(f"  - {field} ({pct:.1f}% of records)")
        if len(fields) > 10:
            print(f"  ... and {len(fields) - 10} more")

# 5. Show most complete records
print("\n5. MOST COMPLETE RECORDS:")
print("-" * 40)

cursor.execute(f"""
    SELECT id, apn, pin, field_count, extracted_fields_json
    FROM {table_name}
    WHERE field_count IS NOT NULL
    ORDER BY field_count DESC
    LIMIT 5
""")

top_records = cursor.fetchall()

for idx, (record_id, apn, pin, field_count, json_str) in enumerate(top_records, 1):
    print(f"\n{idx}. Record ID: {record_id}")
    print(f"   APN: {apn}")
    print(f"   PIN: {pin}")
    print(f"   Field Count: {field_count}")
    
    if json_str:
        try:
            data = json.loads(json_str)
            # Show key fields
            key_fields = [
                'Site Address', 'Assessor Parcel No. (APN)', 'PIN Number',
                'Zoning', 'General Plan Land Use', 'Specific Plan Area',
                'Council District', 'Lot Area', 'Jurisdiction'
            ]
            print("   Key Fields:")
            for field in key_fields:
                if field in data:
                    print(f"     {field}: {data[field]}")
        except:
            pass

# 6. Export comprehensive sample
print("\n6. EXPORTING COMPREHENSIVE SAMPLE:")
print("-" * 40)

# Get 1000 records with extracted data
cursor.execute(f"""
    SELECT id, apn, pin, extracted_fields_json, field_count, data_quality
    FROM {table_name}
    WHERE extracted_fields_json IS NOT NULL
    LIMIT 1000
""")

export_records = cursor.fetchall()
export_data = []

for record in export_records:
    record_id, apn, pin, json_str, field_count, data_quality = record
    if json_str:
        try:
            data = json.loads(json_str)
            data['_record_id'] = record_id
            data['_apn'] = apn
            data['_pin'] = pin
            data['_field_count'] = field_count
            data['_data_quality'] = data_quality
            export_data.append(data)
        except:
            pass

if export_data:
    df = pd.DataFrame(export_data)
    
    # Reorder columns to put metadata first
    meta_cols = ['_record_id', '_apn', '_pin', '_field_count', '_data_quality']
    other_cols = [col for col in df.columns if col not in meta_cols]
    df = df[meta_cols + other_cols]
    
    # Save to CSV
    csv_filename = 'zimas_comprehensive_property_data_1000.csv'
    df.to_csv(csv_filename, index=False)
    
    print(f"\nExported {len(df)} records to {csv_filename}")
    print(f"Columns: {len(df.columns)}")
    print(f"Average fields per record: {df['_field_count'].mean():.1f}")
    print(f"Max fields in a record: {df['_field_count'].max()}")
    print(f"Min fields in a record: {df['_field_count'].min()}")

# 7. Show sample records with full detail
print("\n7. DETAILED SAMPLE RECORDS:")
print("-" * 40)

cursor.execute(f"""
    SELECT id, apn, pin, extracted_fields_json, field_count
    FROM {table_name}
    WHERE extracted_fields_json IS NOT NULL
    AND field_count > 90
    LIMIT 3
""")

detailed_samples = cursor.fetchall()

for idx, (record_id, apn, pin, json_str, field_count) in enumerate(detailed_samples, 1):
    print(f"\n{'='*60}")
    print(f"DETAILED RECORD {idx}:")
    print(f"{'='*60}")
    print(f"Record ID: {record_id}")
    print(f"APN: {apn}")
    print(f"PIN: {pin}")
    print(f"Total Fields: {field_count}")
    
    if json_str:
        try:
            data = json.loads(json_str)
            
            # Group and display fields
            for category, field_list in categories.items():
                category_data = {k: v for k, v in data.items() if k in field_list}
                if category_data:
                    print(f"\n{category}:")
                    for field, value in category_data.items():
                        if value and str(value).strip():
                            print(f"  {field}: {value}")
        except:
            print("Error parsing JSON data")

print("\n" + "=" * 80)
print("ANALYSIS COMPLETE")
print("=" * 80)

conn.close()