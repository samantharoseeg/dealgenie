import sqlite3
import pandas as pd
import json
from collections import Counter

# Connect to the database
db_path = 'zimas_ajax_last_half.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("=" * 80)
print("ZIMAS DATABASE ANALYSIS")
print("=" * 80)

# 1. Get all table schemas
print("\n1. TABLE SCHEMAS:")
print("-" * 40)
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
for table in tables:
    table_name = table[0]
    print(f"\nTable: {table_name}")
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    print(f"Columns ({len(columns)}):")
    for col in columns[:10]:  # Show first 10 columns
        print(f"  - {col[1]} ({col[2]})")
    if len(columns) > 10:
        print(f"  ... and {len(columns) - 10} more columns")

# 2. Count total records and field distribution
print("\n" + "=" * 80)
print("2. RECORD COUNT AND FIELD DISTRIBUTION:")
print("-" * 40)

# Assuming main table is 'properties' or similar - let's check what tables exist
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
table_names = [t[0] for t in cursor.fetchall()]
print(f"Tables found: {table_names}")

# Use the first table found
if table_names:
    main_table = table_names[0]
    print(f"\nAnalyzing table: {main_table}")
    
    # Get total record count
    cursor.execute(f"SELECT COUNT(*) FROM {main_table}")
    total_records = cursor.fetchone()[0]
    print(f"Total records: {total_records:,}")
    
    # Get all columns
    cursor.execute(f"PRAGMA table_info({main_table})")
    all_columns = [col[1] for col in cursor.fetchall()]
    print(f"Total columns: {len(all_columns)}")
    
    # Analyze field completeness
    print("\nField completeness analysis (sampling first 1000 records):")
    cursor.execute(f"SELECT * FROM {main_table} LIMIT 1000")
    sample_data = cursor.fetchall()
    
    if sample_data:
        # Count non-null values per column
        field_completeness = {}
        for col_idx, col_name in enumerate(all_columns):
            non_null_count = sum(1 for row in sample_data if row[col_idx] is not None and str(row[col_idx]).strip())
            field_completeness[col_name] = (non_null_count / len(sample_data)) * 100
        
        # Show top 20 most complete fields
        sorted_fields = sorted(field_completeness.items(), key=lambda x: x[1], reverse=True)
        print("\nTop 20 most complete fields:")
        for field, pct in sorted_fields[:20]:
            print(f"  {field}: {pct:.1f}% complete")

# 3. Analyze top 5 most complete records
print("\n" + "=" * 80)
print("3. TOP 5 MOST COMPLETE RECORDS (90+ fields):")
print("-" * 40)

if table_names:
    # Count non-null fields per record
    cursor.execute(f"SELECT * FROM {main_table} LIMIT 5000")
    records = cursor.fetchall()
    
    record_completeness = []
    for idx, record in enumerate(records):
        non_null_count = sum(1 for val in record if val is not None and str(val).strip())
        record_completeness.append((idx, non_null_count, record))
    
    # Sort by completeness
    record_completeness.sort(key=lambda x: x[1], reverse=True)
    
    print(f"\nTop 5 most complete records:")
    for i, (idx, field_count, record) in enumerate(record_completeness[:5], 1):
        print(f"\n{i}. Record index {idx}: {field_count}/{len(all_columns)} fields populated")
        # Show some key fields if they exist
        key_fields = ['address', 'apn', 'pin', 'zoning', 'general_plan', 'lot_size']
        for field in key_fields:
            if field in all_columns:
                field_idx = all_columns.index(field)
                value = record[field_idx]
                if value:
                    print(f"   {field}: {value}")

# 4. Identify key fields
print("\n" + "=" * 80)
print("4. KEY FIELDS IDENTIFICATION:")
print("-" * 40)

if table_names and all_columns:
    key_fields_to_find = [
        'address', 'street_address', 'site_address',
        'apn', 'assessor_parcel_number', 'parcel',
        'pin', 'parcel_identification_number',
        'zoning', 'zone', 'zoning_code',
        'general_plan', 'general_plan_land_use', 'plan_area',
        'overlay', 'overlay_zones', 'specific_plan',
        'council_district', 'cd', 'council',
        'lot_size', 'lot_area', 'parcel_size', 'square_feet',
        'year_built', 'built_year', 'construction_year',
        'building_area', 'building_size', 'gross_area',
        'units', 'dwelling_units', 'residential_units',
        'stories', 'floors', 'building_height',
        'parking', 'parking_spaces',
        'use', 'land_use', 'property_use', 'use_code',
        'tract', 'block', 'lot',
        'legal_description', 'legal_desc',
        'owner', 'owner_name', 'property_owner'
    ]
    
    found_fields = {}
    for field in all_columns:
        field_lower = field.lower()
        for key in key_fields_to_find:
            if key in field_lower:
                if key not in found_fields:
                    found_fields[key] = []
                found_fields[key].append(field)
    
    print("\nFound key fields:")
    for category, fields in sorted(found_fields.items()):
        print(f"  {category}: {', '.join(fields)}")
    
    # Also list exact column names that might be relevant
    print("\n\nAll columns (grouped by potential relevance):")
    
    property_cols = [c for c in all_columns if any(x in c.lower() for x in ['address', 'street', 'site'])]
    if property_cols:
        print(f"\nAddress fields: {', '.join(property_cols)}")
    
    parcel_cols = [c for c in all_columns if any(x in c.lower() for x in ['apn', 'parcel', 'pin', 'assessor'])]
    if parcel_cols:
        print(f"\nParcel fields: {', '.join(parcel_cols)}")
    
    zoning_cols = [c for c in all_columns if any(x in c.lower() for x in ['zone', 'zoning', 'general_plan', 'overlay', 'specific_plan'])]
    if zoning_cols:
        print(f"\nZoning fields: {', '.join(zoning_cols)}")
    
    size_cols = [c for c in all_columns if any(x in c.lower() for x in ['size', 'area', 'square', 'feet', 'acres'])]
    if size_cols:
        print(f"\nSize fields: {', '.join(size_cols)}")

# 5. Show 3 sample records with full structure
print("\n" + "=" * 80)
print("5. SAMPLE RECORDS WITH FULL FIELD STRUCTURE:")
print("-" * 40)

if table_names:
    cursor.execute(f"SELECT * FROM {main_table} LIMIT 3")
    sample_records = cursor.fetchall()
    
    for i, record in enumerate(sample_records, 1):
        print(f"\n{'='*60}")
        print(f"SAMPLE RECORD {i}:")
        print(f"{'='*60}")
        
        # Create a dictionary of non-null fields
        record_dict = {}
        for col_idx, col_name in enumerate(all_columns):
            value = record[col_idx]
            if value is not None and str(value).strip():
                record_dict[col_name] = value
        
        # Group fields by category for better readability
        categories = {
            'IDENTIFIERS': ['pin', 'apn', 'tract', 'block', 'lot'],
            'ADDRESS': ['address', 'street', 'site'],
            'ZONING': ['zone', 'zoning', 'general_plan', 'overlay', 'specific_plan'],
            'SIZE': ['size', 'area', 'square', 'feet', 'acres'],
            'BUILDING': ['building', 'built', 'year', 'stories', 'height', 'units'],
            'ADMINISTRATIVE': ['council', 'district', 'cd', 'community', 'neighborhood']
        }
        
        for category, keywords in categories.items():
            category_fields = {}
            for field, value in record_dict.items():
                if any(kw in field.lower() for kw in keywords):
                    category_fields[field] = value
            
            if category_fields:
                print(f"\n{category}:")
                for field, value in category_fields.items():
                    print(f"  {field}: {value}")
        
        # Show any remaining fields
        shown_fields = set()
        for keywords in categories.values():
            for field in record_dict:
                if any(kw in field.lower() for kw in keywords):
                    shown_fields.add(field)
        
        other_fields = {k: v for k, v in record_dict.items() if k not in shown_fields}
        if other_fields:
            print(f"\nOTHER FIELDS:")
            for field, value in list(other_fields.items())[:20]:  # Limit to 20 fields
                print(f"  {field}: {value}")
            if len(other_fields) > 20:
                print(f"  ... and {len(other_fields) - 20} more fields")

# 6. Export 1000-record sample as CSV
print("\n" + "=" * 80)
print("6. EXPORTING 1000-RECORD SAMPLE TO CSV:")
print("-" * 40)

if table_names:
    # Read 1000 records into pandas DataFrame
    query = f"SELECT * FROM {main_table} LIMIT 1000"
    df = pd.read_sql_query(query, conn)
    
    # Save to CSV
    csv_filename = 'zimas_sample_1000_records.csv'
    df.to_csv(csv_filename, index=False)
    print(f"\nExported {len(df)} records to {csv_filename}")
    print(f"File contains {len(df.columns)} columns")
    
    # Show basic statistics
    print("\nBasic statistics of exported data:")
    print(f"  - Records: {len(df)}")
    print(f"  - Columns: {len(df.columns)}")
    print(f"  - Total cells: {len(df) * len(df.columns)}")
    non_null_cells = df.notna().sum().sum()
    print(f"  - Non-null cells: {non_null_cells} ({(non_null_cells / (len(df) * len(df.columns)) * 100):.1f}%)")
    
    # Show columns with highest completeness in sample
    completeness = (df.notna().sum() / len(df) * 100).sort_values(ascending=False)
    print("\nTop 10 most complete columns in sample:")
    for col, pct in completeness.head(10).items():
        print(f"  {col}: {pct:.1f}%")

print("\n" + "=" * 80)
print("ANALYSIS COMPLETE")
print("=" * 80)

# Close connection
conn.close()