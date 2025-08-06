import pandas as pd
import numpy as np

# Load the cleaned data
df = pd.read_csv('clean_zimas_ready_for_scoring.csv')

print("="*100)
print("CLEANED DATA VERIFICATION REPORT")
print("="*100)

print(f"\nDataset Shape: {df.shape[0]} records × {df.shape[1]} columns")

print("\n1. SAMPLE CLEANED RECORDS:")
print("-"*100)

# Show 3 complete records
sample_cols = [
    'assessor_parcel_id', 'site_address', 'base_zoning', 
    'lot_size_sqft', 'building_size_sqft', 'year_built',
    'total_assessed_value', 'overlay_count', 'toc_eligible'
]

print("\nSample Records with Key Fields:")
print(df[sample_cols].head(3).to_string())

print("\n2. DATA QUALITY METRICS:")
print("-"*100)

# Check numeric conversions
numeric_fields = ['lot_size_sqft', 'building_size_sqft', 'total_assessed_value', 'year_built']
for field in numeric_fields:
    if field in df.columns:
        non_null = df[field].notna().sum()
        mean_val = df[field].mean()
        median_val = df[field].median()
        print(f"\n{field}:")
        print(f"  Non-null: {non_null}/{len(df)} ({non_null/len(df)*100:.1f}%)")
        print(f"  Mean: {mean_val:,.0f}")
        print(f"  Median: {median_val:,.0f}")

print("\n3. BASE ZONING DISTRIBUTION:")
print("-"*100)
zoning_counts = df['base_zoning'].value_counts().head(10)
print(zoning_counts.to_string())

print("\n4. OVERLAY ZONES ANALYSIS:")
print("-"*100)
overlay_dist = df['overlay_count'].value_counts().sort_index()
print("Properties by overlay count:")
print(overlay_dist.to_string())

print("\n5. DEVELOPMENT INDICATORS:")
print("-"*100)
if 'toc_eligible' in df.columns:
    toc_count = df['toc_eligible'].sum()
    print(f"TOC Eligible Properties: {toc_count} ({toc_count/len(df)*100:.1f}%)")

high_transit = df['high_quality_transit'].value_counts()
print(f"\nHigh Quality Transit Access:")
for val, count in high_transit.items():
    print(f"  {val}: {count} ({count/len(df)*100:.1f}%)")

print("\n6. ADDRESS STANDARDIZATION EXAMPLES:")
print("-"*100)
print(df[['site_address']].dropna().head(5).to_string())

print("\n7. APN FORMAT VERIFICATION:")
print("-"*100)
# Check APN format
apn_sample = df['assessor_parcel_id'].dropna().head(10)
print("Sample standardized APNs:")
for apn in apn_sample:
    print(f"  {apn}")

# Check format consistency
if df['assessor_parcel_id'].notna().any():
    apn_lengths = df['assessor_parcel_id'].dropna().str.len().value_counts()
    print(f"\nAPN length distribution:")
    print(apn_lengths.head())

print("\n8. DATA COMPLETENESS SCORES:")
print("-"*100)
completeness_stats = df['data_completeness_score'].describe()
print(f"Mean completeness: {completeness_stats['mean']:.1f}%")
print(f"Median completeness: {completeness_stats['50%']:.1f}%")
print(f"Min completeness: {completeness_stats['min']:.1f}%")
print(f"Max completeness: {completeness_stats['max']:.1f}%")

# Show distribution
print("\nCompleteness distribution:")
bins = [0, 50, 60, 70, 80, 90, 100]
labels = ['<50%', '50-60%', '60-70%', '70-80%', '80-90%', '90-100%']
df['completeness_bin'] = pd.cut(df['data_completeness_score'], bins=bins, labels=labels, include_lowest=True)
print(df['completeness_bin'].value_counts().sort_index().to_string())

print("\n9. KEY FIELDS FOR DEVELOPMENT SCORING:")
print("-"*100)

scoring_fields = {
    'Zoning': ['base_zoning', 'general_plan_land_use'],
    'Size': ['lot_size_sqft', 'building_size_sqft'],
    'Value': ['total_assessed_value'],
    'Development Potential': ['overlay_count', 'toc_eligible', 'opportunity_zone'],
    'Location': ['council_district', 'community_plan_area']
}

for category, fields in scoring_fields.items():
    print(f"\n{category}:")
    for field in fields:
        if field in df.columns:
            completeness = df[field].notna().sum() / len(df) * 100
            unique_vals = df[field].nunique()
            print(f"  {field}: {completeness:.1f}% complete, {unique_vals} unique values")

print("\n" + "="*100)
print("✓ DATA VERIFICATION COMPLETE - Ready for DealGenie Scoring!")
print("="*100)