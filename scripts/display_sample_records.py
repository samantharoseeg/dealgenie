import pandas as pd

# Load the cleaned data
df = pd.read_csv('clean_zimas_ready_for_scoring.csv')

print("=" * 100)
print("CLEAN ZIMAS DATA - SAMPLE RECORDS")
print("=" * 100)

# Select a few diverse records to show
sample_indices = [0, 100, 500, 670, 900]  # Mix of different property types
samples = df.iloc[sample_indices]

for idx, row in samples.iterrows():
    print(f"\n{'='*80}")
    print(f"RECORD #{row['property_id']}")
    print(f"{'='*80}")
    
    # Core Identifiers
    print("\n📍 PROPERTY IDENTIFIERS:")
    print(f"  APN: {row['assessor_parcel_id']}")
    print(f"  PIN: {row['pin']}")
    print(f"  Address: {row['site_address']}")
    print(f"  Zip: {row['zip_code']}")
    
    # Zoning & Planning
    print("\n🏗️ ZONING & PLANNING:")
    print(f"  Base Zoning: {row['base_zoning']} ({row['base_zoning_description']})")
    print(f"  Full Zoning Code: {row['full_zoning_code']}")
    print(f"  General Plan: {row['general_plan_land_use']}")
    print(f"  Overlay Count: {row['overlay_count']}")
    if pd.notna(row['specific_plan_area']):
        print(f"  Specific Plan: {row['specific_plan_area']}")
    
    # Property Metrics
    print("\n📏 PROPERTY METRICS:")
    print(f"  Lot Size: {row['lot_size_sqft']:,.0f} sq ft ({row['lot_size_sqft']/43560:.2f} acres)")
    if pd.notna(row['building_size_sqft']):
        print(f"  Building Size: {row['building_size_sqft']:,.0f} sq ft")
        print(f"  FAR: {row['building_size_sqft']/row['lot_size_sqft']:.2f}")
    if pd.notna(row['year_built']):
        print(f"  Year Built: {int(row['year_built'])}")
    if pd.notna(row['number_of_units']):
        print(f"  Units: {int(row['number_of_units'])}")
    
    # Valuation
    print("\n💰 VALUATION:")
    print(f"  Land Value: ${row['assessed_land_value']:,.0f}")
    print(f"  Improvement Value: ${row['assessed_improvement_value']:,.0f}")
    print(f"  Total Assessed: ${row['total_assessed_value']:,.0f}")
    if row['assessed_improvement_value'] > 0:
        land_ratio = row['assessed_land_value'] / row['total_assessed_value']
        print(f"  Land/Total Ratio: {land_ratio:.1%}")
    
    # Development Indicators
    print("\n🚀 DEVELOPMENT POTENTIAL:")
    print(f"  TOC Eligible: {'✅ Yes' if row['toc_eligible'] else '❌ No'}")
    print(f"  Opportunity Zone: {row['opportunity_zone']}")
    if pd.notna(row['high_quality_transit']):
        print(f"  High Quality Transit: {row['high_quality_transit']}")
    print(f"  Residential Market Area: {row['residential_market_area']}")
    print(f"  Commercial Market Area: {row['commercial_market_area']}")
    
    # Administrative
    print("\n🏛️ ADMINISTRATIVE:")
    print(f"  Council District: {row['council_district']}")
    print(f"  Community Plan: {row['community_plan_area']}")
    print(f"  Neighborhood Council: {row['neighborhood_council']}")
    
    # Environmental & Constraints
    print("\n⚠️ ENVIRONMENTAL FACTORS:")
    if pd.notna(row['methane_zone']):
        print(f"  Methane Zone: {row['methane_zone']}")
    if pd.notna(row['flood_zone']):
        print(f"  Flood Zone: {row['flood_zone']}")
    if pd.notna(row['fault_zone']):
        print(f"  Fault Zone: {row['fault_zone']}")
    
    # Data Quality
    print(f"\n📊 Data Completeness: {row['data_completeness_score']:.1f}%")

print("\n" + "=" * 100)
print("STATISTICAL SUMMARY")
print("=" * 100)

# Show some aggregate stats
print(f"\nDataset Overview:")
print(f"  Total Properties: {len(df):,}")
print(f"  Unique Council Districts: {df['council_district'].nunique()}")
print(f"  Unique Community Plans: {df['community_plan_area'].nunique()}")
print(f"  Average Lot Size: {df['lot_size_sqft'].mean():,.0f} sq ft")
print(f"  Average Building Size: {df['building_size_sqft'].mean():,.0f} sq ft")
print(f"  Average Assessed Value: ${df['total_assessed_value'].mean():,.0f}")
print(f"  TOC Eligible: {df['toc_eligible'].sum():,} ({df['toc_eligible'].mean()*100:.1f}%)")

print("\nZoning Distribution (Top 5):")
zoning_counts = df['base_zoning'].value_counts().head()
for zone, count in zoning_counts.items():
    print(f"  {zone}: {count} properties ({count/len(df)*100:.1f}%)")

print("\n✅ Sample display complete!")