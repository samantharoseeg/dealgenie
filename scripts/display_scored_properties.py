import pandas as pd
import json

# Load the scored data
df = pd.read_csv('scored_zimas_properties.csv')

print("="*100)
print("DEALGENIE SCORED PROPERTIES - ANALYSIS")
print("="*100)

# Show top 10 properties with detailed breakdown
print("\nTOP 10 INVESTMENT OPPORTUNITIES WITH SCORE BREAKDOWN:")
print("-"*100)

top_10 = df.nlargest(10, 'development_score')

for idx, row in top_10.iterrows():
    print(f"\n{'='*80}")
    print(f"RANK #{list(top_10.index).index(idx) + 1} - SCORE: {row['development_score']:.1f} - TIER: {row['investment_tier']}")
    print(f"{'='*80}")
    
    # Basic info
    print(f"📍 PROPERTY:")
    print(f"   APN: {row['assessor_parcel_id']}")
    address = row['site_address'] if pd.notna(row['site_address']) else 'No Address Listed'
    print(f"   Address: {address}")
    print(f"   Council District: {row['council_district'] if pd.notna(row['council_district']) else 'N/A'}")
    
    # Zoning and size
    print(f"\n🏗️ ZONING & SIZE:")
    print(f"   Base Zoning: {row['base_zoning']}")
    print(f"   General Plan: {row['general_plan_land_use'] if pd.notna(row['general_plan_land_use']) else 'N/A'}")
    print(f"   Lot Size: {row['lot_size_sqft']:,.0f} sqft ({row['lot_size_sqft']/43560:.2f} acres)")
    if pd.notna(row['building_size_sqft']):
        print(f"   Building Size: {row['building_size_sqft']:,.0f} sqft")
        if row['lot_size_sqft'] > 0:
            far = row['building_size_sqft'] / row['lot_size_sqft']
            print(f"   Current FAR: {far:.2f}")
    
    # Development indicators
    print(f"\n🚀 DEVELOPMENT POTENTIAL:")
    print(f"   Suggested Use: {row['suggested_use']}")
    print(f"   TOC Eligible: {'✅ Yes' if row['toc_eligible'] else '❌ No'}")
    print(f"   High Quality Transit: {row['high_quality_transit'] if pd.notna(row['high_quality_transit']) else 'N/A'}")
    print(f"   Overlay Count: {row['overlay_count']}")
    
    # Financial
    if pd.notna(row['total_assessed_value']) and row['total_assessed_value'] > 0:
        land_ratio = row['assessed_land_value'] / row['total_assessed_value']
        print(f"\n💰 FINANCIAL:")
        print(f"   Total Value: ${row['total_assessed_value']:,.0f}")
        print(f"   Land/Total Ratio: {land_ratio:.1%}")
        print(f"   $/sqft Land: ${row['assessed_land_value']/row['lot_size_sqft']:.0f}")
    
    # Score breakdown
    if pd.notna(row['score_breakdown']):
        breakdown = json.loads(row['score_breakdown'])
        print(f"\n📊 SCORE BREAKDOWN:")
        print(f"   Zoning Score: {breakdown.get('zoning_score', 0):.1f} (weight: 35%)")
        print(f"   Lot Size Score: {breakdown.get('lot_size_score', 0):.1f} (weight: 25%)")
        print(f"   Transit Bonus: {breakdown.get('transit_bonus', 0):.1f} (weight: 20%)")
        print(f"   Financial Score: {breakdown.get('financial_score', 0):.1f} (weight: 10%)")
        print(f"   Risk Penalty: -{breakdown.get('risk_penalty', 0):.1f} (weight: 10%)")

# Show score distribution
print("\n" + "="*100)
print("SCORE DISTRIBUTION ANALYSIS:")
print("-"*50)

score_ranges = [
    (0, 20, "Very Low"),
    (20, 30, "Low"),
    (30, 40, "Moderate"),
    (40, 50, "Good"),
    (50, 60, "Very Good"),
    (60, 70, "Excellent"),
    (70, 100, "Outstanding")
]

print("\nProperties by Score Range:")
for min_score, max_score, label in score_ranges:
    count = len(df[(df['development_score'] >= min_score) & (df['development_score'] < max_score)])
    pct = count / len(df) * 100
    print(f"   {min_score:3d}-{max_score:3d} ({label:12s}): {count:4d} properties ({pct:5.1f}%)")

# Show use case distribution by score
print("\n" + "="*100)
print("TOP USE CASES BY AVERAGE SCORE:")
print("-"*50)

use_scores = df.groupby('suggested_use')['development_score'].agg(['mean', 'count'])
use_scores = use_scores[use_scores['count'] >= 3].sort_values('mean', ascending=False).head(10)

for use, row in use_scores.iterrows():
    print(f"   {use:40s}: {row['mean']:.1f} avg (n={int(row['count'])})")

# TOC analysis
print("\n" + "="*100)
print("TOC ELIGIBLE PROPERTIES ANALYSIS:")
print("-"*50)

toc_props = df[df['toc_eligible'] == True].sort_values('development_score', ascending=False).head(10)
print(f"\nTop 10 TOC-Eligible Properties:")
for idx, row in toc_props.iterrows():
    address = row['site_address'] if pd.notna(row['site_address']) else 'No Address'
    print(f"   {row['development_score']:.1f} | {row['assessor_parcel_id']} | {address[:40]} | {row['suggested_use']}")

# High land value ratio properties
print("\n" + "="*100)
print("HIGH REDEVELOPMENT POTENTIAL (Land Value >70%):")
print("-"*50)

df['land_value_ratio'] = df['assessed_land_value'] / df['total_assessed_value']
high_land = df[df['land_value_ratio'] > 0.70].sort_values('development_score', ascending=False).head(10)

print(f"\nTop Properties with High Land Value Ratio:")
for idx, row in high_land.iterrows():
    address = row['site_address'] if pd.notna(row['site_address']) else 'No Address'
    print(f"   Score: {row['development_score']:.1f} | Land Ratio: {row['land_value_ratio']:.1%} | "
          f"{row['base_zoning']} | {address[:30]} | {row['suggested_use']}")

# Council district opportunities
print("\n" + "="*100)
print("BEST OPPORTUNITIES BY COUNCIL DISTRICT:")
print("-"*50)

for district in df['council_district'].dropna().unique()[:5]:  # Top 5 districts
    district_props = df[df['council_district'] == district].nlargest(1, 'development_score')
    if not district_props.empty:
        row = district_props.iloc[0]
        print(f"\n{district}:")
        print(f"   Best Score: {row['development_score']:.1f}")
        print(f"   APN: {row['assessor_parcel_id']}")
        print(f"   Suggested Use: {row['suggested_use']}")

print("\n" + "="*100)
print("✓ ANALYSIS COMPLETE - Properties scored and ready for investment decisions!")
print("="*100)