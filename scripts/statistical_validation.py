#!/usr/bin/env python3
"""
Statistical validation script for diverse parcel sampling across LA County.
Ensures comprehensive coverage of different zoning types, neighborhoods, and property characteristics.
"""

import pandas as pd
import numpy as np
import json
import sys
import os
from pathlib import Path
from collections import defaultdict
import random

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

class LACountyStatisticalValidator:
    def __init__(self, csv_path: str = "scraper/la_parcels_complete_merged.csv"):
        self.csv_path = csv_path
        self.df = None
        self.validation_results = {}
        
    def load_data(self, sample_size: int = None):
        """Load LA County parcel data."""
        print(f"Loading LA County parcel data from {self.csv_path}...")
        
        if sample_size:
            print(f"Using sample of {sample_size:,} parcels for analysis")
            self.df = pd.read_csv(self.csv_path, nrows=sample_size)
        else:
            print("Loading full dataset...")
            self.df = pd.read_csv(self.csv_path)
        
        print(f"Loaded {len(self.df):,} parcels")
        print(f"Columns: {self.df.columns.tolist()}")
        
        return len(self.df)
    
    def analyze_data_quality(self):
        """Analyze data quality and completeness."""
        print("\n🔍 DATA QUALITY ANALYSIS")
        print("="*50)
        
        quality_analysis = {
            "total_records": len(self.df),
            "column_completeness": {},
            "data_types": {},
            "unique_values": {}
        }
        
        # Analyze each column
        for col in self.df.columns:
            non_null_count = self.df[col].notna().sum()
            completeness = non_null_count / len(self.df)
            
            quality_analysis["column_completeness"][col] = {
                "non_null_count": int(non_null_count),
                "completeness_percentage": round(completeness * 100, 2),
                "null_count": int(len(self.df) - non_null_count)
            }
            
            quality_analysis["data_types"][col] = str(self.df[col].dtype)
            
            if self.df[col].dtype == 'object':
                unique_count = self.df[col].nunique()
                quality_analysis["unique_values"][col] = unique_count
            
            # Print summary
            print(f"{col:20} | {completeness*100:5.1f}% complete | "
                  f"{non_null_count:7,} records")
        
        self.validation_results["data_quality"] = quality_analysis
        return quality_analysis
    
    def analyze_geographic_distribution(self):
        """Analyze geographic distribution of parcels."""
        print("\n🗺️  GEOGRAPHIC DISTRIBUTION ANALYSIS")
        print("="*50)
        
        geo_analysis = {}
        
        # ZIP Code distribution
        if 'zip_code' in self.df.columns:
            zip_dist = self.df['zip_code'].value_counts().head(20)
            geo_analysis["top_zip_codes"] = zip_dist.to_dict()
            
            print("Top ZIP Codes by parcel count:")
            for zip_code, count in zip_dist.items():
                print(f"  {zip_code}: {count:,} parcels")
        
        # City distribution (if available)
        if 'city' in self.df.columns:
            city_dist = self.df['city'].value_counts().head(15)
            geo_analysis["top_cities"] = city_dist.to_dict()
            
            print(f"\nTop Cities by parcel count:")
            for city, count in city_dist.items():
                print(f"  {city}: {count:,} parcels")
        
        # Address patterns
        if 'address' in self.df.columns:
            # Count addresses with unit numbers, APT, etc.
            multi_unit_patterns = self.df['address'].str.contains(
                r'(APT|UNIT|#|\bSTE\b|\bBLDG\b)', case=False, na=False
            ).sum()
            
            geo_analysis["multi_unit_addresses"] = {
                "count": int(multi_unit_patterns),
                "percentage": round(multi_unit_patterns / len(self.df) * 100, 2)
            }
            
            print(f"\nMulti-unit addresses: {multi_unit_patterns:,} "
                  f"({multi_unit_patterns/len(self.df)*100:.1f}%)")
        
        self.validation_results["geographic_distribution"] = geo_analysis
        return geo_analysis
    
    def analyze_zoning_distribution(self):
        """Analyze zoning type distribution."""
        print("\n🏗️  ZONING DISTRIBUTION ANALYSIS")
        print("="*50)
        
        if 'zoning' not in self.df.columns:
            print("⚠️  No zoning column found")
            return {}
        
        zoning_analysis = {}
        
        # Full zoning distribution
        zoning_dist = self.df['zoning'].value_counts()
        zoning_analysis["full_distribution"] = zoning_dist.to_dict()
        
        # Categorize zoning types
        zoning_categories = {
            'Residential': ['R1', 'R2', 'R3', 'R4', 'R5', 'RAS', 'RD', 'RS', 'RE'],
            'Commercial': ['C1', 'C2', 'C4', 'C5', 'CR', 'CM', 'CW'],
            'Industrial': ['M1', 'M2', 'M3', 'MR'],
            'Mixed Use': ['RAS4', 'C1.5', 'LAC'],
            'Special': ['OS', 'PF', 'A1', 'A2']
        }
        
        categorized = defaultdict(int)
        
        for zoning in self.df['zoning'].dropna():
            found_category = False
            for category, codes in zoning_categories.items():
                if any(code in str(zoning) for code in codes):
                    categorized[category] += 1
                    found_category = True
                    break
            if not found_category:
                categorized['Other'] += 1
        
        zoning_analysis["categorized"] = dict(categorized)
        
        # Print results
        print("Zoning Categories:")
        total_categorized = sum(categorized.values())
        for category, count in categorized.items():
            percentage = count / total_categorized * 100 if total_categorized > 0 else 0
            print(f"  {category:12} | {count:7,} parcels ({percentage:5.1f}%)")
        
        print(f"\nTop Individual Zoning Codes:")
        for zoning, count in zoning_dist.head(15).items():
            percentage = count / len(self.df) * 100
            print(f"  {zoning:8} | {count:7,} parcels ({percentage:5.1f}%)")
        
        self.validation_results["zoning_distribution"] = zoning_analysis
        return zoning_analysis
    
    def analyze_property_characteristics(self):
        """Analyze property size and building characteristics."""
        print("\n🏠 PROPERTY CHARACTERISTICS ANALYSIS")
        print("="*50)
        
        prop_analysis = {}
        
        # Lot area analysis
        if 'lot_area_sqft' in self.df.columns:
            lot_areas = self.df['lot_area_sqft'].dropna()
            
            prop_analysis["lot_area_stats"] = {
                "count": int(len(lot_areas)),
                "mean": float(lot_areas.mean()),
                "median": float(lot_areas.median()),
                "min": float(lot_areas.min()),
                "max": float(lot_areas.max()),
                "std": float(lot_areas.std())
            }
            
            # Lot size categories
            lot_categories = {
                "Small (< 5,000 sqft)": (lot_areas < 5000).sum(),
                "Medium (5K-10K sqft)": ((lot_areas >= 5000) & (lot_areas < 10000)).sum(),
                "Large (10K-20K sqft)": ((lot_areas >= 10000) & (lot_areas < 20000)).sum(),
                "Very Large (> 20K sqft)": (lot_areas >= 20000).sum()
            }
            
            prop_analysis["lot_size_categories"] = {k: int(v) for k, v in lot_categories.items()}
            
            print("Lot Area Statistics:")
            print(f"  Mean: {lot_areas.mean():,.0f} sqft")
            print(f"  Median: {lot_areas.median():,.0f} sqft")
            print(f"  Range: {lot_areas.min():,.0f} - {lot_areas.max():,.0f} sqft")
            
            print("\nLot Size Categories:")
            for category, count in lot_categories.items():
                percentage = count / len(lot_areas) * 100
                print(f"  {category:20} | {count:7,} ({percentage:5.1f}%)")
        
        # Building area analysis (if available)
        building_cols = [col for col in self.df.columns if 'building' in col.lower() or 'bldg' in col.lower()]
        if building_cols:
            print(f"\nBuilding-related columns found: {building_cols}")
            for col in building_cols[:3]:  # Analyze first 3 building columns
                if self.df[col].dtype in ['int64', 'float64']:
                    values = self.df[col].dropna()
                    if len(values) > 0:
                        print(f"  {col}: Mean={values.mean():.0f}, Median={values.median():.0f}")
        
        self.validation_results["property_characteristics"] = prop_analysis
        return prop_analysis
    
    def create_stratified_sample(self, total_size: int = 1000, by_zoning: bool = True, 
                               by_geography: bool = True) -> pd.DataFrame:
        """Create a stratified sample ensuring representation across key dimensions."""
        print(f"\n🎯 CREATING STRATIFIED SAMPLE ({total_size:,} parcels)")
        print("="*50)
        
        samples = []
        sample_info = {}
        
        if by_zoning and 'zoning' in self.df.columns:
            # Sample by zoning categories
            zoning_groups = self.df.groupby('zoning')
            zoning_samples = {}
            
            # Get proportional sample sizes
            for zoning, group in zoning_groups:
                if len(group) >= 5:  # Only include zones with at least 5 parcels
                    proportion = len(group) / len(self.df)
                    sample_size = max(1, int(total_size * 0.4 * proportion))  # 40% of total by zoning
                    sample_size = min(sample_size, len(group))  # Don't exceed group size
                    
                    zone_sample = group.sample(n=sample_size, random_state=42)
                    samples.append(zone_sample)
                    zoning_samples[zoning] = sample_size
            
            sample_info["zoning_samples"] = zoning_samples
            print(f"Zoning-based sampling: {len(zoning_samples)} zones, "
                  f"{sum(zoning_samples.values())} parcels")
        
        if by_geography and 'zip_code' in self.df.columns:
            # Sample by ZIP codes
            zip_groups = self.df.groupby('zip_code')
            zip_samples = {}
            
            # Focus on top ZIP codes
            top_zips = self.df['zip_code'].value_counts().head(30).index
            
            for zip_code in top_zips:
                group = zip_groups.get_group(zip_code)
                proportion = len(group) / len(self.df)
                sample_size = max(1, int(total_size * 0.3 * proportion))  # 30% of total by geography
                sample_size = min(sample_size, len(group))
                
                zip_sample = group.sample(n=sample_size, random_state=43)
                samples.append(zip_sample)
                zip_samples[zip_code] = sample_size
            
            sample_info["zip_samples"] = zip_samples
            print(f"Geographic sampling: {len(zip_samples)} ZIP codes, "
                  f"{sum(zip_samples.values())} parcels")
        
        # Random sample for remaining 30%
        remaining_size = max(0, int(total_size * 0.3))
        if remaining_size > 0:
            random_sample = self.df.sample(n=min(remaining_size, len(self.df)), random_state=44)
            samples.append(random_sample)
            sample_info["random_sample"] = remaining_size
            print(f"Random sampling: {remaining_size} parcels")
        
        # Combine and deduplicate
        if samples:
            combined_df = pd.concat(samples, ignore_index=True)
            stratified_sample = combined_df.drop_duplicates(subset=['apn']).head(total_size)
        else:
            # Fallback to random sample
            stratified_sample = self.df.sample(n=min(total_size, len(self.df)), random_state=45)
        
        print(f"\nFinal stratified sample: {len(stratified_sample):,} unique parcels")
        
        # Analyze sample composition
        self.analyze_sample_composition(stratified_sample)
        
        # Save sample
        stratified_sample.to_csv('stratified_sample_parcels.csv', index=False)
        print(f"💾 Saved to: stratified_sample_parcels.csv")
        
        # Save APNs
        apns = stratified_sample['apn'].tolist()
        with open('stratified_sample_apns.txt', 'w') as f:
            for apn in apns:
                f.write(f"{apn}\n")
        print(f"💾 Saved APNs to: stratified_sample_apns.txt")
        
        self.validation_results["stratified_sample"] = {
            "sample_size": len(stratified_sample),
            "sample_info": sample_info,
            "composition": self.get_sample_composition(stratified_sample)
        }
        
        return stratified_sample
    
    def analyze_sample_composition(self, sample_df: pd.DataFrame):
        """Analyze the composition of the sample."""
        print(f"\n📊 SAMPLE COMPOSITION ANALYSIS")
        print("-"*30)
        
        # Zoning composition
        if 'zoning' in sample_df.columns:
            zoning_dist = sample_df['zoning'].value_counts().head(10)
            print("Top zoning types in sample:")
            for zoning, count in zoning_dist.items():
                percentage = count / len(sample_df) * 100
                print(f"  {zoning:8} | {count:4} parcels ({percentage:4.1f}%)")
        
        # Geographic composition
        if 'zip_code' in sample_df.columns:
            zip_dist = sample_df['zip_code'].value_counts().head(10)
            print(f"\nTop ZIP codes in sample:")
            for zip_code, count in zip_dist.items():
                percentage = count / len(sample_df) * 100
                print(f"  {zip_code:8} | {count:4} parcels ({percentage:4.1f}%)")
    
    def get_sample_composition(self, sample_df: pd.DataFrame) -> dict:
        """Get detailed composition of the sample."""
        composition = {}
        
        if 'zoning' in sample_df.columns:
            composition['zoning'] = sample_df['zoning'].value_counts().to_dict()
        
        if 'zip_code' in sample_df.columns:
            composition['zip_code'] = sample_df['zip_code'].value_counts().to_dict()
        
        if 'city' in sample_df.columns:
            composition['city'] = sample_df['city'].value_counts().to_dict()
        
        return composition
    
    def generate_test_scenarios(self, sample_df: pd.DataFrame) -> dict:
        """Generate specific test scenarios for DealGenie validation."""
        print(f"\n🧪 GENERATING TEST SCENARIOS")
        print("="*40)
        
        scenarios = {}
        
        # Scenario 1: Multi-family focused
        if 'zoning' in sample_df.columns:
            multifamily_zones = ['R3', 'R4', 'R5', 'RAS4', 'LAR3', 'LAR4']
            mf_parcels = sample_df[sample_df['zoning'].str.contains('|'.join(multifamily_zones), na=False)]
            scenarios['multifamily_focused'] = {
                "description": "High-density residential parcels ideal for multifamily development",
                "apn_count": len(mf_parcels),
                "apns": mf_parcels['apn'].head(50).tolist(),
                "template": "multifamily"
            }
            print(f"Multifamily scenario: {len(mf_parcels)} parcels")
        
        # Scenario 2: Commercial opportunities
        if 'zoning' in sample_df.columns:
            commercial_zones = ['C1', 'C2', 'C4', 'CR', 'CM']
            commercial_parcels = sample_df[sample_df['zoning'].str.contains('|'.join(commercial_zones), na=False)]
            scenarios['commercial_opportunities'] = {
                "description": "Commercial zoned parcels for retail/office development",
                "apn_count": len(commercial_parcels),
                "apns": commercial_parcels['apn'].head(30).tolist(),
                "template": "commercial"
            }
            print(f"Commercial scenario: {len(commercial_parcels)} parcels")
        
        # Scenario 3: Large lot development
        if 'lot_area_sqft' in sample_df.columns:
            large_lots = sample_df[sample_df['lot_area_sqft'] > 10000]
            scenarios['large_lot_development'] = {
                "description": "Large parcels (>10K sqft) suitable for major development",
                "apn_count": len(large_lots),
                "apns": large_lots['apn'].head(40).tolist(),
                "template": "residential"
            }
            print(f"Large lot scenario: {len(large_lots)} parcels")
        
        # Scenario 4: Geographic diversity
        if 'zip_code' in sample_df.columns:
            diverse_zips = sample_df['zip_code'].value_counts().head(20).index
            diverse_sample = []
            for zip_code in diverse_zips:
                zip_parcels = sample_df[sample_df['zip_code'] == zip_code]
                diverse_sample.extend(zip_parcels['apn'].head(2).tolist())  # 2 per ZIP
            
            scenarios['geographic_diversity'] = {
                "description": "Geographically diverse parcels across multiple ZIP codes",
                "apn_count": len(diverse_sample),
                "apns": diverse_sample,
                "template": "multifamily"
            }
            print(f"Geographic diversity scenario: {len(diverse_sample)} parcels")
        
        # Save test scenarios
        with open('test_scenarios.json', 'w') as f:
            json.dump(scenarios, f, indent=2)
        
        print(f"💾 Test scenarios saved to: test_scenarios.json")
        
        self.validation_results["test_scenarios"] = scenarios
        return scenarios
    
    def run_complete_validation(self, sample_size: int = 50000):
        """Run complete statistical validation analysis."""
        print("="*80)
        print("🏠 LA COUNTY PARCEL STATISTICAL VALIDATION")
        print("="*80)
        
        # Load data
        self.load_data(sample_size)
        
        # Run all analyses
        self.analyze_data_quality()
        self.analyze_geographic_distribution()
        self.analyze_zoning_distribution()
        self.analyze_property_characteristics()
        
        # Create stratified sample
        stratified_sample = self.create_stratified_sample(total_size=1000)
        
        # Generate test scenarios
        self.generate_test_scenarios(stratified_sample)
        
        # Save complete results
        timestamp = int(time.time())
        results_file = f"statistical_validation_results_{timestamp}.json"
        
        with open(results_file, 'w') as f:
            json.dump(self.validation_results, f, indent=2, default=str)
        
        print(f"\n💾 Complete results saved to: {results_file}")
        
        # Final summary
        print(f"\n{'='*80}")
        print("✅ STATISTICAL VALIDATION COMPLETE")
        print("="*80)
        print(f"📊 Analyzed {len(self.df):,} parcels")
        print(f"🎯 Created stratified sample of {len(stratified_sample):,} parcels")
        print(f"🧪 Generated {len(self.validation_results.get('test_scenarios', {})):,} test scenarios")
        print(f"\nFiles created:")
        print(f"  • stratified_sample_parcels.csv")
        print(f"  • stratified_sample_apns.txt") 
        print(f"  • test_scenarios.json")
        print(f"  • {results_file}")
        
        return self.validation_results

if __name__ == "__main__":
    # Check if CSV file exists
    csv_path = "scraper/la_parcels_complete_merged.csv"
    if not os.path.exists(csv_path):
        print(f"❌ Error: CSV file not found at {csv_path}")
        print("Please ensure the LA County parcel data is available")
        sys.exit(1)
    
    # Initialize validator
    validator = LACountyStatisticalValidator(csv_path)
    
    # Run complete validation
    results = validator.run_complete_validation(sample_size=50000)  # Use 50K sample for speed
    
    print(f"\n🎉 Statistical validation completed!")
    print(f"📈 Ready for DealGenie performance testing with validated samples")