#!/usr/bin/env python3
"""
Extract sample APNs from LA County parcel data for DealGenie scoring tests.
This script provides various sampling methods to test different scenarios.
"""

import pandas as pd
import sys
import os
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

def extract_sample_apns(csv_path: str, sample_size: int = 100):
    """Extract sample APNs and generate test commands."""
    
    print(f"Loading parcel data from {csv_path}...")
    
    # Read CSV with limited rows for quick sampling
    df = pd.read_csv(csv_path, nrows=sample_size)
    
    print(f"\nLoaded {len(df)} parcels")
    print(f"Columns: {df.columns.tolist()}")
    
    # Get sample APNs
    sample_apns = df['apn'].tolist()
    
    print(f"\n{'='*60}")
    print("Sample APNs for testing (first 10):")
    print(f"{'='*60}")
    
    for i, apn in enumerate(sample_apns[:10], 1):
        print(f"{i}. APN: {apn}")
    
    print(f"\n{'='*60}")
    print("Test Commands for DealGenie Scoring:")
    print(f"{'='*60}")
    
    # Generate test commands for different templates
    templates = ['multifamily', 'residential', 'commercial', 'industrial', 'retail']
    
    for template in templates:
        print(f"\n# {template.upper()} Template Tests:")
        for apn in sample_apns[:3]:  # First 3 APNs per template
            print(f"python cli/dg_score.py score --template {template} --apn {apn}")
    
    print(f"\n{'='*60}")
    print("Batch Testing Command:")
    print(f"{'='*60}")
    print(f"python cli/batch_score.py --input sample_apns.txt --template multifamily")
    
    # Save APNs to file for batch processing
    with open('sample_apns.txt', 'w') as f:
        for apn in sample_apns:
            f.write(f"{apn}\n")
    
    print(f"\nSaved {len(sample_apns)} APNs to sample_apns.txt")
    
    # Show property distribution
    if 'zoning' in df.columns:
        print(f"\n{'='*60}")
        print("Zoning Distribution in Sample:")
        print(f"{'='*60}")
        print(df['zoning'].value_counts().head(10))
    
    if 'zip_code' in df.columns:
        print(f"\n{'='*60}")
        print("ZIP Code Distribution in Sample:")
        print(f"{'='*60}")
        print(df['zip_code'].value_counts().head(10))
    
    return sample_apns

def extract_diverse_sample(csv_path: str, total_size: int = 1000):
    """Extract a diverse sample across different property types and areas."""
    
    print(f"Loading full parcel data for diverse sampling...")
    
    # Read full dataset
    df = pd.read_csv(csv_path)
    
    print(f"Total parcels: {len(df):,}")
    
    # Sample strategy for diversity
    samples = []
    
    # 1. Random sample (40%)
    random_sample = df.sample(n=min(int(total_size * 0.4), len(df)), random_state=42)
    samples.append(random_sample)
    
    # 2. Sample by zoning if available (30%)
    if 'zoning' in df.columns:
        zoning_groups = df.groupby('zoning')
        n_per_zone = max(1, int(total_size * 0.3 / len(zoning_groups)))
        zoning_sample = zoning_groups.apply(lambda x: x.sample(min(n_per_zone, len(x)), random_state=42))
        samples.append(zoning_sample.reset_index(drop=True))
    
    # 3. Sample by ZIP code if available (30%)
    if 'zip_code' in df.columns:
        zip_groups = df.groupby('zip_code')
        n_per_zip = max(1, int(total_size * 0.3 / min(50, len(zip_groups))))
        top_zips = df['zip_code'].value_counts().head(50).index
        zip_sample = df[df['zip_code'].isin(top_zips)].groupby('zip_code').apply(
            lambda x: x.sample(min(n_per_zip, len(x)), random_state=42)
        )
        samples.append(zip_sample.reset_index(drop=True))
    
    # Combine and deduplicate
    diverse_df = pd.concat(samples, ignore_index=True).drop_duplicates(subset=['apn'])
    diverse_df = diverse_df.head(total_size)
    
    print(f"\nExtracted {len(diverse_df)} diverse parcels")
    
    # Save diverse sample
    diverse_df.to_csv('diverse_sample_parcels.csv', index=False)
    print(f"Saved diverse sample to diverse_sample_parcels.csv")
    
    # Save APNs
    with open('diverse_apns.txt', 'w') as f:
        for apn in diverse_df['apn'].tolist():
            f.write(f"{apn}\n")
    
    print(f"Saved APNs to diverse_apns.txt")
    
    return diverse_df

if __name__ == "__main__":
    csv_path = "scraper/la_parcels_complete_merged.csv"
    
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found at {csv_path}")
        sys.exit(1)
    
    print("="*60)
    print("LA County Parcel Sample Extraction")
    print("="*60)
    
    # Extract basic sample
    print("\n1. EXTRACTING BASIC SAMPLE (100 parcels)")
    print("-"*40)
    basic_apns = extract_sample_apns(csv_path, sample_size=100)
    
    # Extract diverse sample
    print("\n2. EXTRACTING DIVERSE SAMPLE (1000 parcels)")
    print("-"*40)
    diverse_df = extract_diverse_sample(csv_path, total_size=1000)
    
    print("\n" + "="*60)
    print("Extraction Complete!")
    print("="*60)
    print("\nNext steps:")
    print("1. Run individual APN tests with the commands above")
    print("2. Run batch tests using sample_apns.txt or diverse_apns.txt")
    print("3. Check performance metrics from the benchmark script")