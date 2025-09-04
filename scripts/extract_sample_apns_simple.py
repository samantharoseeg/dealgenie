#!/usr/bin/env python3
"""
Extract sample APNs from LA County parcel data for DealGenie scoring tests.
Simple version using only built-in Python libraries (no pandas required).
"""

import csv
import sys
import os
import random
from pathlib import Path
from collections import defaultdict, Counter

def extract_sample_apns(csv_path: str, sample_size: int = 100):
    """Extract sample APNs and generate test commands."""
    
    print(f"Loading parcel data from {csv_path}...")
    
    apns = []
    zoning_data = []
    zip_data = []
    headers = []
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)  # Get headers
            
            print(f"CSV Headers: {headers}")
            
            # Find APN column index
            apn_col = None
            zoning_col = None
            zip_col = None
            
            for i, header in enumerate(headers):
                if header.lower() == 'apn':  # Exact match for main APN column
                    apn_col = i
                elif 'zoning' in header.lower() and 'code' in header.lower():
                    zoning_col = i
                elif header.lower() == 'zip_code':  # Exact match for ZIP code
                    zip_col = i
            
            if apn_col is None:
                print("Error: Could not find APN column")
                return []
            
            print(f"Found APN column at index {apn_col}")
            if zoning_col is not None:
                print(f"Found zoning column at index {zoning_col}")
            if zip_col is not None:
                print(f"Found ZIP column at index {zip_col}")
            
            # Read sample rows
            row_count = 0
            for row in reader:
                if len(row) > apn_col and row[apn_col]:
                    apns.append(row[apn_col])
                    
                    if zoning_col is not None and len(row) > zoning_col:
                        zoning_data.append(row[zoning_col])
                    else:
                        zoning_data.append('Unknown')
                    
                    if zip_col is not None and len(row) > zip_col:
                        zip_data.append(row[zip_col])
                    else:
                        zip_data.append('Unknown')
                
                row_count += 1
                if row_count >= sample_size:
                    break
    
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return []
    
    print(f"\nLoaded {len(apns)} parcels from CSV")
    
    # Get sample APNs
    sample_apns = apns[:min(sample_size, len(apns))]
    
    print(f"\n{'='*60}")
    print("Sample APNs for testing (first 10):")
    print(f"{'='*60}")
    
    for i, apn in enumerate(sample_apns[:10], 1):
        zoning = zoning_data[i-1] if i-1 < len(zoning_data) else 'Unknown'
        zip_code = zip_data[i-1] if i-1 < len(zip_data) else 'Unknown'
        print(f"{i}. APN: {apn} | Zoning: {zoning} | ZIP: {zip_code}")
    
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
    try:
        with open('sample_apns.txt', 'w') as f:
            for apn in sample_apns:
                f.write(f"{apn}\n")
        print(f"\nSaved {len(sample_apns)} APNs to sample_apns.txt")
    except Exception as e:
        print(f"Error saving APNs: {e}")
    
    # Show distributions if available
    if zoning_data and zoning_data[0] != 'Unknown':
        print(f"\n{'='*60}")
        print("Zoning Distribution in Sample:")
        print(f"{'='*60}")
        zoning_counts = Counter([z for z in zoning_data[:len(sample_apns)] if z != 'Unknown'])
        for zoning, count in zoning_counts.most_common(10):
            percentage = count / len(sample_apns) * 100
            print(f"  {zoning:8} | {count:4} parcels ({percentage:4.1f}%)")
    
    if zip_data and zip_data[0] != 'Unknown':
        print(f"\n{'='*60}")
        print("ZIP Code Distribution in Sample:")
        print(f"{'='*60}")
        zip_counts = Counter([z for z in zip_data[:len(sample_apns)] if z != 'Unknown'])
        for zip_code, count in zip_counts.most_common(10):
            percentage = count / len(sample_apns) * 100
            print(f"  {zip_code:8} | {count:4} parcels ({percentage:4.1f}%)")
    
    return sample_apns

def extract_diverse_sample(csv_path: str, total_size: int = 1000):
    """Extract a diverse sample across different property types and areas."""
    
    print(f"Loading parcel data for diverse sampling...")
    
    all_data = []
    headers = []
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
            
            # Find column indices
            apn_col = None
            zoning_col = None
            zip_col = None
            
            for i, header in enumerate(headers):
                if header.lower() == 'apn':  # Exact match for main APN column
                    apn_col = i
                elif 'zoning' in header.lower() and 'code' in header.lower():
                    zoning_col = i
                elif header.lower() == 'zip_code':  # Exact match for ZIP code
                    zip_col = i
            
            if apn_col is None:
                print("Error: Could not find APN column")
                return []
            
            # Read all data (limit to reasonable amount)
            row_count = 0
            for row in reader:
                if len(row) > apn_col and row[apn_col]:
                    data_row = {
                        'apn': row[apn_col],
                        'zoning': row[zoning_col] if zoning_col and len(row) > zoning_col else 'Unknown',
                        'zip': row[zip_col] if zip_col and len(row) > zip_col else 'Unknown'
                    }
                    all_data.append(data_row)
                
                row_count += 1
                if row_count >= 10000:  # Limit to first 10K for performance
                    break
    
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return []
    
    print(f"Loaded {len(all_data):,} parcels for diverse sampling")
    
    # Create diverse sample
    diverse_sample = []
    
    # 1. Random sample (40%)
    random_size = int(total_size * 0.4)
    random_sample = random.sample(all_data, min(random_size, len(all_data)))
    diverse_sample.extend(random_sample)
    
    # 2. Sample by zoning (30%)
    if zoning_col is not None:
        zoning_groups = defaultdict(list)
        for item in all_data:
            if item['zoning'] != 'Unknown':
                zoning_groups[item['zoning']].append(item)
        
        zoning_size = int(total_size * 0.3)
        per_zone = max(1, zoning_size // len(zoning_groups))
        
        for zoning, items in list(zoning_groups.items())[:20]:  # Top 20 zones
            sample_size = min(per_zone, len(items))
            diverse_sample.extend(random.sample(items, sample_size))
    
    # 3. Sample by ZIP (30%)
    if zip_col is not None:
        zip_groups = defaultdict(list)
        for item in all_data:
            if item['zip'] != 'Unknown':
                zip_groups[item['zip']].append(item)
        
        zip_size = int(total_size * 0.3)
        per_zip = max(1, zip_size // min(50, len(zip_groups)))
        
        for zip_code, items in list(zip_groups.items())[:50]:  # Top 50 ZIPs
            sample_size = min(per_zip, len(items))
            diverse_sample.extend(random.sample(items, sample_size))
    
    # Remove duplicates and limit size
    seen_apns = set()
    unique_sample = []
    for item in diverse_sample:
        if item['apn'] not in seen_apns:
            seen_apns.add(item['apn'])
            unique_sample.append(item)
        if len(unique_sample) >= total_size:
            break
    
    print(f"\nExtracted {len(unique_sample)} diverse parcels")
    
    # Save diverse sample
    try:
        with open('diverse_sample_parcels.csv', 'w', newline='') as f:
            if unique_sample:
                writer = csv.DictWriter(f, fieldnames=unique_sample[0].keys())
                writer.writeheader()
                writer.writerows(unique_sample)
        print(f"Saved diverse sample to diverse_sample_parcels.csv")
        
        # Save APNs
        with open('diverse_apns.txt', 'w') as f:
            for item in unique_sample:
                f.write(f"{item['apn']}\n")
        print(f"Saved APNs to diverse_apns.txt")
        
    except Exception as e:
        print(f"Error saving files: {e}")
    
    return unique_sample

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
    diverse_sample = extract_diverse_sample(csv_path, total_size=1000)
    
    print("\n" + "="*60)
    print("Extraction Complete!")
    print("="*60)
    print("\nNext steps:")
    print("1. Run individual APN tests with the commands above")
    print("2. Run batch tests using sample_apns.txt or diverse_apns.txt")
    print("3. Check performance metrics from the benchmark script")
    print(f"\nFiles created:")
    print(f"  • sample_apns.txt ({len(basic_apns)} APNs)")
    print(f"  • diverse_sample_parcels.csv ({len(diverse_sample)} parcels)")
    print(f"  • diverse_apns.txt ({len(diverse_sample)} APNs)")