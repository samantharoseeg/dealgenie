#!/usr/bin/env python3
"""
Realistic LA Property Sampler
Generates properties from actual LA geography including challenging areas
"""

import random
import argparse
import csv
import sys
from pathlib import Path
from typing import List, Dict, Any

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scoring.engine import calculate_score

# Real LA area profiles based on actual conditions
LA_AREA_PROFILES = {
    # Problem Areas (50% target)
    "90001": {  # South LA
        "name": "South LA",
        "category": "problem",
        "zoning_options": ["R1", "R2", "C1"],
        "lot_size_range": (3500, 7500),
        "transit_range": (25, 45),
        "income_range": (28000, 45000),
        "density_range": (12000, 18000),
        "price_psf_range": (200, 350),
        "crime_factor": 1.8,
        "school_quality": 3.2
    },
    "90002": {  # Watts
        "name": "Watts", 
        "category": "problem",
        "zoning_options": ["R1", "R2", "M1"],
        "lot_size_range": (4000, 8000),
        "transit_range": (20, 40),
        "income_range": (25000, 42000),
        "density_range": (8000, 15000),
        "price_psf_range": (180, 320),
        "crime_factor": 1.9,
        "school_quality": 2.8
    },
    "90058": {  # Vernon - Pure Industrial
        "name": "Vernon Industrial",
        "category": "problem", 
        "zoning_options": ["M1", "M2", "M3"],
        "lot_size_range": (20000, 100000),
        "transit_range": (15, 35),
        "income_range": (35000, 55000),
        "density_range": (1200, 3000),
        "price_psf_range": (120, 250),
        "crime_factor": 1.3,
        "school_quality": 4.0
    },
    "90255": {  # Huntington Park
        "name": "Huntington Park",
        "category": "problem",
        "zoning_options": ["R2", "C1", "M1"],
        "lot_size_range": (3000, 6500),
        "transit_range": (30, 50),
        "income_range": (32000, 48000),
        "density_range": (15000, 22000),
        "price_psf_range": (220, 380),
        "crime_factor": 1.6,
        "school_quality": 3.5
    },
    "91331": {  # Pacoima - Flood Zone
        "name": "Pacoima",
        "category": "problem",
        "zoning_options": ["R1", "R2", "M1"],
        "lot_size_range": (4500, 9000),
        "transit_range": (20, 35),
        "income_range": (38000, 58000),
        "density_range": (6000, 12000),
        "price_psf_range": (250, 400),
        "crime_factor": 1.4,
        "school_quality": 4.2,
        "flood_risk": True
    },
    
    # Mediocre Areas (30% target)
    "91706": {  # Baldwin Park - Suburban Sprawl
        "name": "Baldwin Park",
        "category": "mediocre",
        "zoning_options": ["R1", "R2", "C1"],
        "lot_size_range": (5000, 8500),
        "transit_range": (25, 45),
        "income_range": (48000, 68000),
        "density_range": (7000, 12000),
        "price_psf_range": (380, 520),
        "crime_factor": 1.1,
        "school_quality": 5.5
    },
    "90717": {  # Lomita - Rural/Isolated
        "name": "Lomita",
        "category": "mediocre",
        "zoning_options": ["R1", "RE"],
        "lot_size_range": (6000, 12000),
        "transit_range": (15, 30),
        "income_range": (55000, 80000),
        "density_range": (3000, 6000),
        "price_psf_range": (450, 650),
        "crime_factor": 0.7,
        "school_quality": 6.2
    },
    "90280": {  # South Gate - Working Class
        "name": "South Gate",
        "category": "mediocre",
        "zoning_options": ["R1", "R2", "C1"],
        "lot_size_range": (4000, 7000),
        "transit_range": (35, 55),
        "income_range": (42000, 62000),
        "density_range": (10000, 16000),
        "price_psf_range": (320, 480),
        "crime_factor": 1.2,
        "school_quality": 4.8
    },
    
    # Good Areas (20% target)
    "90210": {  # Beverly Hills
        "name": "Beverly Hills",
        "category": "good",
        "zoning_options": ["R1", "RE", "C2"],
        "lot_size_range": (8000, 25000),
        "transit_range": (55, 75),
        "income_range": (95000, 180000),
        "density_range": (4000, 8000),
        "price_psf_range": (800, 1500),
        "crime_factor": 0.3,
        "school_quality": 9.2
    },
    "90404": {  # Santa Monica
        "name": "Santa Monica",
        "category": "good", 
        "zoning_options": ["R2", "R3", "C2"],
        "lot_size_range": (3500, 8000),
        "transit_range": (70, 90),
        "income_range": (85000, 140000),
        "density_range": (12000, 20000),
        "price_psf_range": (900, 1400),
        "crime_factor": 0.6,
        "school_quality": 8.5
    },
    "91423": {  # Sherman Oaks
        "name": "Sherman Oaks",
        "category": "good",
        "zoning_options": ["R1", "R2", "C1"],
        "lot_size_range": (6000, 12000),
        "transit_range": (50, 70),
        "income_range": (78000, 125000),
        "density_range": (6000, 11000),
        "price_psf_range": (650, 950),
        "crime_factor": 0.5,
        "school_quality": 8.8
    }
}


def generate_realistic_properties(sample_size: int, template: str) -> List[Dict[str, Any]]:
    """Generate properties from realistic LA geography"""
    
    # Target distribution: 50% problem, 30% mediocre, 20% good
    area_distribution = {
        "problem": 0.50,
        "mediocre": 0.30, 
        "good": 0.20
    }
    
    # Create weighted area list
    area_choices = []
    for zip_code, profile in LA_AREA_PROFILES.items():
        category = profile["category"]
        target_pct = area_distribution[category]
        count = int(sample_size * target_pct / len([p for p in LA_AREA_PROFILES.values() if p["category"] == category]))
        area_choices.extend([zip_code] * count)
    
    # Fill remaining slots randomly
    while len(area_choices) < sample_size:
        area_choices.append(random.choice(list(LA_AREA_PROFILES.keys())))
    
    # Shuffle and trim to exact size
    random.shuffle(area_choices)
    area_choices = area_choices[:sample_size]
    
    properties = []
    
    for i, zip_code in enumerate(area_choices):
        profile = LA_AREA_PROFILES[zip_code]
        
        # Generate base property features
        apn = f"REAL{template.upper()}{i+1:04d}"
        
        # Calculate adjusted features based on area challenges
        base_transit = random.randint(*profile['transit_range'])
        base_income = random.randint(*profile['income_range'])
        
        # Apply area-specific penalties/bonuses
        crime_penalty = (profile.get('crime_factor', 1.0) - 1.0) * 10
        school_bonus = (profile.get('school_quality', 5.0) - 5.0) * 2
        flood_penalty = 15 if profile.get('flood_risk', False) else 0
        
        # Adjust transit score for area challenges
        adjusted_transit = max(10, base_transit - crime_penalty + school_bonus - flood_penalty)
        
        features = {
            'apn': apn,
            'site_address': f"{random.randint(100, 9999)} {random.choice(['Main', 'Central', 'Pacific', 'Vermont', 'Western'])} {'St' if random.random() > 0.5 else 'Ave'}",
            'site_city': profile['name'],
            'site_zip': zip_code,
            'zoning': random.choice(profile['zoning_options']),
            'lot_size_sqft': random.randint(*profile['lot_size_range']),
            'year_built': random.randint(1950, 2010),
            'median_income': base_income,
            'population_density': random.randint(*profile['density_range']),
            'transit_score': int(adjusted_transit),
            'price_per_sqft': random.uniform(*profile['price_psf_range']),
            
            # Area quality indicators
            'area_category': profile['category'],
            'area_name': profile['name'],
            'crime_factor': profile.get('crime_factor', 1.0),
            'school_quality': profile.get('school_quality', 5.0),
            'flood_risk': profile.get('flood_risk', False),
            
            # Additional risk factors based on area
            'toxic_sites_nearby': random.randint(0, 3) if profile['category'] == 'problem' else 0,
            'airport_noise_level': random.randint(55, 75) if zip_code in ['90045', '90230'] else random.randint(40, 60),
            'homeless_encampments_nearby': random.randint(1, 4) if profile['category'] == 'problem' else 0,
            'freeway_distance_ft': random.randint(200, 800) if random.random() < 0.3 else random.randint(1000, 5000),
            'industrial_facilities_nearby': random.randint(2, 6) if 'Industrial' in profile['name'] else random.randint(0, 2),
            
            # Standard demographics
            'total_population': random.randint(8000, 25000),
            'housing_units': random.randint(2000, 10000),
            'median_rent': int(base_income * 0.3 / 12),  # 30% of income
            'college_degree_pct': max(10.0, min(70.0, (base_income - 30000) / 1500)),
            'unemployment_rate': max(3.0, 15.0 - (base_income / 8000)),
            
            # Location (approximate)
            'latitude': round(34.0522 + random.uniform(-0.15, 0.15), 6),
            'longitude': round(-118.2437 + random.uniform(-0.25, 0.25), 6)
        }
        
        # Calculate derived features
        features['building_sqft'] = features['lot_size_sqft'] * random.uniform(0.25, 0.75)
        features['assessed_value'] = features['building_sqft'] * features['price_per_sqft']
        features['far'] = features['building_sqft'] / features['lot_size_sqft']
        
        properties.append(features)
    
    return properties


def score_realistic_properties(properties: List[Dict[str, Any]], template: str, output_file: str):
    """Score realistic properties and save results"""
    
    print(f"Scoring {len(properties)} realistic LA properties with {template} template...")
    
    results = []
    category_counts = {"problem": 0, "mediocre": 0, "good": 0}
    
    for i, prop_features in enumerate(properties):
        try:
            # Calculate score
            score_result = calculate_score(prop_features, template)
            
            # Track category distribution
            category = prop_features.get('area_category', 'unknown')
            if category in category_counts:
                category_counts[category] += 1
            
            # Create result record
            result = {
                'apn': prop_features['apn'],
                'area_name': prop_features.get('area_name', 'Unknown'),
                'zip_code': prop_features.get('site_zip', ''),
                'area_category': category,
                'zoning': prop_features.get('zoning', 'N/A'),
                'lot_size_sqft': prop_features.get('lot_size_sqft', 0),
                'transit_score': prop_features.get('transit_score', 0),
                'median_income': prop_features.get('median_income', 0),
                'crime_factor': prop_features.get('crime_factor', 1.0),
                'school_quality': prop_features.get('school_quality', 5.0),
                'flood_risk': prop_features.get('flood_risk', False),
                'template': template,
                'score': score_result['score'],
                'base_score': score_result.get('base_score', score_result['score']),
                'total_penalties': score_result.get('total_penalties', 0),
                'penalty_count': len(score_result.get('penalties', {})),
                'explanation': score_result['explanation'][:100] + '...'
            }
            
            # Add component scores
            for comp_name, comp_score in score_result.get('component_scores', {}).items():
                result[f'{comp_name}_component'] = comp_score
            
            results.append(result)
            
            if (i + 1) % 25 == 0:
                print(f"  Processed {i + 1}/{len(properties)} properties...")
                
        except Exception as e:
            print(f"Error scoring property {prop_features.get('apn', 'unknown')}: {e}")
    
    # Save results
    if results:
        fieldnames = results[0].keys()
        
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
    
    # Print summary
    print(f"\n✅ Realistic scoring complete!")
    print(f"   - Template: {template}")
    print(f"   - Properties scored: {len(results)}")
    print(f"   - Output file: {output_file}")
    
    if results:
        scores = [r['score'] for r in results]
        print(f"   - Score range: {min(scores):.1f} - {max(scores):.1f}")
        print(f"   - Average score: {sum(scores)/len(scores):.2f}")
        
        # Category breakdown
        print(f"   - Area distribution:")
        total = len(results)
        for category, count in category_counts.items():
            pct = count / total * 100
            avg_score = sum(r['score'] for r in results if r['area_category'] == category) / max(count, 1)
            print(f"     {category.capitalize():>8}: {count:2d} ({pct:4.1f}%) - Avg: {avg_score:.2f}")
    
    return results


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Realistic LA Property Scoring with Geographic Diversity"
    )
    
    parser.add_argument(
        '--template',
        choices=['multifamily', 'commercial', 'residential', 'industrial', 'retail'],
        required=True,
        help='Scoring template to use'
    )
    parser.add_argument(
        '--sample-size',
        type=int,
        default=200,
        help='Number of properties to generate and score'
    )
    parser.add_argument(
        '--output',
        help='Output CSV file path (default: realistic_{template}_validation.csv)'
    )
    
    args = parser.parse_args()
    
    # Set default output filename
    if not args.output:
        args.output = f"realistic_{args.template}_validation.csv"
    
    # Generate realistic properties
    print("🌍 Generating realistic LA property sample...")
    print(f"Target distribution: 50% problem areas, 30% mediocre, 20% good")
    
    properties = generate_realistic_properties(args.sample_size, args.template)
    
    # Score properties
    results = score_realistic_properties(properties, args.template, args.output)
    
    return len(results) > 0


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)