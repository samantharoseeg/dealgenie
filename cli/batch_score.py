#!/usr/bin/env python3
"""
DealGenie Batch Scoring CLI
Generates scores for multiple properties to support statistical validation
"""

import argparse
import csv
import json
import sys
import random
import logging
from pathlib import Path
from typing import List, Dict, Any

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scoring.engine import calculate_score
from features.feature_matrix import get_feature_matrix, get_default_features

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


def generate_diverse_test_data(sample_size: int, template: str) -> List[Dict[str, Any]]:
    """Generate diverse property data for testing"""
    
    # Base property characteristics by LA area type
    area_types = {
        'downtown_core': {
            'zoning_options': ['R5', 'C4', 'C2', 'RAS4'],
            'lot_size_range': (5000, 25000),
            'transit_range': (75, 95),
            'income_range': (55000, 85000),
            'density_range': (12000, 25000),
            'price_psf_range': (400, 700)
        },
        'mid_city': {
            'zoning_options': ['R3', 'R4', 'C1', 'RAS3'],
            'lot_size_range': (4000, 15000),
            'transit_range': (60, 80),
            'income_range': (45000, 70000),
            'density_range': (8000, 15000),
            'price_psf_range': (350, 550)
        },
        'suburban': {
            'zoning_options': ['R1', 'R2', 'C1'],
            'lot_size_range': (6000, 12000),
            'transit_range': (25, 50),
            'income_range': (65000, 120000),
            'density_range': (3000, 7000),
            'price_psf_range': (500, 800)
        },
        'industrial_corridor': {
            'zoning_options': ['M1', 'M2', 'MR1', 'C2'],
            'lot_size_range': (15000, 75000),
            'transit_range': (20, 45),
            'income_range': (35000, 55000),
            'density_range': (1500, 4000),
            'price_psf_range': (150, 350)
        },
        'commercial_corridor': {
            'zoning_options': ['C1', 'C2', 'C4', 'CM'],
            'lot_size_range': (3000, 20000),
            'transit_range': (55, 85),
            'income_range': (40000, 75000),
            'density_range': (6000, 18000),
            'price_psf_range': (300, 600)
        }
    }
    
    # Weight area types based on template
    if template == 'industrial':
        area_weights = {'industrial_corridor': 50, 'commercial_corridor': 30, 'mid_city': 15, 'downtown_core': 3, 'suburban': 2}
    elif template == 'retail':
        area_weights = {'commercial_corridor': 40, 'mid_city': 25, 'downtown_core': 20, 'suburban': 10, 'industrial_corridor': 5}
    elif template == 'multifamily':
        area_weights = {'mid_city': 35, 'downtown_core': 30, 'commercial_corridor': 20, 'suburban': 10, 'industrial_corridor': 5}
    elif template == 'commercial':
        area_weights = {'commercial_corridor': 35, 'downtown_core': 30, 'mid_city': 25, 'suburban': 7, 'industrial_corridor': 3}
    else:  # residential
        area_weights = {'suburban': 40, 'mid_city': 30, 'commercial_corridor': 15, 'downtown_core': 10, 'industrial_corridor': 5}
    
    # Create weighted area selection
    area_choices = []
    for area, weight in area_weights.items():
        area_choices.extend([area] * weight)
    
    properties = []
    
    for i in range(sample_size):
        # Select area type
        area_type = random.choice(area_choices)
        area_config = area_types[area_type]
        
        # Generate property features
        apn = f"TEST{template.upper()}{i+1:04d}"
        
        features = {
            'apn': apn,
            'site_address': f"{random.randint(100, 9999)} {random.choice(['Main', 'Broadway', 'Spring', 'Hope', 'Figueroa'])} St",
            'site_city': 'Los Angeles',
            'site_zip': random.choice(['90013', '90014', '90015', '90017', '90012']),
            'zoning': random.choice(area_config['zoning_options']),
            'lot_size_sqft': random.randint(*area_config['lot_size_range']),
            'year_built': random.randint(1940, 2020),
            'latitude': round(34.0522 + random.uniform(-0.05, 0.05), 6),
            'longitude': round(-118.2437 + random.uniform(-0.05, 0.05), 6),
            'total_population': random.randint(8000, 20000),
            'median_income': random.randint(*area_config['income_range']),
            'population_density': random.randint(*area_config['density_range']),
            'housing_units': random.randint(2000, 8000),
            'median_rent': random.randint(1800, 3500),
            'college_degree_pct': random.uniform(15.0, 65.0),
            'unemployment_rate': random.uniform(3.0, 12.0),
            'transit_score': random.randint(*area_config['transit_range']),
            'price_per_sqft': random.uniform(*area_config['price_psf_range']),
            'area_type': area_type
        }
        
        # Calculate building sqft and assessed value
        features['building_sqft'] = features['lot_size_sqft'] * random.uniform(0.3, 0.8)
        features['assessed_value'] = features['building_sqft'] * features['price_per_sqft']
        features['far'] = features['building_sqft'] / features['lot_size_sqft']
        
        # Add highway access for industrial
        if template == 'industrial':
            features['highway_access'] = random.randint(40, 90)
        
        properties.append(features)
    
    return properties


def batch_score_properties(properties: List[Dict[str, Any]], template: str) -> List[Dict[str, Any]]:
    """Score multiple properties and return results"""
    results = []
    
    print(f"Scoring {len(properties)} properties with {template} template...")
    
    for i, prop_features in enumerate(properties):
        try:
            # Calculate score
            score_result = calculate_score(prop_features, template)
            
            # Create result record
            result = {
                'apn': prop_features['apn'],
                'address': prop_features.get('site_address', 'N/A'),
                'zoning': prop_features.get('zoning', 'N/A'),
                'lot_size_sqft': prop_features.get('lot_size_sqft', 0),
                'transit_score': prop_features.get('transit_score', 0),
                'median_income': prop_features.get('median_income', 0),
                'area_type': prop_features.get('area_type', 'unknown'),
                'template': template,
                'score': score_result['score'],
                'zoning_component': score_result['component_scores'].get('zoning', 0),
                'lot_size_component': score_result['component_scores'].get('lot_size', 0),
                'transit_component': score_result['component_scores'].get('transit', 0),
                'demographics_component': score_result['component_scores'].get('demographics', 0),
                'market_component': score_result['component_scores'].get('market', 0)
            }
            
            results.append(result)
            
            if (i + 1) % 20 == 0:
                print(f"  Processed {i + 1}/{len(properties)} properties...")
                
        except Exception as e:
            logger.error(f"Error scoring property {prop_features.get('apn', 'unknown')}: {e}")
    
    return results


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="DealGenie Batch Property Scoring",
        formatter_class=argparse.RawDescriptionHelpFormatter
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
        default=100,
        help='Number of properties to generate and score'
    )
    parser.add_argument(
        '--output',
        required=True,
        help='Output CSV file path'
    )
    parser.add_argument(
        '--use-db',
        action='store_true',
        help='Use actual database properties instead of generated test data'
    )
    
    args = parser.parse_args()
    
    if args.use_db:
        # TODO: Query actual properties from database
        print("Database mode not yet implemented, using generated test data")
        properties = generate_diverse_test_data(args.sample_size, args.template)
    else:
        # Generate test properties
        properties = generate_diverse_test_data(args.sample_size, args.template)
    
    # Score all properties
    results = batch_score_properties(properties, args.template)
    
    # Write results to CSV
    if results:
        fieldnames = results[0].keys()
        
        with open(args.output, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        
        print(f"\n✅ Batch scoring complete!")
        print(f"   - Template: {args.template}")
        print(f"   - Properties scored: {len(results)}")
        print(f"   - Output file: {args.output}")
        print(f"   - Score range: {min(r['score'] for r in results):.1f} - {max(r['score'] for r in results):.1f}")
        
        # Quick statistics
        scores = [r['score'] for r in results]
        avg_score = sum(scores) / len(scores)
        print(f"   - Average score: {avg_score:.2f}")
    else:
        print("❌ No results generated")
        sys.exit(1)


if __name__ == '__main__':
    main()