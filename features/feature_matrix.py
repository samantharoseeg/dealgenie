import logging
from typing import Dict, Any, Optional
import os

try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_feature_matrix(apn: str) -> Dict[str, Any]:
    """
    Get feature matrix for a property from CSV data (LA County parcels).
    
    Args:
        apn: Assessor Parcel Number
    
    Returns:
        Dictionary containing property features
    """
    try:
        # Use CSV-based feature extraction for LA County data
        from .csv_feature_matrix import get_feature_matrix as get_csv_features
        return get_csv_features(apn)
    except Exception as e:
        logger.error(f"Error loading from CSV, falling back to defaults: {e}")
        return get_default_features(apn)


def get_default_features(apn: str) -> Dict[str, Any]:
    """
    Get default features when database is not available.
    Used for testing purposes.
    """
    # Sample default features for testing
    default_features = {
        'apn': apn,
        'site_address': '123 Main St',
        'site_city': 'Los Angeles',
        'site_zip': '90001',
        'zoning': 'R3',
        'lot_size_sqft': 7500,
        'year_built': 1960,
        'building_sqft': 3500,
        'assessed_value': 850000,
        'census_geoid': '06037123456',
        'latitude': 34.0522,
        'longitude': -118.2437,
        'total_population': 15000,
        'median_income': 65000,
        'population_density': 8500,
        'housing_units': 5000,
        'median_rent': 2200,
        'college_degree_pct': 35.0,
        'unemployment_rate': 5.5,
        'price_per_sqft': 242.86,
        'transit_score': 65,
        'far': 0.47,
        'development_potential': 7.5,
        
        # Risk factors (default - no major risks)
        'crime_factor': 1.0,
        'flood_risk': False,
        'toxic_sites_nearby': 0,
        'superfund_site_nearby': False,
        'airport_noise_level': 45,
        'near_airport': False,
        'homeless_encampments_nearby': 0,
        'homeless_population_density': 15,
        'freeway_distance_ft': 2500,
        'industrial_facilities_nearby': 1,
        'air_quality_index': 75,
        'seismic_risk_level': 'moderate',
        'utility_deficiencies': []
    }
    
    # Override with specific test data for known APNs
    test_data = {
        '5306050014': {
            'zoning': 'R4',
            'lot_size_sqft': 12000,
            'transit_score': 75,
            'median_income': 78000,
            'crime_factor': 0.8,  # Low crime area
            'air_quality_index': 65
        },
        '5309100014': {
            'zoning': 'C2',
            'lot_size_sqft': 8500,
            'transit_score': 82,
            'median_income': 85000,
            'crime_factor': 0.9,
            'freeway_distance_ft': 1200  # Close to freeway
        },
        '5309130032': {
            'zoning': 'R3',
            'lot_size_sqft': 6200,
            'transit_score': 58,
            'median_income': 62000,
            'crime_factor': 1.3,  # Higher crime
            'homeless_encampments_nearby': 1
        },
        # Add some high-risk test properties
        'HIGHRISK001': {
            'zoning': 'R2',
            'lot_size_sqft': 5500,
            'transit_score': 35,
            'median_income': 42000,
            'crime_factor': 1.8,  # Very high crime
            'flood_risk': True,  # In flood zone
            'toxic_sites_nearby': 2,
            'airport_noise_level': 72,  # Near airport
            'homeless_encampments_nearby': 4,
            'freeway_distance_ft': 300  # Very close to freeway
        },
        'LOWRISK001': {
            'zoning': 'R1',
            'lot_size_sqft': 9500,
            'transit_score': 85,
            'median_income': 125000,
            'crime_factor': 0.3,  # Very low crime
            'flood_risk': False,
            'toxic_sites_nearby': 0,
            'airport_noise_level': 40,
            'homeless_encampments_nearby': 0,
            'freeway_distance_ft': 3500,
            'air_quality_index': 45,
            'seismic_risk_level': 'low'
        }
    }
    
    if apn in test_data:
        default_features.update(test_data[apn])
    
    return default_features


def calculate_development_potential(features: Dict[str, Any]) -> float:
    """
    Calculate development potential score based on features.
    
    Args:
        features: Property features dictionary
    
    Returns:
        Development potential score (0-10)
    """
    score = 5.0  # Base score
    
    # Zoning bonus
    zoning = str(features.get('zoning', '')).upper()
    if 'R5' in zoning or 'R4' in zoning or 'C' in zoning:
        score += 2.0
    elif 'R3' in zoning or 'RAS' in zoning:
        score += 1.5
    elif 'R2' in zoning:
        score += 0.5
    
    # Lot size bonus
    lot_size = features.get('lot_size_sqft', 0)
    if lot_size > 15000:
        score += 1.5
    elif lot_size > 10000:
        score += 1.0
    elif lot_size > 7500:
        score += 0.5
    
    # Transit bonus
    transit = features.get('transit_score', 0)
    if transit > 70:
        score += 1.0
    elif transit > 50:
        score += 0.5
    
    # Cap at 10
    return min(score, 10.0)