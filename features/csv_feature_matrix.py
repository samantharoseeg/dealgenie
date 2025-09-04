import logging
import csv
import os
from typing import Dict, Any, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CSVFeatureMatrix:
    """CSV-based feature matrix for LA County parcel data."""
    
    def __init__(self, csv_path: str = "scraper/la_parcels_complete_merged.csv"):
        self.csv_path = csv_path
        self.headers = []
        self.header_index = {}
        self._load_headers()
        
    def _load_headers(self):
        """Load CSV headers for column mapping."""
        if not os.path.exists(self.csv_path):
            logger.error(f"CSV file not found: {self.csv_path}")
            return
            
        try:
            with open(self.csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                self.headers = next(reader)
                self.header_index = {header: i for i, header in enumerate(self.headers)}
            
            logger.info(f"Loaded {len(self.headers)} CSV columns")
            
        except Exception as e:
            logger.error(f"Error loading CSV headers: {e}")
    
    def find_apn_data(self, apn: str) -> Optional[list]:
        """Find row data for a specific APN."""
        if not os.path.exists(self.csv_path):
            return None
            
        try:
            with open(self.csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader)  # Skip headers
                
                for row in reader:
                    if len(row) > 0 and row[0] == str(apn):  # APN is first column
                        return row
                        
        except Exception as e:
            logger.error(f"Error searching for APN {apn}: {e}")
        
        return None
    
    def get_column_value(self, row: list, column_name: str, default=None):
        """Get value from row by column name."""
        if column_name in self.header_index:
            col_index = self.header_index[column_name]
            if col_index < len(row):
                value = row[col_index].strip()
                return value if value else default
        return default
    
    def parse_numeric(self, value: str, default: float = 0.0) -> float:
        """Parse numeric value from CSV, handling various formats."""
        if not value or value.lower() in ['', 'null', 'none', 'n/a']:
            return default
            
        # Remove common formatting
        cleaned = value.replace(',', '').replace('$', '').replace('(', '').replace(')', '')
        
        # Handle formats like "4,648.0 (sq ft)"
        if '(' in value or 'sq ft' in value.lower() or 'ac)' in value.lower():
            parts = cleaned.split()
            for part in parts:
                try:
                    return float(part)
                except ValueError:
                    continue
        
        try:
            return float(cleaned)
        except (ValueError, TypeError):
            return default
    
    def calculate_transit_score(self, zip_code: str, zoning: str, features: dict) -> float:
        """Calculate transit accessibility score based on location data."""
        base_score = 50.0  # Default
        
        # High-density transit areas in LA
        transit_zip_codes = {
            '90028': 85,  # Hollywood
            '90038': 80,  # Mid-City
            '90010': 85,  # Koreatown
            '90005': 80,  # Koreatown
            '90026': 75,  # Silver Lake
            '90027': 70,  # Los Feliz
            '90004': 75,  # Los Feliz/Silver Lake
            '90019': 70,  # Mid-City
            '90035': 65,  # Pico-Robertson (our test area)
            '90036': 70,  # Fairfax
            '90048': 75,  # West Hollywood
            '90069': 80,  # West Hollywood
        }
        
        if zip_code in transit_zip_codes:
            base_score = transit_zip_codes[zip_code]
        
        # Adjust based on transit indicators from CSV
        if features.get('divTab3_ab_2097_within_a_half_mile_of_a_major_transit_stop') == 'Yes':
            base_score += 15
        if features.get('divTab3_high_quality_transit_corridor') == 'High':
            base_score += 10
        if features.get('divTab3_transit_oriented_communities') == 'Yes':
            base_score += 20
            
        return min(base_score, 100.0)
    
    def calculate_demographics_score(self, zip_code: str, zoning: str) -> dict:
        """Calculate demographics based on ZIP code and area characteristics."""
        # LA County demographic estimates by ZIP code (simplified)
        demographics_data = {
            '90035': {'median_income': 85000, 'population_density': 12500, 'crime_factor': 0.8},  # Pico-Robertson
            '90028': {'median_income': 65000, 'population_density': 18500, 'crime_factor': 1.2},  # Hollywood
            '90210': {'median_income': 125000, 'population_density': 8500, 'crime_factor': 0.4}, # Beverly Hills
            '90024': {'median_income': 95000, 'population_density': 11000, 'crime_factor': 0.6}, # Westwood
            '90272': {'median_income': 145000, 'population_density': 4500, 'crime_factor': 0.3}, # Pacific Palisades
            '90049': {'median_income': 135000, 'population_density': 6000, 'crime_factor': 0.4}, # Brentwood
            '90019': {'median_income': 52000, 'population_density': 16000, 'crime_factor': 1.4},  # Mid-City
            '90037': {'median_income': 38000, 'population_density': 14500, 'crime_factor': 1.8},  # South LA
            '90003': {'median_income': 35000, 'population_density': 15500, 'crime_factor': 1.9},  # South LA
        }
        
        default_demo = {'median_income': 65000, 'population_density': 10000, 'crime_factor': 1.0}
        return demographics_data.get(zip_code, default_demo)
    
    def get_feature_matrix(self, apn: str) -> Dict[str, Any]:
        """Get comprehensive feature matrix for an APN from CSV data."""
        logger.info(f"Loading features for APN {apn} from CSV")
        
        # Find APN data in CSV
        row_data = self.find_apn_data(apn)
        
        if not row_data:
            logger.warning(f"APN {apn} not found in CSV, using defaults")
            return self._get_default_features(apn)
        
        logger.info(f"Found APN {apn} in CSV with {len(row_data)} fields")
        
        # Extract core property data
        features = {
            'apn': apn,
            
            # Basic property info
            'site_address': self.get_column_value(row_data, 'site_address', '123 Main St'),
            'site_city': 'Los Angeles',  # All LA County
            'site_zip': self.get_column_value(row_data, 'zip_code', '90001'),
            
            # Zoning (critical for scoring)
            'zoning': self.get_column_value(row_data, 'zoning_code', 'R1'),
            
            # Property characteristics
            'lot_size_sqft': self.parse_numeric(self.get_column_value(row_data, 'lot_parcel_area', '0'), 7500),
            'building_sqft': self.parse_numeric(self.get_column_value(row_data, 'building_square_footage', '0'), 2500),
            'year_built': int(self.parse_numeric(self.get_column_value(row_data, 'building_year_built', '1975'), 1975)),
            'number_of_units': int(self.parse_numeric(self.get_column_value(row_data, 'number_of_units', '1'), 1)),
            
            # Financial data
            'assessed_value': self.parse_numeric(self.get_column_value(row_data, 'divTab4_assessed_improvement_val', '500000'), 500000),
            'last_sale_amount': self.parse_numeric(self.get_column_value(row_data, 'divTab4_last_sale_amount', '0'), 0),
            
            # Geographic identifiers
            'census_geoid': self.get_column_value(row_data, 'divTab2_census_tract', '06037000000'),
            'council_district': self.get_column_value(row_data, 'divTab2_council_district', 'CD 1'),
            'neighborhood_council': self.get_column_value(row_data, 'divTab2_neighborhood_council', 'Unknown'),
            
            # Special zones and overlays
            'historic_preservation': self.get_column_value(row_data, 'divTab3_historic_preservation_overlay_zone') == 'Yes',
            'hillside_area': self.get_column_value(row_data, 'divTab3_hillside_area') == 'Yes',
            'coastal_zone': self.get_column_value(row_data, 'divTab7_coastal_zone') == 'Yes',
            'flood_zone': self.get_column_value(row_data, 'divTab7_flood_zone', 'Outside Flood Zone') != 'Outside Flood Zone',
            'fire_hazard_zone': self.get_column_value(row_data, 'divTab7_very_high_fire_hazard_severity_zone') == 'Yes',
            
            # Environmental factors
            'methane_hazard': self.get_column_value(row_data, 'divTab7_methane_hazard_site') != '',
            'airport_hazard': self.get_column_value(row_data, 'divTab7_airport_hazard') == 'Yes',
            'oil_well_adjacency': self.get_column_value(row_data, 'divTab7_oil_well_adjacency') != '',
            'liquefaction': self.get_column_value(row_data, 'divTab8_liquefaction') == 'Yes',
            'landslide': self.get_column_value(row_data, 'divTab8_landslide') == 'Yes',
        }
        
        # Calculate derived metrics
        if features['building_sqft'] > 0 and features['assessed_value'] > 0:
            features['price_per_sqft'] = features['assessed_value'] / features['building_sqft']
        else:
            features['price_per_sqft'] = 300  # Default LA average
        
        if features['lot_size_sqft'] > 0 and features['building_sqft'] > 0:
            features['far'] = features['building_sqft'] / features['lot_size_sqft']
        else:
            features['far'] = 0.5
        
        # Calculate transit score
        features['transit_score'] = self.calculate_transit_score(
            features['site_zip'], 
            features['zoning'], 
            features
        )
        
        # Add demographics data
        demo_data = self.calculate_demographics_score(features['site_zip'], features['zoning'])
        features.update({
            'total_population': demo_data['population_density'] * (features['lot_size_sqft'] / 43560) * 100,  # Rough estimate
            'median_income': demo_data['median_income'],
            'population_density': demo_data['population_density'],
            'crime_factor': demo_data['crime_factor'],
        })
        
        # Risk factors (mostly defaults, some from CSV)
        features.update({
            'flood_risk': features['flood_zone'],
            'toxic_sites_nearby': 1 if features['oil_well_adjacency'] else 0,
            'superfund_site_nearby': False,  # Would need EPA data
            'airport_noise_level': 70 if features['airport_hazard'] else 45,
            'near_airport': features['airport_hazard'],
            'homeless_encampments_nearby': 0,  # Would need current survey data
            'homeless_population_density': 25,  # LA County average
            'freeway_distance_ft': 2000,  # Default - would need GIS calculation
            'industrial_facilities_nearby': 1,
            'air_quality_index': 75,  # LA average
            'seismic_risk_level': 'moderate',  # Most of LA
            'utility_deficiencies': []
        })
        
        # Calculate development potential
        features['development_potential'] = self._calculate_development_potential(features)
        
        logger.info(f"Successfully extracted {len(features)} features for APN {apn}")
        logger.info(f"Key features: zoning={features['zoning']}, lot_size={features['lot_size_sqft']}, address={features['site_address']}")
        
        return features
    
    def _calculate_development_potential(self, features: Dict[str, Any]) -> float:
        """Calculate development potential score based on features."""
        score = 5.0  # Base score
        
        # Zoning bonus
        zoning = str(features.get('zoning', '')).upper()
        if any(z in zoning for z in ['R5', 'R4', 'C2', 'C4']):
            score += 2.0
        elif any(z in zoning for z in ['R3', 'RAS']):
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
        
        return min(score, 10.0)
    
    def _get_default_features(self, apn: str) -> Dict[str, Any]:
        """Fallback default features when APN not found."""
        logger.warning(f"Using default features for APN {apn}")
        
        return {
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

# Global instance for easy access
_csv_feature_matrix = None

def get_feature_matrix(apn: str) -> Dict[str, Any]:
    """Get feature matrix for a property from CSV data."""
    global _csv_feature_matrix
    
    if _csv_feature_matrix is None:
        _csv_feature_matrix = CSVFeatureMatrix()
    
    return _csv_feature_matrix.get_feature_matrix(apn)