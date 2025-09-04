import pandas as pd
import numpy as np
import json
import re
from typing import Union, Dict, Any
import warnings
warnings.filterwarnings('ignore')

class ZIMASDataCleaner:
    """Comprehensive data cleaning pipeline for ZIMAS property data"""
    
    def __init__(self):
        self.base_zoning_map = self.create_base_zoning_map()
        self.cleaning_stats = {}
        
    def create_base_zoning_map(self) -> Dict[str, str]:
        """Create mapping for base zoning categories"""
        return {
            'R1': 'Single Family Residential',
            'R2': 'Two Family Residential', 
            'R3': 'Multiple Family Residential',
            'R4': 'High Density Multiple Residential',
            'R5': 'Very High Density Multiple Residential',
            'C1': 'Limited Commercial',
            'C2': 'General Commercial',
            'C4': 'Highway Commercial',
            'CM': 'Commercial Manufacturing',
            'M1': 'Limited Industrial',
            'M2': 'Light Industrial',
            'M3': 'Heavy Industrial',
            'P': 'Parking',
            'PF': 'Public Facilities',
            'OS': 'Open Space',
            'A1': 'Agricultural',
            'A2': 'Agricultural',
            'RE': 'Residential Estate'
        }
    
    def clean_numeric_value(self, value: Union[str, float], field_type: str = 'general') -> float:
        """Extract numeric value from strings with units, commas, etc."""
        if pd.isna(value):
            return np.nan
            
        # Convert to string for processing
        value_str = str(value)
        
        # Remove common patterns
        value_str = value_str.replace('$', '')
        value_str = value_str.replace(',', '')
        value_str = value_str.replace('\r\n', '')
        value_str = value_str.replace('\n', '')
        
        # Extract numeric value based on field type
        if field_type == 'area':
            # Handle area values like "7,172.3 (sq ft)" or "0.16 (ac)"
            match = re.search(r'([\d,]+\.?\d*)', value_str)
            if match:
                numeric_val = float(match.group(1).replace(',', ''))
                # Convert acres to sq ft if needed
                if '(ac)' in value_str or 'acre' in value_str.lower():
                    numeric_val = numeric_val * 43560
                return numeric_val
        else:
            # General numeric extraction
            match = re.search(r'([\d,]+\.?\d*)', value_str)
            if match:
                return float(match.group(1).replace(',', ''))
        
        return np.nan
    
    def standardize_apn(self, apn: str) -> str:
        """Standardize APN format to XXXX-XXX-XXX"""
        if pd.isna(apn):
            return np.nan
            
        # Remove any existing formatting
        apn_clean = str(apn).replace('-', '').replace(' ', '').strip()
        
        # Remove leading zeros if present
        apn_clean = apn_clean.lstrip('0')
        
        # Standard LA County APN format is 10 digits: 4-3-3
        if len(apn_clean) >= 10:
            return f"{apn_clean[:4]}-{apn_clean[4:7]}-{apn_clean[7:10]}"
        
        return apn_clean
    
    def clean_address(self, address: str) -> str:
        """Standardize address formatting"""
        if pd.isna(address):
            return np.nan
            
        address = str(address).strip()
        
        # Remove \r\n characters
        address = address.replace('\r\n', ' ').replace('\n', ' ')
        
        # Standardize directional prefixes
        directions = {
            ' N ': ' NORTH ',
            ' S ': ' SOUTH ',
            ' E ': ' EAST ',
            ' W ': ' WEST ',
            ' NE ': ' NORTHEAST ',
            ' NW ': ' NORTHWEST ',
            ' SE ': ' SOUTHEAST ',
            ' SW ': ' SOUTHWEST '
        }
        
        for abbr, full in directions.items():
            if abbr in address:
                # Check if it's truly a direction (not part of a word)
                pattern = r'\b' + abbr.strip() + r'\b'
                address = re.sub(pattern, abbr.strip(), address, flags=re.IGNORECASE)
        
        # Standardize common street abbreviations
        abbreviations = {
            ' STREET': ' ST',
            ' AVENUE': ' AVE',
            ' ROAD': ' RD',
            ' BOULEVARD': ' BLVD',
            ' DRIVE': ' DR',
            ' PLACE': ' PL',
            ' LANE': ' LN',
            ' COURT': ' CT',
            ' CIRCLE': ' CIR',
            ' PARKWAY': ' PKWY'
        }
        
        for full, abbr in abbreviations.items():
            address = re.sub(full, abbr, address, flags=re.IGNORECASE)
        
        # Clean up multiple spaces
        address = ' '.join(address.split())
        
        return address.upper()
    
    def extract_base_zoning(self, zoning_code: str) -> str:
        """Extract base zoning from complex zoning codes"""
        if pd.isna(zoning_code):
            return np.nan
            
        zoning_code = str(zoning_code).upper().strip()
        
        # Remove \r\n characters
        zoning_code = zoning_code.replace('\r\n', '').replace('\n', '')
        
        # Extract base zoning (e.g., R3-1VL-O → R3)
        for base_code in self.base_zoning_map.keys():
            if zoning_code.startswith(base_code):
                return base_code
        
        # Try to find zoning pattern
        match = re.search(r'^([A-Z]+\d+)', zoning_code)
        if match:
            return match.group(1)
        
        return zoning_code
    
    def count_overlays(self, row: pd.Series) -> int:
        """Count number of overlay zones and special areas"""
        overlay_count = 0
        
        overlay_fields = [
            'divTab3_specific_plan_area',
            'divTab3_historic_preservation_overlay_zone',
            'divTab3_cdo_community_design_overlay', 
            'divTab3_cpio_community_plan_imp_overlay',
            'divTab3_nso_neighborhood_stabilization_overlay',
            'divTab3_rio_river_implementation_overlay',
            'divTab3_adaptive_reuse_incentive_area',
            'divTab3_opportunity_corridors_incentive_area',
            'specific_plan_area'
        ]
        
        for field in overlay_fields:
            if field in row and pd.notna(row[field]):
                value = str(row[field]).strip()
                if value and value.lower() not in ['none', 'not eligible', 'n/a', '']:
                    overlay_count += 1
        
        # Check for overlay suffix in zoning code
        if 'zoning_code' in row and pd.notna(row['zoning_code']):
            if '-O' in str(row['zoning_code']):
                overlay_count += 1
        
        return overlay_count
    
    def is_toc_eligible(self, value: str) -> bool:
        """Determine TOC eligibility as boolean"""
        if pd.isna(value):
            return False
            
        value_str = str(value).lower().strip()
        
        # Check for positive indicators
        if any(x in value_str for x in ['tier', 'eligible', 'yes']):
            if 'not eligible' not in value_str:
                return True
        
        return False
    
    def parse_json_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse nested JSON columns into flat structure"""
        parsed_records = []
        
        for idx, row in df.iterrows():
            record = {
                '_id': row.get('_id'),
                '_apn': row.get('_apn'),
                '_pin': row.get('_pin'),
                '_field_count': row.get('_field_count'),
                '_quality': row.get('_quality')
            }
            
            # Parse property_data JSON
            if 'property_data' in row and pd.notna(row['property_data']):
                try:
                    if isinstance(row['property_data'], str):
                        # Try json.loads first
                        try:
                            property_data = json.loads(row['property_data'].replace("'", '"'))
                        except:
                            # Fallback to eval
                            property_data = eval(row['property_data'])
                        record.update(property_data)
                    elif isinstance(row['property_data'], dict):
                        record.update(row['property_data'])
                except Exception as e:
                    print(f"Error parsing property_data for row {idx}: {e}")
            
            parsed_records.append(record)
        
        return pd.DataFrame(parsed_records)
    
    def clean_dataset(self, df: pd.DataFrame) -> pd.DataFrame:
        """Main cleaning function for the dataset"""
        print("Starting data cleaning pipeline...")
        print(f"Initial shape: {df.shape}")
        
        # Step 1: Parse JSON data
        print("\n1. Parsing nested JSON structures...")
        df_clean = self.parse_json_data(df)
        print(f"   Columns after parsing: {len(df_clean.columns)}")
        
        # Step 2: Clean APN
        print("\n2. Standardizing APN format...")
        if 'apn' in df_clean.columns:
            df_clean['clean_apn'] = df_clean['apn'].apply(self.standardize_apn)
        elif '_apn' in df_clean.columns:
            df_clean['clean_apn'] = df_clean['_apn'].apply(self.standardize_apn)
        
        # Step 3: Clean lot size
        print("\n3. Extracting numeric lot sizes...")
        lot_size_fields = ['lot_parcel_area', 'divTab1_lotparcel_area', 'divTab4_apn_area']
        for field in lot_size_fields:
            if field in df_clean.columns:
                df_clean['lot_size_sqft'] = df_clean[field].apply(
                    lambda x: self.clean_numeric_value(x, 'area')
                )
                break
        
        # Step 4: Clean building size
        print("\n4. Extracting numeric building sizes...")
        building_fields = ['building_square_footage', 'divTab4_building_square_footage']
        for field in building_fields:
            if field in df_clean.columns:
                df_clean['building_size_sqft'] = df_clean[field].apply(
                    lambda x: self.clean_numeric_value(x, 'area')
                )
                break
        
        # Step 5: Clean assessed values
        print("\n5. Cleaning currency fields...")
        value_fields = ['divTab4_assessed_land_val', 'divTab4_assessed_improvement_val']
        for field in value_fields:
            if field in df_clean.columns:
                df_clean[f'{field}_numeric'] = df_clean[field].apply(
                    lambda x: self.clean_numeric_value(x, 'currency')
                )
        
        # Calculate total assessed value
        if 'divTab4_assessed_land_val_numeric' in df_clean.columns and \
           'divTab4_assessed_improvement_val_numeric' in df_clean.columns:
            df_clean['assessed_value_numeric'] = (
                df_clean['divTab4_assessed_land_val_numeric'].fillna(0) +
                df_clean['divTab4_assessed_improvement_val_numeric'].fillna(0)
            )
        
        # Step 6: Clean addresses
        print("\n6. Standardizing addresses...")
        address_fields = ['site_address', 'divTab1_site_address', 'divTab10_address']
        for field in address_fields:
            if field in df_clean.columns:
                df_clean['clean_address'] = df_clean[field].apply(self.clean_address)
                break
        
        # Step 7: Extract base zoning
        print("\n7. Extracting base zoning categories...")
        zoning_fields = ['zoning_code', 'divTab3_zoning']
        for field in zoning_fields:
            if field in df_clean.columns:
                df_clean['base_zoning'] = df_clean[field].apply(self.extract_base_zoning)
                df_clean['base_zoning_description'] = df_clean['base_zoning'].map(self.base_zoning_map)
                break
        
        # Step 8: Count overlays
        print("\n8. Counting overlay zones...")
        df_clean['overlay_count'] = df_clean.apply(self.count_overlays, axis=1)
        
        # Step 9: TOC eligibility
        print("\n9. Determining TOC eligibility...")
        if 'divTab3_transit_oriented_communities' in df_clean.columns:
            df_clean['toc_eligible'] = df_clean['divTab3_transit_oriented_communities'].apply(
                self.is_toc_eligible
            )
        
        # Step 10: Clean other text fields (remove \r\n)
        print("\n10. Cleaning text fields...")
        text_columns = df_clean.select_dtypes(include=['object']).columns
        for col in text_columns:
            df_clean[col] = df_clean[col].apply(
                lambda x: str(x).replace('\r\n', ' ').replace('\n', ' ').strip() 
                if pd.notna(x) else x
            )
        
        # Step 11: Extract year built as numeric
        year_fields = ['building_year_built', 'divTab4_year_built', 'divTab10_year_built']
        for field in year_fields:
            if field in df_clean.columns:
                df_clean['year_built_numeric'] = pd.to_numeric(
                    df_clean[field], errors='coerce'
                )
                break
        
        # Step 12: Extract number of units as numeric
        unit_fields = ['divTab4_number_of_units', 'number_of_units']
        for field in unit_fields:
            if field in df_clean.columns:
                df_clean['units_numeric'] = pd.to_numeric(
                    df_clean[field], errors='coerce'
                )
                break
        
        print("\n✓ Data cleaning complete!")
        return df_clean
    
    def create_dealgenie_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map cleaned data to DealGenie target schema"""
        print("\nMapping to DealGenie schema...")
        
        # Define the target schema mapping
        schema_mapping = {
            # Core identifiers
            'property_id': '_id',
            'assessor_parcel_id': 'clean_apn',
            'pin': 'pin_number',
            
            # Location
            'site_address': 'clean_address',
            'zip_code': 'zip_code',
            'council_district': 'council_district',
            'community_plan_area': 'divTab2_community_plan_area',
            'neighborhood_council': 'divTab2_neighborhood_council',
            
            # Zoning & Planning
            'base_zoning': 'base_zoning',
            'base_zoning_description': 'base_zoning_description',
            'full_zoning_code': 'zoning_code',
            'general_plan_land_use': 'general_plan_land_use',
            'specific_plan_area': 'divTab3_specific_plan_area',
            'overlay_count': 'overlay_count',
            
            # Property characteristics
            'lot_size_sqft': 'lot_size_sqft',
            'building_size_sqft': 'building_size_sqft',
            'year_built': 'year_built_numeric',
            'number_of_units': 'units_numeric',
            'use_code': 'divTab4_use_code',
            
            # Valuation
            'assessed_land_value': 'divTab4_assessed_land_val_numeric',
            'assessed_improvement_value': 'divTab4_assessed_improvement_val_numeric',
            'total_assessed_value': 'assessed_value_numeric',
            
            # Development indicators
            'toc_eligible': 'toc_eligible',
            'opportunity_zone': 'divTab3_opportunity_corridors_incentive_area',
            'high_quality_transit': 'divTab3_high_quality_transit_corridor',
            'residential_market_area': 'divTab3_residential_market_area',
            'commercial_market_area': 'divTab3_nonresidential_market_area',
            
            # Environmental
            'methane_zone': 'divTab7_methane_hazard_site',
            'flood_zone': 'divTab7_flood_zone',
            'fault_zone': 'divTab8_alquistpriolo_fault_zone',
            
            # Housing regulations
            'rent_stabilization': 'divTab4_rent_stabilization_ordinance',
            'housing_replacement_required': 'divTab10_he_replacement_required'
        }
        
        # Create new dataframe with DealGenie schema
        dealgenie_df = pd.DataFrame()
        
        for target_col, source_col in schema_mapping.items():
            if source_col in df.columns:
                dealgenie_df[target_col] = df[source_col]
            else:
                # Try alternate field names
                alt_source = source_col.replace('divTab', 'div_tab')
                if alt_source in df.columns:
                    dealgenie_df[target_col] = df[alt_source]
                else:
                    dealgenie_df[target_col] = np.nan
        
        # Add data quality score
        dealgenie_df['data_completeness_score'] = (
            dealgenie_df.notna().sum(axis=1) / len(dealgenie_df.columns) * 100
        ).round(1)
        
        print(f"✓ Mapped to {len(dealgenie_df.columns)} DealGenie fields")
        return dealgenie_df
    
    def show_transformations(self, original_df: pd.DataFrame, cleaned_df: pd.DataFrame, n_examples: int = 5):
        """Show before/after examples of cleaning transformations"""
        print("\n" + "="*100)
        print("DATA TRANSFORMATION EXAMPLES")
        print("="*100)
        
        # Parse original data for comparison
        original_parsed = self.parse_json_data(original_df.head(n_examples))
        cleaned_sample = cleaned_df.head(n_examples)
        
        transformations = [
            ('APN Standardization', 'apn', 'clean_apn'),
            ('Lot Size Extraction', 'lot_parcel_area', 'lot_size_sqft'),
            ('Address Cleaning', 'site_address', 'clean_address'),
            ('Base Zoning', 'zoning_code', 'base_zoning'),
            ('Assessed Value', 'divTab4_assessed_land_val', 'divTab4_assessed_land_val_numeric')
        ]
        
        for trans_name, orig_field, clean_field in transformations:
            print(f"\n{trans_name}:")
            print("-" * 50)
            
            for i in range(min(3, n_examples)):
                if orig_field in original_parsed.columns and clean_field in cleaned_sample.columns:
                    orig_val = original_parsed.iloc[i].get(orig_field, 'N/A')
                    clean_val = cleaned_sample.iloc[i].get(clean_field, 'N/A')
                    print(f"  Before: {orig_val}")
                    print(f"  After:  {clean_val}")
                    print()


# Main execution
if __name__ == "__main__":
    print("="*100)
    print("ZIMAS DATA CLEANING PIPELINE")
    print("="*100)
    
    # Initialize cleaner
    cleaner = ZIMASDataCleaner()
    
    # Load the data
    print("\nLoading data...")
    df_original = pd.read_csv('zimas_property_sample_1000.csv')
    print(f"Loaded {len(df_original)} records with {len(df_original.columns)} columns")
    
    # Clean the data
    df_cleaned = cleaner.clean_dataset(df_original)
    
    # Create DealGenie schema
    df_dealgenie = cleaner.create_dealgenie_schema(df_cleaned)
    
    # Show transformations
    cleaner.show_transformations(df_original, df_cleaned)
    
    # Export cleaned data
    output_file = 'clean_zimas_ready_for_scoring.csv'
    df_dealgenie.to_csv(output_file, index=False)
    print(f"\n✓ Cleaned data exported to '{output_file}'")
    print(f"  Records: {len(df_dealgenie)}")
    print(f"  Columns: {len(df_dealgenie.columns)}")
    
    # Show data quality summary
    print("\n" + "="*100)
    print("DATA QUALITY SUMMARY")
    print("="*100)
    
    print("\nField Completeness:")
    completeness = (df_dealgenie.notna().sum() / len(df_dealgenie) * 100).sort_values(ascending=False)
    
    print("\nHigh Quality Fields (>90% complete):")
    high_quality = completeness[completeness > 90]
    for field, pct in high_quality.head(10).items():
        print(f"  {field}: {pct:.1f}%")
    
    print("\nCritical Development Fields:")
    critical_fields = [
        'assessor_parcel_id', 'site_address', 'base_zoning', 
        'lot_size_sqft', 'general_plan_land_use', 'overlay_count'
    ]
    for field in critical_fields:
        if field in completeness:
            print(f"  {field}: {completeness[field]:.1f}%")
    
    print("\n✓ Pipeline complete! Data is ready for DealGenie scoring.")
    
    
# Create a reusable function for processing full dataset
def process_full_dataset(input_db_path: str, output_csv_path: str, batch_size: int = 10000):
    """
    Process the full ZIMAS database in batches
    
    Args:
        input_db_path: Path to SQLite database
        output_csv_path: Path for output CSV file
        batch_size: Number of records to process at once
    """
    import sqlite3
    
    print(f"Processing full dataset from {input_db_path}")
    
    # Connect to database
    conn = sqlite3.connect(input_db_path)
    
    # Get total record count
    total_records = pd.read_sql_query(
        "SELECT COUNT(*) as count FROM comprehensive_property_data WHERE extracted_fields_json IS NOT NULL",
        conn
    )['count'][0]
    
    print(f"Total records to process: {total_records}")
    
    # Initialize cleaner
    cleaner = ZIMASDataCleaner()
    
    # Process in batches
    all_cleaned_data = []
    offset = 0
    
    while offset < total_records:
        print(f"\nProcessing batch: {offset} to {offset + batch_size}")
        
        # Load batch
        query = f"""
            SELECT id, apn, pin, extracted_fields_json, field_count, data_quality
            FROM comprehensive_property_data
            WHERE extracted_fields_json IS NOT NULL
            LIMIT {batch_size} OFFSET {offset}
        """
        
        batch_df = pd.read_sql_query(query, conn)
        
        # Create proper structure for cleaning
        batch_records = []
        for _, row in batch_df.iterrows():
            record = {
                '_id': row['id'],
                '_apn': row['apn'],
                '_pin': row['pin'],
                '_field_count': row['field_count'],
                '_quality': row['data_quality']
            }
            
            # Parse JSON
            if row['extracted_fields_json']:
                try:
                    data = json.loads(row['extracted_fields_json'])
                    if 'property_data' in data:
                        record['property_data'] = data['property_data']
                except:
                    pass
            
            batch_records.append(record)
        
        batch_input = pd.DataFrame(batch_records)
        
        # Clean batch
        batch_cleaned = cleaner.clean_dataset(batch_input)
        batch_dealgenie = cleaner.create_dealgenie_schema(batch_cleaned)
        
        all_cleaned_data.append(batch_dealgenie)
        
        offset += batch_size
        
        # Save progress periodically
        if len(all_cleaned_data) % 5 == 0:
            temp_df = pd.concat(all_cleaned_data, ignore_index=True)
            temp_df.to_csv(f"{output_csv_path}.tmp", index=False)
            print(f"  Saved temporary progress ({len(temp_df)} records)")
    
    # Combine all batches
    final_df = pd.concat(all_cleaned_data, ignore_index=True)
    
    # Save final output
    final_df.to_csv(output_csv_path, index=False)
    
    conn.close()
    
    print(f"\n✓ Full dataset processed!")
    print(f"  Total records: {len(final_df)}")
    print(f"  Output saved to: {output_csv_path}")
    
    return final_df