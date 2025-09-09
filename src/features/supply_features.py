#!/usr/bin/env python3
"""
Supply Features Engineering for LA Property Intelligence
Computes permit pipeline and velocity features using clustering results

Features:
- 12-month permit pipeline within 0.5/1/2 mile buffers using Haversine distance
- Permit velocity trends using project_clusters data (3/6/12 month windows)
- Market velocity proxy using clustered projects and units data
- Asset type velocities using LA permit_type classifications
- Multi-project development handling via existing clustering results
- Block group and parcel level feature outputs
- Integration with governance metadata structure
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import math
import json
from typing import Dict, List, Tuple, Optional, Union
from dataclasses import dataclass
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class SupplyFeatureConfig:
    """Configuration for supply feature engineering"""
    buffer_distances_miles: List[float] = None  # 0.5, 1.0, 2.0 mile buffers
    velocity_windows_months: List[int] = None   # 3, 6, 12 month windows
    reference_date: str = None                  # Analysis reference date
    min_project_value: float = 10000.0         # Minimum project value to include
    include_noise_permits: bool = False         # Include unclustered permits (cluster_id = -1)
    
    def __post_init__(self):
        if self.buffer_distances_miles is None:
            self.buffer_distances_miles = [1.0, 2.0, 3.0, 5.0]  # Larger buffers for LA sprawl
        if self.velocity_windows_months is None:
            self.velocity_windows_months = [3, 6, 12]
        if self.reference_date is None:
            self.reference_date = datetime.now().strftime('%Y-%m-%d')

class SupplyFeaturesEngine:
    """Main engine for computing supply-side features from permit data"""
    
    def __init__(self, db_path: str = "./data/dealgenie.db"):
        self.db_path = db_path
        self.config = SupplyFeatureConfig()
        self.reference_date = pd.to_datetime(self.config.reference_date)
        
        # Miles to meters conversion
        self.MILES_TO_METERS = 1609.344
        
        # LA-specific permit type classifications (corrected to avoid overly broad matching)
        self.LA_ASSET_TYPES = {
            'Residential': [
                '1 OR 2 FAMILY DWELLING', 'APARTMENT', 'SINGLE FAMILY', 'MULTI-FAMILY', 
                'RESIDENTIAL', 'DWELLING', 'FAMILY', 'ADU', 'ACCESSORY DWELLING',
                'SWIMMING-POOL', 'POOL', 'SPA'
            ],
            'Commercial': [
                'COMMERCIAL', 'OFFICE', 'RETAIL', 'RESTAURANT', 'HOTEL', 'MIXED USE',
                'BUSINESS', 'STORE', 'SHOP', 'MEDICAL', 'CLINIC', 'TENANT IMPROVEMENT'
            ],
            'Industrial': [
                'INDUSTRIAL', 'WAREHOUSE', 'MANUFACTURING', 'STORAGE', 'FACTORY',
                'DISTRIBUTION', 'LOGISTICS'
            ],
            'Infrastructure': [
                'UTILITY', 'PUBLIC WORKS', 'TRANSPORTATION', 'COMMUNICATION',
                'GRADING', 'NONBLDG'
            ],
            'Other': [
                'DEMOLITION', 'SIGN', 'TEMPORARY', 'SPECIAL EVENT'
            ]
        }
    
    def haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate Haversine distance between two points in meters"""
        # Convert decimal degrees to radians
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        
        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        
        # Radius of earth in meters
        r = 6371000
        return c * r
    
    def load_permits_data(self) -> pd.DataFrame:
        """Load permits data with clustering assignments from database"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            permit_id,
            application_date,
            issue_date,
            status,
            permit_type,
            permit_subtype,
            work_description,
            address,
            apn,
            council_district,
            units_proposed,
            units_net_change,
            estimated_cost,
            latitude,
            longitude,
            cluster_id,
            cluster_run_id,
            scraped_at
        FROM raw_permits
        WHERE latitude IS NOT NULL 
          AND longitude IS NOT NULL
          AND estimated_cost >= ?
        ORDER BY application_date DESC
        """
        
        df = pd.read_sql_query(query, conn, params=[self.config.min_project_value])
        conn.close()
        
        # Convert date columns
        for date_col in ['application_date', 'issue_date', 'scraped_at']:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        
        # Fill missing dates and ensure we have usable dates
        df['effective_date'] = df['application_date'].fillna(df['issue_date'])
        df = df[df['effective_date'].notna()]
        
        logger.info(f"Loaded {len(df)} permits for supply feature analysis")
        return df
    
    def load_project_clusters(self) -> pd.DataFrame:
        """Load project cluster aggregations from database"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            project_cluster_id,
            run_id,
            permits_count,
            duplicates_count,
            centroid_latitude,
            centroid_longitude,
            spatial_extent_meters,
            earliest_permit_date,
            latest_permit_date,
            project_duration_days,
            total_estimated_cost,
            weighted_avg_cost,
            max_estimated_cost,
            total_units_proposed,
            total_units_net_change,
            council_districts,
            unique_apns,
            permit_types,
            primary_corridor,
            has_assembly_opportunity,
            is_megaproject,
            avg_permit_weight,
            created_at
        FROM project_clusters
        ORDER BY total_estimated_cost DESC
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Convert date columns
        for date_col in ['earliest_permit_date', 'latest_permit_date', 'created_at']:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        
        # Parse JSON fields
        json_fields = ['council_districts', 'unique_apns', 'permit_types']
        for field in json_fields:
            df[field] = df[field].apply(lambda x: json.loads(x) if pd.notna(x) and x.strip() else [])
        
        logger.info(f"Loaded {len(df)} project clusters for analysis")
        return df
    
    def classify_asset_type(self, permit_type: str, permit_subtype: str = None, 
                           work_description: str = None) -> str:
        """Classify permit into asset type category with improved priority logic"""
        
        # Prioritize permit_subtype as it's usually more specific
        subtype_text = str(permit_subtype).upper() if pd.notna(permit_subtype) else ''
        type_text = str(permit_type).upper() if pd.notna(permit_type) else ''
        desc_text = str(work_description).upper() if pd.notna(work_description) else ''
        
        # First check permit_subtype for clear commercial indicators
        if any(keyword in subtype_text for keyword in ['COMMERCIAL', 'OFFICE', 'RETAIL']):
            return 'Commercial'
        
        # Then check for clear residential indicators in subtype
        if any(keyword in subtype_text for keyword in ['FAMILY DWELLING', 'APARTMENT', 'RESIDENTIAL']):
            return 'Residential'
        
        # For less clear cases, check all text combined
        full_text = f'{type_text} {subtype_text} {desc_text}'
        
        # Check each asset type category (excluding Other to ensure it's last resort)
        for asset_type, keywords in self.LA_ASSET_TYPES.items():
            if asset_type == 'Other':
                continue
            for keyword in keywords:
                if keyword.upper() in full_text:
                    return asset_type
        
        return 'Other'
    
    def extract_units_from_description(self, work_description: str, 
                                     units_proposed: Optional[int] = None) -> int:
        """Extract unit count from work description using pattern matching"""
        if pd.notna(units_proposed) and units_proposed > 0:
            return int(units_proposed)
        
        if pd.isna(work_description):
            return 0
        
        # Common patterns in LA permit descriptions
        import re
        desc = str(work_description).upper()
        
        # Pattern matching for unit counts
        patterns = [
            r'(\d+)\s*UNIT[S]?',
            r'(\d+)\s*DWELLING[S]?',
            r'(\d+)\s*APARTMENT[S]?',
            r'(\d+)\s*CONDO[S]?',
            r'(\d+)\s*HOME[S]?',
            r'(\d+)\s*STORY\s+(\d+)\s*UNIT',  # Multi-story buildings
            r'BUILDING\s+WITH\s+(\d+)',
            r'(\d+)\s*FAMILY'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, desc)
            if match:
                # For multi-group patterns, take the last number (usually unit count)
                numbers = [int(g) for g in match.groups() if g.isdigit()]
                if numbers:
                    return max(numbers)
        
        # Default: assume 1 unit for residential projects, 0 for others
        if any(kw in desc for kw in ['DWELLING', 'RESIDENTIAL', 'HOUSE', 'APARTMENT']):
            return 1
        
        return 0
    
    def compute_permit_pipeline_features(self, target_df: pd.DataFrame, 
                                       permits_df: pd.DataFrame) -> pd.DataFrame:
        """Compute 12-month permit pipeline within distance buffers"""
        
        logger.info("Computing permit pipeline features for distance buffers...")
        
        # Prepare results dataframe
        results = target_df.copy()
        
        # Date filters for pipeline analysis
        pipeline_start = self.reference_date - timedelta(days=365)  # 12 months back
        future_end = self.reference_date + timedelta(days=365)      # 12 months forward
        
        # Filter permits to relevant timeframe
        relevant_permits = permits_df[
            (permits_df['effective_date'] >= pipeline_start) & 
            (permits_df['effective_date'] <= future_end)
        ].copy()
        
        # Add asset type classification
        relevant_permits['asset_type'] = relevant_permits.apply(
            lambda row: self.classify_asset_type(row['permit_type'], row['permit_subtype'], 
                                               row['work_description']), axis=1
        )
        
        # Add extracted unit counts
        relevant_permits['extracted_units'] = relevant_permits.apply(
            lambda row: self.extract_units_from_description(row['work_description'], 
                                                          row['units_proposed']), axis=1
        )
        
        # Initialize feature columns for each buffer distance
        for distance_miles in self.config.buffer_distances_miles:
            distance_m = distance_miles * self.MILES_TO_METERS
            prefix = f"pipeline_{distance_miles}mi"
            
            # Initialize columns
            results[f"{prefix}_total_permits"] = 0
            results[f"{prefix}_total_value"] = 0.0
            results[f"{prefix}_total_units"] = 0
            results[f"{prefix}_avg_project_value"] = 0.0
            
            # Asset type specific columns
            for asset_type in self.LA_ASSET_TYPES.keys():
                results[f"{prefix}_{asset_type.lower()}_permits"] = 0
                results[f"{prefix}_{asset_type.lower()}_value"] = 0.0
                results[f"{prefix}_{asset_type.lower()}_units"] = 0
            
            # Time-based pipeline columns
            results[f"{prefix}_permits_last_6m"] = 0
            results[f"{prefix}_permits_next_6m"] = 0
            results[f"{prefix}_permits_last_12m"] = 0
        
        # Compute features for each target location
        for idx, target_row in results.iterrows():
            if idx % 100 == 0:
                logger.info(f"Processing location {idx}/{len(results)}")
            
            target_lat = target_row['latitude']
            target_lon = target_row['longitude']
            
            if pd.isna(target_lat) or pd.isna(target_lon):
                continue
            
            # Calculate distances to all permits
            distances = relevant_permits.apply(
                lambda p: self.haversine_distance(target_lat, target_lon, p['latitude'], p['longitude']),
                axis=1
            )
            
            # Process each buffer distance
            for distance_miles in self.config.buffer_distances_miles:
                distance_m = distance_miles * self.MILES_TO_METERS
                prefix = f"pipeline_{distance_miles}mi"
                
                # Filter permits within buffer
                within_buffer = relevant_permits[distances <= distance_m].copy()
                
                if len(within_buffer) == 0:
                    continue
                
                # Basic aggregations
                results.loc[idx, f"{prefix}_total_permits"] = len(within_buffer)
                results.loc[idx, f"{prefix}_total_value"] = within_buffer['estimated_cost'].sum()
                results.loc[idx, f"{prefix}_total_units"] = within_buffer['extracted_units'].sum()
                results.loc[idx, f"{prefix}_avg_project_value"] = within_buffer['estimated_cost'].mean()
                
                # Asset type aggregations
                for asset_type in self.LA_ASSET_TYPES.keys():
                    asset_permits = within_buffer[within_buffer['asset_type'] == asset_type]
                    if len(asset_permits) > 0:
                        results.loc[idx, f"{prefix}_{asset_type.lower()}_permits"] = len(asset_permits)
                        results.loc[idx, f"{prefix}_{asset_type.lower()}_value"] = asset_permits['estimated_cost'].sum()
                        results.loc[idx, f"{prefix}_{asset_type.lower()}_units"] = asset_permits['extracted_units'].sum()
                
                # Time-based aggregations
                six_months_ago = self.reference_date - timedelta(days=180)
                six_months_ahead = self.reference_date + timedelta(days=180)
                twelve_months_ago = self.reference_date - timedelta(days=365)
                
                last_6m = within_buffer[
                    (within_buffer['effective_date'] >= six_months_ago) & 
                    (within_buffer['effective_date'] <= self.reference_date)
                ]
                next_6m = within_buffer[
                    (within_buffer['effective_date'] >= self.reference_date) & 
                    (within_buffer['effective_date'] <= six_months_ahead)
                ]
                last_12m = within_buffer[
                    (within_buffer['effective_date'] >= twelve_months_ago) & 
                    (within_buffer['effective_date'] <= self.reference_date)
                ]
                
                results.loc[idx, f"{prefix}_permits_last_6m"] = len(last_6m)
                results.loc[idx, f"{prefix}_permits_next_6m"] = len(next_6m)
                results.loc[idx, f"{prefix}_permits_last_12m"] = len(last_12m)
        
        logger.info("Completed permit pipeline feature computation")
        return results
    
    def compute_velocity_trends(self, target_df: pd.DataFrame, 
                              clusters_df: pd.DataFrame) -> pd.DataFrame:
        """Compute permit velocity trends using project_clusters data"""
        
        logger.info("Computing velocity trends from project clusters...")
        
        results = target_df.copy()
        
        # Initialize velocity trend columns
        for window_months in self.config.velocity_windows_months:
            for distance_miles in self.config.buffer_distances_miles:
                prefix = f"velocity_{window_months}m_{distance_miles}mi"
                results[f"{prefix}_projects_started"] = 0
                results[f"{prefix}_projects_completed"] = 0
                results[f"{prefix}_total_value"] = 0.0
                results[f"{prefix}_total_units"] = 0
                results[f"{prefix}_avg_duration"] = 0.0
                results[f"{prefix}_megaprojects"] = 0
                results[f"{prefix}_assembly_opportunities"] = 0
        
        # Process each target location
        for idx, target_row in results.iterrows():
            if idx % 100 == 0:
                logger.info(f"Processing velocity for location {idx}/{len(results)}")
            
            target_lat = target_row['latitude'] 
            target_lon = target_row['longitude']
            
            if pd.isna(target_lat) or pd.isna(target_lon):
                continue
            
            # Calculate distances to all project clusters
            distances = clusters_df.apply(
                lambda c: self.haversine_distance(target_lat, target_lon, 
                                                c['centroid_latitude'], c['centroid_longitude']),
                axis=1
            )
            
            # Process each time window and distance buffer
            for window_months in self.config.velocity_windows_months:
                window_start = self.reference_date - timedelta(days=window_months * 30)
                
                for distance_miles in self.config.buffer_distances_miles:
                    distance_m = distance_miles * self.MILES_TO_METERS
                    prefix = f"velocity_{window_months}m_{distance_miles}mi"
                    
                    # Filter clusters within buffer and time window
                    within_buffer = clusters_df[distances <= distance_m].copy()
                    
                    if len(within_buffer) == 0:
                        continue
                    
                    # Projects started in window (based on earliest_permit_date)
                    started_in_window = within_buffer[
                        (within_buffer['earliest_permit_date'] >= window_start) &
                        (within_buffer['earliest_permit_date'] <= self.reference_date)
                    ]
                    
                    # Projects completed in window (based on latest_permit_date + estimated completion)
                    # Assume projects are "completed" if latest permit date + duration is in the past
                    completed_in_window = within_buffer[
                        (within_buffer['latest_permit_date'].notna()) &
                        ((within_buffer['latest_permit_date'] + 
                          pd.to_timedelta(within_buffer['project_duration_days'], unit='D')) <= self.reference_date) &
                        ((within_buffer['latest_permit_date'] + 
                          pd.to_timedelta(within_buffer['project_duration_days'], unit='D')) >= window_start)
                    ]
                    
                    # Aggregate velocity metrics
                    results.loc[idx, f"{prefix}_projects_started"] = len(started_in_window)
                    results.loc[idx, f"{prefix}_projects_completed"] = len(completed_in_window)
                    
                    if len(started_in_window) > 0:
                        results.loc[idx, f"{prefix}_total_value"] = started_in_window['total_estimated_cost'].sum()
                        results.loc[idx, f"{prefix}_total_units"] = started_in_window['total_units_proposed'].sum()
                        results.loc[idx, f"{prefix}_avg_duration"] = started_in_window['project_duration_days'].mean()
                        results.loc[idx, f"{prefix}_megaprojects"] = started_in_window['is_megaproject'].sum()
                        results.loc[idx, f"{prefix}_assembly_opportunities"] = started_in_window['has_assembly_opportunity'].sum()
        
        logger.info("Completed velocity trend computation")
        return results
    
    def compute_market_velocity_proxy(self, target_df: pd.DataFrame, 
                                    permits_df: pd.DataFrame, 
                                    clusters_df: pd.DataFrame) -> pd.DataFrame:
        """Generate market velocity proxy using clustered projects and units data"""
        
        logger.info("Computing market velocity proxy...")
        
        results = target_df.copy()
        
        # Initialize market velocity columns
        for distance_miles in self.config.buffer_distances_miles:
            prefix = f"market_velocity_{distance_miles}mi"
            results[f"{prefix}_permits_per_month"] = 0.0
            results[f"{prefix}_value_per_month"] = 0.0
            results[f"{prefix}_units_per_month"] = 0.0
            results[f"{prefix}_projects_per_month"] = 0.0
            results[f"{prefix}_avg_project_size"] = 0.0
            results[f"{prefix}_development_intensity"] = 0.0  # Projects per square mile
            results[f"{prefix}_market_momentum"] = 0.0       # Accelerating vs decelerating
        
        # Calculate 12-month rolling velocity for each location
        for idx, target_row in results.iterrows():
            if idx % 100 == 0:
                logger.info(f"Processing market velocity for location {idx}/{len(results)}")
            
            target_lat = target_row['latitude']
            target_lon = target_row['longitude']
            
            if pd.isna(target_lat) or pd.isna(target_lon):
                continue
            
            # Get permits and clusters within buffers
            permit_distances = permits_df.apply(
                lambda p: self.haversine_distance(target_lat, target_lon, p['latitude'], p['longitude']),
                axis=1
            )
            
            cluster_distances = clusters_df.apply(
                lambda c: self.haversine_distance(target_lat, target_lon, 
                                                c['centroid_latitude'], c['centroid_longitude']),
                axis=1
            )
            
            for distance_miles in self.config.buffer_distances_miles:
                distance_m = distance_miles * self.MILES_TO_METERS
                prefix = f"market_velocity_{distance_miles}mi"
                
                # Get permits and clusters within buffer
                permits_in_buffer = permits_df[permit_distances <= distance_m].copy()
                clusters_in_buffer = clusters_df[cluster_distances <= distance_m].copy()
                
                if len(permits_in_buffer) == 0:
                    continue
                
                # Calculate 12-month velocity metrics
                twelve_months_ago = self.reference_date - timedelta(days=365)
                recent_permits = permits_in_buffer[
                    permits_in_buffer['effective_date'] >= twelve_months_ago
                ]
                recent_clusters = clusters_in_buffer[
                    clusters_in_buffer['earliest_permit_date'] >= twelve_months_ago
                ]
                
                # Monthly averages
                months_in_period = 12
                if len(recent_permits) > 0:
                    results.loc[idx, f"{prefix}_permits_per_month"] = len(recent_permits) / months_in_period
                    results.loc[idx, f"{prefix}_value_per_month"] = recent_permits['estimated_cost'].sum() / months_in_period
                    
                    # Extract units from recent permits
                    recent_permits_with_units = recent_permits.copy()
                    recent_permits_with_units['extracted_units'] = recent_permits_with_units.apply(
                        lambda row: self.extract_units_from_description(row['work_description'], 
                                                                      row['units_proposed']), axis=1
                    )
                    results.loc[idx, f"{prefix}_units_per_month"] = recent_permits_with_units['extracted_units'].sum() / months_in_period
                
                if len(recent_clusters) > 0:
                    results.loc[idx, f"{prefix}_projects_per_month"] = len(recent_clusters) / months_in_period
                    results.loc[idx, f"{prefix}_avg_project_size"] = recent_clusters['permits_count'].mean()
                    
                    # Development intensity (projects per square mile)
                    buffer_area_sq_miles = math.pi * (distance_miles ** 2)
                    results.loc[idx, f"{prefix}_development_intensity"] = len(recent_clusters) / buffer_area_sq_miles
                
                # Market momentum (6-month trend comparison)
                six_months_ago = self.reference_date - timedelta(days=180)
                recent_6m = recent_permits[recent_permits['effective_date'] >= six_months_ago]
                earlier_6m = recent_permits[
                    (recent_permits['effective_date'] >= twelve_months_ago) &
                    (recent_permits['effective_date'] < six_months_ago)
                ]
                
                if len(earlier_6m) > 0 and len(recent_6m) > 0:
                    recent_rate = len(recent_6m) / 6
                    earlier_rate = len(earlier_6m) / 6
                    momentum = (recent_rate - earlier_rate) / earlier_rate if earlier_rate > 0 else 0
                    results.loc[idx, f"{prefix}_market_momentum"] = momentum
        
        logger.info("Completed market velocity proxy computation")
        return results
    
    def generate_block_group_features(self, permits_df: pd.DataFrame, 
                                    clusters_df: pd.DataFrame) -> pd.DataFrame:
        """Generate supply features aggregated at council district level (proxy for block groups)"""
        
        logger.info("Generating council district level supply features...")
        
        # Use council districts as geographic aggregation units since we don't have block groups
        council_districts = permits_df[
            permits_df['council_district'].notna()
        ].groupby('council_district').agg({
            'latitude': 'mean',
            'longitude': 'mean',
            'apn': 'count'  # Count as proxy for activity level
        }).reset_index()
        council_districts.rename(columns={'apn': 'parcels_in_district'}, inplace=True)
        
        if len(council_districts) == 0:
            logger.warning("No council district data available")
            return pd.DataFrame()
        
        # Compute features for council district centroids
        logger.info(f"Computing features for {len(council_districts)} council districts")
        
        # Add permit pipeline features
        council_districts = self.compute_permit_pipeline_features(council_districts, permits_df)
        
        # Add velocity trends
        council_districts = self.compute_velocity_trends(council_districts, clusters_df)
        
        # Add market velocity proxy
        council_districts = self.compute_market_velocity_proxy(council_districts, permits_df, clusters_df)
        
        # Add council district specific aggregations
        district_stats = permits_df[permits_df['council_district'].notna()].groupby('council_district').agg({
            'permit_id': 'count',
            'estimated_cost': ['sum', 'mean', 'std'],
            'units_proposed': ['sum', 'mean'],
            'cluster_id': 'nunique'
        }).reset_index()
        
        # Flatten column names
        district_stats.columns = ['council_district', 'total_permits_in_district', 
                                 'total_value_in_district', 'avg_value_in_district', 'std_value_in_district',
                                 'total_units_in_district', 'avg_units_in_district', 'unique_clusters_in_district']
        
        # Merge with computed features
        council_districts = council_districts.merge(district_stats, on='council_district', how='left')
        
        logger.info("Completed council district feature generation")
        return council_districts
    
    def generate_parcel_features(self, permits_df: pd.DataFrame, 
                               clusters_df: pd.DataFrame) -> pd.DataFrame:
        """Generate supply features at parcel (APN) level"""
        
        logger.info("Generating parcel level supply features...")
        
        # Get unique APNs with coordinates and available metadata
        parcels = permits_df.groupby('apn').agg({
            'latitude': 'mean',
            'longitude': 'mean', 
            'council_district': 'first'
        }).reset_index()
        
        logger.info(f"Computing features for {len(parcels)} parcels")
        
        # Compute features for parcel centroids
        # Use appropriate buffer distances for parcel-level analysis in LA
        original_buffers = self.config.buffer_distances_miles
        self.config.buffer_distances_miles = [0.5, 1.0, 2.0, 3.0]  # Appropriate buffers for LA parcels
        
        try:
            # Add permit pipeline features
            parcels = self.compute_permit_pipeline_features(parcels, permits_df)
            
            # Add velocity trends
            parcels = self.compute_velocity_trends(parcels, clusters_df)
            
            # Add market velocity proxy
            parcels = self.compute_market_velocity_proxy(parcels, permits_df, clusters_df)
            
        finally:
            # Restore original buffer distances
            self.config.buffer_distances_miles = original_buffers
        
        # Add parcel-specific aggregations
        parcel_stats = permits_df.groupby('apn').agg({
            'permit_id': 'count',
            'estimated_cost': ['sum', 'mean', 'max'],
            'units_proposed': ['sum', 'max'],
            'cluster_id': 'first',
            'effective_date': ['min', 'max'],
            'permit_type': lambda x: list(x.unique())
        }).reset_index()
        
        # Flatten column names
        parcel_stats.columns = ['apn', 'permits_on_parcel', 'total_value_on_parcel', 
                              'avg_value_on_parcel', 'max_value_on_parcel',
                              'total_units_on_parcel', 'max_units_on_parcel',
                              'parcel_cluster_id', 'first_permit_date', 'last_permit_date',
                              'permit_types_on_parcel']
        
        # Calculate parcel development timeline
        parcel_stats['development_span_days'] = (
            pd.to_datetime(parcel_stats['last_permit_date']) - 
            pd.to_datetime(parcel_stats['first_permit_date'])
        ).dt.days
        
        # Merge with computed features
        parcels = parcels.merge(parcel_stats, on='apn', how='left')
        
        logger.info("Completed parcel feature generation")
        return parcels
    
    def run_supply_feature_pipeline(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Execute the complete supply feature engineering pipeline"""
        
        logger.info("Starting supply feature engineering pipeline...")
        
        # Load data
        permits_df = self.load_permits_data()
        clusters_df = self.load_project_clusters()
        
        if len(permits_df) == 0:
            raise ValueError("No permits data available for feature engineering")
        
        # Generate features at different geographic levels
        logger.info("Generating block group level features...")
        block_group_features = self.generate_block_group_features(permits_df, clusters_df)
        
        logger.info("Generating parcel level features...")
        parcel_features = self.generate_parcel_features(permits_df, clusters_df)
        
        logger.info("Supply feature engineering pipeline completed successfully!")
        
        return block_group_features, parcel_features
    
    def get_permits_within_buffer(self, target_lat: float, target_lon: float, 
                                 distance_miles: float) -> pd.DataFrame:
        """Get permits within a specified distance buffer from target coordinates"""
        permits_df = self.load_permits_data()
        
        # Convert distance to meters for Haversine calculation
        distance_meters = distance_miles * 1609.34
        
        # Calculate distances and filter
        distances = permits_df.apply(
            lambda row: self.haversine_distance(
                target_lat, target_lon, 
                row['latitude'], row['longitude']
            ), axis=1
        )
        
        within_buffer = permits_df[distances <= distance_meters].copy()
        within_buffer['distance_miles'] = distances[distances <= distance_meters] / 1609.34
        
        return within_buffer.sort_values('distance_miles')
    
    def calculate_velocity_trends(self, target_lat: float, target_lon: float, 
                                distance_miles: float = 1.0) -> dict:
        """Calculate permit velocity trends around target coordinates"""
        permits_df = self.load_permits_data()
        clusters_df = self.load_project_clusters()
        
        # Create target DataFrame for velocity computation
        target_df = pd.DataFrame({
            'latitude': [target_lat],
            'longitude': [target_lon],
            'location_id': ['test_location']
        })
        
        # Compute velocity trends (method only takes target_df and clusters_df)
        velocity_features = self.compute_velocity_trends(target_df, clusters_df)
        
        # Extract metrics for the test location
        if len(velocity_features) > 0:
            metrics = velocity_features.iloc[0].to_dict()
            # Filter velocity-related columns
            velocity_metrics = {
                k: v for k, v in metrics.items() 
                if 'velocity' in k or 'trend' in k or 'momentum' in k
            }
            return velocity_metrics
        
        return {}
    
    def _clean_dataframe_for_sql(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean DataFrame for SQLite compatibility by converting list columns to JSON strings"""
        df_clean = df.copy()
        
        for col in df_clean.columns:
            # Convert list columns to JSON strings
            if df_clean[col].dtype == 'object':
                sample_val = df_clean[col].dropna().iloc[0] if not df_clean[col].dropna().empty else None
                if isinstance(sample_val, list):
                    df_clean[col] = df_clean[col].apply(
                        lambda x: json.dumps(x) if isinstance(x, list) else x
                    )
        
        return df_clean
    
    def save_features_to_database(self, block_group_features: pd.DataFrame, 
                                parcel_features: pd.DataFrame) -> None:
        """Save computed features to database tables"""
        
        logger.info("Saving supply features to database...")
        
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Clean and save block group features
            if len(block_group_features) > 0:
                bg_clean = self._clean_dataframe_for_sql(block_group_features)
                bg_clean.to_sql(
                    'supply_features_block_group', conn, 
                    if_exists='replace', index=False
                )
                logger.info(f"Saved {len(bg_clean)} block group features")
            
            # Clean and save parcel features  
            if len(parcel_features) > 0:
                parcel_clean = self._clean_dataframe_for_sql(parcel_features)
                parcel_clean.to_sql(
                    'supply_features_parcel', conn,
                    if_exists='replace', index=False
                )
                logger.info(f"Saved {len(parcel_clean)} parcel features")
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Error saving features to database: {e}")
            conn.rollback()
        finally:
            conn.close()


def main():
    """Main execution function for testing supply feature engineering"""
    
    engine = SupplyFeaturesEngine()
    
    try:
        # Run the complete pipeline
        block_group_features, parcel_features = engine.run_supply_feature_pipeline()
        
        # Display summary statistics
        print(f"\n🎯 SUPPLY FEATURES ENGINEERING RESULTS")
        print(f"📊 Block Group Features: {len(block_group_features)} records")
        print(f"🏠 Parcel Features: {len(parcel_features)} records")
        
        if len(block_group_features) > 0:
            print(f"\n📈 BLOCK GROUP FEATURE SUMMARY:")
            print(f"   Average permits per 1mi buffer: {block_group_features['pipeline_1.0mi_total_permits'].mean():.1f}")
            print(f"   Average project value per 1mi: ${block_group_features['pipeline_1.0mi_total_value'].mean():,.0f}")
            print(f"   Average units per 1mi buffer: {block_group_features['pipeline_1.0mi_total_units'].mean():.1f}")
            
            # Show top council districts by activity
            print(f"\n🏆 TOP 5 MOST ACTIVE COUNCIL DISTRICTS:")
            top_cds = block_group_features.nlargest(5, 'pipeline_1.0mi_total_permits')
            for _, cd in top_cds.iterrows():
                print(f"   CD {cd['council_district']}: {cd['pipeline_1.0mi_total_permits']} permits, "
                      f"${cd['pipeline_1.0mi_total_value']:,.0f} value")
        
        if len(parcel_features) > 0:
            print(f"\n🏘️ PARCEL FEATURE SUMMARY:")
            # Use available buffer column (0.5mi is the smallest available)
            if 'pipeline_0.5mi_total_permits' in parcel_features.columns:
                print(f"   Average permits per 0.5mi buffer: {parcel_features['pipeline_0.5mi_total_permits'].mean():.1f}")
            print(f"   Parcels with direct permits: {(parcel_features['permits_on_parcel'] > 0).sum()}")
            print(f"   Average development span: {parcel_features['development_span_days'].mean():.0f} days")
            
            # Show most active parcels
            active_parcels = parcel_features[parcel_features['permits_on_parcel'] > 1]
            if len(active_parcels) > 0:
                print(f"\n🏗️ MOST ACTIVE PARCELS:")
                top_parcels = active_parcels.nlargest(5, 'permits_on_parcel')
                for _, parcel in top_parcels.iterrows():
                    print(f"   APN {parcel['apn']}: {parcel['permits_on_parcel']} permits, "
                          f"${parcel['total_value_on_parcel']:,.0f} total value")
        
        # Save to database
        engine.save_features_to_database(block_group_features, parcel_features)
        
        print(f"\n✅ Supply features saved to database tables:")
        print(f"   - supply_features_block_group")
        print(f"   - supply_features_parcel")
        
    except Exception as e:
        logger.error(f"Supply feature engineering failed: {e}")
        raise


if __name__ == "__main__":
    main()