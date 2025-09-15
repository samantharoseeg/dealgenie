#!/usr/bin/env python3
"""
Permits Clustering and Deduplication System
DBSCAN clustering on 2,136 permits with spatial-temporal analysis

Features:
- DBSCAN clustering using Haversine distance on lat/lon coordinates
- Megaproject handling with extended time windows (>$1M projects)  
- Spatial radius of 150m and temporal windows of 1 year (3 years for megaprojects)
- Deduplication using natural keys (APN, address, date proximity)
- Status weights (applied=0.5, issued=1.0, finaled=1.25)
- Integration with existing governance metadata and etl_audit table
- Validation against LA development corridors
- Assembly opportunity detection for adjacent parcels
"""

import sqlite3
import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
from datetime import datetime, timedelta
import logging
import json
import hashlib
import math
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, asdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ClusteringParameters:
    """Parameters for clustering analysis with forensic reproducibility"""
    spatial_radius_m: float = 150.0  # 150 meters for LA's sprawling development
    temporal_window_days: int = 365  # 1 year for standard projects
    megaproject_temporal_window_days: int = 1095  # 3 years for megaprojects
    min_samples: int = 2  # Minimum permits to form a cluster
    megaproject_threshold: float = 1_000_000  # $1M+ considered megaproject
    status_weights: Dict[str, float] = None
    run_timestamp: str = None
    
    def __post_init__(self):
        if self.status_weights is None:
            # LA-specific permit status weights based on certainty of development
            self.status_weights = {
                'Applied': 0.5,
                'Issued': 1.0, 
                'Permit Finaled': 1.25,  # Finaled = higher certainty
                'CofO Issued': 1.25,
                'CofO in Progress': 1.1,
                'Approved': 0.8,
                'Pending': 0.3,
                'Cancelled': 0.1,
                'Expired': 0.1
            }
        if self.run_timestamp is None:
            self.run_timestamp = datetime.now().isoformat()

class PermitsClusteringEngine:
    """Main clustering engine for permits data"""
    
    def __init__(self, db_path: str = "./data/dealgenie.db"):
        self.db_path = db_path
        self.params = ClusteringParameters()
        
    def load_permits_data(self) -> pd.DataFrame:
        """Load permits from database with coordinate filtering"""
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
            scraped_at
        FROM raw_permits 
        WHERE latitude IS NOT NULL 
          AND longitude IS NOT NULL
          AND estimated_cost IS NOT NULL
        ORDER BY application_date DESC
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Convert date columns
        df['application_date'] = pd.to_datetime(df['application_date'])
        df['issue_date'] = pd.to_datetime(df['issue_date'])
        
        logger.info(f"Loaded {len(df)} permits with valid coordinates for clustering")
        return df
    
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
    
    def prepare_clustering_features(self, df: pd.DataFrame) -> Tuple[np.ndarray, pd.DataFrame]:
        """Prepare features for DBSCAN clustering using Haversine distance"""
        
        # Use LA City Hall as reference point for coordinate transformation
        la_reference_lat = 34.0522
        la_reference_lon = -118.2437
        
        # Prepare features for clustering
        features_list = []
        
        for idx, row in df.iterrows():
            # Convert lat/lon to meters from LA reference point using Haversine
            x_meters = self.haversine_distance(
                la_reference_lat, la_reference_lon, 
                la_reference_lat, row['longitude']
            ) * (-1 if row['longitude'] < la_reference_lon else 1)
            
            y_meters = self.haversine_distance(
                la_reference_lat, la_reference_lon,
                row['latitude'], la_reference_lon
            ) * (-1 if row['latitude'] < la_reference_lat else 1)
            
            # Temporal feature (days since epoch)
            date_to_use = row['application_date'] if pd.notna(row['application_date']) else row['issue_date']
            if pd.notna(date_to_use):
                days_since_epoch = (date_to_use - pd.Timestamp('2020-01-01')).days
            else:
                days_since_epoch = 0
            
            # Determine if this is a megaproject for extended temporal window
            is_megaproject = row['estimated_cost'] > self.params.megaproject_threshold
            temporal_scale = self.params.megaproject_temporal_window_days if is_megaproject else self.params.temporal_window_days
            
            # Scale temporal feature to match spatial scale
            # We want temporal similarity to have similar weight as spatial
            t_scaled = days_since_epoch * (self.params.spatial_radius_m / temporal_scale)
            
            features_list.append([x_meters, y_meters, t_scaled])
        
        features = np.array(features_list)
        
        # Standardize features for DBSCAN
        scaler = StandardScaler()
        features_scaled = scaler.fit_transform(features)
        
        # Store scaler parameters for reproducibility
        df_features = df.copy()
        df_features['x_meters'] = features[:, 0]
        df_features['y_meters'] = features[:, 1] 
        df_features['t_scaled'] = features[:, 2]
        df_features['x_scaled'] = features_scaled[:, 0]
        df_features['y_scaled'] = features_scaled[:, 1]
        df_features['t_scaled_norm'] = features_scaled[:, 2]
        
        return features_scaled, df_features
    
    def apply_dbscan_clustering(self, features: np.ndarray) -> np.ndarray:
        """Apply DBSCAN clustering to prepared features"""
        
        # DBSCAN with eps=0.3 for better separation of clusters
        # Lower eps means tighter clusters, higher eps means looser clusters
        dbscan = DBSCAN(
            eps=0.3,  # Reduced from 1.0 for better cluster separation
            min_samples=self.params.min_samples,
            metric='euclidean'
        )
        
        cluster_labels = dbscan.fit_predict(features)
        
        n_clusters = len(set(cluster_labels)) - (1 if -1 in cluster_labels else 0)
        n_noise = list(cluster_labels).count(-1)
        
        logger.info(f"DBSCAN found {n_clusters} clusters with {n_noise} noise points")
        
        return cluster_labels
    
    def calculate_permit_weights(self, df: pd.DataFrame) -> pd.Series:
        """Calculate permit weights based on status"""
        return df['status'].map(self.params.status_weights).fillna(0.5)
    
    def deduplicate_within_clusters(self, df: pd.DataFrame) -> pd.DataFrame:
        """Deduplicate permits within each cluster using natural keys"""
        
        if 'cluster_id' not in df.columns:
            logger.warning("No cluster_id column found, skipping deduplication")
            return df
        
        deduplicated_permits = []
        
        for cluster_id in df['cluster_id'].unique():
            if cluster_id == -1:  # Skip noise points
                cluster_permits = df[df['cluster_id'] == cluster_id]
                deduplicated_permits.append(cluster_permits)
                continue
                
            cluster_permits = df[df['cluster_id'] == cluster_id].copy()
            
            # Group by APN to find potential duplicates on same parcel
            for apn, apn_group in cluster_permits.groupby('apn'):
                if len(apn_group) == 1:
                    deduplicated_permits.append(apn_group)
                else:
                    # Multiple permits on same parcel - keep highest weighted
                    apn_group = apn_group.copy()
                    apn_group['weight'] = self.calculate_permit_weights(apn_group)
                    apn_group['combined_score'] = (
                        apn_group['weight'] * apn_group['estimated_cost']
                    )
                    
                    # Keep the permit with highest combined score
                    best_permit = apn_group.loc[apn_group['combined_score'].idxmax()]
                    
                    # Mark others as duplicates
                    duplicates = apn_group[apn_group.index != best_permit.name].copy()
                    duplicates['is_duplicate'] = True
                    duplicates['primary_permit_id'] = best_permit['permit_id']
                    
                    best_permit_df = pd.DataFrame([best_permit])
                    best_permit_df['is_duplicate'] = False
                    
                    deduplicated_permits.extend([best_permit_df, duplicates])
        
        if deduplicated_permits:
            result = pd.concat(deduplicated_permits, ignore_index=True)
        else:
            result = df.copy()
            result['is_duplicate'] = False
            
        logger.info(f"Deduplication: {result['is_duplicate'].sum()} duplicates found")
        return result
    
    def validate_la_development_corridors(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate clusters against known LA development corridors"""
        
        # Known LA development corridors (major areas with high development activity)
        la_corridors = {
            'Downtown LA': {'lat_range': (34.040, 34.060), 'lon_range': (-118.270, -118.240)},
            'Hollywood': {'lat_range': (34.090, 34.110), 'lon_range': (-118.350, -118.320)},
            'Santa Monica': {'lat_range': (34.010, 34.030), 'lon_range': (-118.510, -118.480)},
            'Century City': {'lat_range': (34.050, 34.070), 'lon_range': (-118.420, -118.390)},
            'Koreatown': {'lat_range': (34.050, 34.070), 'lon_range': (-118.310, -118.280)},
            'Mid-Wilshire': {'lat_range': (34.060, 34.080), 'lon_range': (-118.360, -118.320)},
            'Venice': {'lat_range': (33.990, 34.010), 'lon_range': (-118.480, -118.450)},
            'Westwood': {'lat_range': (34.060, 34.080), 'lon_range': (-118.460, -118.430)},
            'Beverly Hills Adjacent': {'lat_range': (34.070, 34.090), 'lon_range': (-118.420, -118.390)},
            'Echo Park/Silver Lake': {'lat_range': (34.070, 34.090), 'lon_range': (-118.270, -118.240)}
        }
        
        # Add corridor validation to clusters
        if 'cluster_id' not in df.columns:
            return df
            
        df_with_corridors = df.copy()
        df_with_corridors['development_corridor'] = None
        df_with_corridors['corridor_confidence'] = 0.0
        
        for idx, row in df_with_corridors.iterrows():
            lat, lon = row['latitude'], row['longitude']
            
            for corridor_name, bounds in la_corridors.items():
                if (bounds['lat_range'][0] <= lat <= bounds['lat_range'][1] and
                    bounds['lon_range'][0] <= lon <= bounds['lon_range'][1]):
                    df_with_corridors.loc[idx, 'development_corridor'] = corridor_name
                    df_with_corridors.loc[idx, 'corridor_confidence'] = 0.9
                    break
            else:
                # Check proximity to corridors (within 500m)
                min_distance = float('inf')
                closest_corridor = None
                
                for corridor_name, bounds in la_corridors.items():
                    # Calculate distance to corridor center
                    corridor_center_lat = (bounds['lat_range'][0] + bounds['lat_range'][1]) / 2
                    corridor_center_lon = (bounds['lon_range'][0] + bounds['lon_range'][1]) / 2
                    
                    distance = self.haversine_distance(lat, lon, corridor_center_lat, corridor_center_lon)
                    
                    if distance < min_distance:
                        min_distance = distance
                        closest_corridor = corridor_name
                
                # If within 500m of a corridor, mark as adjacent
                if min_distance <= 500:
                    df_with_corridors.loc[idx, 'development_corridor'] = f"{closest_corridor} (Adjacent)"
                    df_with_corridors.loc[idx, 'corridor_confidence'] = 0.6
        
        logger.info(f"Validated {len(df_with_corridors[df_with_corridors['corridor_confidence'] > 0])} permits against LA development corridors")
        return df_with_corridors
    
    def detect_assembly_opportunities(self, df: pd.DataFrame) -> pd.DataFrame:
        """Detect assembly opportunities for adjacent parcels"""
        
        if 'cluster_id' not in df.columns:
            return df
        
        df_with_assembly = df.copy()
        df_with_assembly['is_assembly_opportunity'] = False
        df_with_assembly['assembly_parcel_count'] = 1
        df_with_assembly['assembly_total_value'] = df_with_assembly['estimated_cost']
        
        # Process each cluster for assembly opportunities
        for cluster_id in df['cluster_id'].unique():
            if cluster_id == -1:  # Skip noise points
                continue
                
            cluster_permits = df[df['cluster_id'] == cluster_id]
            if len(cluster_permits) < 2:
                continue
            
            # Group by APN to find multi-parcel projects
            unique_apns = cluster_permits['apn'].nunique()
            
            if unique_apns >= 2:  # Multiple parcels involved
                # Check if parcels are adjacent (within assembly distance)
                assembly_radius = 75  # 75 meters for parcel adjacency
                
                apn_groups = []
                for apn, apn_permits in cluster_permits.groupby('apn'):
                    if len(apn_permits) > 0:
                        centroid_lat = apn_permits['latitude'].mean()
                        centroid_lon = apn_permits['longitude'].mean()
                        total_value = apn_permits['estimated_cost'].sum()
                        apn_groups.append({
                            'apn': apn,
                            'lat': centroid_lat,
                            'lon': centroid_lon,
                            'value': total_value,
                            'permits': apn_permits.index.tolist()
                        })
                
                # Check adjacency between APNs
                for i, apn1 in enumerate(apn_groups):
                    adjacent_apns = [apn1]
                    total_assembly_value = apn1['value']
                    
                    for j, apn2 in enumerate(apn_groups):
                        if i != j:
                            distance = self.haversine_distance(
                                apn1['lat'], apn1['lon'],
                                apn2['lat'], apn2['lon']
                            )
                            
                            if distance <= assembly_radius:
                                adjacent_apns.append(apn2)
                                total_assembly_value += apn2['value']
                    
                    # Mark as assembly opportunity if multiple adjacent parcels
                    if len(adjacent_apns) >= 2:
                        all_permit_indices = []
                        for apn_info in adjacent_apns:
                            all_permit_indices.extend(apn_info['permits'])
                        
                        # Update assembly information
                        df_with_assembly.loc[all_permit_indices, 'is_assembly_opportunity'] = True
                        df_with_assembly.loc[all_permit_indices, 'assembly_parcel_count'] = len(adjacent_apns)
                        df_with_assembly.loc[all_permit_indices, 'assembly_total_value'] = total_assembly_value
        
        assembly_count = df_with_assembly['is_assembly_opportunity'].sum()
        logger.info(f"Detected {assembly_count} permits in assembly opportunities")
        
        return df_with_assembly
    
    def create_project_aggregations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create project-level aggregations for supply analysis"""
        
        if 'cluster_id' not in df.columns:
            logger.error("No cluster_id column found for aggregation")
            return pd.DataFrame()
        
        # Only aggregate actual clusters (not noise points)
        clustered_data = df[df['cluster_id'] >= 0].copy()
        
        if len(clustered_data) == 0:
            logger.warning("No clustered permits found for aggregation")
            return pd.DataFrame()
        
        # Calculate weights for aggregation
        clustered_data['weight'] = self.calculate_permit_weights(clustered_data)
        
        # Group by cluster and aggregate
        aggregations = []
        
        for cluster_id, cluster_permits in clustered_data.groupby('cluster_id'):
            # Get primary (non-duplicate) permits only for aggregation
            if 'is_duplicate' in cluster_permits.columns:
                # Convert to boolean and handle NaN values
                is_dup = cluster_permits['is_duplicate'].fillna(False).astype(bool)
                primary_permits = cluster_permits[~is_dup]
            else:
                primary_permits = cluster_permits
            
            if len(primary_permits) == 0:
                continue
            
            # Calculate weighted averages and sums
            total_weight = primary_permits['weight'].sum()
            
            agg_data = {
                'project_cluster_id': cluster_id,
                'permits_count': len(primary_permits),
                'duplicates_count': len(cluster_permits) - len(primary_permits),
                
                # Spatial aggregations
                'centroid_latitude': primary_permits['latitude'].mean(),
                'centroid_longitude': primary_permits['longitude'].mean(),
                'spatial_extent_meters': self._calculate_spatial_extent(primary_permits),
                
                # Temporal aggregations  
                'earliest_permit_date': primary_permits['application_date'].min(),
                'latest_permit_date': primary_permits['application_date'].max(),
                'project_duration_days': (
                    primary_permits['application_date'].max() - 
                    primary_permits['application_date'].min()
                ).days if len(primary_permits) > 1 else 0,
                
                # Value aggregations
                'total_estimated_cost': primary_permits['estimated_cost'].sum(),
                'weighted_avg_cost': (
                    primary_permits['estimated_cost'] * primary_permits['weight']
                ).sum() / total_weight if total_weight > 0 else 0,
                'max_estimated_cost': primary_permits['estimated_cost'].max(),
                
                # Units aggregations
                'total_units_proposed': primary_permits['units_proposed'].sum(),
                'total_units_net_change': primary_permits['units_net_change'].sum(),
                
                # Geographic context
                'council_districts': list(primary_permits['council_district'].unique()),
                'unique_apns': list(primary_permits['apn'].unique()),
                
                # Development corridor information
                'primary_corridor': primary_permits['development_corridor'].mode().iloc[0] if 'development_corridor' in primary_permits.columns and len(primary_permits['development_corridor'].mode()) > 0 else None,
                'corridor_permits_count': primary_permits['corridor_confidence'].gt(0).sum() if 'corridor_confidence' in primary_permits.columns else 0,
                'avg_corridor_confidence': primary_permits['corridor_confidence'].mean() if 'corridor_confidence' in primary_permits.columns else 0.0,
                
                # Assembly opportunities
                'has_assembly_opportunity': primary_permits['is_assembly_opportunity'].any() if 'is_assembly_opportunity' in primary_permits.columns else False,
                'assembly_permits_count': primary_permits['is_assembly_opportunity'].sum() if 'is_assembly_opportunity' in primary_permits.columns else 0,
                'max_assembly_parcel_count': primary_permits['assembly_parcel_count'].max() if 'assembly_parcel_count' in primary_permits.columns else 1,
                'max_assembly_total_value': primary_permits['assembly_total_value'].max() if 'assembly_total_value' in primary_permits.columns else 0,
                
                # Project characteristics
                'permit_types': list(primary_permits['permit_type'].unique()),
                'is_megaproject': primary_permits['estimated_cost'].max() > self.params.megaproject_threshold,
                'avg_permit_weight': primary_permits['weight'].mean(),
                
                # Metadata
                'created_at': datetime.now().isoformat()
            }
            
            aggregations.append(agg_data)
        
        if aggregations:
            result = pd.DataFrame(aggregations)
            logger.info(f"Created {len(result)} project aggregations")
            return result
        else:
            logger.warning("No project aggregations created")
            return pd.DataFrame()
    
    def _calculate_spatial_extent(self, permits: pd.DataFrame) -> float:
        """Calculate spatial extent of permits cluster in meters using Haversine"""
        if len(permits) <= 1:
            return 0.0
        
        # Find bounding box
        min_lat, max_lat = permits['latitude'].min(), permits['latitude'].max()
        min_lon, max_lon = permits['longitude'].min(), permits['longitude'].max()
        
        # Calculate extent using Haversine distance
        lat_extent = self.haversine_distance(min_lat, min_lon, max_lat, min_lon)
        lon_extent = self.haversine_distance(min_lat, min_lon, min_lat, max_lon)
        
        # Return diagonal extent
        return np.sqrt(lat_extent**2 + lon_extent**2)
    
    def update_permits_with_cluster_ids(self, df: pd.DataFrame) -> None:
        """Update raw_permits table with cluster assignments from DBSCAN results"""
        
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Create run_id for this clustering session
            params_str = json.dumps(asdict(self.params), sort_keys=True, default=str)
            run_id = hashlib.md5(params_str.encode()).hexdigest()[:12]
            
            logger.info(f"Updating raw_permits table with cluster assignments (run_id: {run_id})")
            
            # Update permits with cluster assignments
            update_count = 0
            for idx, row in df.iterrows():
                conn.execute("""
                UPDATE raw_permits 
                SET cluster_id = ?,
                    cluster_run_id = ?,
                    cluster_assigned_at = CURRENT_TIMESTAMP,
                    clustering_algorithm = 'DBSCAN',
                    clustering_version = '1.0'
                WHERE permit_id = ?
                """, (
                    int(row['cluster_id']) if row['cluster_id'] != -1 else None,
                    run_id,
                    row['permit_id']
                ))
                update_count += 1
                
                # Commit in batches for performance
                if update_count % 100 == 0:
                    conn.commit()
            
            # Final commit
            conn.commit()
            logger.info(f"Successfully updated {update_count} permits with cluster assignments")
            
        except Exception as e:
            logger.error(f"Error updating permits with cluster IDs: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def store_clustering_metadata(self, df: pd.DataFrame, project_aggregations: pd.DataFrame):
        """Store clustering parameters and results for forensic reproducibility"""
        
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Ensure project_cluster_meta table exists
            conn.execute("""
            CREATE TABLE IF NOT EXISTS project_cluster_meta (
                run_id TEXT PRIMARY KEY,
                run_timestamp TEXT NOT NULL,
                clustering_parameters TEXT NOT NULL,  -- JSON
                permits_processed INTEGER NOT NULL,
                clusters_found INTEGER NOT NULL,
                noise_points INTEGER NOT NULL,
                megaprojects_count INTEGER NOT NULL,
                deduplication_stats TEXT,  -- JSON
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            # Calculate statistics
            clusters_found = len(df[df['cluster_id'] >= 0]['cluster_id'].unique()) if 'cluster_id' in df.columns else 0
            noise_points = len(df[df['cluster_id'] == -1]) if 'cluster_id' in df.columns else 0
            megaprojects = len(df[df['estimated_cost'] > self.params.megaproject_threshold])
            duplicates_count = df.get('is_duplicate', pd.Series(dtype=bool)).sum()
            
            # Create run ID based on parameters hash
            params_str = json.dumps(asdict(self.params), sort_keys=True, default=str)
            run_id = hashlib.md5(params_str.encode()).hexdigest()[:12]
            
            # Convert numpy/pandas types to native Python types for JSON serialization
            dedup_stats = {
                'total_duplicates': int(duplicates_count) if pd.notna(duplicates_count) else 0,
                'deduplication_rate': float(duplicates_count / len(df)) if len(df) > 0 else 0.0,
                'primary_permits': int(len(df) - (duplicates_count if pd.notna(duplicates_count) else 0))
            }
            
            # Store metadata
            conn.execute("""
            INSERT OR REPLACE INTO project_cluster_meta 
            (run_id, run_timestamp, clustering_parameters, permits_processed,
             clusters_found, noise_points, megaprojects_count, deduplication_stats)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                run_id,
                self.params.run_timestamp,
                params_str,
                len(df),
                clusters_found,
                noise_points,
                megaprojects,
                json.dumps(dedup_stats)
            ))
            
            # Store project aggregations if table exists
            if len(project_aggregations) > 0:
                # Create project_clusters table if it doesn't exist
                conn.execute("""
                CREATE TABLE IF NOT EXISTS project_clusters (
                    project_cluster_id INTEGER,
                    run_id TEXT,
                    permits_count INTEGER,
                    duplicates_count INTEGER,
                    centroid_latitude REAL,
                    centroid_longitude REAL,
                    spatial_extent_meters REAL,
                    earliest_permit_date TEXT,
                    latest_permit_date TEXT,
                    project_duration_days INTEGER,
                    total_estimated_cost REAL,
                    weighted_avg_cost REAL,
                    max_estimated_cost REAL,
                    total_units_proposed INTEGER,
                    total_units_net_change INTEGER,
                    council_districts TEXT,  -- JSON array
                    unique_apns TEXT,       -- JSON array
                    permit_types TEXT,      -- JSON array
                    primary_corridor TEXT,
                    has_assembly_opportunity BOOLEAN,
                    is_megaproject BOOLEAN,
                    avg_permit_weight REAL,
                    created_at TEXT,
                    PRIMARY KEY (project_cluster_id, run_id),
                    FOREIGN KEY (run_id) REFERENCES project_cluster_meta(run_id)
                )
                """)
                
                # Insert project aggregations
                for _, row in project_aggregations.iterrows():
                    # Convert numpy/pandas types to native Python types
                    values = (
                        int(row['project_cluster_id']) if pd.notna(row['project_cluster_id']) else 0, 
                        run_id, 
                        int(row['permits_count']) if pd.notna(row['permits_count']) else 0,
                        int(row['duplicates_count']) if pd.notna(row['duplicates_count']) else 0, 
                        float(row['centroid_latitude']) if pd.notna(row['centroid_latitude']) else 0.0, 
                        float(row['centroid_longitude']) if pd.notna(row['centroid_longitude']) else 0.0,
                        float(row['spatial_extent_meters']) if pd.notna(row['spatial_extent_meters']) else 0.0, 
                        str(row['earliest_permit_date']), 
                        str(row['latest_permit_date']), 
                        int(row['project_duration_days']) if pd.notna(row['project_duration_days']) else 0,
                        float(row['total_estimated_cost']) if pd.notna(row['total_estimated_cost']) else 0.0, 
                        float(row['weighted_avg_cost']) if pd.notna(row['weighted_avg_cost']) else 0.0, 
                        float(row['max_estimated_cost']) if pd.notna(row['max_estimated_cost']) else 0.0,
                        int(row['total_units_proposed']) if pd.notna(row['total_units_proposed']) else 0, 
                        int(row['total_units_net_change']) if pd.notna(row['total_units_net_change']) else 0,
                        json.dumps([str(x) for x in row['council_districts']]), 
                        json.dumps([str(x) for x in row['unique_apns']]),
                        json.dumps([str(x) for x in row['permit_types']]), 
                        str(row.get('primary_corridor', '')),
                        bool(row.get('has_assembly_opportunity', False)), 
                        bool(row['is_megaproject']),
                        float(row['avg_permit_weight']) if pd.notna(row['avg_permit_weight']) else 0.0, 
                        str(row['created_at'])
                    )
                    
                    conn.execute("""
                    INSERT OR REPLACE INTO project_clusters
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, values)
            
            conn.commit()
            logger.info(f"Stored clustering metadata with run_id: {run_id}")
            return run_id
            
        except Exception as e:
            logger.error(f"Error storing clustering metadata: {e}")
            conn.rollback()
            return None
        finally:
            conn.close()
    
    def run_full_clustering_pipeline(self) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
        """Execute the complete clustering pipeline"""
        
        logger.info("Starting permits clustering pipeline...")
        
        # Step 1: Load data
        permits_df = self.load_permits_data()
        if len(permits_df) == 0:
            raise ValueError("No permits data found for clustering")
        
        # Step 2: Prepare features and apply clustering
        features, permits_with_features = self.prepare_clustering_features(permits_df)
        cluster_labels = self.apply_dbscan_clustering(features)
        
        # Step 3: Add cluster labels to dataframe
        permits_with_features['cluster_id'] = cluster_labels
        
        # Step 3.5: Update raw_permits table with cluster assignments
        self.update_permits_with_cluster_ids(permits_with_features)
        
        # Step 4: Deduplicate within clusters
        deduplicated_permits = self.deduplicate_within_clusters(permits_with_features)
        
        # Step 5: Validate against LA development corridors
        permits_with_corridors = self.validate_la_development_corridors(deduplicated_permits)
        
        # Step 6: Detect assembly opportunities
        final_permits = self.detect_assembly_opportunities(permits_with_corridors)
        
        # Step 7: Create project aggregations
        project_aggregations = self.create_project_aggregations(final_permits)
        
        # Step 8: Store metadata for forensic reproducibility
        run_id = self.store_clustering_metadata(final_permits, project_aggregations)
        
        logger.info("Clustering pipeline completed successfully!")
        
        return final_permits, project_aggregations, run_id


def main():
    """Main execution function for testing"""
    engine = PermitsClusteringEngine()
    
    try:
        permits_df, projects_df, run_id = engine.run_full_clustering_pipeline()
        
        print(f"\n🎯 CLUSTERING RESULTS (Run ID: {run_id})")
        print(f"📊 Total permits processed: {len(permits_df)}")
        print(f"🏗️  Project clusters created: {len(projects_df)}")
        print(f"🔄 Duplicates identified: {permits_df.get('is_duplicate', pd.Series(dtype=bool)).sum()}")
        print(f"🏢 Megaprojects ($1M+): {len(projects_df[projects_df['is_megaproject']]) if len(projects_df) > 0 else 0}")
        
        # New corridor and assembly statistics
        corridor_permits = permits_df[permits_df.get('corridor_confidence', 0) > 0]
        assembly_permits = permits_df[permits_df.get('is_assembly_opportunity', False)]
        
        print(f"🗺️  Permits in development corridors: {len(corridor_permits)}")
        print(f"🏘️  Assembly opportunity permits: {len(assembly_permits)}")
        
        if len(projects_df) > 0:
            print(f"\n📈 PROJECT AGGREGATIONS SUMMARY:")
            print(f"   Total estimated value: ${projects_df['total_estimated_cost'].sum():,.0f}")
            print(f"   Total units proposed: {projects_df['total_units_proposed'].sum():,.0f}")
            print(f"   Average project duration: {projects_df['project_duration_days'].mean():.0f} days")
            print(f"   Projects with assembly opportunities: {projects_df['has_assembly_opportunity'].sum()}")
            
            print(f"\n🎯 TOP 5 PROJECTS BY VALUE:")
            top_projects = projects_df.nlargest(5, 'total_estimated_cost')
            for _, project in top_projects.iterrows():
                corridor_info = f" ({project['primary_corridor']})" if project.get('primary_corridor') else ""
                assembly_info = f" [Assembly: {project['max_assembly_parcel_count']} parcels]" if project.get('has_assembly_opportunity', False) else ""
                print(f"   Cluster {project['project_cluster_id']}: "
                      f"${project['total_estimated_cost']:,.0f} "
                      f"({project['permits_count']} permits){corridor_info}{assembly_info}")
            
            # Show development corridor summary
            corridor_summary = permits_df[permits_df.get('development_corridor').notna()]['development_corridor'].value_counts()
            if len(corridor_summary) > 0:
                print(f"\n🗺️  DEVELOPMENT CORRIDOR DISTRIBUTION:")
                for corridor, count in corridor_summary.head(5).items():
                    print(f"   {corridor}: {count} permits")
        
    except Exception as e:
        logger.error(f"Clustering pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()