"""
DealGenie Batch Processing Engine v1.2

Implements template pre-filtering, batch feature matrix generation, and shared z-score caching
for optimized multi-template scoring across parcels in the same census block group.
"""

import logging
import numpy as np
import pandas as pd
from typing import Dict, Any, List, Optional, Tuple, Set
from collections import defaultdict
from pathlib import Path

logger = logging.getLogger(__name__)

class BatchProcessor:
    """Handles batch processing optimization for multi-template scoring"""
    
    def __init__(self, zoning_engine, compatibility_threshold: float = 0.5):
        """
        Initialize batch processor
        
        Args:
            zoning_engine: ZoningConstraintsEngine instance
            compatibility_threshold: Minimum compatibility for template inclusion (≥ 0.5)
        """
        self.zoning_engine = zoning_engine
        self.compatibility_threshold = compatibility_threshold
        self.templates = ['retail', 'office', 'multifamily', 'residential', 'commercial', 'industrial', 'mixed_use']
        
        # Cache for z-score statistics by census block group
        self.census_block_cache = {}
        
        # Feature columns for matrix generation
        self.feature_columns = [
            'lot_size_sqft', 'transit_score', 'population_density', 
            'median_income', 'price_per_sqft', 'crime_factor'
        ]
        
    def pre_filter_templates(
        self, 
        zoning: str,
        templates: Optional[List[str]] = None
    ) -> Tuple[List[str], Dict[str, float]]:
        """
        Pre-filter templates based on compatibility ≥ 0.5
        
        Args:
            zoning: Property zoning code
            templates: Optional list of templates to filter (defaults to all)
            
        Returns:
            Tuple of (viable_templates, compatibility_scores)
        """
        if templates is None:
            templates = self.templates
            
        viable_templates = []
        compatibility_scores = {}
        
        for template in templates:
            # Check basic compatibility
            is_compatible = self.zoning_engine.is_compatible(template, zoning)
            
            if not is_compatible:
                compatibility_scores[template] = 0.0
                continue
            
            # Calculate compatibility strength based on score cap
            score_cap = self.zoning_engine.get_score_cap(template, zoning)
            template_caps = self.zoning_engine.score_caps.get(template, {})
            max_cap = max(template_caps.values()) if template_caps else 10.0
            
            compatibility_score = score_cap / max_cap
            compatibility_scores[template] = compatibility_score
            
            # Include if meets threshold
            if compatibility_score >= self.compatibility_threshold:
                viable_templates.append(template)
        
        logger.info(f"Pre-filtered {len(viable_templates)}/{len(templates)} templates for zoning {zoning}")
        
        return viable_templates, compatibility_scores
    
    def get_census_block_group(self, features: Dict[str, Any]) -> str:
        """
        Extract census block group from features for caching
        
        Args:
            features: Property features dictionary
            
        Returns:
            Census block group identifier
        """
        # Use lat/lng to approximate census block group
        lat = features.get('latitude', 34.05)
        lng = features.get('longitude', -118.25)
        
        # Round to ~0.01 degree precision for grouping (~1km blocks)
        block_lat = round(lat, 2)
        block_lng = round(lng, 2)
        
        return f"CBG_{block_lat}_{block_lng}"
    
    def calculate_z_scores(
        self,
        parcels: List[Dict[str, Any]],
        census_block_group: str
    ) -> Dict[str, Dict[str, float]]:
        """
        Calculate z-score statistics for census block group with caching
        
        Args:
            parcels: List of parcel feature dictionaries
            census_block_group: Census block group identifier
            
        Returns:
            Dictionary mapping feature -> {mean, std} statistics
        """
        # Check cache first
        if census_block_group in self.census_block_cache:
            logger.debug(f"Using cached z-scores for {census_block_group}")
            return self.census_block_cache[census_block_group]
        
        # Calculate statistics from parcel data
        z_score_stats = {}
        
        for feature in self.feature_columns:
            values = []
            for parcel in parcels:
                value = parcel.get(feature)
                if value is not None and not pd.isna(value):
                    values.append(float(value))
            
            if len(values) >= 2:  # Need at least 2 values for std
                mean_val = np.mean(values)
                std_val = np.std(values, ddof=1)
                
                # Avoid division by zero
                if std_val == 0:
                    std_val = 1.0
                
                z_score_stats[feature] = {
                    'mean': mean_val,
                    'std': std_val,
                    'count': len(values)
                }
            else:
                # Fallback to regional defaults
                z_score_stats[feature] = {
                    'mean': self._get_regional_default(feature, 'mean'),
                    'std': self._get_regional_default(feature, 'std'),
                    'count': 0
                }
        
        # Cache for future use
        self.census_block_cache[census_block_group] = z_score_stats
        logger.info(f"Cached z-scores for {census_block_group} with {len(parcels)} parcels")
        
        return z_score_stats
    
    def _get_regional_default(self, feature: str, stat_type: str) -> float:
        """Get regional default statistics for LA area"""
        regional_defaults = {
            'lot_size_sqft': {'mean': 7500, 'std': 5000},
            'transit_score': {'mean': 55, 'std': 20},
            'population_density': {'mean': 7000, 'std': 3000},
            'median_income': {'mean': 65000, 'std': 25000},
            'price_per_sqft': {'mean': 550, 'std': 200},
            'crime_factor': {'mean': 1.0, 'std': 0.3}
        }
        
        return regional_defaults.get(feature, {}).get(stat_type, 1.0)
    
    def build_rows(
        self,
        parcels: List[Dict[str, Any]],
        viable_templates: List[str],
        census_block_group: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Build feature matrix for batch processing with z-score normalization
        
        Args:
            parcels: List of parcel feature dictionaries
            viable_templates: Pre-filtered viable templates
            census_block_group: Optional census block group for z-score caching
            
        Returns:
            DataFrame with normalized features for batch processing
        """
        if not parcels:
            return pd.DataFrame()
        
        # Determine census block group if not provided
        if census_block_group is None:
            census_block_group = self.get_census_block_group(parcels[0])
        
        # Get z-score statistics (cached if available)
        z_score_stats = self.calculate_z_scores(parcels, census_block_group)
        
        # Build feature matrix
        rows = []
        
        for parcel_idx, parcel in enumerate(parcels):
            parcel_id = parcel.get('apn', f'parcel_{parcel_idx}')
            zoning = parcel.get('zoning', 'R1')
            
            # Create base row
            base_row = {
                'parcel_id': parcel_id,
                'zoning': zoning,
                'census_block_group': census_block_group
            }
            
            # Add normalized features
            for feature in self.feature_columns:
                raw_value = parcel.get(feature, 0)
                
                # Z-score normalization
                stats = z_score_stats.get(feature, {'mean': 0, 'std': 1})
                normalized_value = (raw_value - stats['mean']) / stats['std']
                
                base_row[feature] = raw_value
                base_row[f'{feature}_zscore'] = normalized_value
            
            # Create template-specific rows
            for template in viable_templates:
                template_row = base_row.copy()
                template_row['template'] = template
                
                # Add template-specific features
                template_row['template_zoning_compatible'] = self.zoning_engine.is_compatible(template, zoning)
                template_row['template_score_cap'] = self.zoning_engine.get_score_cap(template, zoning)
                template_row['template_plausibility_floor'] = self.zoning_engine.get_plausibility_floor(template)
                
                rows.append(template_row)
        
        df = pd.DataFrame(rows)
        logger.info(f"Built feature matrix: {len(df)} rows for {len(parcels)} parcels × {len(viable_templates)} templates")
        
        return df
    
    def batch_pre_filter(
        self,
        parcels: List[Dict[str, Any]]
    ) -> Dict[str, Tuple[List[str], Dict[str, float]]]:
        """
        Batch pre-filter templates for multiple parcels
        
        Args:
            parcels: List of parcel dictionaries with features
            
        Returns:
            Dictionary mapping parcel_id -> (viable_templates, compatibility_scores)
        """
        results = {}
        zoning_groups = defaultdict(list)
        
        # Group parcels by zoning for batch processing
        for parcel in parcels:
            parcel_id = parcel.get('apn', parcel.get('parcel_id', 'unknown'))
            zoning = parcel.get('zoning', 'R1')
            zoning_groups[zoning].append((parcel_id, parcel))
        
        # Process each zoning group
        for zoning, parcel_list in zoning_groups.items():
            viable_templates, compatibility_scores = self.pre_filter_templates(zoning)
            
            # Apply results to all parcels in this zoning group
            for parcel_id, parcel in parcel_list:
                results[parcel_id] = (viable_templates, compatibility_scores)
        
        logger.info(f"Batch pre-filtered {len(parcels)} parcels across {len(zoning_groups)} zoning types")
        
        return results
    
    def clear_cache(self, census_block_group: Optional[str] = None):
        """
        Clear z-score cache for memory management
        
        Args:
            census_block_group: Optional specific block group to clear (clears all if None)
        """
        if census_block_group:
            self.census_block_cache.pop(census_block_group, None)
            logger.info(f"Cleared cache for {census_block_group}")
        else:
            self.census_block_cache.clear()
            logger.info("Cleared all z-score cache")

def create_batch_processor(zoning_engine, compatibility_threshold: float = 0.5) -> BatchProcessor:
    """
    Factory function to create batch processor
    
    Args:
        zoning_engine: ZoningConstraintsEngine instance
        compatibility_threshold: Minimum compatibility threshold
        
    Returns:
        Configured BatchProcessor instance
    """
    return BatchProcessor(zoning_engine, compatibility_threshold)