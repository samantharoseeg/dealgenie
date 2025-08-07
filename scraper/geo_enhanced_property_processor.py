#!/usr/bin/env python3
"""
Geographic Enhanced Property Processor
Comprehensive location intelligence for DealGenie scored properties

Features:
- Load scored properties and enhance with geographic data
- Neighborhood market analysis and gentrification indicators  
- Assembly opportunity identification
- Enhanced scoring with location factors
- Export enhanced dataset with new metrics

Author: DealGenie AI Engine
Version: 1.0
"""

import pandas as pd
import numpy as np
import json
import sys
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from collections import defaultdict, Counter
from datetime import datetime
import ast

# Import our geographic engine
from geographic_intelligence_engine import LAGeographicEngine, GeographicMetrics

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GeographicPropertyEnhancer:
    """Enhance scored properties with comprehensive geographic intelligence"""
    
    def __init__(self):
        self.geo_engine = LAGeographicEngine()
        self.properties_df = None
        self.enhanced_df = None
        self.neighborhood_stats = {}
        self.assembly_opportunities = []
        
    def load_scored_properties(self, file_path: str) -> pd.DataFrame:
        """Load the scored properties dataset"""
        logger.info(f"Loading scored properties from: {file_path}")
        
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Loaded {len(df)} properties successfully")
            
            # Clean and prepare data
            df['zip_code'] = df['zip_code'].fillna(0).astype(str).str.replace('.0', '', regex=False)
            df['site_address'] = df['site_address'].fillna('').astype(str)
            
            self.properties_df = df
            return df
            
        except Exception as e:
            logger.error(f"Error loading properties: {e}")
            raise
    
    def enhance_properties_with_geography(self, batch_size: int = 100) -> pd.DataFrame:
        """Add comprehensive geographic intelligence to all properties"""
        if self.properties_df is None:
            raise ValueError("No properties loaded. Call load_scored_properties first.")
        
        logger.info("🌍 Starting geographic enhancement of properties...")
        
        # Prepare enhanced dataframe
        enhanced_df = self.properties_df.copy()
        
        # Initialize new columns
        geo_columns = [
            'latitude', 'longitude',
            'dist_downtown_miles', 'dist_santa_monica_miles', 'dist_hollywood_miles',
            'dist_lax_miles', 'dist_ucla_miles', 'dist_usc_miles',
            'nearest_metro_station', 'nearest_metro_distance', 'nearest_metro_lines',
            'freeway_distance_miles', 'walkability_score',
            'location_premium_bonus', 'transit_accessibility_bonus', 'highway_access_bonus',
            'total_geographic_bonus', 'enhanced_development_score'
        ]
        
        for col in geo_columns:
            enhanced_df[col] = None
        
        # Process properties in batches
        total_properties = len(enhanced_df)
        processed = 0
        
        for batch_start in range(0, total_properties, batch_size):
            batch_end = min(batch_start + batch_size, total_properties)
            batch_df = enhanced_df.iloc[batch_start:batch_end].copy()
            
            logger.info(f"Processing geographic data for properties {batch_start+1}-{batch_end} of {total_properties}")
            
            # Process each property in the batch
            for idx, row in batch_df.iterrows():
                try:
                    # Get geographic metrics
                    geo_metrics = self.geo_engine.analyze_property_geography(row)
                    
                    # Get coordinates for this property
                    coords = self.geo_engine.geocode_address(row.get('site_address', ''), row.get('zip_code', ''))
                    
                    # Update the enhanced dataframe
                    enhanced_df.loc[idx, 'latitude'] = coords[0] if coords else None
                    enhanced_df.loc[idx, 'longitude'] = coords[1] if coords else None
                    enhanced_df.loc[idx, 'dist_downtown_miles'] = geo_metrics.distance_downtown
                    enhanced_df.loc[idx, 'dist_santa_monica_miles'] = geo_metrics.distance_santa_monica
                    enhanced_df.loc[idx, 'dist_hollywood_miles'] = geo_metrics.distance_hollywood
                    enhanced_df.loc[idx, 'dist_lax_miles'] = geo_metrics.distance_lax
                    enhanced_df.loc[idx, 'dist_ucla_miles'] = geo_metrics.distance_ucla
                    enhanced_df.loc[idx, 'dist_usc_miles'] = geo_metrics.distance_usc
                    enhanced_df.loc[idx, 'nearest_metro_station'] = geo_metrics.nearest_metro_name
                    enhanced_df.loc[idx, 'nearest_metro_distance'] = geo_metrics.nearest_metro_distance
                    enhanced_df.loc[idx, 'nearest_metro_lines'] = ','.join(geo_metrics.nearest_metro_lines)
                    enhanced_df.loc[idx, 'freeway_distance_miles'] = geo_metrics.freeway_distance
                    enhanced_df.loc[idx, 'walkability_score'] = geo_metrics.walkability_score
                    enhanced_df.loc[idx, 'location_premium_bonus'] = geo_metrics.location_premium
                    enhanced_df.loc[idx, 'transit_accessibility_bonus'] = geo_metrics.transit_bonus
                    enhanced_df.loc[idx, 'highway_access_bonus'] = geo_metrics.highway_bonus
                    
                    # Calculate total geographic bonus
                    total_geo_bonus = (geo_metrics.location_premium + 
                                     geo_metrics.transit_bonus + 
                                     geo_metrics.highway_bonus)
                    enhanced_df.loc[idx, 'total_geographic_bonus'] = round(total_geo_bonus, 2)
                    
                    # Calculate enhanced development score
                    original_score = row.get('development_score', 0)
                    enhanced_score = original_score + total_geo_bonus
                    enhanced_df.loc[idx, 'enhanced_development_score'] = round(enhanced_score, 2)
                    
                    processed += 1
                    
                except Exception as e:
                    logger.warning(f"Error processing property {idx}: {e}")
                    processed += 1
                    continue
            
            # Progress update
            logger.info(f"✓ Completed {processed}/{total_properties} properties ({processed/total_properties*100:.1f}%)")
        
        self.enhanced_df = enhanced_df
        logger.info("🎯 Geographic enhancement complete!")
        
        return enhanced_df
    
    def analyze_neighborhood_markets(self) -> Dict[str, Dict]:
        """Analyze neighborhood market characteristics and trends"""
        if self.enhanced_df is None:
            raise ValueError("No enhanced properties available. Run enhance_properties_with_geography first.")
        
        logger.info("📊 Analyzing neighborhood market characteristics...")
        
        neighborhood_stats = {}
        
        # Group by community plan area
        for community in self.enhanced_df['community_plan_area'].unique():
            if pd.isna(community) or community == '':
                continue
                
            community_properties = self.enhanced_df[
                self.enhanced_df['community_plan_area'] == community
            ].copy()
            
            if len(community_properties) < 3:  # Skip areas with too few properties
                continue
            
            # Calculate market metrics
            avg_total_value = community_properties['total_assessed_value'].mean()
            avg_land_value = community_properties['assessed_land_value'].mean()
            avg_improvement_value = community_properties['assessed_improvement_value'].mean()
            
            # Calculate land-to-improvement ratio (gentrification indicator)
            land_to_improvement_ratio = avg_land_value / max(avg_improvement_value, 1)
            
            # Calculate development score metrics
            avg_development_score = community_properties['development_score'].mean()
            avg_enhanced_score = community_properties['enhanced_development_score'].mean()
            
            # Calculate geographic advantages
            avg_walkability = community_properties['walkability_score'].mean()
            avg_metro_distance = community_properties['nearest_metro_distance'].mean()
            
            # Count TOC eligible properties
            toc_count = len(community_properties[community_properties['toc_eligible'] == True])
            toc_percentage = (toc_count / len(community_properties)) * 100
            
            # Identify dominant zoning
            zoning_counts = community_properties['base_zoning'].value_counts()
            dominant_zoning = zoning_counts.index[0] if len(zoning_counts) > 0 else 'Unknown'
            
            # Market velocity indicators
            high_land_ratio_count = len(community_properties[
                (community_properties['assessed_land_value'] / 
                 community_properties['total_assessed_value']) > 0.6
            ])
            
            neighborhood_stats[community] = {
                'property_count': len(community_properties),
                'avg_total_value': round(avg_total_value, 0),
                'avg_land_value': round(avg_land_value, 0),
                'avg_improvement_value': round(avg_improvement_value, 0),
                'land_to_improvement_ratio': round(land_to_improvement_ratio, 2),
                'avg_development_score': round(avg_development_score, 2),
                'avg_enhanced_score': round(avg_enhanced_score, 2),
                'geographic_score_boost': round(avg_enhanced_score - avg_development_score, 2),
                'avg_walkability': round(avg_walkability, 1),
                'avg_metro_distance': round(avg_metro_distance, 2),
                'toc_eligible_count': toc_count,
                'toc_percentage': round(toc_percentage, 1),
                'dominant_zoning': dominant_zoning,
                'gentrification_indicator': high_land_ratio_count,
                'market_tier': self._classify_market_tier(avg_enhanced_score, land_to_improvement_ratio, toc_percentage)
            }
        
        self.neighborhood_stats = neighborhood_stats
        logger.info(f"✓ Analyzed {len(neighborhood_stats)} neighborhoods")
        
        return neighborhood_stats
    
    def _classify_market_tier(self, enhanced_score: float, land_ratio: float, toc_percentage: float) -> str:
        """Classify neighborhood market tier based on key indicators"""
        if enhanced_score >= 50 and (land_ratio >= 2.0 or toc_percentage >= 20):
            return "Prime Growth"
        elif enhanced_score >= 40 and (land_ratio >= 1.5 or toc_percentage >= 10):
            return "Strong Potential"
        elif enhanced_score >= 35 and land_ratio >= 1.2:
            return "Emerging"
        elif enhanced_score >= 30:
            return "Stable"
        else:
            return "Developing"
    
    def identify_assembly_opportunities(self, proximity_threshold: float = 0.1) -> List[Dict]:
        """Identify properties suitable for land assembly"""
        if self.enhanced_df is None:
            raise ValueError("No enhanced properties available.")
        
        logger.info("🏗️ Identifying assembly opportunities...")
        
        assembly_opportunities = []
        processed_properties = set()
        
        # Only consider properties with coordinates and decent scores
        valid_properties = self.enhanced_df[
            (self.enhanced_df['latitude'].notna()) & 
            (self.enhanced_df['longitude'].notna()) &
            (self.enhanced_df['enhanced_development_score'] >= 35)  # Minimum score threshold
        ].copy()
        
        logger.info(f"Analyzing {len(valid_properties)} properties for assembly opportunities...")
        
        for idx, property_row in valid_properties.iterrows():
            if idx in processed_properties:
                continue
                
            property_lat = property_row['latitude']
            property_lng = property_row['longitude']
            
            # Find nearby properties
            nearby_properties = []
            
            for other_idx, other_row in valid_properties.iterrows():
                if other_idx == idx or other_idx in processed_properties:
                    continue
                
                other_lat = other_row['latitude']
                other_lng = other_row['longitude']
                
                # Calculate distance in miles (rough approximation)
                lat_diff = abs(property_lat - other_lat)
                lng_diff = abs(property_lng - other_lng)
                distance = ((lat_diff ** 2 + lng_diff ** 2) ** 0.5) * 69  # Convert to miles
                
                if distance <= proximity_threshold:
                    nearby_properties.append((other_idx, other_row, distance))
            
            # If we found nearby properties, this is an assembly opportunity
            if len(nearby_properties) >= 1:  # At least 2 properties total (original + 1 nearby)
                # Calculate assembly metrics
                all_properties = [(idx, property_row, 0.0)] + nearby_properties
                
                total_lot_size = sum([prop[1]['lot_size_sqft'] for prop in all_properties])
                avg_score = sum([prop[1]['enhanced_development_score'] for prop in all_properties]) / len(all_properties)
                total_assessed_value = sum([prop[1]['total_assessed_value'] for prop in all_properties])
                
                # Check if properties share similar characteristics
                zoning_types = [prop[1]['base_zoning'] for prop in all_properties]
                zoning_consistency = len(set(zoning_types)) <= 2  # Allow some zoning variation
                
                # Check if on same street (simple heuristic)
                addresses = [prop[1]['site_address'] for prop in all_properties if pd.notna(prop[1]['site_address'])]
                street_names = []
                for addr in addresses:
                    parts = str(addr).upper().split()
                    if len(parts) >= 2:
                        # Try to extract street name (skip number and direction)
                        street_part = ' '.join(parts[1:3])  # Take next 1-2 words after number
                        street_names.append(street_part)
                
                same_street = len(set(street_names)) == 1 if street_names else False
                
                assembly_opportunity = {
                    'opportunity_id': f"ASSEMBLY_{len(assembly_opportunities) + 1:03d}",
                    'property_count': len(all_properties),
                    'primary_property_id': property_row['property_id'],
                    'primary_apn': property_row['assessor_parcel_id'],
                    'primary_address': property_row['site_address'],
                    'nearby_properties': [
                        {
                            'property_id': prop[1]['property_id'],
                            'apn': prop[1]['assessor_parcel_id'], 
                            'address': prop[1]['site_address'],
                            'distance_miles': round(prop[2], 3),
                            'score': prop[1]['enhanced_development_score']
                        } for prop in nearby_properties
                    ],
                    'total_lot_size_sqft': total_lot_size,
                    'avg_assembly_score': round(avg_score, 2),
                    'total_assessed_value': total_assessed_value,
                    'zoning_consistency': zoning_consistency,
                    'same_street': same_street,
                    'community_plan_area': property_row['community_plan_area'],
                    'neighborhood_council': property_row['neighborhood_council'],
                    'assembly_potential': self._classify_assembly_potential(
                        len(all_properties), avg_score, total_lot_size, same_street, zoning_consistency
                    )
                }
                
                assembly_opportunities.append(assembly_opportunity)
                
                # Mark all properties in this assembly as processed
                for prop_idx, _, _ in all_properties:
                    processed_properties.add(prop_idx)
        
        # Sort by assembly potential and score
        assembly_opportunities.sort(key=lambda x: (
            x['assembly_potential'] == 'High',
            x['assembly_potential'] == 'Medium',
            x['avg_assembly_score']
        ), reverse=True)
        
        self.assembly_opportunities = assembly_opportunities
        logger.info(f"✓ Identified {len(assembly_opportunities)} assembly opportunities")
        
        return assembly_opportunities
    
    def _classify_assembly_potential(self, property_count: int, avg_score: float, 
                                   total_lot_size: float, same_street: bool, 
                                   zoning_consistency: bool) -> str:
        """Classify assembly opportunity potential"""
        score = 0
        
        # Property count scoring
        if property_count >= 4:
            score += 3
        elif property_count >= 3:
            score += 2
        else:
            score += 1
        
        # Development score scoring
        if avg_score >= 50:
            score += 3
        elif avg_score >= 40:
            score += 2
        elif avg_score >= 35:
            score += 1
        
        # Lot size scoring
        if total_lot_size >= 20000:  # 20k+ sqft
            score += 2
        elif total_lot_size >= 10000:  # 10k+ sqft
            score += 1
        
        # Location consistency bonuses
        if same_street:
            score += 2
        if zoning_consistency:
            score += 1
        
        # Classify based on total score
        if score >= 8:
            return "High"
        elif score >= 5:
            return "Medium"
        else:
            return "Low"
    
    def update_investment_tiers(self) -> pd.DataFrame:
        """Update investment tiers based on enhanced scores"""
        if self.enhanced_df is None:
            raise ValueError("No enhanced properties available.")
        
        logger.info("🎯 Updating investment tiers with geographic enhancements...")
        
        def classify_enhanced_tier(score):
            if score >= 80:
                return "A+"
            elif score >= 70:
                return "A"
            elif score >= 60:
                return "B"
            elif score >= 50:
                return "C+"
            elif score >= 40:
                return "C"
            else:
                return "D"
        
        self.enhanced_df['enhanced_investment_tier'] = self.enhanced_df['enhanced_development_score'].apply(
            classify_enhanced_tier
        )
        
        # Update suggested use based on enhanced score and geographic factors
        self.enhanced_df['enhanced_suggested_use'] = self.enhanced_df.apply(
            self._determine_enhanced_use, axis=1
        )
        
        logger.info("✓ Investment tiers updated with geographic intelligence")
        
        return self.enhanced_df
    
    def _determine_enhanced_use(self, row) -> str:
        """Determine enhanced suggested use based on geographic factors"""
        enhanced_score = row['enhanced_development_score']
        original_use = row['suggested_use']
        location_bonus = row['total_geographic_bonus']
        
        # High location bonus suggests different strategies
        if location_bonus >= 15:  # Significant location advantages
            if enhanced_score >= 60:
                return "Premium Development Opportunity"
            elif enhanced_score >= 50:
                return "Transit-Oriented Development"
            elif enhanced_score >= 40:
                return "Location Premium Hold"
        
        # Assembly opportunities
        if any(row['property_id'] == opp['primary_property_id'] for opp in self.assembly_opportunities):
            return "Assembly Opportunity Lead"
        
        # Transit-specific opportunities
        if row['nearest_metro_distance'] <= 0.5:
            if enhanced_score >= 45:
                return "TOD Development Site"
            else:
                return "Transit-Adjacent Hold"
        
        # Default to original use with enhancement note
        if location_bonus >= 5:
            return f"{original_use} + Location Premium"
        else:
            return original_use
    
    def export_enhanced_dataset(self, output_path: str = "geo_enhanced_scored_properties.csv") -> str:
        """Export the enhanced dataset with all geographic intelligence"""
        if self.enhanced_df is None:
            raise ValueError("No enhanced data to export.")
        
        logger.info(f"📊 Exporting enhanced dataset to: {output_path}")
        
        # Ensure output directory exists
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Export main dataset
        self.enhanced_df.to_csv(output_path, index=False)
        
        # Export neighborhood analysis
        neighborhood_path = output_path.replace('.csv', '_neighborhood_analysis.json')
        with open(neighborhood_path, 'w') as f:
            json.dump(self.neighborhood_stats, f, indent=2, default=str)
        
        # Export assembly opportunities
        assembly_path = output_path.replace('.csv', '_assembly_opportunities.json')
        with open(assembly_path, 'w') as f:
            json.dump(self.assembly_opportunities, f, indent=2, default=str)
        
        # Create summary report
        summary_path = output_path.replace('.csv', '_summary_report.txt')
        self._create_summary_report(summary_path)
        
        logger.info("✅ Enhanced dataset export complete!")
        logger.info(f"   Main dataset: {output_path}")
        logger.info(f"   Neighborhood analysis: {neighborhood_path}")
        logger.info(f"   Assembly opportunities: {assembly_path}")
        logger.info(f"   Summary report: {summary_path}")
        
        return output_path
    
    def _create_summary_report(self, report_path: str):
        """Create comprehensive summary report"""
        with open(report_path, 'w') as f:
            f.write("DEALGENIE GEOGRAPHIC INTELLIGENCE SUMMARY REPORT\n")
            f.write("=" * 60 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Dataset overview
            f.write("DATASET OVERVIEW\n")
            f.write("-" * 30 + "\n")
            f.write(f"Total Properties Analyzed: {len(self.enhanced_df)}\n")
            f.write(f"Properties with Coordinates: {len(self.enhanced_df[self.enhanced_df['latitude'].notna()])}\n")
            f.write(f"Average Geographic Bonus: {self.enhanced_df['total_geographic_bonus'].mean():.2f} points\n")
            f.write(f"Max Geographic Bonus: {self.enhanced_df['total_geographic_bonus'].max():.2f} points\n\n")
            
            # Enhanced tier distribution
            f.write("ENHANCED INVESTMENT TIER DISTRIBUTION\n")
            f.write("-" * 40 + "\n")
            tier_counts = self.enhanced_df['enhanced_investment_tier'].value_counts().sort_index()
            for tier, count in tier_counts.items():
                percentage = (count / len(self.enhanced_df)) * 100
                f.write(f"Tier {tier}: {count} properties ({percentage:.1f}%)\n")
            f.write("\n")
            
            # Top neighborhoods
            f.write("TOP NEIGHBORHOODS BY ENHANCED SCORE\n")
            f.write("-" * 40 + "\n")
            sorted_neighborhoods = sorted(
                self.neighborhood_stats.items(),
                key=lambda x: x[1]['avg_enhanced_score'],
                reverse=True
            )[:10]
            
            for i, (name, stats) in enumerate(sorted_neighborhoods, 1):
                f.write(f"{i:2d}. {name}\n")
                f.write(f"    Enhanced Score: {stats['avg_enhanced_score']:.1f}\n")
                f.write(f"    Geographic Boost: +{stats['geographic_score_boost']:.1f} pts\n")
                f.write(f"    Market Tier: {stats['market_tier']}\n")
                f.write(f"    Property Count: {stats['property_count']}\n\n")
            
            # Assembly opportunities
            f.write("ASSEMBLY OPPORTUNITIES SUMMARY\n")
            f.write("-" * 35 + "\n")
            f.write(f"Total Opportunities Identified: {len(self.assembly_opportunities)}\n")
            
            high_potential = [opp for opp in self.assembly_opportunities if opp['assembly_potential'] == 'High']
            medium_potential = [opp for opp in self.assembly_opportunities if opp['assembly_potential'] == 'Medium']
            
            f.write(f"High Potential: {len(high_potential)}\n")
            f.write(f"Medium Potential: {len(medium_potential)}\n\n")
            
            if high_potential:
                f.write("TOP 5 HIGH-POTENTIAL ASSEMBLY OPPORTUNITIES:\n")
                for i, opp in enumerate(high_potential[:5], 1):
                    f.write(f"{i}. {opp['opportunity_id']}\n")
                    f.write(f"   Address: {opp['primary_address']}\n")
                    f.write(f"   Property Count: {opp['property_count']}\n")
                    f.write(f"   Avg Score: {opp['avg_assembly_score']}\n")
                    f.write(f"   Total Lot Size: {opp['total_lot_size_sqft']:,} sqft\n")
                    f.write(f"   Same Street: {'Yes' if opp['same_street'] else 'No'}\n\n")

def main():
    """Main processing function"""
    logger.info("🌍 DEALGENIE GEOGRAPHIC INTELLIGENCE PROCESSOR")
    logger.info("=" * 60)
    
    # Initialize processor
    processor = GeographicPropertyEnhancer()
    
    # Load scored properties
    scored_properties_path = "sample_data/scored_zimas_properties.csv"
    processor.load_scored_properties(scored_properties_path)
    
    # Enhance with geography
    enhanced_df = processor.enhance_properties_with_geography()
    
    # Analyze neighborhoods
    neighborhood_stats = processor.analyze_neighborhood_markets()
    
    # Identify assembly opportunities
    assembly_opportunities = processor.identify_assembly_opportunities()
    
    # Update investment tiers
    processor.update_investment_tiers()
    
    # Export enhanced dataset
    output_path = processor.export_enhanced_dataset()
    
    logger.info("🎯 GEOGRAPHIC INTELLIGENCE PROCESSING COMPLETE!")
    logger.info(f"Enhanced dataset available at: {output_path}")

if __name__ == "__main__":
    main()