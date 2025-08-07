#!/usr/bin/env python3
"""
LA Property Map Visualization Generator
Create interactive HTML map showing enhanced scored properties

Features:
- Top 50 scoring properties color-coded by tier
- Metro stations and transit lines
- Assembly opportunity clusters
- Neighborhood boundaries and names
- Interactive tooltips with property details

Author: DealGenie AI Engine
Version: 1.0
"""

import pandas as pd
import folium
from folium.plugins import MarkerCluster, HeatMap
import json
import logging
from typing import Dict, List, Tuple

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LAPropertyMapGenerator:
    """Generate interactive map visualization for LA properties"""
    
    def __init__(self):
        self.properties_df = None
        self.assembly_opportunities = []
        self.neighborhood_stats = {}
        
        # LA Map center coordinates
        self.la_center = [34.0522, -118.2437]  # Downtown LA
        
        # Color scheme for investment tiers
        self.tier_colors = {
            'A+': '#FF0000',  # Red - Premium
            'A': '#FF4500',   # Orange Red - Excellent
            'B': '#FFA500',   # Orange - Good
            'C+': '#FFD700',  # Gold - Above Average
            'C': '#FFFF00',   # Yellow - Average
            'D': '#87CEEB'    # Light Blue - Below Average
        }
        
        # Metro line colors
        self.metro_line_colors = {
            'Red': '#DC143C',
            'Purple': '#800080', 
            'Blue': '#0000FF',
            'Green': '#008000',
            'Gold': '#FFD700',
            'Orange': '#FFA500',
            'Expo': '#00CED1',
            'Silver': '#C0C0C0'
        }
    
    def load_enhanced_data(self, 
                          properties_file: str = "geo_enhanced_scored_properties.csv",
                          assembly_file: str = "geo_enhanced_scored_properties_assembly_opportunities.json",
                          neighborhood_file: str = "geo_enhanced_scored_properties_neighborhood_analysis.json"):
        """Load enhanced property data and analysis results"""
        logger.info("Loading enhanced property data for map visualization...")
        
        # Load properties
        self.properties_df = pd.read_csv(properties_file)
        logger.info(f"Loaded {len(self.properties_df)} enhanced properties")
        
        # Load assembly opportunities
        with open(assembly_file, 'r') as f:
            self.assembly_opportunities = json.load(f)
        logger.info(f"Loaded {len(self.assembly_opportunities)} assembly opportunities")
        
        # Load neighborhood analysis
        with open(neighborhood_file, 'r') as f:
            self.neighborhood_stats = json.load(f)
        logger.info(f"Loaded analysis for {len(self.neighborhood_stats)} neighborhoods")
    
    def create_base_map(self) -> folium.Map:
        """Create base map of LA with appropriate zoom and styling"""
        # Create map centered on LA
        m = folium.Map(
            location=self.la_center,
            zoom_start=10,
            tiles='OpenStreetMap',
            prefer_canvas=True
        )
        
        # Add alternative tile layers
        folium.TileLayer(
            'CartoDB positron',
            name='Light Map'
        ).add_to(m)
        
        folium.TileLayer(
            'CartoDB dark_matter',
            name='Dark Map'
        ).add_to(m)
        
        return m
    
    def add_metro_stations(self, map_obj: folium.Map):
        """Add Metro Rail stations to the map"""
        logger.info("Adding Metro stations to map...")
        
        # Define major Metro stations with coordinates
        metro_stations = [
            # Red/Purple Line
            {"name": "Hollywood/Highland", "lat": 34.1022, "lng": -118.3390, "lines": ["Red"]},
            {"name": "Hollywood/Vine", "lat": 34.1016, "lng": -118.3267, "lines": ["Red"]},
            {"name": "Universal City", "lat": 34.1384, "lng": -118.3533, "lines": ["Red"]},
            {"name": "North Hollywood", "lat": 34.1688, "lng": -118.3768, "lines": ["Red", "Orange"]},
            {"name": "Wilshire/Vermont", "lat": 34.0619, "lng": -118.2915, "lines": ["Red", "Purple"]},
            {"name": "MacArthur Park", "lat": 34.0572, "lng": -118.2752, "lines": ["Red", "Purple"]},
            {"name": "7th St/Metro Center", "lat": 34.0485, "lng": -118.2592, "lines": ["Red", "Purple", "Blue"]},
            {"name": "Union Station", "lat": 34.0560, "lng": -118.2345, "lines": ["Red", "Purple", "Gold"]},
            
            # Blue Line
            {"name": "Downtown Long Beach", "lat": 33.7700, "lng": -118.1937, "lines": ["Blue"]},
            {"name": "7th St/Metro Center", "lat": 34.0485, "lng": -118.2592, "lines": ["Blue", "Red", "Purple"]},
            
            # Gold Line
            {"name": "Atlantic", "lat": 34.0315, "lng": -118.1340, "lines": ["Gold"]},
            {"name": "East LA Civic Center", "lat": 34.0315, "lng": -118.1540, "lines": ["Gold"]},
            {"name": "Little Tokyo/Arts District", "lat": 34.0506, "lng": -118.2378, "lines": ["Gold"]},
            
            # Green Line
            {"name": "Redondo Beach", "lat": 33.8486, "lng": -118.3890, "lines": ["Green"]},
            {"name": "LAX/City Bus Center", "lat": 33.9425, "lng": -118.3815, "lines": ["Green"]},
            {"name": "Norwalk", "lat": 33.9022, "lng": -118.0817, "lines": ["Green"]},
            
            # Expo Line
            {"name": "Downtown Santa Monica", "lat": 34.0074, "lng": -118.4852, "lines": ["Expo"]},
            {"name": "Culver City", "lat": 34.0074, "lng": -118.3950, "lines": ["Expo"]},
            {"name": "Expo Park/USC", "lat": 34.0183, "lng": -118.2851, "lines": ["Expo"]},
        ]
        
        # Create Metro station markers
        metro_group = folium.FeatureGroup(name="Metro Stations")
        
        for station in metro_stations:
            # Determine marker color based on lines
            if len(station['lines']) > 2:
                color = 'black'  # Multi-line transfer station
            elif 'Red' in station['lines'] or 'Purple' in station['lines']:
                color = 'red'
            elif 'Blue' in station['lines']:
                color = 'blue'
            elif 'Gold' in station['lines']:
                color = 'orange'
            elif 'Green' in station['lines']:
                color = 'green'
            else:
                color = 'gray'
            
            popup_html = f"""
            <div style="font-family: Arial; font-size: 12px; width: 200px;">
                <h4 style="margin: 5px 0; color: #333;">{station['name']}</h4>
                <p><strong>Lines:</strong> {', '.join(station['lines'])}</p>
                <p><strong>Type:</strong> Metro Rail Station</p>
            </div>
            """
            
            folium.Marker(
                location=[station['lat'], station['lng']],
                popup=folium.Popup(popup_html, max_width=250),
                tooltip=f"{station['name']} ({', '.join(station['lines'])} Line)",
                icon=folium.Icon(
                    color=color,
                    icon='train',
                    prefix='fa'
                )
            ).add_to(metro_group)
        
        metro_group.add_to(map_obj)
    
    def add_top_properties(self, map_obj: folium.Map, top_n: int = 50):
        """Add top scoring properties to the map"""
        logger.info(f"Adding top {top_n} properties to map...")
        
        # Filter valid properties with coordinates
        valid_properties = self.properties_df[
            (self.properties_df['latitude'].notna()) & 
            (self.properties_df['longitude'].notna())
        ].copy()
        
        # Sort by enhanced score and take top N
        top_properties = valid_properties.nlargest(top_n, 'enhanced_development_score')
        
        # Create property markers group
        properties_group = folium.FeatureGroup(name=f"Top {top_n} Properties")
        
        for idx, prop in top_properties.iterrows():
            tier = prop['enhanced_investment_tier']
            color = self.tier_colors.get(tier, '#808080')
            
            # Create detailed popup
            popup_html = f"""
            <div style="font-family: Arial; font-size: 12px; width: 300px;">
                <h4 style="margin: 5px 0; color: #333;">Property #{prop['property_id']}</h4>
                <p><strong>Address:</strong> {prop['site_address']}</p>
                <p><strong>Enhanced Score:</strong> {prop['enhanced_development_score']:.1f} 
                   <span style="color: green;">(+{prop['total_geographic_bonus']:.1f} geo bonus)</span></p>
                <p><strong>Investment Tier:</strong> <span style="color: {color}; font-weight: bold;">{tier}</span></p>
                <p><strong>Zoning:</strong> {prop['base_zoning']}</p>
                <p><strong>Lot Size:</strong> {prop['lot_size_sqft']:,.0f} sq ft</p>
                <p><strong>Assessed Value:</strong> ${prop['total_assessed_value']:,.0f}</p>
                <p><strong>Enhanced Use:</strong> {prop['enhanced_suggested_use']}</p>
                <hr style="margin: 8px 0;">
                <p><strong>Location Intelligence:</strong></p>
                <p>• Distance to Downtown: {prop['dist_downtown_miles']:.1f} mi</p>
                <p>• Distance to Santa Monica: {prop['dist_santa_monica_miles']:.1f} mi</p>
                <p>• Nearest Metro: {prop['nearest_metro_station']} ({prop['nearest_metro_distance']:.1f} mi)</p>
                <p>• Walkability Score: {prop['walkability_score']}/100</p>
                <p>• Geographic Bonuses: Location +{prop['location_premium_bonus']:.1f}, Transit +{prop['transit_accessibility_bonus']:.1f}, Highway +{prop['highway_access_bonus']:.1f}</p>
            </div>
            """
            
            # Create circle marker sized by score
            radius = max(8, min(20, prop['enhanced_development_score'] / 3))  # Scale radius
            
            folium.CircleMarker(
                location=[prop['latitude'], prop['longitude']],
                radius=radius,
                popup=folium.Popup(popup_html, max_width=350),
                tooltip=f"{prop['site_address']} - Score: {prop['enhanced_development_score']:.1f} (Tier {tier})",
                color='black',
                weight=2,
                fill=True,
                fillColor=color,
                fillOpacity=0.8
            ).add_to(properties_group)
        
        properties_group.add_to(map_obj)
        
        return top_properties
    
    def add_assembly_opportunities(self, map_obj: folium.Map):
        """Add assembly opportunity clusters to the map"""
        logger.info("Adding assembly opportunities to map...")
        
        assembly_group = folium.FeatureGroup(name="Assembly Opportunities")
        
        # Filter high and medium potential opportunities
        high_med_opportunities = [
            opp for opp in self.assembly_opportunities 
            if opp['assembly_potential'] in ['High', 'Medium']
        ]
        
        for i, opp in enumerate(high_med_opportunities[:20]):  # Top 20 opportunities
            # Use primary property coordinates as cluster center
            # Find primary property in our dataset
            primary_prop = self.properties_df[
                self.properties_df['property_id'] == opp['primary_property_id']
            ]
            
            if len(primary_prop) == 0 or pd.isna(primary_prop.iloc[0]['latitude']):
                continue
                
            lat = primary_prop.iloc[0]['latitude']
            lng = primary_prop.iloc[0]['longitude']
            
            # Determine cluster color
            cluster_color = '#FF6B6B' if opp['assembly_potential'] == 'High' else '#4ECDC4'
            
            popup_html = f"""
            <div style="font-family: Arial; font-size: 12px; width: 350px;">
                <h4 style="margin: 5px 0; color: #333;">Assembly Opportunity #{i+1}</h4>
                <p><strong>Potential:</strong> <span style="color: {'#FF6B6B' if opp['assembly_potential'] == 'High' else '#4ECDC4'}; font-weight: bold;">{opp['assembly_potential']}</span></p>
                <p><strong>Primary Address:</strong> {opp['primary_address']}</p>
                <p><strong>Property Count:</strong> {opp['property_count']} properties</p>
                <p><strong>Total Lot Size:</strong> {opp['total_lot_size_sqft']:,.0f} sq ft</p>
                <p><strong>Avg Assembly Score:</strong> {opp['avg_assembly_score']:.1f}</p>
                <p><strong>Total Assessed Value:</strong> ${opp['total_assessed_value']:,.0f}</p>
                <p><strong>Same Street:</strong> {'Yes' if opp['same_street'] else 'No'}</p>
                <p><strong>Zoning Consistent:</strong> {'Yes' if opp['zoning_consistency'] else 'No'}</p>
                <p><strong>Neighborhood:</strong> {opp['community_plan_area']}</p>
                <hr style="margin: 8px 0;">
                <p><strong>Nearby Properties:</strong></p>
            """
            
            for nearby in opp['nearby_properties'][:3]:  # Show first 3 nearby properties
                popup_html += f"<p style='margin-left: 15px;'>• {nearby['address']} ({nearby['distance_miles']:.3f} mi, Score: {nearby['score']:.1f})</p>"
            
            if len(opp['nearby_properties']) > 3:
                popup_html += f"<p style='margin-left: 15px;'>... and {len(opp['nearby_properties']) - 3} more</p>"
            
            popup_html += "</div>"
            
            # Create assembly cluster marker
            folium.Marker(
                location=[lat, lng],
                popup=folium.Popup(popup_html, max_width=400),
                tooltip=f"Assembly Opportunity - {opp['property_count']} properties",
                icon=folium.Icon(
                    color='red' if opp['assembly_potential'] == 'High' else 'lightblue',
                    icon='home',
                    prefix='fa'
                )
            ).add_to(assembly_group)
            
            # Add circle to show assembly area
            folium.Circle(
                location=[lat, lng],
                radius=200,  # 200 meter radius
                popup=f"Assembly Cluster {i+1}",
                color=cluster_color,
                weight=3,
                fill=True,
                fillColor=cluster_color,
                fillOpacity=0.2
            ).add_to(assembly_group)
        
        assembly_group.add_to(map_obj)
    
    def add_neighborhood_boundaries(self, map_obj: folium.Map):
        """Add neighborhood information and boundaries"""
        logger.info("Adding neighborhood information...")
        
        # For this demo, we'll add markers for top neighborhoods
        # In production, you'd use actual boundary polygons
        
        neighborhood_group = folium.FeatureGroup(name="Top Neighborhoods")
        
        # Get top 10 neighborhoods by enhanced score
        sorted_neighborhoods = sorted(
            self.neighborhood_stats.items(),
            key=lambda x: x[1]['avg_enhanced_score'],
            reverse=True
        )[:10]
        
        # Approximate neighborhood center coordinates (simplified)
        neighborhood_centers = {
            'Hollywood': [34.1022, -118.3390],
            'West Hollywood': [34.0900, -118.3617],
            'Beverly Hills': [34.0736, -118.4004],
            'Santa Monica': [34.0194, -118.4912],
            'Culver City': [34.0211, -118.3965],
            'West Los Angeles': [34.0450, -118.4430],
            'Silver Lake': [34.0900, -118.2740],
            'Los Feliz': [34.1070, -118.2840],
            'Mid City West': [34.0730, -118.3440],
            'Koreatown': [34.0580, -118.3050]
        }
        
        for i, (neighborhood, stats) in enumerate(sorted_neighborhoods):
            # Try to find approximate coordinates
            coords = None
            for area_name, center_coords in neighborhood_centers.items():
                if area_name.lower() in neighborhood.lower():
                    coords = center_coords
                    break
            
            if not coords:
                # Skip if we don't have coordinates
                continue
            
            # Determine marker color based on market tier
            tier_colors = {
                'Prime Growth': '#FF0000',
                'Strong Potential': '#FF8C00', 
                'Emerging': '#FFD700',
                'Stable': '#90EE90',
                'Developing': '#87CEEB'
            }
            
            color = tier_colors.get(stats['market_tier'], '#808080')
            
            popup_html = f"""
            <div style="font-family: Arial; font-size: 12px; width: 280px;">
                <h4 style="margin: 5px 0; color: #333;">{neighborhood}</h4>
                <p><strong>Market Tier:</strong> <span style="color: {color}; font-weight: bold;">{stats['market_tier']}</span></p>
                <p><strong>Property Count:</strong> {stats['property_count']}</p>
                <p><strong>Avg Enhanced Score:</strong> {stats['avg_enhanced_score']:.1f}</p>
                <p><strong>Geographic Boost:</strong> +{stats['geographic_score_boost']:.1f} pts</p>
                <p><strong>Avg Walkability:</strong> {stats['avg_walkability']:.0f}/100</p>
                <p><strong>TOC Eligible:</strong> {stats['toc_percentage']:.1f}%</p>
                <p><strong>Avg Total Value:</strong> ${stats['avg_total_value']:,.0f}</p>
                <p><strong>Land/Improvement Ratio:</strong> {stats['land_to_improvement_ratio']:.2f}</p>
                <p><strong>Dominant Zoning:</strong> {stats['dominant_zoning']}</p>
            </div>
            """
            
            folium.CircleMarker(
                location=coords,
                radius=15,
                popup=folium.Popup(popup_html, max_width=320),
                tooltip=f"{neighborhood} - {stats['market_tier']}",
                color='black',
                weight=2,
                fill=True,
                fillColor=color,
                fillOpacity=0.6
            ).add_to(neighborhood_group)
        
        neighborhood_group.add_to(map_obj)
    
    def create_legend(self, map_obj: folium.Map):
        """Add legend to the map"""
        legend_html = """
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 200px; height: 200px; 
                    background-color: white; border:2px solid grey; z-index:9999; 
                    font-size:12px; padding: 10px;
                    ">
        <h4>Investment Tiers</h4>
        <p><i class="fa fa-circle" style="color:#FF0000"></i> A+ Premium</p>
        <p><i class="fa fa-circle" style="color:#FF4500"></i> A Excellent</p>
        <p><i class="fa fa-circle" style="color:#FFA500"></i> B Good</p>
        <p><i class="fa fa-circle" style="color:#FFD700"></i> C+ Above Avg</p>
        <p><i class="fa fa-circle" style="color:#FFFF00"></i> C Average</p>
        <p><i class="fa fa-circle" style="color:#87CEEB"></i> D Below Avg</p>
        
        <h4>Symbols</h4>
        <p><i class="fa fa-train" style="color:red"></i> Metro Stations</p>
        <p><i class="fa fa-home" style="color:red"></i> Assembly Opps</p>
        </div>
        """
        map_obj.get_root().html.add_child(folium.Element(legend_html))
    
    def generate_map(self, output_file: str = "la_property_intelligence_map.html") -> str:
        """Generate comprehensive LA property intelligence map"""
        logger.info("🗺️ Generating LA Property Intelligence Map...")
        
        # Create base map
        map_obj = self.create_base_map()
        
        # Add all map layers
        self.add_metro_stations(map_obj)
        top_properties = self.add_top_properties(map_obj)
        self.add_assembly_opportunities(map_obj)
        self.add_neighborhood_boundaries(map_obj)
        
        # Add legend
        self.create_legend(map_obj)
        
        # Add layer control
        folium.LayerControl().add_to(map_obj)
        
        # Save map
        map_obj.save(output_file)
        
        logger.info(f"✅ Map generated successfully: {output_file}")
        logger.info(f"   Top properties plotted: {len(top_properties)}")
        logger.info(f"   Assembly opportunities: {len(self.assembly_opportunities)}")
        logger.info(f"   Neighborhoods analyzed: {len(self.neighborhood_stats)}")
        
        return output_file

def main():
    """Generate the LA property intelligence map"""
    generator = LAPropertyMapGenerator()
    
    # Load enhanced data
    generator.load_enhanced_data()
    
    # Generate map
    map_file = generator.generate_map()
    
    print(f"🗺️ LA Property Intelligence Map created: {map_file}")
    print("Open this file in your web browser to explore the interactive map!")

if __name__ == "__main__":
    main()