#!/usr/bin/env python3
"""
User-Customizable Search Preferences System
Allows users to adjust filter importance weights and set hard limits with real-time ranking updates
"""

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
import sqlite3
import time
import json
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import sys
import os

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from data_source_attribution import DataSourceLinkGenerator

app = FastAPI(
    title="User-Customizable Property Search System", 
    version="4.0.0",
    description="Property search with customizable user preferences and real-time ranking adjustments"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SEARCH_DB = "search_idx_parcel.db"

# Initialize components
source_generator = DataSourceLinkGenerator()

# User preferences storage (in production this would be in database)
user_preferences = {}
user_sessions = {}

class SearchWeights(BaseModel):
    """User-customizable search weights"""
    crime_weight: float = Field(default=35.0, ge=0.0, le=50.0, description="Crime score importance (0-50%)")
    location_weight: float = Field(default=25.0, ge=0.0, le=40.0, description="Location premium importance (0-40%)")
    property_type_weight: float = Field(default=20.0, ge=0.0, le=30.0, description="Property type relevance (0-30%)")
    development_weight: float = Field(default=15.0, ge=0.0, le=25.0, description="Development potential (0-25%)")
    data_quality_weight: float = Field(default=5.0, ge=0.0, le=15.0, description="Data quality importance (0-15%)")

class HardLimits(BaseModel):
    """User-defined hard filter limits"""
    max_crime_score: Optional[float] = Field(default=None, ge=0.0, le=100.0, description="Maximum acceptable crime score")
    min_property_value: Optional[float] = Field(default=None, ge=0.0, description="Minimum property value")
    min_property_size: Optional[float] = Field(default=None, ge=0.0, description="Minimum property size (sqft)")
    required_zoning_types: Optional[List[str]] = Field(default=None, description="Required zoning types only")
    max_year_built_age: Optional[int] = Field(default=None, ge=0, le=200, description="Maximum property age (years)")

class UserPreferences(BaseModel):
    """Complete user preference profile"""
    user_id: str
    preference_name: str
    weights: SearchWeights
    hard_limits: HardLimits
    created_at: str
    last_used: str

class PreferenceProfile(BaseModel):
    """Predefined preference profiles"""
    profile_name: str
    description: str
    weights: SearchWeights
    hard_limits: HardLimits

# Predefined preference profiles
PREFERENCE_PROFILES = {
    "safety_focused": PreferenceProfile(
        profile_name="Safety-Focused Buyer",
        description="Prioritizes low crime areas above all else",
        weights=SearchWeights(
            crime_weight=50.0,
            location_weight=20.0,
            property_type_weight=15.0,
            development_weight=10.0,
            data_quality_weight=5.0
        ),
        hard_limits=HardLimits(
            max_crime_score=30.0,
            min_property_value=None,
            min_property_size=None,
            required_zoning_types=None,
            max_year_built_age=None
        )
    ),
    "location_premium": PreferenceProfile(
        profile_name="Location Premium Hunter",
        description="Values prime locations and property type match",
        weights=SearchWeights(
            crime_weight=20.0,
            location_weight=40.0,
            property_type_weight=30.0,
            development_weight=5.0,
            data_quality_weight=5.0
        ),
        hard_limits=HardLimits(
            max_crime_score=60.0,
            min_property_value=500000.0,
            min_property_size=1200.0,
            required_zoning_types=["R1", "R2", "RD"],
            max_year_built_age=None
        )
    ),
    "development_investor": PreferenceProfile(
        profile_name="Development Investor",
        description="Focuses on development potential and value opportunities",
        weights=SearchWeights(
            crime_weight=15.0,
            location_weight=25.0,
            property_type_weight=10.0,
            development_weight=25.0,
            data_quality_weight=10.0
        ),
        hard_limits=HardLimits(
            max_crime_score=75.0,
            min_property_value=None,
            min_property_size=5000.0,
            required_zoning_types=["C1", "C2", "M1", "LAR"],
            max_year_built_age=50
        )
    ),
    "balanced_buyer": PreferenceProfile(
        profile_name="Balanced Home Buyer",
        description="Balanced approach considering all factors equally",
        weights=SearchWeights(
            crime_weight=25.0,
            location_weight=25.0,
            property_type_weight=25.0,
            development_weight=15.0,
            data_quality_weight=10.0
        ),
        hard_limits=HardLimits(
            max_crime_score=50.0,
            min_property_value=300000.0,
            min_property_size=800.0,
            required_zoning_types=None,
            max_year_built_age=None
        )
    )
}

def get_db_connection():
    """Get database connection"""
    try:
        conn = sqlite3.connect(SEARCH_DB)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")

def calculate_weighted_score(property_data: Dict, weights: SearchWeights) -> float:
    """Calculate weighted property score based on user preferences"""
    
    # Normalize weights to percentages
    total_weight = (weights.crime_weight + weights.location_weight + 
                   weights.property_type_weight + weights.development_weight + 
                   weights.data_quality_weight)
    
    if total_weight == 0:
        total_weight = 100.0
    
    # Calculate individual scores (0-100 scale) using actual database columns
    # Use actual crime_score from database, handle None values
    crime_raw = property_data.get('crime_score')
    crime_score = max(0, 100 - (crime_raw if crime_raw is not None else 50))  # Lower crime = higher score
    
    # Estimate location score based on coordinates (proximity to city centers)
    lat = property_data.get('latitude', 34.0522)
    lng = property_data.get('longitude', -118.2437)
    # Simple distance from downtown LA as location premium proxy
    downtown_distance = abs(lat - 34.0522) + abs(lng + 118.2437)
    location_score = max(0, min(100, 100 - (downtown_distance * 1000)))  # Closer = better
    
    # Property type score based on zoning (commercial/mixed use = higher development)
    zoning = property_data.get('zoning_code', '')
    if any(zone in zoning.upper() for zone in ['C1', 'C2', 'M1', 'LAR', 'RD']):
        property_type_score = 80
    elif any(zone in zoning.upper() for zone in ['R1', 'R2', 'R3']):
        property_type_score = 60
    else:
        property_type_score = 50
    
    # Development score based on property age and size
    year_built = property_data.get('year_built')
    sqft = property_data.get('sqft')
    age = 2024 - year_built if year_built and year_built > 1900 else 50
    size_factor = min(100, (sqft / 50) if sqft and sqft > 0 else 50)  # Normalize by size
    development_score = max(0, min(100, (100 - age) * 0.5 + size_factor * 0.5))
    
    # Data quality score from actual database field
    data_quality_raw = property_data.get('data_quality')
    data_quality_score = min(100, (data_quality_raw if data_quality_raw is not None else 50))
    
    # Apply weights
    weighted_score = (
        (crime_score * weights.crime_weight / total_weight) +
        (location_score * weights.location_weight / total_weight) +
        (property_type_score * weights.property_type_weight / total_weight) +
        (development_score * weights.development_weight / total_weight) +
        (data_quality_score * weights.data_quality_weight / total_weight)
    )
    
    return round(weighted_score, 2)

def apply_hard_limits(properties: List[Dict], hard_limits: HardLimits) -> List[Dict]:
    """Apply hard limit filters to property list"""
    
    filtered_properties = []
    
    for prop in properties:
        # Check crime score limit
        if hard_limits.max_crime_score is not None:
            if prop.get('crime_score', 100) > hard_limits.max_crime_score:
                continue
        
        # Check property value limit
        if hard_limits.min_property_value is not None:
            # Use rough estimate based on sqft since we don't have actual property values
            prop_size = prop.get('sqft', 0)
            estimated_value = prop_size * 400 if prop_size > 0 else 0  # $400/sqft rough estimate
            if estimated_value < hard_limits.min_property_value:
                continue
        
        # Check property size limit
        if hard_limits.min_property_size is not None:
            prop_size = prop.get('sqft', 0)
            if prop_size < hard_limits.min_property_size:
                continue
        
        # Check zoning type requirements
        if hard_limits.required_zoning_types is not None:
            prop_zoning = prop.get('zoning_code', '')
            if not any(req_zone in prop_zoning for req_zone in hard_limits.required_zoning_types):
                continue
        
        # Check property age limit
        if hard_limits.max_year_built_age is not None:
            year_built = prop.get('year_built', 0)
            if year_built > 0:
                property_age = datetime.now().year - year_built
                if property_age > hard_limits.max_year_built_age:
                    continue
        
        filtered_properties.append(prop)
    
    return filtered_properties

def enhance_property_with_scores(property_data: Dict) -> Dict:
    """Enhance property with simulated scoring factors"""
    enhanced = property_data.copy()
    
    # Add simulated scoring factors (in production these would be calculated from real data)
    enhanced['location_premium'] = min(100, max(0, 80 - abs(property_data.get('latitude', 34.0) - 34.05) * 1000))
    enhanced['type_match_score'] = 75  # Simulated property type match score
    enhanced['development_potential'] = min(100, (property_data.get('sqft', 1000) / 100) + 
                                          (50 if 'commercial' in str(property_data.get('property_type', '')).lower() else 30))
    enhanced['data_completeness'] = 0.85  # Simulated data completeness
    
    return enhanced

@app.get("/")
async def root():
    return {
        "message": "User-Customizable Property Search System",
        "version": "4.0.0",
        "features": [
            "Customizable search weight preferences",
            "Hard limit filters (crime, value, size, zoning)",
            "Real-time ranking updates",
            "Predefined user profiles",
            "Session persistence",
            "Weight impact visualization"
        ],
        "profiles": list(PREFERENCE_PROFILES.keys()),
        "endpoints": [
            "/preferences/interface - Interactive preference editor",
            "/preferences/profiles - Predefined preference profiles",
            "/search/with-preferences - Search with custom preferences",
            "/preferences/compare - Compare different preference profiles"
        ]
    }

@app.get("/preferences/interface", response_class=HTMLResponse)
async def preference_interface():
    """Interactive web interface for setting user preferences"""
    
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Property Search Preferences</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
            .container { max-width: 800px; margin: 0 auto; background: white; padding: 20px; border-radius: 10px; }
            .weight-control { margin: 15px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }
            .slider { width: 100%; margin: 10px 0; }
            .value-display { font-weight: bold; color: #2196F3; }
            .hard-limit { margin: 10px 0; padding: 10px; background: #f9f9f9; border-radius: 5px; }
            .profile-button { padding: 10px 15px; margin: 5px; background: #4CAF50; color: white; border: none; border-radius: 5px; cursor: pointer; }
            .search-button { padding: 15px 30px; background: #2196F3; color: white; border: none; border-radius: 5px; cursor: pointer; font-size: 16px; }
            .total-weight { font-size: 18px; font-weight: bold; margin: 15px 0; }
            h1, h2 { color: #333; }
            .warning { color: #ff6b35; font-weight: bold; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🎯 Property Search Preferences</h1>
            <p>Customize your search by adjusting importance weights and setting hard limits.</p>
            
            <h2>📊 Quick Profiles</h2>
            <button class="profile-button" onclick="loadProfile('safety_focused')">🛡️ Safety-Focused</button>
            <button class="profile-button" onclick="loadProfile('location_premium')">📍 Location Premium</button>
            <button class="profile-button" onclick="loadProfile('development_investor')">🏗️ Development Investor</button>
            <button class="profile-button" onclick="loadProfile('balanced_buyer')">⚖️ Balanced Buyer</button>
            
            <h2>⚖️ Importance Weights</h2>
            <div class="total-weight">Total Weight: <span id="totalWeight">100</span>%</div>
            
            <div class="weight-control">
                <label>🚨 Crime Safety Weight: <span class="value-display" id="crimeValue">35</span>%</label>
                <input type="range" class="slider" id="crimeWeight" min="0" max="50" value="35" oninput="updateWeights()">
                <small>How important is low crime score (0-50%)</small>
            </div>
            
            <div class="weight-control">
                <label>📍 Location Premium Weight: <span class="value-display" id="locationValue">25</span>%</label>
                <input type="range" class="slider" id="locationWeight" min="0" max="40" value="25" oninput="updateWeights()">
                <small>How important is prime location (0-40%)</small>
            </div>
            
            <div class="weight-control">
                <label>🏠 Property Type Weight: <span class="value-display" id="typeValue">20</span>%</label>
                <input type="range" class="slider" id="typeWeight" min="0" max="30" value="20" oninput="updateWeights()">
                <small>How important is property type match (0-30%)</small>
            </div>
            
            <div class="weight-control">
                <label>🏗️ Development Potential Weight: <span class="value-display" id="devValue">15</span>%</label>
                <input type="range" class="slider" id="devWeight" min="0" max="25" value="15" oninput="updateWeights()">
                <small>How important is development potential (0-25%)</small>
            </div>
            
            <div class="weight-control">
                <label>📊 Data Quality Weight: <span class="value-display" id="qualityValue">5</span>%</label>
                <input type="range" class="slider" id="qualityWeight" min="0" max="15" value="5" oninput="updateWeights()">
                <small>How important is data completeness (0-15%)</small>
            </div>
            
            <h2>🚧 Hard Limits</h2>
            <div class="hard-limit">
                <label>Maximum Crime Score: </label>
                <input type="number" id="maxCrime" min="0" max="100" placeholder="No limit">
                <small> (0-100, properties above this will be excluded)</small>
            </div>
            
            <div class="hard-limit">
                <label>Minimum Property Value: $</label>
                <input type="number" id="minValue" min="0" placeholder="No limit">
                <small> (minimum assessed value required)</small>
            </div>
            
            <div class="hard-limit">
                <label>Minimum Property Size: </label>
                <input type="number" id="minSize" min="0" placeholder="No limit"> sqft
                <small> (minimum square footage required)</small>
            </div>
            
            <div class="hard-limit">
                <label>Required Zoning Types: </label>
                <input type="text" id="zoningTypes" placeholder="e.g., R1,R2,C1">
                <small> (comma-separated, only these zones will be included)</small>
            </div>
            
            <div class="hard-limit">
                <label>Maximum Property Age: </label>
                <input type="number" id="maxAge" min="0" placeholder="No limit"> years
                <small> (exclude properties older than this)</small>
            </div>
            
            <h2>🔍 Test Your Preferences</h2>
            <input type="text" id="searchAddress" placeholder="Enter address (e.g., Hollywood)" style="width: 300px; padding: 10px; margin: 10px 0;">
            <br>
            <button class="search-button" onclick="testSearch()">🔍 Search with My Preferences</button>
            
            <div id="results" style="margin-top: 20px;"></div>
        </div>
        
        <script>
            const profiles = {
                'safety_focused': {
                    crime: 50, location: 20, type: 15, dev: 10, quality: 5,
                    maxCrime: 30, minValue: '', minSize: '', zoning: '', maxAge: ''
                },
                'location_premium': {
                    crime: 20, location: 40, type: 30, dev: 5, quality: 5,
                    maxCrime: 60, minValue: 500000, minSize: 1200, zoning: 'R1,R2,RD', maxAge: ''
                },
                'development_investor': {
                    crime: 15, location: 25, type: 10, dev: 25, quality: 10,
                    maxCrime: 75, minValue: '', minSize: 5000, zoning: 'C1,C2,M1,LAR', maxAge: 50
                },
                'balanced_buyer': {
                    crime: 25, location: 25, type: 25, dev: 15, quality: 10,
                    maxCrime: 50, minValue: 300000, minSize: 800, zoning: '', maxAge: ''
                }
            };
            
            function loadProfile(profileName) {
                const profile = profiles[profileName];
                document.getElementById('crimeWeight').value = profile.crime;
                document.getElementById('locationWeight').value = profile.location;
                document.getElementById('typeWeight').value = profile.type;
                document.getElementById('devWeight').value = profile.dev;
                document.getElementById('qualityWeight').value = profile.quality;
                
                document.getElementById('maxCrime').value = profile.maxCrime;
                document.getElementById('minValue').value = profile.minValue;
                document.getElementById('minSize').value = profile.minSize;
                document.getElementById('zoningTypes').value = profile.zoning;
                document.getElementById('maxAge').value = profile.maxAge;
                
                updateWeights();
            }
            
            function updateWeights() {
                const crime = parseInt(document.getElementById('crimeWeight').value);
                const location = parseInt(document.getElementById('locationWeight').value);
                const type = parseInt(document.getElementById('typeWeight').value);
                const dev = parseInt(document.getElementById('devWeight').value);
                const quality = parseInt(document.getElementById('qualityWeight').value);
                
                document.getElementById('crimeValue').textContent = crime;
                document.getElementById('locationValue').textContent = location;
                document.getElementById('typeValue').textContent = type;
                document.getElementById('devValue').textContent = dev;
                document.getElementById('qualityValue').textContent = quality;
                
                const total = crime + location + type + dev + quality;
                document.getElementById('totalWeight').textContent = total;
                
                if (total > 100) {
                    document.getElementById('totalWeight').className = 'warning';
                } else {
                    document.getElementById('totalWeight').className = '';
                }
            }
            
            function testSearch() {
                const address = document.getElementById('searchAddress').value;
                if (!address) {
                    alert('Please enter a search address');
                    return;
                }
                
                const weights = {
                    crime_weight: parseFloat(document.getElementById('crimeWeight').value),
                    location_weight: parseFloat(document.getElementById('locationWeight').value),
                    property_type_weight: parseFloat(document.getElementById('typeWeight').value),
                    development_weight: parseFloat(document.getElementById('devWeight').value),
                    data_quality_weight: parseFloat(document.getElementById('qualityWeight').value)
                };
                
                const hardLimits = {};
                if (document.getElementById('maxCrime').value) hardLimits.max_crime_score = parseFloat(document.getElementById('maxCrime').value);
                if (document.getElementById('minValue').value) hardLimits.min_property_value = parseFloat(document.getElementById('minValue').value);
                if (document.getElementById('minSize').value) hardLimits.min_property_size = parseFloat(document.getElementById('minSize').value);
                if (document.getElementById('zoningTypes').value) hardLimits.required_zoning_types = document.getElementById('zoningTypes').value.split(',');
                if (document.getElementById('maxAge').value) hardLimits.max_year_built_age = parseInt(document.getElementById('maxAge').value);
                
                const searchData = {
                    address: address,
                    weights: weights,
                    hard_limits: hardLimits,
                    limit: 3
                };
                
                document.getElementById('results').innerHTML = '<p>Searching with your preferences...</p>';
                
                fetch('/search/with-preferences', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(searchData)
                })
                .then(response => response.json())
                .then(data => {
                    let html = '<h3>🏠 Search Results with Your Preferences</h3>';
                    
                    if (data.properties && data.properties.length > 0) {
                        html += '<div style="background: #e8f5e8; padding: 10px; border-radius: 5px; margin: 10px 0;">';
                        html += '<strong>Results found: ' + data.properties.length + '</strong><br>';
                        html += 'Weighted scoring applied with your preferences<br>';
                        html += 'Properties excluded by hard limits: ' + (data.filter_summary.total_examined - data.properties.length);
                        html += '</div>';
                        
                        data.properties.forEach((prop, index) => {
                            html += '<div style="border: 1px solid #ddd; margin: 10px 0; padding: 15px; border-radius: 5px;">';
                            html += '<h4>' + (index + 1) + '. ' + prop.address + '</h4>';
                            html += '<p><strong>Weighted Score: ' + prop.weighted_score + '/100</strong></p>';
                            html += '<p>Crime Score: ' + prop.crime_score + ' | Size: ' + (prop.sqft || 'N/A') + ' sqft | Built: ' + (prop.year_built || 'N/A') + '</p>';
                            if (prop.score_breakdown) {
                                html += '<details><summary>Score Breakdown</summary>';
                                html += '<ul>';
                                for (const [factor, score] of Object.entries(prop.score_breakdown)) {
                                    html += '<li>' + factor + ': ' + score + '</li>';
                                }
                                html += '</ul></details>';
                            }
                            html += '</div>';
                        });
                    } else {
                        html += '<p>No properties found matching your criteria. Try adjusting your preferences.</p>';
                    }
                    
                    document.getElementById('results').innerHTML = html;
                })
                .catch(error => {
                    document.getElementById('results').innerHTML = '<p style="color: red;">Error: ' + error.message + '</p>';
                });
            }
            
            // Initialize
            updateWeights();
        </script>
    </body>
    </html>
    """
    
    return HTMLResponse(content=html_content)

@app.get("/preferences/profiles")
async def get_preference_profiles():
    """Get all predefined preference profiles"""
    
    profiles_data = {}
    for profile_id, profile in PREFERENCE_PROFILES.items():
        profiles_data[profile_id] = {
            "profile_name": profile.profile_name,
            "description": profile.description,
            "weights": profile.weights.__dict__,
            "hard_limits": profile.hard_limits.__dict__
        }
    
    return {
        "predefined_profiles": profiles_data,
        "total_profiles": len(profiles_data),
        "usage_instructions": {
            "select_profile": "Use profile_id in /search/with-preferences endpoint",
            "customize": "Modify weights and limits as needed",
            "save": "Save custom preferences for future use"
        }
    }

@app.post("/search/with-preferences")
async def search_with_preferences(
    search_request: Dict[str, Any]
):
    """Search properties using custom user preferences"""
    
    start_time = time.time()
    
    try:
        # Extract search parameters
        address = search_request.get('address', '')
        profile_id = search_request.get('profile_id')
        custom_weights = search_request.get('weights')
        custom_hard_limits = search_request.get('hard_limits', {})
        limit = search_request.get('limit', 10)
        
        # Determine weights to use
        if profile_id and profile_id in PREFERENCE_PROFILES:
            profile = PREFERENCE_PROFILES[profile_id]
            weights = profile.weights
            hard_limits = profile.hard_limits
        elif custom_weights:
            weights = SearchWeights(**custom_weights)
            hard_limits = HardLimits(**custom_hard_limits)
        else:
            # Use default balanced profile
            weights = PREFERENCE_PROFILES['balanced_buyer'].weights
            hard_limits = PREFERENCE_PROFILES['balanced_buyer'].hard_limits
        
        # Get properties from database
        conn = get_db_connection()
        cursor = conn.cursor()
        
        base_query = """
        SELECT apn, site_address as address, latitude, longitude, 
               property_type, zoning_code, crime_score,
               year_built, sqft, units
        FROM search_idx_parcel
        """
        
        conditions = []
        params = []
        
        if address:
            conditions.append("UPPER(site_address) LIKE UPPER(?)")
            params.append(f"%{address}%")
        
        if conditions:
            base_query += " WHERE " + " AND ".join(conditions)
        
        base_query += f" ORDER BY crime_score ASC LIMIT {limit * 3}"  # Get more to apply filters
        
        cursor.execute(base_query, params)
        raw_properties = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        total_examined = len(raw_properties)
        
        # Enhance properties with scoring factors
        enhanced_properties = []
        for prop in raw_properties:
            enhanced_prop = enhance_property_with_scores(prop)
            enhanced_properties.append(enhanced_prop)
        
        # Apply hard limits
        filtered_properties = apply_hard_limits(enhanced_properties, hard_limits)
        
        # Calculate weighted scores and rank
        scored_properties = []
        for prop in filtered_properties:
            weighted_score = calculate_weighted_score(prop, weights)
            prop['weighted_score'] = weighted_score
            
            # Add score breakdown for transparency
            prop['score_breakdown'] = {
                'crime_component': round((100 - prop.get('crime_score', 50)) * weights.crime_weight / 100, 2),
                'location_component': round(prop.get('location_premium', 50) * weights.location_weight / 100, 2),
                'property_type_component': round(prop.get('type_match_score', 70) * weights.property_type_weight / 100, 2),
                'development_component': round(prop.get('development_potential', 60) * weights.development_weight / 100, 2),
                'data_quality_component': round(prop.get('data_completeness', 0.8) * 100 * weights.data_quality_weight / 100, 2)
            }
            
            scored_properties.append(prop)
        
        # Sort by weighted score (highest first)
        scored_properties.sort(key=lambda x: x['weighted_score'], reverse=True)
        
        # Limit results
        final_properties = scored_properties[:limit]
        
        response_time = (time.time() - start_time) * 1000
        
        return {
            "search_metadata": {
                "address": address,
                "profile_used": profile_id if profile_id else "custom",
                "results_count": len(final_properties),
                "response_time_ms": round(response_time, 2),
                "timestamp": datetime.now().isoformat()
            },
            "preference_settings": {
                "weights_used": weights.__dict__,
                "hard_limits_applied": hard_limits.__dict__,
                "total_weight_percentage": sum([
                    weights.crime_weight, weights.location_weight, 
                    weights.property_type_weight, weights.development_weight, 
                    weights.data_quality_weight
                ])
            },
            "filter_summary": {
                "total_examined": total_examined,
                "after_hard_limits": len(filtered_properties),
                "properties_excluded_by_limits": total_examined - len(filtered_properties),
                "final_results": len(final_properties)
            },
            "properties": final_properties,
            "ranking_explanation": {
                "methodology": "Properties ranked by weighted score using your preferences",
                "weight_distribution": {
                    "crime_safety": f"{weights.crime_weight}%",
                    "location_premium": f"{weights.location_weight}%", 
                    "property_type_match": f"{weights.property_type_weight}%",
                    "development_potential": f"{weights.development_weight}%",
                    "data_quality": f"{weights.data_quality_weight}%"
                }
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search error: {str(e)}")

@app.post("/preferences/compare")
async def compare_preference_profiles(
    comparison_request: Dict[str, Any]
):
    """Compare how different preference profiles rank the same properties"""
    
    try:
        address = comparison_request.get('address', 'Hollywood')
        profiles_to_compare = comparison_request.get('profiles', ['safety_focused', 'location_premium', 'development_investor'])
        limit = comparison_request.get('limit', 5)
        
        # Get base property data
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
        SELECT apn, site_address as address, latitude, longitude, 
               property_type, zoning_code, crime_score,
               year_built, sqft, units
        FROM search_idx_parcel
        WHERE UPPER(site_address) LIKE UPPER(?)
        ORDER BY crime_score ASC 
        LIMIT ?
        """, (f"%{address}%", limit))
        
        raw_properties = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        if not raw_properties:
            return {"message": "No properties found for comparison", "address": address}
        
        # Enhance properties
        enhanced_properties = []
        for prop in raw_properties:
            enhanced_prop = enhance_property_with_scores(prop)
            enhanced_properties.append(enhanced_prop)
        
        # Compare rankings across profiles
        comparison_results = {}
        
        for profile_id in profiles_to_compare:
            if profile_id not in PREFERENCE_PROFILES:
                continue
                
            profile = PREFERENCE_PROFILES[profile_id]
            
            # Apply hard limits
            filtered_props = apply_hard_limits(enhanced_properties, profile.hard_limits)
            
            # Calculate scores
            scored_props = []
            for prop in filtered_props:
                weighted_score = calculate_weighted_score(prop, profile.weights)
                prop_copy = prop.copy()
                prop_copy['weighted_score'] = weighted_score
                scored_props.append(prop_copy)
            
            # Sort by score
            scored_props.sort(key=lambda x: x['weighted_score'], reverse=True)
            
            comparison_results[profile_id] = {
                "profile_name": profile.profile_name,
                "description": profile.description,
                "properties_after_limits": len(scored_props),
                "top_properties": [
                    {
                        "rank": i + 1,
                        "apn": prop['apn'],
                        "address": prop['address'],
                        "weighted_score": prop['weighted_score'],
                        "crime_score": prop.get('crime_score'),
                        "sqft": prop.get('sqft')
                    }
                    for i, prop in enumerate(scored_props[:3])
                ]
            }
        
        # Create ranking comparison matrix
        ranking_matrix = {}
        for prop in enhanced_properties[:3]:
            apn = prop['apn']
            ranking_matrix[apn] = {
                "address": prop['address'],
                "rankings_by_profile": {}
            }
            
            for profile_id, results in comparison_results.items():
                # Find this property's rank in this profile
                rank = None
                score = None
                for i, top_prop in enumerate(results['top_properties']):
                    if top_prop['apn'] == apn:
                        rank = i + 1
                        score = top_prop['weighted_score']
                        break
                
                ranking_matrix[apn]["rankings_by_profile"][profile_id] = {
                    "rank": rank or "Not in top 3",
                    "score": score,
                    "profile_name": PREFERENCE_PROFILES[profile_id].profile_name
                }
        
        return {
            "comparison_summary": {
                "address_searched": address,
                "profiles_compared": len(comparison_results),
                "properties_analyzed": len(enhanced_properties),
                "comparison_type": "ranking_differences"
            },
            "profile_results": comparison_results,
            "ranking_matrix": ranking_matrix,
            "insights": {
                "ranking_differences": "Different profiles prioritize different factors",
                "score_variations": "Same property can score very differently based on user preferences",
                "filter_impact": "Hard limits can exclude properties entirely from some profiles"
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Comparison error: {str(e)}")

@app.post("/preferences/save")
async def save_user_preferences(
    preference_data: Dict[str, Any]
):
    """Save custom user preferences for future use"""
    
    try:
        user_id = preference_data.get('user_id') or str(uuid.uuid4())
        preference_name = preference_data.get('preference_name', 'My Custom Preferences')
        
        weights = SearchWeights(**preference_data.get('weights', {}))
        hard_limits = HardLimits(**preference_data.get('hard_limits', {}))
        
        user_pref = UserPreferences(
            user_id=user_id,
            preference_name=preference_name,
            weights=weights,
            hard_limits=hard_limits,
            created_at=datetime.now().isoformat(),
            last_used=datetime.now().isoformat()
        )
        
        # Save to storage (in production this would be database)
        user_preferences[user_id] = user_pref
        
        return {
            "message": "Preferences saved successfully",
            "user_id": user_id,
            "preference_name": preference_name,
            "saved_preferences": user_pref.__dict__,
            "usage": f"Use user_id '{user_id}' in future searches"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Save error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8009)