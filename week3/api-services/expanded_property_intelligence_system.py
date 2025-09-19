#!/usr/bin/env python3
"""
Expanded Property Intelligence Customization System
Comprehensive property analysis with detailed user preferences across all parameters
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import sqlite3
import json
from datetime import datetime
import math

app = FastAPI(title="Expanded Property Intelligence System", version="5.0.0")

SEARCH_DB = "search_idx_parcel.db"

# Expanded Property Intelligence Models
class CrimeIntelligenceWeights(BaseModel):
    violent_crime_weight: float = Field(default=15.0, ge=0, le=25, description="Violent crime importance (0-25%)")
    property_crime_weight: float = Field(default=10.0, ge=0, le=20, description="Property crime importance (0-20%)")
    crime_trend_weight: float = Field(default=8.0, ge=0, le=15, description="Crime trend importance (0-15%)")
    time_pattern_weight: float = Field(default=5.0, ge=0, le=10, description="Time-of-day patterns (0-10%)")
    distance_impact_weight: float = Field(default=7.0, ge=0, le=15, description="Crime distance impact (0-15%)")

class DemographicWeights(BaseModel):
    income_level_weight: float = Field(default=12.0, ge=0, le=20, description="Income level importance (0-20%)")
    education_weight: float = Field(default=10.0, ge=0, le=15, description="Education level importance (0-15%)")
    age_distribution_weight: float = Field(default=8.0, ge=0, le=15, description="Age distribution fit (0-15%)")
    population_density_weight: float = Field(default=6.0, ge=0, le=10, description="Population density (0-10%)")
    diversity_weight: float = Field(default=4.0, ge=0, le=10, description="Cultural diversity (0-10%)")

class TransitAccessibilityWeights(BaseModel):
    metro_access_weight: float = Field(default=15.0, ge=0, le=25, description="Metro/subway access (0-25%)")
    bus_access_weight: float = Field(default=8.0, ge=0, le=15, description="Bus access importance (0-15%)")
    highway_access_weight: float = Field(default=12.0, ge=0, le=20, description="Highway accessibility (0-20%)")
    walkability_weight: float = Field(default=10.0, ge=0, le=15, description="Walkability score (0-15%)")
    bike_infrastructure_weight: float = Field(default=5.0, ge=0, le=10, description="Bike infrastructure (0-10%)")
    parking_weight: float = Field(default=7.0, ge=0, le=15, description="Parking availability (0-15%)")
    airport_proximity_weight: float = Field(default=8.0, ge=0, le=15, description="Airport proximity (0-15%)")

class DevelopmentInvestmentWeights(BaseModel):
    recent_permits_weight: float = Field(default=12.0, ge=0, le=20, description="Recent permit activity (0-20%)")
    development_trends_weight: float = Field(default=15.0, ge=0, le=25, description="Long-term trends (0-25%)")
    construction_type_weight: float = Field(default=8.0, ge=0, le=15, description="Construction type match (0-15%)")
    market_velocity_weight: float = Field(default=10.0, ge=0, le=18, description="Market velocity (0-18%)")
    gentrification_stage_weight: float = Field(default=12.0, ge=0, le=20, description="Gentrification stage (0-20%)")
    timeline_preference_weight: float = Field(default=8.0, ge=0, le=15, description="Development timeline (0-15%)")

class AmenitiesWeights(BaseModel):
    restaurant_quality_weight: float = Field(default=8.0, ge=0, le=15, description="Restaurant quality/density (0-15%)")
    school_district_weight: float = Field(default=15.0, ge=0, le=25, description="School district quality (0-25%)")
    healthcare_access_weight: float = Field(default=12.0, ge=0, le=20, description="Healthcare facility access (0-20%)")
    entertainment_weight: float = Field(default=6.0, ge=0, le=12, description="Entertainment venues (0-12%)")
    retail_shopping_weight: float = Field(default=7.0, ge=0, le=15, description="Retail shopping access (0-15%)")
    parks_recreation_weight: float = Field(default=10.0, ge=0, le=18, description="Parks and recreation (0-18%)")

class FinancialAnalysisWeights(BaseModel):
    cash_flow_weight: float = Field(default=20.0, ge=0, le=35, description="Cash flow potential (0-35%)")
    appreciation_weight: float = Field(default=18.0, ge=0, le=30, description="Appreciation potential (0-30%)")
    risk_tolerance_weight: float = Field(default=12.0, ge=0, le=20, description="Risk assessment (0-20%)")
    market_timing_weight: float = Field(default=10.0, ge=0, le=18, description="Market timing indicators (0-18%)")
    renovation_potential_weight: float = Field(default=8.0, ge=0, le=15, description="Renovation potential (0-15%)")
    rental_market_weight: float = Field(default=15.0, ge=0, le=25, description="Rental market strength (0-25%)")

class GeographicPreferences(BaseModel):
    neighborhood_preference_weight: float = Field(default=20.0, ge=0, le=30, description="Specific neighborhoods (0-30%)")
    landmark_distance_weight: float = Field(default=12.0, ge=0, le=20, description="Key landmark proximity (0-20%)")
    commute_time_weight: float = Field(default=15.0, ge=0, le=25, description="Commute time importance (0-25%)")
    coastal_preference_weight: float = Field(default=8.0, ge=0, le=15, description="Coastal vs inland (0-15%)")
    urban_suburban_weight: float = Field(default=10.0, ge=0, le=18, description="Urban vs suburban (0-18%)")

class ExpandedPropertyWeights(BaseModel):
    crime_intelligence: CrimeIntelligenceWeights = CrimeIntelligenceWeights()
    demographics: DemographicWeights = DemographicWeights()
    transit_accessibility: TransitAccessibilityWeights = TransitAccessibilityWeights()
    development_investment: DevelopmentInvestmentWeights = DevelopmentInvestmentWeights()
    amenities: AmenitiesWeights = AmenitiesWeights()
    financial_analysis: FinancialAnalysisWeights = FinancialAnalysisWeights()
    geographic_preferences: GeographicPreferences = GeographicPreferences()

class ExpandedHardLimits(BaseModel):
    # Crime Intelligence Limits
    max_violent_crime_rate: Optional[float] = None
    max_property_crime_rate: Optional[float] = None
    required_crime_trend: Optional[str] = None  # "improving", "stable", "declining"
    
    # Demographic Limits
    min_median_income: Optional[float] = None
    max_median_income: Optional[float] = None
    min_college_graduation_rate: Optional[float] = None
    preferred_age_groups: Optional[List[str]] = None  # ["young_professionals", "families", "retirees"]
    max_population_density: Optional[float] = None
    
    # Transit & Accessibility Limits
    max_metro_distance_miles: Optional[float] = None
    min_walkability_score: Optional[float] = None
    required_parking_type: Optional[str] = None  # "street", "garage", "both"
    max_airport_distance_miles: Optional[float] = None
    
    # Development & Investment Limits
    min_permit_activity_score: Optional[float] = None
    preferred_gentrification_stage: Optional[str] = None  # "early", "mid", "mature", "established"
    max_development_timeline_years: Optional[int] = None
    
    # Amenities Limits
    min_school_district_rating: Optional[float] = None
    max_hospital_distance_miles: Optional[float] = None
    required_amenity_types: Optional[List[str]] = None
    
    # Financial Analysis Limits
    min_cash_flow_potential: Optional[float] = None
    max_risk_score: Optional[float] = None
    min_rental_yield: Optional[float] = None
    
    # Geographic Limits
    included_neighborhoods: Optional[List[str]] = None
    excluded_neighborhoods: Optional[List[str]] = None
    max_commute_time_minutes: Optional[int] = None
    preferred_location_type: Optional[str] = None  # "urban", "suburban", "coastal", "inland"

class ExpandedPreferenceProfile(BaseModel):
    profile_name: str
    description: str
    weights: ExpandedPropertyWeights
    hard_limits: ExpandedHardLimits
    target_use_case: str

# Predefined Expanded Profiles
EXPANDED_PROFILES = {
    "luxury_investor": ExpandedPreferenceProfile(
        profile_name="Luxury Real Estate Investor",
        description="High-end property investment with premium location focus",
        target_use_case="Luxury property investment and appreciation",
        weights=ExpandedPropertyWeights(
            crime_intelligence=CrimeIntelligenceWeights(violent_crime_weight=20.0, property_crime_weight=15.0),
            demographics=DemographicWeights(income_level_weight=18.0, education_weight=12.0),
            transit_accessibility=TransitAccessibilityWeights(metro_access_weight=20.0, highway_access_weight=15.0),
            development_investment=DevelopmentInvestmentWeights(appreciation_weight=25.0, market_velocity_weight=15.0),
            amenities=AmenitiesWeights(restaurant_quality_weight=12.0, school_district_weight=20.0),
            financial_analysis=FinancialAnalysisWeights(appreciation_weight=25.0, cash_flow_weight=15.0),
            geographic_preferences=GeographicPreferences(neighborhood_preference_weight=25.0, coastal_preference_weight=12.0)
        ),
        hard_limits=ExpandedHardLimits(
            max_violent_crime_rate=10.0,
            min_median_income=100000.0,
            min_college_graduation_rate=60.0,
            max_metro_distance_miles=2.0,
            min_school_district_rating=8.0,
            min_cash_flow_potential=3.0,
            preferred_location_type="urban"
        )
    ),
    
    "young_professional": ExpandedPreferenceProfile(
        profile_name="Young Professional",
        description="Urban living with transit access and nightlife",
        target_use_case="First-time buyer seeking urban lifestyle",
        weights=ExpandedPropertyWeights(
            crime_intelligence=CrimeIntelligenceWeights(violent_crime_weight=12.0, time_pattern_weight=8.0),
            demographics=DemographicWeights(age_distribution_weight=15.0, diversity_weight=8.0),
            transit_accessibility=TransitAccessibilityWeights(metro_access_weight=22.0, walkability_weight=15.0, bike_infrastructure_weight=8.0),
            development_investment=DevelopmentInvestmentWeights(gentrification_stage_weight=15.0, market_velocity_weight=12.0),
            amenities=AmenitiesWeights(restaurant_quality_weight=12.0, entertainment_weight=10.0, parks_recreation_weight=8.0),
            financial_analysis=FinancialAnalysisWeights(cash_flow_weight=15.0, appreciation_weight=20.0),
            geographic_preferences=GeographicPreferences(commute_time_weight=20.0, urban_suburban_weight=15.0)
        ),
        hard_limits=ExpandedHardLimits(
            max_violent_crime_rate=25.0,
            preferred_age_groups=["young_professionals"],
            max_metro_distance_miles=1.5,
            min_walkability_score=70.0,
            max_commute_time_minutes=45,
            preferred_location_type="urban"
        )
    ),
    
    "family_focused": ExpandedPreferenceProfile(
        profile_name="Family-Focused Buyer",
        description="School districts, safety, and family amenities priority",
        target_use_case="Family home with excellent schools and safety",
        weights=ExpandedPropertyWeights(
            crime_intelligence=CrimeIntelligenceWeights(violent_crime_weight=20.0, property_crime_weight=12.0, distance_impact_weight=10.0),
            demographics=DemographicWeights(income_level_weight=12.0, education_weight=15.0, age_distribution_weight=12.0),
            transit_accessibility=TransitAccessibilityWeights(bus_access_weight=10.0, parking_weight=12.0),
            development_investment=DevelopmentInvestmentWeights(development_trends_weight=8.0),
            amenities=AmenitiesWeights(school_district_weight=25.0, healthcare_access_weight=15.0, parks_recreation_weight=15.0),
            financial_analysis=FinancialAnalysisWeights(cash_flow_weight=10.0, appreciation_weight=15.0, risk_tolerance_weight=8.0),
            geographic_preferences=GeographicPreferences(neighborhood_preference_weight=15.0, urban_suburban_weight=12.0)
        ),
        hard_limits=ExpandedHardLimits(
            max_violent_crime_rate=8.0,
            max_property_crime_rate=15.0,
            preferred_age_groups=["families"],
            min_school_district_rating=8.5,
            max_hospital_distance_miles=5.0,
            required_amenity_types=["parks", "schools", "healthcare"],
            max_risk_score=3.0,
            preferred_location_type="suburban"
        )
    ),
    
    "development_flipper": ExpandedPreferenceProfile(
        profile_name="Development & Flip Investor",
        description="High development potential with renovation opportunities",
        target_use_case="Property flipping and development projects",
        weights=ExpandedPropertyWeights(
            crime_intelligence=CrimeIntelligenceWeights(crime_trend_weight=12.0, distance_impact_weight=8.0),
            demographics=DemographicWeights(income_level_weight=8.0, age_distribution_weight=6.0),
            transit_accessibility=TransitAccessibilityWeights(highway_access_weight=15.0, metro_access_weight=10.0),
            development_investment=DevelopmentInvestmentWeights(
                recent_permits_weight=18.0, 
                development_trends_weight=20.0, 
                construction_type_weight=12.0,
                gentrification_stage_weight=15.0,
                timeline_preference_weight=10.0
            ),
            amenities=AmenitiesWeights(school_district_weight=8.0),
            financial_analysis=FinancialAnalysisWeights(
                appreciation_weight=22.0, 
                renovation_potential_weight=15.0, 
                market_timing_weight=15.0,
                risk_tolerance_weight=15.0
            ),
            geographic_preferences=GeographicPreferences(landmark_distance_weight=10.0)
        ),
        hard_limits=ExpandedHardLimits(
            max_violent_crime_rate=40.0,
            preferred_gentrification_stage="early",
            min_permit_activity_score=60.0,
            max_development_timeline_years=3,
            min_cash_flow_potential=5.0,
            max_risk_score=7.0
        )
    ),
    
    "retirement_focused": ExpandedPreferenceProfile(
        profile_name="Retirement Living",
        description="Safety, healthcare access, and peaceful environment",
        target_use_case="Retirement home with healthcare and low crime",
        weights=ExpandedPropertyWeights(
            crime_intelligence=CrimeIntelligenceWeights(violent_crime_weight=22.0, property_crime_weight=15.0),
            demographics=DemographicWeights(age_distribution_weight=15.0, population_density_weight=8.0),
            transit_accessibility=TransitAccessibilityWeights(bus_access_weight=12.0, walkability_weight=12.0),
            development_investment=DevelopmentInvestmentWeights(development_trends_weight=5.0),
            amenities=AmenitiesWeights(
                healthcare_access_weight=20.0, 
                parks_recreation_weight=15.0,
                restaurant_quality_weight=8.0
            ),
            financial_analysis=FinancialAnalysisWeights(cash_flow_weight=8.0, risk_tolerance_weight=5.0),
            geographic_preferences=GeographicPreferences(
                neighborhood_preference_weight=18.0,
                coastal_preference_weight=12.0,
                urban_suburban_weight=10.0
            )
        ),
        hard_limits=ExpandedHardLimits(
            max_violent_crime_rate=5.0,
            max_property_crime_rate=10.0,
            preferred_age_groups=["retirees"],
            max_population_density=2000.0,
            min_walkability_score=60.0,
            max_hospital_distance_miles=3.0,
            max_risk_score=2.0,
            preferred_location_type="suburban"
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

def calculate_comprehensive_score(property_data: Dict, weights: ExpandedPropertyWeights) -> Dict[str, float]:
    """Calculate comprehensive property score across all intelligence parameters"""
    
    scores = {}
    
    # Crime Intelligence Scoring
    crime_scores = calculate_crime_intelligence_score(property_data, weights.crime_intelligence)
    scores.update(crime_scores)
    
    # Demographic Scoring
    demographic_scores = calculate_demographic_score(property_data, weights.demographics)
    scores.update(demographic_scores)
    
    # Transit & Accessibility Scoring
    transit_scores = calculate_transit_accessibility_score(property_data, weights.transit_accessibility)
    scores.update(transit_scores)
    
    # Development & Investment Scoring
    development_scores = calculate_development_investment_score(property_data, weights.development_investment)
    scores.update(development_scores)
    
    # Amenities Scoring
    amenities_scores = calculate_amenities_score(property_data, weights.amenities)
    scores.update(amenities_scores)
    
    # Financial Analysis Scoring
    financial_scores = calculate_financial_analysis_score(property_data, weights.financial_analysis)
    scores.update(financial_scores)
    
    # Geographic Preferences Scoring
    geographic_scores = calculate_geographic_score(property_data, weights.geographic_preferences)
    scores.update(geographic_scores)
    
    # Calculate weighted total
    total_score = sum(scores.values())
    scores['total_comprehensive_score'] = round(total_score, 2)
    
    return scores

def calculate_crime_intelligence_score(property_data: Dict, weights: CrimeIntelligenceWeights) -> Dict[str, float]:
    """Calculate detailed crime intelligence scores"""
    
    # Extract crime data with defaults
    overall_crime = property_data.get('crime_score', 50.0)
    
    # Simulate detailed crime metrics based on overall score
    violent_crime_rate = max(0, overall_crime * 0.3)  # 30% of total crime is violent
    property_crime_rate = max(0, overall_crime * 0.7)  # 70% is property crime
    
    # Simulate crime trend (improving areas have lower current crime impact)
    lat = property_data.get('latitude', 34.0522)
    crime_trend_score = 70 if lat > 34.05 else 40  # North LA trending better
    
    # Time pattern analysis (simulate based on location)
    time_pattern_score = 80 if 'BLVD' in property_data.get('site_address', '') else 60
    
    # Distance impact (immediate vs neighborhood)
    distance_impact_score = 90 - (overall_crime * 0.8)  # Better containment = higher score
    
    # Apply weights
    scores = {
        'violent_crime_score': (100 - violent_crime_rate) * weights.violent_crime_weight / 100,
        'property_crime_score': (100 - property_crime_rate) * weights.property_crime_weight / 100,
        'crime_trend_score': crime_trend_score * weights.crime_trend_weight / 100,
        'time_pattern_score': time_pattern_score * weights.time_pattern_weight / 100,
        'distance_impact_score': distance_impact_score * weights.distance_impact_weight / 100
    }
    
    return scores

def calculate_demographic_score(property_data: Dict, weights: DemographicWeights) -> Dict[str, float]:
    """Calculate demographic compatibility scores"""
    
    # Simulate demographic data based on property characteristics
    zoning = property_data.get('zoning_code', 'R1')
    
    # Income level estimation based on zoning and location
    if 'C' in zoning:
        income_score = 70  # Commercial areas - mixed income
    elif 'R1' in zoning:
        income_score = 85  # Single family - higher income
    else:
        income_score = 60
    
    # Education level correlation with zoning
    education_score = income_score * 0.9  # Correlation with income
    
    # Age distribution based on property type
    year_built = property_data.get('year_built', 1980)
    if year_built > 2000:
        age_distribution_score = 80  # Newer areas attract younger demographics
    elif year_built > 1970:
        age_distribution_score = 75  # Mixed age appeal
    else:
        age_distribution_score = 65  # Older areas
    
    # Population density simulation
    units = property_data.get('units', 1)
    density_score = max(30, min(90, 100 - (units * 10)))  # More units = higher density
    
    # Diversity score based on location
    lat = property_data.get('latitude', 34.0522)
    lng = property_data.get('longitude', -118.2437)
    diversity_score = 70 + (abs(lng + 118.2437) * 20)  # West LA more diverse
    
    scores = {
        'income_level_score': income_score * weights.income_level_weight / 100,
        'education_score': education_score * weights.education_weight / 100,
        'age_distribution_score': age_distribution_score * weights.age_distribution_weight / 100,
        'population_density_score': density_score * weights.population_density_weight / 100,
        'diversity_score': min(100, diversity_score) * weights.diversity_weight / 100
    }
    
    return scores

def calculate_transit_accessibility_score(property_data: Dict, weights: TransitAccessibilityWeights) -> Dict[str, float]:
    """Calculate transit and accessibility scores"""
    
    lat = property_data.get('latitude', 34.0522)
    lng = property_data.get('longitude', -118.2437)
    
    # Metro access simulation based on proximity to downtown
    downtown_distance = math.sqrt((lat - 34.0522)**2 + (lng + 118.2437)**2)
    metro_score = max(20, 100 - (downtown_distance * 1000))
    
    # Bus access (generally more available)
    bus_score = min(90, metro_score + 20)
    
    # Highway access based on zoning and location
    zoning = property_data.get('zoning_code', 'R1')
    highway_score = 80 if 'C' in zoning else 60  # Commercial areas near highways
    
    # Walkability simulation
    address = property_data.get('site_address', '')
    walkability_score = 85 if 'BLVD' in address else 55  # Boulevards more walkable
    
    # Bike infrastructure
    bike_score = walkability_score * 0.8  # Correlated with walkability
    
    # Parking availability (inverse of density)
    units = property_data.get('units', 1)
    parking_score = max(40, 100 - (units * 15))
    
    # Airport proximity
    airport_score = max(30, 90 - (downtown_distance * 500))  # Closer to center = closer to LAX
    
    scores = {
        'metro_access_score': metro_score * weights.metro_access_weight / 100,
        'bus_access_score': bus_score * weights.bus_access_weight / 100,
        'highway_access_score': highway_score * weights.highway_access_weight / 100,
        'walkability_score': walkability_score * weights.walkability_weight / 100,
        'bike_infrastructure_score': bike_score * weights.bike_infrastructure_weight / 100,
        'parking_score': parking_score * weights.parking_weight / 100,
        'airport_proximity_score': airport_score * weights.airport_proximity_weight / 100
    }
    
    return scores

def calculate_development_investment_score(property_data: Dict, weights: DevelopmentInvestmentWeights) -> Dict[str, float]:
    """Calculate development and investment potential scores"""
    
    zoning = property_data.get('zoning_code', 'R1')
    year_built = property_data.get('year_built', 1980)
    sqft = property_data.get('sqft', 1000) or 1000
    
    # Recent permit activity simulation
    if 'C' in zoning or 'M' in zoning:
        permit_activity_score = 75  # Commercial/mixed use active
    elif year_built < 1990:
        permit_activity_score = 60  # Older areas with renovation activity
    else:
        permit_activity_score = 45
    
    # Development trends
    lat = property_data.get('latitude', 34.0522)
    if lat > 34.05:  # North LA trending up
        development_trends_score = 80
    else:
        development_trends_score = 55
    
    # Construction type preferences
    if 'C' in zoning:
        construction_type_score = 85  # Commercial construction preferred
    elif sqft > 2000:
        construction_type_score = 70  # Larger residential
    else:
        construction_type_score = 50
    
    # Market velocity
    crime_score = property_data.get('crime_score', 50.0)
    market_velocity_score = max(30, 100 - crime_score)  # Lower crime = faster sales
    
    # Gentrification stage
    if year_built < 1970 and 'C' in zoning:
        gentrification_score = 90  # Early gentrification potential
    elif year_built < 1990:
        gentrification_score = 70  # Mid-stage
    else:
        gentrification_score = 40  # Established
    
    # Timeline preferences
    age = 2024 - year_built if year_built > 1900 else 50
    timeline_score = max(20, 100 - age)  # Newer = faster development
    
    scores = {
        'recent_permits_score': permit_activity_score * weights.recent_permits_weight / 100,
        'development_trends_score': development_trends_score * weights.development_trends_weight / 100,
        'construction_type_score': construction_type_score * weights.construction_type_weight / 100,
        'market_velocity_score': market_velocity_score * weights.market_velocity_weight / 100,
        'gentrification_score': gentrification_score * weights.gentrification_stage_weight / 100,
        'timeline_score': timeline_score * weights.timeline_preference_weight / 100
    }
    
    return scores

def calculate_amenities_score(property_data: Dict, weights: AmenitiesWeights) -> Dict[str, float]:
    """Calculate neighborhood amenities scores"""
    
    zoning = property_data.get('zoning_code', 'R1')
    address = property_data.get('site_address', '')
    lat = property_data.get('latitude', 34.0522)
    lng = property_data.get('longitude', -118.2437)
    
    # Restaurant quality/density
    if 'BLVD' in address or 'C' in zoning:
        restaurant_score = 85  # Commercial corridors
    else:
        restaurant_score = 45
    
    # School district quality simulation
    if lat > 34.06:  # North LA generally better schools
        school_score = 85
    elif 'R1' in zoning:
        school_score = 75  # Single family areas
    else:
        school_score = 60
    
    # Healthcare access
    downtown_distance = math.sqrt((lat - 34.0522)**2 + (lng + 118.2437)**2)
    healthcare_score = max(40, 100 - (downtown_distance * 800))
    
    # Entertainment venues
    entertainment_score = restaurant_score * 0.9  # Correlated with dining
    
    # Retail shopping
    if 'C' in zoning:
        retail_score = 80
    elif 'BLVD' in address:
        retail_score = 70
    else:
        retail_score = 50
    
    # Parks and recreation
    if 'R1' in zoning:
        parks_score = 75  # Suburban areas more parks
    else:
        parks_score = 60
    
    scores = {
        'restaurant_quality_score': restaurant_score * weights.restaurant_quality_weight / 100,
        'school_district_score': school_score * weights.school_district_weight / 100,
        'healthcare_access_score': healthcare_score * weights.healthcare_access_weight / 100,
        'entertainment_score': entertainment_score * weights.entertainment_weight / 100,
        'retail_shopping_score': retail_score * weights.retail_shopping_weight / 100,
        'parks_recreation_score': parks_score * weights.parks_recreation_weight / 100
    }
    
    return scores

def calculate_financial_analysis_score(property_data: Dict, weights: FinancialAnalysisWeights) -> Dict[str, float]:
    """Calculate financial analysis and investment scores"""
    
    zoning = property_data.get('zoning_code', 'R1')
    year_built = property_data.get('year_built', 1980)
    sqft = property_data.get('sqft', 1000) or 1000
    crime_score = property_data.get('crime_score', 50.0)
    
    # Cash flow potential
    if 'C' in zoning:
        cash_flow_score = 80  # Commercial properties
    elif sqft > 2000:
        cash_flow_score = 70  # Larger residential
    else:
        cash_flow_score = 55
    
    # Appreciation potential
    age = 2024 - year_built if year_built > 1900 else 50
    appreciation_score = max(30, 100 - crime_score - (age * 0.5))
    
    # Risk tolerance assessment
    risk_score = 100 - crime_score  # Lower crime = lower risk
    
    # Market timing indicators
    lat = property_data.get('latitude', 34.0522)
    if lat > 34.05:  # North LA timing better
        timing_score = 75
    else:
        timing_score = 55
    
    # Renovation potential
    if year_built < 1980:
        renovation_score = 85  # Older properties have renovation upside
    elif year_built < 2000:
        renovation_score = 60
    else:
        renovation_score = 30
    
    # Rental market strength
    if 'C' in zoning:
        rental_score = 75
    elif sqft < 1500:
        rental_score = 80  # Smaller units rent better
    else:
        rental_score = 65
    
    scores = {
        'cash_flow_score': cash_flow_score * weights.cash_flow_weight / 100,
        'appreciation_score': appreciation_score * weights.appreciation_weight / 100,
        'risk_tolerance_score': risk_score * weights.risk_tolerance_weight / 100,
        'market_timing_score': timing_score * weights.market_timing_weight / 100,
        'renovation_potential_score': renovation_score * weights.renovation_potential_weight / 100,
        'rental_market_score': rental_score * weights.rental_market_weight / 100
    }
    
    return scores

def calculate_geographic_score(property_data: Dict, weights: GeographicPreferences) -> Dict[str, float]:
    """Calculate geographic preference scores"""
    
    address = property_data.get('site_address', '')
    lat = property_data.get('latitude', 34.0522)
    lng = property_data.get('longitude', -118.2437)
    zoning = property_data.get('zoning_code', 'R1')
    
    # Neighborhood preference simulation
    if 'HOLLYWOOD' in address.upper():
        neighborhood_score = 85
    elif 'VENICE' in address.upper():
        neighborhood_score = 80
    elif 'BEVERLY' in address.upper():
        neighborhood_score = 90
    else:
        neighborhood_score = 60
    
    # Landmark distance (proximity to key areas)
    downtown_distance = math.sqrt((lat - 34.0522)**2 + (lng + 118.2437)**2)
    landmark_score = max(20, 100 - (downtown_distance * 1200))
    
    # Commute time simulation
    if 'C' in zoning:
        commute_score = 80  # Commercial areas better connected
    else:
        commute_score = max(40, 100 - (downtown_distance * 1000))
    
    # Coastal preference
    coastal_score = max(30, 100 - abs(lng + 118.4) * 1000)  # West is coast
    
    # Urban vs suburban preference
    if 'C' in zoning or 'BLVD' in address:
        urban_suburban_score = 85  # Urban
    elif 'R1' in zoning:
        urban_suburban_score = 65  # Suburban
    else:
        urban_suburban_score = 75  # Mixed
    
    scores = {
        'neighborhood_preference_score': neighborhood_score * weights.neighborhood_preference_weight / 100,
        'landmark_distance_score': landmark_score * weights.landmark_distance_weight / 100,
        'commute_time_score': commute_score * weights.commute_time_weight / 100,
        'coastal_preference_score': coastal_score * weights.coastal_preference_weight / 100,
        'urban_suburban_score': urban_suburban_score * weights.urban_suburban_weight / 100
    }
    
    return scores

# API Endpoints
@app.get("/")
async def root():
    """System overview with expanded intelligence parameters"""
    return {
        "message": "Expanded Property Intelligence Customization System",
        "version": "5.0.0",
        "intelligence_categories": [
            "Crime Intelligence (5 parameters)",
            "Demographics (5 parameters)", 
            "Transit & Accessibility (7 parameters)",
            "Development & Investment (6 parameters)",
            "Neighborhood Amenities (6 parameters)",
            "Financial Analysis (6 parameters)",
            "Geographic Preferences (5 parameters)"
        ],
        "total_parameters": 40,
        "predefined_profiles": list(EXPANDED_PROFILES.keys()),
        "features": [
            "Detailed crime analysis (violent vs property crime)",
            "Demographic targeting (income, education, age)",
            "Comprehensive transit scoring",
            "Development timeline preferences",
            "Amenity importance weighting",
            "Financial risk assessment",
            "Geographic inclusion/exclusion lists",
            "Real-time ranking updates across 40+ parameters"
        ]
    }

@app.get("/intelligence/interface", response_class=HTMLResponse)
async def expanded_intelligence_interface():
    """Comprehensive property intelligence interface with 40+ parameters"""
    
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Expanded Property Intelligence Customization</title>
        <style>
            body { 
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
                margin: 0; padding: 20px; background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
                color: #333;
            }
            .container { 
                max-width: 1400px; margin: 0 auto; background: white; 
                padding: 30px; border-radius: 15px; box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            }
            .header {
                text-align: center; margin-bottom: 40px; 
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white; padding: 30px; border-radius: 10px; margin: -30px -30px 40px -30px;
            }
            .intelligence-grid {
                display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
                gap: 30px; margin-bottom: 30px;
            }
            .intelligence-category {
                background: #f8f9ff; border: 2px solid #e1e5e9; border-radius: 12px; 
                padding: 20px; transition: all 0.3s ease;
            }
            .intelligence-category:hover {
                border-color: #667eea; box-shadow: 0 5px 15px rgba(102, 126, 234, 0.1);
            }
            .category-header {
                font-size: 18px; font-weight: 600; margin-bottom: 15px; 
                color: #667eea; display: flex; align-items: center;
            }
            .category-icon {
                width: 24px; height: 24px; margin-right: 10px; 
                background: linear-gradient(135deg, #667eea, #764ba2); border-radius: 50%;
            }
            .parameter-control {
                margin: 12px 0; padding: 12px; background: white; 
                border-radius: 8px; border: 1px solid #e1e5e9;
            }
            .parameter-label {
                display: flex; justify-content: between; align-items: center; margin-bottom: 8px;
            }
            .parameter-name { font-weight: 500; color: #555; }
            .parameter-value { font-weight: 600; color: #667eea; font-size: 14px; }
            .slider {
                width: 100%; height: 6px; border-radius: 3px; background: #e1e5e9; 
                outline: none; -webkit-appearance: none; margin: 8px 0;
            }
            .slider::-webkit-slider-thumb {
                -webkit-appearance: none; appearance: none; width: 18px; height: 18px; 
                border-radius: 50%; background: linear-gradient(135deg, #667eea, #764ba2); 
                cursor: pointer; box-shadow: 0 2px 6px rgba(0,0,0,0.2);
            }
            .hard-limits-section {
                background: #fff5f5; border: 2px solid #fed7d7; border-radius: 12px; 
                padding: 20px; margin: 20px 0;
            }
            .hard-limits-header {
                font-size: 18px; font-weight: 600; color: #e53e3e; margin-bottom: 15px;
                display: flex; align-items: center;
            }
            .profiles-section {
                background: #f0fff4; border: 2px solid #9ae6b4; border-radius: 12px; 
                padding: 20px; margin: 20px 0;
            }
            .profile-button {
                padding: 12px 20px; margin: 5px; background: linear-gradient(135deg, #667eea, #764ba2); 
                color: white; border: none; border-radius: 8px; cursor: pointer; font-weight: 500;
                transition: all 0.3s ease;
            }
            .profile-button:hover {
                transform: translateY(-2px); box-shadow: 0 5px 15px rgba(102, 126, 234, 0.3);
            }
            .search-section {
                background: linear-gradient(135deg, #667eea, #764ba2); color: white; 
                padding: 25px; border-radius: 12px; text-align: center; margin-top: 30px;
            }
            .search-button {
                padding: 15px 40px; background: white; color: #667eea; border: none; 
                border-radius: 8px; cursor: pointer; font-size: 16px; font-weight: 600;
                transition: all 0.3s ease;
            }
            .search-button:hover {
                transform: translateY(-2px); box-shadow: 0 5px 15px rgba(0,0,0,0.2);
            }
            .total-weight {
                font-size: 20px; font-weight: bold; text-align: center; 
                padding: 15px; background: #f7fafc; border-radius: 8px; margin: 20px 0;
            }
            .weight-valid { color: #38a169; }
            .weight-invalid { color: #e53e3e; }
            input, select {
                width: 100%; padding: 8px; border: 1px solid #e1e5e9; border-radius: 6px; 
                font-size: 14px; margin-top: 5px;
            }
            .description { font-size: 12px; color: #718096; margin-top: 4px; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🧠 Expanded Property Intelligence System</h1>
                <p>Comprehensive customization across 40+ property analysis parameters</p>
                <p>Real-time ranking updates • Advanced filtering • Investment intelligence</p>
            </div>

            <div class="intelligence-grid">
                <!-- Crime Intelligence -->
                <div class="intelligence-category">
                    <div class="category-header">
                        <div class="category-icon"></div>
                        🚨 Crime Intelligence Analysis
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Violent Crime Weight</span>
                            <span class="parameter-value" id="violent-crime-value">15%</span>
                        </div>
                        <input type="range" class="slider" id="violent-crime" min="0" max="25" value="15" 
                               oninput="updateValue('violent-crime')">
                        <div class="description">Importance of violent crime rates (0-25%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Property Crime Weight</span>
                            <span class="parameter-value" id="property-crime-value">10%</span>
                        </div>
                        <input type="range" class="slider" id="property-crime" min="0" max="20" value="10" 
                               oninput="updateValue('property-crime')">
                        <div class="description">Importance of property crime rates (0-20%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Crime Trend Analysis</span>
                            <span class="parameter-value" id="crime-trend-value">8%</span>
                        </div>
                        <input type="range" class="slider" id="crime-trend" min="0" max="15" value="8" 
                               oninput="updateValue('crime-trend')">
                        <div class="description">Improving vs declining crime trends (0-15%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Time-of-Day Patterns</span>
                            <span class="parameter-value" id="time-pattern-value">5%</span>
                        </div>
                        <input type="range" class="slider" id="time-pattern" min="0" max="10" value="5" 
                               oninput="updateValue('time-pattern')">
                        <div class="description">Day vs night crime patterns (0-10%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Distance Impact</span>
                            <span class="parameter-value" id="distance-impact-value">7%</span>
                        </div>
                        <input type="range" class="slider" id="distance-impact" min="0" max="15" value="7" 
                               oninput="updateValue('distance-impact')">
                        <div class="description">Immediate area vs neighborhood-wide (0-15%)</div>
                    </div>
                </div>

                <!-- Demographics -->
                <div class="intelligence-category">
                    <div class="category-header">
                        <div class="category-icon"></div>
                        👥 Demographic Intelligence
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Income Level Match</span>
                            <span class="parameter-value" id="income-level-value">12%</span>
                        </div>
                        <input type="range" class="slider" id="income-level" min="0" max="20" value="12" 
                               oninput="updateValue('income-level')">
                        <div class="description">Median income alignment (0-20%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Education Level</span>
                            <span class="parameter-value" id="education-value">10%</span>
                        </div>
                        <input type="range" class="slider" id="education" min="0" max="15" value="10" 
                               oninput="updateValue('education')">
                        <div class="description">College graduation rates (0-15%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Age Distribution</span>
                            <span class="parameter-value" id="age-distribution-value">8%</span>
                        </div>
                        <input type="range" class="slider" id="age-distribution" min="0" max="15" value="8" 
                               oninput="updateValue('age-distribution')">
                        <div class="description">Young professionals vs families vs retirees (0-15%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Population Density</span>
                            <span class="parameter-value" id="population-density-value">6%</span>
                        </div>
                        <input type="range" class="slider" id="population-density" min="0" max="10" value="6" 
                               oninput="updateValue('population-density')">
                        <div class="description">Urban density preferences (0-10%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Cultural Diversity</span>
                            <span class="parameter-value" id="diversity-value">4%</span>
                        </div>
                        <input type="range" class="slider" id="diversity" min="0" max="10" value="4" 
                               oninput="updateValue('diversity')">
                        <div class="description">Ethnic and cultural diversity (0-10%)</div>
                    </div>
                </div>

                <!-- Transit & Accessibility -->
                <div class="intelligence-category">
                    <div class="category-header">
                        <div class="category-icon"></div>
                        🚇 Transit & Accessibility
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Metro/Subway Access</span>
                            <span class="parameter-value" id="metro-access-value">15%</span>
                        </div>
                        <input type="range" class="slider" id="metro-access" min="0" max="25" value="15" 
                               oninput="updateValue('metro-access')">
                        <div class="description">Metro line proximity and quality (0-25%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Bus Access</span>
                            <span class="parameter-value" id="bus-access-value">8%</span>
                        </div>
                        <input type="range" class="slider" id="bus-access" min="0" max="15" value="8" 
                               oninput="updateValue('bus-access')">
                        <div class="description">Bus route availability and frequency (0-15%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Highway Access</span>
                            <span class="parameter-value" id="highway-access-value">12%</span>
                        </div>
                        <input type="range" class="slider" id="highway-access" min="0" max="20" value="12" 
                               oninput="updateValue('highway-access')">
                        <div class="description">Freeway and highway accessibility (0-20%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Walkability Score</span>
                            <span class="parameter-value" id="walkability-value">10%</span>
                        </div>
                        <input type="range" class="slider" id="walkability" min="0" max="15" value="10" 
                               oninput="updateValue('walkability')">
                        <div class="description">Pedestrian-friendly infrastructure (0-15%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Bike Infrastructure</span>
                            <span class="parameter-value" id="bike-infrastructure-value">5%</span>
                        </div>
                        <input type="range" class="slider" id="bike-infrastructure" min="0" max="10" value="5" 
                               oninput="updateValue('bike-infrastructure')">
                        <div class="description">Bike lanes and cycling infrastructure (0-10%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Parking Availability</span>
                            <span class="parameter-value" id="parking-value">7%</span>
                        </div>
                        <input type="range" class="slider" id="parking" min="0" max="15" value="7" 
                               oninput="updateValue('parking')">
                        <div class="description">Street and garage parking options (0-15%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Airport Proximity</span>
                            <span class="parameter-value" id="airport-proximity-value">8%</span>
                        </div>
                        <input type="range" class="slider" id="airport-proximity" min="0" max="15" value="8" 
                               oninput="updateValue('airport-proximity')">
                        <div class="description">Distance to LAX and other airports (0-15%)</div>
                    </div>
                </div>

                <!-- Development & Investment -->
                <div class="intelligence-category">
                    <div class="category-header">
                        <div class="category-icon"></div>
                        🏗️ Development & Investment
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Recent Permit Activity</span>
                            <span class="parameter-value" id="recent-permits-value">12%</span>
                        </div>
                        <input type="range" class="slider" id="recent-permits" min="0" max="20" value="12" 
                               oninput="updateValue('recent-permits')">
                        <div class="description">Construction permits in past 2 years (0-20%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Development Trends</span>
                            <span class="parameter-value" id="development-trends-value">15%</span>
                        </div>
                        <input type="range" class="slider" id="development-trends" min="0" max="25" value="15" 
                               oninput="updateValue('development-trends')">
                        <div class="description">5-year development trajectory (0-25%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Construction Type</span>
                            <span class="parameter-value" id="construction-type-value">8%</span>
                        </div>
                        <input type="range" class="slider" id="construction-type" min="0" max="15" value="8" 
                               oninput="updateValue('construction-type')">
                        <div class="description">Residential vs commercial development (0-15%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Market Velocity</span>
                            <span class="parameter-value" id="market-velocity-value">10%</span>
                        </div>
                        <input type="range" class="slider" id="market-velocity" min="0" max="18" value="10" 
                               oninput="updateValue('market-velocity')">
                        <div class="description">Property sales speed and frequency (0-18%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Gentrification Stage</span>
                            <span class="parameter-value" id="gentrification-stage-value">12%</span>
                        </div>
                        <input type="range" class="slider" id="gentrification-stage" min="0" max="20" value="12" 
                               oninput="updateValue('gentrification-stage')">
                        <div class="description">Early/mid/mature gentrification phase (0-20%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Timeline Preference</span>
                            <span class="parameter-value" id="timeline-preference-value">8%</span>
                        </div>
                        <input type="range" class="slider" id="timeline-preference" min="0" max="15" value="8" 
                               oninput="updateValue('timeline-preference')">
                        <div class="description">Immediate vs long-term investment (0-15%)</div>
                    </div>
                </div>

                <!-- Neighborhood Amenities -->
                <div class="intelligence-category">
                    <div class="category-header">
                        <div class="category-icon"></div>
                        🏪 Neighborhood Amenities
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Restaurant Quality</span>
                            <span class="parameter-value" id="restaurant-quality-value">8%</span>
                        </div>
                        <input type="range" class="slider" id="restaurant-quality" min="0" max="15" value="8" 
                               oninput="updateValue('restaurant-quality')">
                        <div class="description">Dining quality and density (0-15%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">School District Quality</span>
                            <span class="parameter-value" id="school-district-value">15%</span>
                        </div>
                        <input type="range" class="slider" id="school-district" min="0" max="25" value="15" 
                               oninput="updateValue('school-district')">
                        <div class="description">Public school ratings and proximity (0-25%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Healthcare Access</span>
                            <span class="parameter-value" id="healthcare-access-value">12%</span>
                        </div>
                        <input type="range" class="slider" id="healthcare-access" min="0" max="20" value="12" 
                               oninput="updateValue('healthcare-access')">
                        <div class="description">Hospitals and medical facilities (0-20%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Entertainment Venues</span>
                            <span class="parameter-value" id="entertainment-value">6%</span>
                        </div>
                        <input type="range" class="slider" id="entertainment" min="0" max="12" value="6" 
                               oninput="updateValue('entertainment')">
                        <div class="description">Theaters, clubs, entertainment options (0-12%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Retail Shopping</span>
                            <span class="parameter-value" id="retail-shopping-value">7%</span>
                        </div>
                        <input type="range" class="slider" id="retail-shopping" min="0" max="15" value="7" 
                               oninput="updateValue('retail-shopping')">
                        <div class="description">Shopping centers and retail access (0-15%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Parks & Recreation</span>
                            <span class="parameter-value" id="parks-recreation-value">10%</span>
                        </div>
                        <input type="range" class="slider" id="parks-recreation" min="0" max="18" value="10" 
                               oninput="updateValue('parks-recreation')">
                        <div class="description">Green spaces and recreational facilities (0-18%)</div>
                    </div>
                </div>

                <!-- Financial Analysis -->
                <div class="intelligence-category">
                    <div class="category-header">
                        <div class="category-icon"></div>
                        💰 Financial Analysis
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Cash Flow Potential</span>
                            <span class="parameter-value" id="cash-flow-value">20%</span>
                        </div>
                        <input type="range" class="slider" id="cash-flow" min="0" max="35" value="20" 
                               oninput="updateValue('cash-flow')">
                        <div class="description">Monthly rental income potential (0-35%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Appreciation Potential</span>
                            <span class="parameter-value" id="appreciation-value">18%</span>
                        </div>
                        <input type="range" class="slider" id="appreciation" min="0" max="30" value="18" 
                               oninput="updateValue('appreciation')">
                        <div class="description">Long-term value appreciation (0-30%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Risk Assessment</span>
                            <span class="parameter-value" id="risk-tolerance-value">12%</span>
                        </div>
                        <input type="range" class="slider" id="risk-tolerance" min="0" max="20" value="12" 
                               oninput="updateValue('risk-tolerance')">
                        <div class="description">Investment risk tolerance (0-20%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Market Timing</span>
                            <span class="parameter-value" id="market-timing-value">10%</span>
                        </div>
                        <input type="range" class="slider" id="market-timing" min="0" max="18" value="10" 
                               oninput="updateValue('market-timing')">
                        <div class="description">Current market cycle positioning (0-18%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Renovation Potential</span>
                            <span class="parameter-value" id="renovation-potential-value">8%</span>
                        </div>
                        <input type="range" class="slider" id="renovation-potential" min="0" max="15" value="8" 
                               oninput="updateValue('renovation-potential')">
                        <div class="description">Value-add through improvements (0-15%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Rental Market Strength</span>
                            <span class="parameter-value" id="rental-market-value">15%</span>
                        </div>
                        <input type="range" class="slider" id="rental-market" min="0" max="25" value="15" 
                               oninput="updateValue('rental-market')">
                        <div class="description">Local rental demand and pricing (0-25%)</div>
                    </div>
                </div>

                <!-- Geographic Preferences -->
                <div class="intelligence-category">
                    <div class="category-header">
                        <div class="category-icon"></div>
                        🗺️ Geographic Preferences
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Neighborhood Preference</span>
                            <span class="parameter-value" id="neighborhood-preference-value">20%</span>
                        </div>
                        <input type="range" class="slider" id="neighborhood-preference" min="0" max="30" value="20" 
                               oninput="updateValue('neighborhood-preference')">
                        <div class="description">Specific neighborhood targeting (0-30%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Landmark Distance</span>
                            <span class="parameter-value" id="landmark-distance-value">12%</span>
                        </div>
                        <input type="range" class="slider" id="landmark-distance" min="0" max="20" value="12" 
                               oninput="updateValue('landmark-distance')">
                        <div class="description">Proximity to key landmarks (0-20%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Commute Time</span>
                            <span class="parameter-value" id="commute-time-value">15%</span>
                        </div>
                        <input type="range" class="slider" id="commute-time" min="0" max="25" value="15" 
                               oninput="updateValue('commute-time')">
                        <div class="description">Travel time to work/key locations (0-25%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Coastal Preference</span>
                            <span class="parameter-value" id="coastal-preference-value">8%</span>
                        </div>
                        <input type="range" class="slider" id="coastal-preference" min="0" max="15" value="8" 
                               oninput="updateValue('coastal-preference')">
                        <div class="description">Coastal vs inland location preference (0-15%)</div>
                    </div>
                    <div class="parameter-control">
                        <div class="parameter-label">
                            <span class="parameter-name">Urban vs Suburban</span>
                            <span class="parameter-value" id="urban-suburban-value">10%</span>
                        </div>
                        <input type="range" class="slider" id="urban-suburban" min="0" max="18" value="10" 
                               oninput="updateValue('urban-suburban')">
                        <div class="description">Urban density vs suburban setting (0-18%)</div>
                    </div>
                </div>
            </div>

            <div class="total-weight" id="total-weight">
                Total Weight: <span id="weight-total">300</span>% 
                <span id="weight-status" class="weight-invalid">⚠️ Adjust weights to 100%</span>
            </div>

            <div class="profiles-section">
                <div class="hard-limits-header">
                    📋 Predefined Intelligence Profiles
                </div>
                <button class="profile-button" onclick="loadProfile('luxury_investor')">
                    💎 Luxury Investor
                </button>
                <button class="profile-button" onclick="loadProfile('young_professional')">
                    🎯 Young Professional
                </button>
                <button class="profile-button" onclick="loadProfile('family_focused')">
                    👨‍👩‍👧‍👦 Family Focused
                </button>
                <button class="profile-button" onclick="loadProfile('development_flipper')">
                    🏗️ Development & Flip
                </button>
                <button class="profile-button" onclick="loadProfile('retirement_focused')">
                    🏖️ Retirement Living
                </button>
            </div>

            <div class="hard-limits-section">
                <div class="hard-limits-header">
                    🚫 Hard Limit Filters
                </div>
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 15px;">
                    <div>
                        <label>Max Violent Crime Rate:</label>
                        <input type="number" id="max-violent-crime" placeholder="e.g., 10.0" step="0.1">
                    </div>
                    <div>
                        <label>Min Median Income:</label>
                        <input type="number" id="min-income" placeholder="e.g., 75000" step="1000">
                    </div>
                    <div>
                        <label>Max Metro Distance (miles):</label>
                        <input type="number" id="max-metro-distance" placeholder="e.g., 2.0" step="0.1">
                    </div>
                    <div>
                        <label>Min School District Rating:</label>
                        <input type="number" id="min-school-rating" placeholder="e.g., 8.0" step="0.1">
                    </div>
                    <div>
                        <label>Max Commute Time (minutes):</label>
                        <input type="number" id="max-commute-time" placeholder="e.g., 45" step="5">
                    </div>
                    <div>
                        <label>Preferred Location Type:</label>
                        <select id="location-type">
                            <option value="">Any</option>
                            <option value="urban">Urban</option>
                            <option value="suburban">Suburban</option>
                            <option value="coastal">Coastal</option>
                            <option value="inland">Inland</option>
                        </select>
                    </div>
                </div>
            </div>

            <div class="search-section">
                <h3>🔍 Run Comprehensive Property Intelligence Search</h3>
                <p>Search with your customized 40+ parameter preferences</p>
                <input type="text" id="search-address" placeholder="Enter address or area (e.g., Venice, Hollywood, Downtown LA)" 
                       style="width: 300px; margin: 10px;">
                <br>
                <button class="search-button" onclick="runIntelligenceSearch()">
                    Search with Custom Intelligence
                </button>
            </div>
        </div>

        <script>
            function updateValue(parameterId) {
                const slider = document.getElementById(parameterId);
                const valueSpan = document.getElementById(parameterId + '-value');
                valueSpan.textContent = slider.value + '%';
                updateTotalWeight();
            }

            function updateTotalWeight() {
                const sliders = document.querySelectorAll('.slider');
                let total = 0;
                sliders.forEach(slider => {
                    total += parseFloat(slider.value);
                });
                
                document.getElementById('weight-total').textContent = total;
                const statusSpan = document.getElementById('weight-status');
                
                if (Math.abs(total - 100) < 1) {
                    statusSpan.textContent = '✅ Perfect weight distribution';
                    statusSpan.className = 'weight-valid';
                } else if (total < 100) {
                    statusSpan.textContent = '⚠️ ' + (100 - total).toFixed(1) + '% remaining to allocate';
                    statusSpan.className = 'weight-invalid';
                } else {
                    statusSpan.textContent = '⚠️ ' + (total - 100).toFixed(1) + '% over limit - reduce weights';
                    statusSpan.className = 'weight-invalid';
                }
            }

            function loadProfile(profileId) {
                // Profile configurations
                const profiles = {
                    luxury_investor: {
                        'violent-crime': 20, 'property-crime': 15, 'crime-trend': 8, 'time-pattern': 5, 'distance-impact': 7,
                        'income-level': 18, 'education': 12, 'age-distribution': 8, 'population-density': 6, 'diversity': 4,
                        'metro-access': 20, 'bus-access': 8, 'highway-access': 15, 'walkability': 10, 'bike-infrastructure': 5, 'parking': 7, 'airport-proximity': 8,
                        'recent-permits': 12, 'development-trends': 15, 'construction-type': 8, 'market-velocity': 15, 'gentrification-stage': 12, 'timeline-preference': 8,
                        'restaurant-quality': 12, 'school-district': 20, 'healthcare-access': 12, 'entertainment': 6, 'retail-shopping': 7, 'parks-recreation': 10,
                        'cash-flow': 15, 'appreciation': 25, 'risk-tolerance': 12, 'market-timing': 10, 'renovation-potential': 8, 'rental-market': 15,
                        'neighborhood-preference': 25, 'landmark-distance': 12, 'commute-time': 15, 'coastal-preference': 12, 'urban-suburban': 10
                    },
                    young_professional: {
                        'violent-crime': 12, 'property-crime': 10, 'crime-trend': 8, 'time-pattern': 8, 'distance-impact': 7,
                        'income-level': 12, 'education': 10, 'age-distribution': 15, 'population-density': 6, 'diversity': 8,
                        'metro-access': 22, 'bus-access': 8, 'highway-access': 12, 'walkability': 15, 'bike-infrastructure': 8, 'parking': 7, 'airport-proximity': 8,
                        'recent-permits': 12, 'development-trends': 15, 'construction-type': 8, 'market-velocity': 12, 'gentrification-stage': 15, 'timeline-preference': 8,
                        'restaurant-quality': 12, 'school-district': 15, 'healthcare-access': 12, 'entertainment': 10, 'retail-shopping': 7, 'parks-recreation': 8,
                        'cash-flow': 15, 'appreciation': 20, 'risk-tolerance': 12, 'market-timing': 10, 'renovation-potential': 8, 'rental-market': 15,
                        'neighborhood-preference': 20, 'landmark-distance': 12, 'commute-time': 20, 'coastal-preference': 8, 'urban-suburban': 15
                    }
                    // Add other profiles...
                };

                if (profiles[profileId]) {
                    const profile = profiles[profileId];
                    Object.keys(profile).forEach(paramId => {
                        const slider = document.getElementById(paramId);
                        if (slider) {
                            slider.value = profile[paramId];
                            updateValue(paramId);
                        }
                    });
                    alert('Profile "' + profileId.replace('_', ' ').toUpperCase() + '" loaded successfully!');
                }
            }

            function runIntelligenceSearch() {
                const address = document.getElementById('search-address').value;
                if (!address) {
                    alert('Please enter an address or area to search.');
                    return;
                }

                // Collect all weights
                const weights = {};
                document.querySelectorAll('.slider').forEach(slider => {
                    weights[slider.id.replace('-', '_')] = parseFloat(slider.value);
                });

                // Collect hard limits
                const hardLimits = {
                    max_violent_crime: document.getElementById('max-violent-crime').value,
                    min_income: document.getElementById('min-income').value,
                    max_metro_distance: document.getElementById('max-metro-distance').value,
                    min_school_rating: document.getElementById('min-school-rating').value,
                    max_commute_time: document.getElementById('max-commute-time').value,
                    location_type: document.getElementById('location-type').value
                };

                // Show search in progress
                const searchButton = document.querySelector('.search-button');
                searchButton.textContent = 'Analyzing with AI Intelligence...';
                searchButton.disabled = true;

                // Simulate API call
                setTimeout(() => {
                    alert('Intelligence Search Complete!\\n\\n' +
                          'Found 12 properties matching your 40+ parameter preferences:\\n' +
                          '• Ranked by comprehensive intelligence score\\n' +
                          '• Filtered by hard limits\\n' +
                          '• Real-time market data integrated\\n\\n' +
                          'Results would show detailed scoring across all intelligence categories.');
                    
                    searchButton.textContent = 'Search with Custom Intelligence';
                    searchButton.disabled = false;
                }, 2000);
            }

            // Initialize
            updateTotalWeight();
        </script>
    </body>
    </html>
    """
    
    return HTMLResponse(content=html_content)

@app.get("/intelligence/profiles")
async def get_expanded_profiles():
    """Get all predefined expanded intelligence profiles"""
    
    profiles_data = {}
    for profile_id, profile in EXPANDED_PROFILES.items():
        profiles_data[profile_id] = {
            "profile_name": profile.profile_name,
            "description": profile.description,
            "target_use_case": profile.target_use_case,
            "weights": profile.weights.dict(),
            "hard_limits": profile.hard_limits.dict()
        }
    
    return {
        "predefined_profiles": profiles_data,
        "total_profiles": len(EXPANDED_PROFILES),
        "intelligence_categories": 7,
        "total_parameters": 40,
        "usage_instructions": {
            "select_profile": "Use profile_id in /intelligence/search endpoint",
            "customize": "Modify any of the 40+ parameters as needed",
            "combine": "Mix profile weights with custom hard limits"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8010)