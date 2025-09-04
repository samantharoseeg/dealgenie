"""
Geographic Calibration for LA Properties

Implements location-based confidence adjustments based on LA neighborhood tiers.
Premium areas get confidence boosts while challenging areas get penalties.
"""

from typing import Dict, Tuple, Any

class GeographicCalibrator:
    """Calibrates confidence based on LA neighborhood characteristics"""
    
    def __init__(self):
        # Define confidence multipliers based on neighborhood characteristics
        # Using median income and price/sqft as proxies for neighborhood quality
        self.income_thresholds = {
            'premium': 100000,     # Beverly Hills, Bel Air tier
            'high': 80000,         # West LA, Santa Monica tier  
            'mid': 55000,          # Mid-City, Hollywood tier
            'challenging': 40000   # South LA, Watts tier
        }
        
        self.price_thresholds = {
            'premium': 900,        # $900+ per sqft
            'high': 600,          # $600-900 per sqft
            'mid': 400,           # $400-600 per sqft
            'challenging': 300    # Below $300 per sqft
        }
        
        # Confidence adjustments by tier
        self.confidence_multipliers = {
            'premium': 1.25,      # 25% confidence boost
            'high': 1.10,         # 10% confidence boost
            'mid': 1.00,          # No adjustment
            'challenging': 0.80   # 20% confidence penalty (was 0.85)
        }
        
        # Score adjustments by tier (increased differentiation)
        self.score_adjustments = {
            'premium': 1.4,       # +1.4 point bonus (increased for better separation)
            'high': 0.8,          # +0.8 point bonus (increased)
            'mid': 0.0,           # No adjustment
            'challenging': -1.2   # -1.2 point penalty (increased penalty)
        }
    
    def get_neighborhood_tier(self, features: Dict[str, Any]) -> str:
        """
        Determine neighborhood tier based on income and price metrics
        
        Args:
            features: Property features including median_income and price_per_sqft
            
        Returns:
            Neighborhood tier: 'premium', 'high', 'mid', or 'challenging'
        """
        median_income = features.get('median_income', 50000)
        price_per_sqft = features.get('price_per_sqft', 400)
        
        # Determine tier based on both metrics
        if median_income >= self.income_thresholds['premium'] and price_per_sqft >= self.price_thresholds['premium']:
            return 'premium'
        elif median_income >= self.income_thresholds['high'] and price_per_sqft >= self.price_thresholds['high']:
            return 'high'
        elif median_income >= self.income_thresholds['mid'] and price_per_sqft >= self.price_thresholds['mid']:
            return 'mid'
        else:
            return 'challenging'
    
    def adjust_confidence_for_location(
        self, 
        base_confidence: float,
        features: Dict[str, Any]
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Adjust confidence score based on geographic location
        
        Args:
            base_confidence: Original confidence score (0-1)
            features: Property features
            
        Returns:
            Tuple of (adjusted_confidence, location_analysis)
        """
        tier = self.get_neighborhood_tier(features)
        multiplier = self.confidence_multipliers[tier]
        
        # Apply multiplier but cap at 1.0
        adjusted_confidence = min(base_confidence * multiplier, 1.0)
        
        analysis = {
            'neighborhood_tier': tier,
            'confidence_multiplier': multiplier,
            'base_confidence': round(base_confidence, 3),
            'adjusted_confidence': round(adjusted_confidence, 3),
            'adjustment': round(adjusted_confidence - base_confidence, 3),
            'median_income': features.get('median_income', 0),
            'price_per_sqft': features.get('price_per_sqft', 0)
        }
        
        return adjusted_confidence, analysis
    
    def adjust_score_for_location(
        self,
        base_score: float,
        features: Dict[str, Any]
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Apply subtle score adjustments based on location
        
        Args:
            base_score: Original template score (0-10)
            features: Property features
            
        Returns:
            Tuple of (adjusted_score, adjustment_details)
        """
        tier = self.get_neighborhood_tier(features)
        adjustment = self.score_adjustments[tier]
        
        # Apply adjustment but keep within 0-10 range
        adjusted_score = max(0, min(10, base_score + adjustment))
        
        details = {
            'neighborhood_tier': tier,
            'base_score': round(base_score, 1),
            'adjustment': adjustment,
            'adjusted_score': round(adjusted_score, 1)
        }
        
        return adjusted_score, details
    
    def get_location_premium_factor(self, features: Dict[str, Any]) -> float:
        """
        Calculate a location premium factor for use in scoring
        
        Args:
            features: Property features
            
        Returns:
            Premium factor (0.5 to 1.5 range)
        """
        tier = self.get_neighborhood_tier(features)
        
        tier_factors = {
            'premium': 1.5,
            'high': 1.2,
            'mid': 1.0,
            'challenging': 0.7
        }
        
        return tier_factors.get(tier, 1.0)