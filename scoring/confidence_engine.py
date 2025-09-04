"""
DealGenie Confidence Scoring Engine v1.2

Implements confidence scoring formula with weighted components:
- 50% score margin (sigmoid function)  
- 30% data coverage percentage
- 20% zoning compatibility
"""

import math
import logging
from typing import Dict, Any, List, Optional, Tuple
from .geographic_calibration import GeographicCalibrator

logger = logging.getLogger(__name__)

class ConfidenceEngine:
    """Handles confidence scoring with weighted components"""
    
    def __init__(self):
        """Initialize confidence scoring engine with RECALIBRATED weights"""
        # RECALIBRATED Component weights for LA market realities
        # Reduced margin weight, increased zoning/coverage importance
        self.SCORE_MARGIN_WEIGHT = 0.25   # 25% (was 50%) - less emphasis on margin
        self.DATA_COVERAGE_WEIGHT = 0.25   # 25% (was 30%) - slightly less weight
        self.ZONING_COMPAT_WEIGHT = 0.30   # 30% (was 20%) - more emphasis on compatibility
        self.SCORE_QUALITY_WEIGHT = 0.20   # 20% (new) - reward high absolute scores
        
        # RECALIBRATED Sigmoid for more realistic confidence
        # Gentler curve, lower midpoint = higher confidence for smaller margins
        self.SIGMOID_STEEPNESS = 1.2  # Was 2.0 - gentler curve
        self.SIGMOID_MIDPOINT = 1.0   # Was 2.0 - lower midpoint
        
        # Geographic calibrator for location adjustments
        self.geo_calibrator = GeographicCalibrator()
        
        # Data coverage thresholds
        self.REQUIRED_FIELDS = [
            'zoning', 'lot_size_sqft', 'transit_score', 
            'population_density', 'median_income', 'price_per_sqft'
        ]
        
    def calculate_score_margin_confidence(
        self, 
        primary_score: float,
        secondary_score: float
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate confidence based on score margin using sigmoid function
        
        Args:
            primary_score: Highest template score
            secondary_score: Second highest template score
            
        Returns:
            Tuple of (confidence_component, analysis_dict)
        """
        score_margin = primary_score - secondary_score
        
        # Sigmoid function: 1 / (1 + e^(-k*(x - x0)))
        # Where k = steepness, x0 = midpoint, x = score_margin
        sigmoid_input = -self.SIGMOID_STEEPNESS * (score_margin - self.SIGMOID_MIDPOINT)
        sigmoid_confidence = 1.0 / (1.0 + math.exp(sigmoid_input))
        
        analysis = {
            'primary_score': primary_score,
            'secondary_score': secondary_score,
            'score_margin': round(score_margin, 2),
            'sigmoid_confidence': round(sigmoid_confidence, 3),
            'sigmoid_params': {
                'steepness': self.SIGMOID_STEEPNESS,
                'midpoint': self.SIGMOID_MIDPOINT
            }
        }
        
        return sigmoid_confidence, analysis
    
    def calculate_data_coverage_confidence(
        self, 
        features: Dict[str, Any]
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate confidence based on data coverage percentage
        
        Args:
            features: Property features dictionary
            
        Returns:
            Tuple of (confidence_component, analysis_dict)
        """
        # Check which required fields are present and non-null
        present_fields = []
        missing_fields = []
        
        for field in self.REQUIRED_FIELDS:
            value = features.get(field)
            if value is not None and value != '' and value != 0:
                present_fields.append(field)
            else:
                missing_fields.append(field)
        
        # Calculate coverage percentage
        coverage_percentage = len(present_fields) / len(self.REQUIRED_FIELDS)
        
        analysis = {
            'total_required_fields': len(self.REQUIRED_FIELDS),
            'present_fields': present_fields,
            'missing_fields': missing_fields,
            'coverage_count': len(present_fields),
            'coverage_percentage': round(coverage_percentage, 3),
            'required_fields': self.REQUIRED_FIELDS
        }
        
        return coverage_percentage, analysis
    
    def calculate_zoning_compatibility_confidence(
        self,
        template: str,
        zoning: str,
        zoning_engine
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate confidence based on zoning compatibility
        
        Args:
            template: Primary development template
            zoning: Property zoning code
            zoning_engine: ZoningConstraintsEngine instance
            
        Returns:
            Tuple of (confidence_component, analysis_dict)
        """
        # Check if template-zoning combination is compatible
        is_compatible = zoning_engine.is_compatible(template, zoning)
        
        if not is_compatible:
            analysis = {
                'template': template,
                'zoning': zoning,
                'compatible': False,
                'compatibility_confidence': 0.0,
                'reason': 'incompatible_combination'
            }
            return 0.0, analysis
        
        # Calculate compatibility strength based on score cap relative to maximum
        score_cap = zoning_engine.get_score_cap(template, zoning)
        
        # Get maximum possible cap for this template across all zones
        template_caps = zoning_engine.score_caps.get(template, {})
        max_cap = max(template_caps.values()) if template_caps else 10.0
        
        # Normalize to 0-1 scale (compatibility strength)
        compatibility_strength = score_cap / max_cap
        
        analysis = {
            'template': template,
            'zoning': zoning,
            'compatible': True,
            'score_cap': score_cap,
            'max_possible_cap': max_cap,
            'compatibility_strength': round(compatibility_strength, 3),
            'compatibility_confidence': round(compatibility_strength, 3)
        }
        
        return compatibility_strength, analysis
    
    def calculate_score_quality_confidence(
        self,
        primary_score: float
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate confidence based on absolute score quality
        High scores (8+) get high confidence, low scores get low confidence
        
        Args:
            primary_score: The primary template score (0-10)
            
        Returns:
            Tuple of (confidence_component, analysis_dict)
        """
        # Map score ranges to confidence
        if primary_score >= 8.0:
            quality_confidence = 0.9 + (primary_score - 8.0) * 0.05  # 0.9-1.0 for 8-10
        elif primary_score >= 6.0:
            quality_confidence = 0.6 + (primary_score - 6.0) * 0.15  # 0.6-0.9 for 6-8
        elif primary_score >= 4.0:
            quality_confidence = 0.3 + (primary_score - 4.0) * 0.15  # 0.3-0.6 for 4-6
        else:
            quality_confidence = primary_score * 0.075  # 0-0.3 for 0-4
        
        analysis = {
            'primary_score': round(primary_score, 1),
            'quality_confidence': round(quality_confidence, 3),
            'score_tier': 'high' if primary_score >= 8 else 'medium' if primary_score >= 6 else 'low'
        }
        
        return quality_confidence, analysis
    
    def calculate_overall_confidence(
        self,
        primary_score: float,
        secondary_score: float,
        features: Dict[str, Any],
        template: str,
        zoning: str,
        zoning_engine
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate overall confidence score using RECALIBRATED weighted components
        Now includes score quality and geographic adjustments
        
        Args:
            primary_score: Highest template score
            secondary_score: Second highest template score  
            features: Property features dictionary
            template: Primary development template
            zoning: Property zoning code
            zoning_engine: ZoningConstraintsEngine instance
            
        Returns:
            Tuple of (overall_confidence, detailed_analysis)
        """
        # Calculate individual components
        margin_conf, margin_analysis = self.calculate_score_margin_confidence(
            primary_score, secondary_score
        )
        
        coverage_conf, coverage_analysis = self.calculate_data_coverage_confidence(
            features
        )
        
        compat_conf, compat_analysis = self.calculate_zoning_compatibility_confidence(
            template, zoning, zoning_engine
        )
        
        # NEW: Score quality component
        quality_conf, quality_analysis = self.calculate_score_quality_confidence(
            primary_score
        )
        
        # Calculate weighted overall confidence with new weights
        base_confidence = (
            margin_conf * self.SCORE_MARGIN_WEIGHT +
            coverage_conf * self.DATA_COVERAGE_WEIGHT + 
            compat_conf * self.ZONING_COMPAT_WEIGHT +
            quality_conf * self.SCORE_QUALITY_WEIGHT
        )
        
        # Apply geographic adjustment
        overall_confidence, geo_analysis = self.geo_calibrator.adjust_confidence_for_location(
            base_confidence, features
        )
        
        # Create detailed analysis
        analysis = {
            'overall_confidence': round(overall_confidence, 3),
            'base_confidence': round(base_confidence, 3),
            'geographic_adjustment': geo_analysis,
            'component_weights': {
                'score_margin': self.SCORE_MARGIN_WEIGHT,
                'data_coverage': self.DATA_COVERAGE_WEIGHT,
                'zoning_compatibility': self.ZONING_COMPAT_WEIGHT,
                'score_quality': self.SCORE_QUALITY_WEIGHT
            },
            'components': {
                'score_margin': {
                    'confidence': round(margin_conf, 3),
                    'weighted_contribution': round(margin_conf * self.SCORE_MARGIN_WEIGHT, 3),
                    **margin_analysis
                },
                'data_coverage': {
                    'confidence': round(coverage_conf, 3),
                    'weighted_contribution': round(coverage_conf * self.DATA_COVERAGE_WEIGHT, 3),
                    **coverage_analysis
                },
                'zoning_compatibility': {
                    'confidence': round(compat_conf, 3),
                    'weighted_contribution': round(compat_conf * self.ZONING_COMPAT_WEIGHT, 3),
                    **compat_analysis
                },
                'score_quality': {
                    'confidence': round(quality_conf, 3),
                    'weighted_contribution': round(quality_conf * self.SCORE_QUALITY_WEIGHT, 3),
                    **quality_analysis
                }
            }
        }
        
        # Add confidence level interpretation (FIXED THRESHOLDS)
        if overall_confidence >= 0.8:
            analysis['confidence_level'] = 'high'
            analysis['interpretation'] = 'High confidence in scoring accuracy'
        elif overall_confidence >= 0.6:
            analysis['confidence_level'] = 'medium'
            analysis['interpretation'] = 'Moderate confidence, acceptable for recommendations'
        elif overall_confidence >= 0.5:
            analysis['confidence_level'] = 'low'
            analysis['interpretation'] = 'Low confidence, flag as uncertain'
        else:
            analysis['confidence_level'] = 'very_low'
            analysis['interpretation'] = 'Very low confidence, do not make primary recommendations'
        
        return overall_confidence, analysis

def calculate_confidence_score(
    primary_score: float,
    secondary_score: float,
    features: Dict[str, Any],
    template: str,
    zoning: str,
    zoning_engine
) -> Tuple[float, Dict[str, Any]]:
    """
    Convenience function to calculate confidence score
    
    Args:
        primary_score: Highest template score
        secondary_score: Second highest template score
        features: Property features dictionary
        template: Primary development template
        zoning: Property zoning code
        zoning_engine: ZoningConstraintsEngine instance
        
    Returns:
        Tuple of (confidence_score, detailed_analysis)
    """
    engine = ConfidenceEngine()
    return engine.calculate_overall_confidence(
        primary_score, secondary_score, features, template, zoning, zoning_engine
    )