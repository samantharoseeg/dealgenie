"""
DealGenie Multi-Template Scoring Engine v1.2

Core multi-template scoring engine that processes all viable templates,
applies constraints, calculates confidence scores, and determines 
primary/secondary/tertiary recommendations based on threshold rules.
"""

import logging
import yaml
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
from datetime import datetime

from .engine import calculate_score
from .zoning_engine import ZoningConstraintsEngine
from .multi_template_engine import MultiTemplateEngine
from .confidence_engine import ConfidenceEngine
from .batch_processor import BatchProcessor
from .business_logic_fixes import determine_recommendations_fixed, format_business_guidance
from .geographic_calibration import GeographicCalibrator

logger = logging.getLogger(__name__)

class MultiTemplateScorer:
    """Core multi-template scoring engine"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize multi-template scorer
        
        Args:
            config_path: Path to environment_v12.yml config file
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent / "config" / "environment_v12.yml"
        
        # Load configuration
        self.config = self._load_config(config_path)
        self.thresholds = self.config.get('confidence_thresholds', {})
        
        # Initialize component engines
        self.zoning_engine = ZoningConstraintsEngine()
        self.multi_engine = MultiTemplateEngine(config_path)
        self.confidence_engine = ConfidenceEngine()
        self.batch_processor = BatchProcessor(self.zoning_engine)
        self.geo_calibrator = GeographicCalibrator()
        
        # Recommendation thresholds from config
        self.CONF_PRIMARY_MIN = self.thresholds.get('CONF_PRIMARY_MIN', 1.5)
        self.CONF_SECONDARY_MIN = self.thresholds.get('CONF_SECONDARY_MIN', 1.0)
        self.CONF_TERNARY_MIN = self.thresholds.get('CONF_TERNARY_MIN', 0.5)
        self.TEMPLATE_VIABLE_MIN = self.thresholds.get('TEMPLATE_VIABLE_MIN', 2.0)
        self.TEMPLATE_STRONG_MIN = self.thresholds.get('TEMPLATE_STRONG_MIN', 6.0)
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Loaded multi-template config from {config_path}")
            return config
        except Exception as e:
            logger.error(f"Failed to load config from {config_path}: {e}")
            return {
                'confidence_thresholds': {
                    'CONF_PRIMARY_MIN': 1.5,
                    'CONF_SECONDARY_MIN': 1.0,
                    'CONF_TERNARY_MIN': 0.5,
                    'TEMPLATE_VIABLE_MIN': 2.0,
                    'TEMPLATE_STRONG_MIN': 6.0
                }
            }
    
    def score_single_template(
        self,
        features: Dict[str, Any],
        template: str,
        apply_constraints: bool = True
    ) -> Dict[str, Any]:
        """
        Score a single template with optional constraint application
        
        Args:
            features: Property features dictionary
            template: Template to score
            apply_constraints: Whether to apply zoning constraints
            
        Returns:
            Template scoring result dictionary
        """
        try:
            # Calculate raw score using existing engine
            raw_result = calculate_score(features, template)
            raw_score = raw_result['score']
            zoning = features.get('zoning', 'R1')
            
            result = {
                'template': template,
                'raw_score': raw_score,
                'constrained_score': raw_score,
                'viable': raw_score >= self.TEMPLATE_VIABLE_MIN,
                'strong': raw_score >= self.TEMPLATE_STRONG_MIN,
                'zoning': zoning,
                'constraints_applied': {},
                'raw_result': raw_result
            }
            
            # Apply zoning constraints if requested
            if apply_constraints:
                constrained_score, constraints = self.zoning_engine.apply_constraints(
                    raw_score, template, zoning
                )
                
                # Apply geographic adjustment to score
                adjusted_score, geo_details = self.geo_calibrator.adjust_score_for_location(
                    constrained_score, features
                )
                
                result['constrained_score'] = adjusted_score
                result['constraints_applied'] = constraints
                result['geographic_adjustment'] = geo_details
                result['viable'] = adjusted_score >= self.TEMPLATE_VIABLE_MIN
                result['strong'] = adjusted_score >= self.TEMPLATE_STRONG_MIN
            
            return result
            
        except Exception as e:
            logger.error(f"Error scoring template {template}: {e}")
            return {
                'template': template,
                'raw_score': 0.0,
                'constrained_score': 0.0,
                'viable': False,
                'strong': False,
                'error': str(e)
            }
    
    def process_multi_template(
        self,
        features: Dict[str, Any],
        force_multi_template: bool = False
    ) -> Dict[str, Any]:
        """
        Process all viable templates for a parcel and determine recommendations
        
        Args:
            features: Property features dictionary
            force_multi_template: Force multi-template even if triggers not met
            
        Returns:
            Complete multi-template scoring result
        """
        zoning = features.get('zoning', 'R1')
        parcel_id = features.get('apn', features.get('parcel_id', 'unknown'))
        
        # Step 1: Pre-filter viable templates
        viable_templates, compatibility_scores = self.batch_processor.pre_filter_templates(zoning)
        
        if not viable_templates:
            logger.warning(f"No viable templates for parcel {parcel_id} with zoning {zoning}")
            return self._create_empty_result(parcel_id, zoning, "no_viable_templates")
        
        # Step 2: Score all viable templates
        template_results = {}
        template_scores = {}
        
        for template in viable_templates:
            result = self.score_single_template(features, template, apply_constraints=True)
            template_results[template] = result
            template_scores[template] = result['constrained_score']
        
        # Step 3: Check if multi-template triggers are met (unless forced)
        should_run_multi = force_multi_template
        trigger_analysis = {}
        
        if not force_multi_template:
            should_run_multi, trigger_analysis = self.multi_engine.should_run_multi_template(
                zoning, template_scores, compatibility_scores
            )
        
        # If multi-template not triggered, return single best template
        if not should_run_multi:
            best_template = max(template_scores.items(), key=lambda x: x[1])
            return self._create_single_template_result(
                parcel_id, zoning, best_template[0], template_results[best_template[0]]
            )
        
        # Step 4: Calculate confidence scores for all viable templates
        sorted_scores = sorted(template_scores.values(), reverse=True)
        primary_score = sorted_scores[0] if sorted_scores else 0.0
        secondary_score = sorted_scores[1] if len(sorted_scores) > 1 else 0.0
        
        template_confidences = {}
        
        for template, result in template_results.items():
            if result['viable']:
                confidence, conf_analysis = self.confidence_engine.calculate_overall_confidence(
                    result['constrained_score'], secondary_score, features, template, zoning, self.zoning_engine
                )
                template_confidences[template] = {
                    'confidence': confidence,
                    'analysis': conf_analysis
                }
        
        # Step 5: Determine recommendations with FIXED business logic
        recommendations = determine_recommendations_fixed(
            template_results, template_confidences, template_scores, 
            {
                'CONF_SECONDARY_MIN': self.CONF_SECONDARY_MIN,
                'CONF_TERNARY_MIN': self.CONF_TERNARY_MIN
            }
        )
        
        # Add business guidance
        recommendations['business_guidance'] = format_business_guidance(recommendations)
        
        # Step 6: Create comprehensive result
        result = {
            'parcel_id': parcel_id,
            'zoning': zoning,
            'multi_template_triggered': should_run_multi,
            'trigger_analysis': trigger_analysis,
            'viable_templates_count': len(viable_templates),
            'scored_templates_count': len([r for r in template_results.values() if r['viable']]),
            'template_results': template_results,
            'template_confidences': template_confidences,
            'recommendations': recommendations,
            'compatibility_scores': compatibility_scores,
            'processing_timestamp': datetime.now().isoformat()
        }
        
        return result
    
    def _determine_recommendations(
        self,
        template_results: Dict[str, Dict[str, Any]],
        template_confidences: Dict[str, Dict[str, Any]], 
        template_scores: Dict[str, float]
    ) -> Dict[str, Any]:
        """
        Determine primary/secondary/tertiary recommendations based on threshold rules
        
        Args:
            template_results: Template scoring results
            template_confidences: Template confidence scores
            template_scores: Template final scores
            
        Returns:
            Recommendations dictionary with primary/secondary/tertiary
        """
        # Filter to viable templates only
        viable_results = {
            template: result for template, result in template_results.items()
            if result['viable']
        }
        
        if not viable_results:
            return {
                'primary': None,
                'secondary': None,
                'tertiary': None,
                'reason': 'no_viable_templates'
            }
        
        # Sort by constrained score descending
        sorted_templates = sorted(
            viable_results.items(),
            key=lambda x: x[1]['constrained_score'],
            reverse=True
        )
        
        recommendations = {
            'primary': None,
            'secondary': None,
            'tertiary': None
        }
        
        # Calculate score gaps
        scores = [result['constrained_score'] for _, result in sorted_templates]
        
        # Primary recommendation (always the highest scoring viable template)
        if sorted_templates:
            primary_template, primary_result = sorted_templates[0]
            primary_score = primary_result['constrained_score']
            
            recommendations['primary'] = {
                'template': primary_template,
                'score': primary_score,
                'confidence': template_confidences.get(primary_template, {}).get('confidence', 0.0),
                'strong': primary_result['strong'],
                'gap_to_next': scores[0] - scores[1] if len(scores) > 1 else float('inf')
            }
        
        # Secondary recommendation (if gap ≥ CONF_SECONDARY_MIN)
        if len(sorted_templates) > 1:
            secondary_template, secondary_result = sorted_templates[1]
            secondary_score = secondary_result['constrained_score']
            gap = scores[0] - scores[1]
            
            if gap >= self.CONF_SECONDARY_MIN:
                recommendations['secondary'] = {
                    'template': secondary_template,
                    'score': secondary_score,
                    'confidence': template_confidences.get(secondary_template, {}).get('confidence', 0.0),
                    'strong': secondary_result['strong'],
                    'gap_to_next': scores[1] - scores[2] if len(scores) > 2 else float('inf'),
                    'gap_from_primary': gap
                }
        
        # Tertiary recommendation (if gap ≥ CONF_TERNARY_MIN)  
        if len(sorted_templates) > 2 and recommendations['secondary']:
            tertiary_template, tertiary_result = sorted_templates[2]
            tertiary_score = tertiary_result['constrained_score']
            gap = scores[1] - scores[2]
            
            if gap >= self.CONF_TERNARY_MIN:
                recommendations['tertiary'] = {
                    'template': tertiary_template,
                    'score': tertiary_score,
                    'confidence': template_confidences.get(tertiary_template, {}).get('confidence', 0.0),
                    'strong': tertiary_result['strong'],
                    'gap_from_secondary': gap
                }
        
        return recommendations
    
    def _create_empty_result(self, parcel_id: str, zoning: str, reason: str) -> Dict[str, Any]:
        """Create empty result for parcels with no viable templates"""
        return {
            'parcel_id': parcel_id,
            'zoning': zoning,
            'multi_template_triggered': False,
            'viable_templates_count': 0,
            'scored_templates_count': 0,
            'template_results': {},
            'template_confidences': {},
            'recommendations': {
                'primary': None,
                'secondary': None,
                'tertiary': None,
                'reason': reason
            },
            'processing_timestamp': datetime.now().isoformat()
        }
    
    def _create_single_template_result(
        self,
        parcel_id: str,
        zoning: str,
        template: str,
        template_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create result for single template (multi-template not triggered)"""
        return {
            'parcel_id': parcel_id,
            'zoning': zoning,
            'multi_template_triggered': False,
            'trigger_analysis': {'triggered': False, 'reason': 'single_template_sufficient'},
            'viable_templates_count': 1,
            'scored_templates_count': 1,
            'template_results': {template: template_result},
            'template_confidences': {},
            'recommendations': {
                'primary': {
                    'template': template,
                    'score': template_result['constrained_score'],
                    'confidence': 0.8,  # Default high confidence for single template
                    'strong': template_result['strong']
                },
                'secondary': None,
                'tertiary': None
            },
            'processing_timestamp': datetime.now().isoformat()
        }

def score_parcel_multi_template(
    features: Dict[str, Any],
    force_multi_template: bool = False,
    config_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Convenience function to score a parcel with multi-template logic
    
    Args:
        features: Property features dictionary
        force_multi_template: Force multi-template even if not triggered
        config_path: Optional path to config file
        
    Returns:
        Multi-template scoring result
    """
    scorer = MultiTemplateScorer(config_path)
    return scorer.process_multi_template(features, force_multi_template)