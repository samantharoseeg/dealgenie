"""
DealGenie Multi-Template Engine v1.2

Implements compatibility matrix logic and multi-template trigger rules.
Determines when to run multi-template scoring based on trigger conditions.
"""

import yaml
import logging
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)

class MultiTemplateEngine:
    """Handles multi-template scoring triggers and compatibility analysis"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize multi-template engine
        
        Args:
            config_path: Path to environment_v12.yml config file
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent / "config" / "environment_v12.yml"
        
        self.config = self._load_config(config_path)
        self.templates = self.config.get('templates', [])
        self.thresholds = self.config.get('confidence_thresholds', {})
        self.zoning_codes = self.config.get('zoning_codes', [])
        
        # Multi-template trigger thresholds
        self.CLOSE_SCORES_DELTA = 1.0  # Δ ≤ 1.0 triggers multi-template
        self.HIGH_COMPATIBILITY_THRESHOLD = 0.6  # ≥ 0.6 compatibility
        
        # Mixed-use zoning codes that trigger multi-template
        self.MIXED_USE_ZONES = ['CM', 'CR', 'C2', 'C4', 'RAS3', 'RAS4']
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load environment configuration from YAML"""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Loaded multi-template config from {config_path}")
            return config
        except Exception as e:
            logger.error(f"Failed to load config from {config_path}: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Return default configuration if config file unavailable"""
        return {
            'templates': ['retail', 'office', 'multifamily', 'residential', 'commercial', 'industrial'],
            'confidence_thresholds': {
                'CONF_PRIMARY_MIN': 1.5,
                'CONF_SECONDARY_MIN': 1.0,
                'CONF_TERNARY_MIN': 0.5
            },
            'zoning_codes': ['R1', 'R2', 'R3', 'R4', 'R5', 'C1', 'C2', 'CM', 'CR', 'CC', 'M1', 'M2', 'M3', 'A1', 'OS']
        }
    
    def has_mixed_use_zoning(self, zoning: str) -> bool:
        """
        Check if property has mixed-use zoning that triggers multi-template
        
        Args:
            zoning: Property zoning code
            
        Returns:
            True if zoning indicates mixed-use potential
        """
        return any(zone in zoning for zone in self.MIXED_USE_ZONES)
    
    def calculate_template_compatibility(
        self, 
        zoning: str,
        zoning_engine
    ) -> Dict[str, float]:
        """
        Calculate compatibility score for all templates with given zoning
        
        Args:
            zoning: Property zoning code
            zoning_engine: ZoningConstraintsEngine instance
            
        Returns:
            Dictionary mapping template -> compatibility score (0.0-1.0)
        """
        compatibility_scores = {}
        
        for template in self.templates:
            # Base compatibility from zoning matrix
            is_compatible = zoning_engine.is_compatible(template, zoning)
            
            if not is_compatible:
                compatibility_scores[template] = 0.0
                continue
            
            # Calculate compatibility score based on score cap relative to max possible
            score_cap = zoning_engine.get_score_cap(template, zoning)
            template_caps = zoning_engine.score_caps.get(template, {})
            max_possible_cap = max(template_caps.values()) if template_caps else 10.0
            
            # Normalize to 0-1 scale
            compatibility_scores[template] = score_cap / max_possible_cap
        
        return compatibility_scores
    
    def has_close_scores(self, template_scores: Dict[str, float]) -> Tuple[bool, Dict[str, Any]]:
        """
        Check if templates have close scores (Δ ≤ 1.0)
        
        Args:
            template_scores: Dictionary mapping template -> score
            
        Returns:
            Tuple of (has_close_scores, analysis_dict)
        """
        if len(template_scores) < 2:
            return False, {'reason': 'insufficient_templates', 'count': len(template_scores)}
        
        # Sort scores descending
        sorted_scores = sorted(template_scores.items(), key=lambda x: x[1], reverse=True)
        
        # Check gaps between consecutive scores
        close_pairs = []
        for i in range(len(sorted_scores) - 1):
            current = sorted_scores[i]
            next_score = sorted_scores[i + 1]
            delta = current[1] - next_score[1]
            
            if delta <= self.CLOSE_SCORES_DELTA:
                close_pairs.append({
                    'template1': current[0],
                    'score1': current[1],
                    'template2': next_score[0],
                    'score2': next_score[1],
                    'delta': round(delta, 2)
                })
        
        has_close = len(close_pairs) > 0
        
        analysis = {
            'has_close_scores': has_close,
            'close_pairs': close_pairs,
            'top_score': sorted_scores[0][1] if sorted_scores else 0.0,
            'score_range': sorted_scores[0][1] - sorted_scores[-1][1] if len(sorted_scores) > 1 else 0.0,
            'threshold': self.CLOSE_SCORES_DELTA
        }
        
        return has_close, analysis
    
    def has_high_compatibility(
        self,
        compatibility_scores: Dict[str, float]
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Check if property has high compatibility (≥ 0.6) with multiple templates
        
        Args:
            compatibility_scores: Template compatibility scores
            
        Returns:
            Tuple of (has_high_compatibility, analysis_dict)
        """
        high_compat_templates = {
            template: score for template, score in compatibility_scores.items()
            if score >= self.HIGH_COMPATIBILITY_THRESHOLD
        }
        
        has_high = len(high_compat_templates) >= 2
        
        analysis = {
            'has_high_compatibility': has_high,
            'high_compatibility_templates': high_compat_templates,
            'threshold': self.HIGH_COMPATIBILITY_THRESHOLD,
            'max_compatibility': max(compatibility_scores.values()) if compatibility_scores else 0.0,
            'avg_compatibility': sum(compatibility_scores.values()) / len(compatibility_scores) if compatibility_scores else 0.0
        }
        
        return has_high, analysis
    
    def should_run_multi_template(
        self,
        zoning: str,
        template_scores: Optional[Dict[str, float]] = None,
        compatibility_scores: Optional[Dict[str, float]] = None
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Determine if multi-template scoring should be triggered
        
        Args:
            zoning: Property zoning code
            template_scores: Optional pre-calculated template scores
            compatibility_scores: Optional pre-calculated compatibility scores
            
        Returns:
            Tuple of (should_run, trigger_analysis)
        """
        trigger_analysis = {
            'zoning': zoning,
            'triggers': {},
            'triggered': False,
            'primary_trigger': None
        }
        
        # Trigger 1: Mixed-use zoning
        mixed_use_trigger = self.has_mixed_use_zoning(zoning)
        trigger_analysis['triggers']['mixed_use_zoning'] = {
            'triggered': mixed_use_trigger,
            'zoning': zoning,
            'mixed_use_zones': self.MIXED_USE_ZONES
        }
        
        # Trigger 2: Close scores (if provided)
        if template_scores:
            close_scores_trigger, close_analysis = self.has_close_scores(template_scores)
            trigger_analysis['triggers']['close_scores'] = {
                'triggered': close_scores_trigger,
                **close_analysis
            }
        else:
            trigger_analysis['triggers']['close_scores'] = {
                'triggered': False,
                'reason': 'no_template_scores_provided'
            }
        
        # Trigger 3: High compatibility (if provided)
        if compatibility_scores:
            high_compat_trigger, compat_analysis = self.has_high_compatibility(compatibility_scores)
            trigger_analysis['triggers']['high_compatibility'] = {
                'triggered': high_compat_trigger,
                **compat_analysis
            }
        else:
            trigger_analysis['triggers']['high_compatibility'] = {
                'triggered': False,
                'reason': 'no_compatibility_scores_provided'
            }
        
        # Core trigger logic (10 lines)
        triggered_conditions = []
        
        # Trigger 1: Mixed-use zoning
        if mixed_use_trigger:
            triggered_conditions.append('mixed_use_zoning')
        
        # Trigger 2: Close scores (Δ ≤ 1.0)  
        if template_scores and self.has_close_scores(template_scores)[0]:
            triggered_conditions.append('close_scores')
        
        # Trigger 3: High compatibility (≥ 0.6)
        if compatibility_scores and self.has_high_compatibility(compatibility_scores)[0]:
            triggered_conditions.append('high_compatibility')
        
        # Final determination
        trigger_analysis['triggered'] = len(triggered_conditions) > 0
        trigger_analysis['triggered_conditions'] = triggered_conditions
        trigger_analysis['primary_trigger'] = triggered_conditions[0] if triggered_conditions else None
        
        return trigger_analysis['triggered'], trigger_analysis

def should_run_multi_template_scoring(
    zoning: str,
    template_scores: Optional[Dict[str, float]] = None,
    compatibility_scores: Optional[Dict[str, float]] = None,
    config_path: Optional[str] = None
) -> Tuple[bool, Dict[str, Any]]:
    """
    Convenience function to check multi-template triggers
    
    Args:
        zoning: Property zoning code
        template_scores: Optional template scores
        compatibility_scores: Optional compatibility scores  
        config_path: Optional path to config file
        
    Returns:
        Tuple of (should_run, trigger_analysis)
    """
    engine = MultiTemplateEngine(config_path)
    return engine.should_run_multi_template(zoning, template_scores, compatibility_scores)