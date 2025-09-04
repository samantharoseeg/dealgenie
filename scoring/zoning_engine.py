"""
DealGenie Zoning Constraints Engine v1.2

Implements zoning constraints logic including caps, plausibility floors,
compatibility matrix, and fallback handling for unknown zoning codes.
"""

import yaml
import logging
from typing import Dict, Any, Optional, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)

class ZoningConstraintsEngine:
    """Handles zoning constraints and compatibility logic"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize with zoning constraints configuration
        
        Args:
            config_path: Path to zoning_v12.yml config file
        """
        if config_path is None:
            # Try LA-enhanced config first, fallback to v12
            la_enhanced_path = Path(__file__).parent / "constraints" / "zoning_la_enhanced.yml"
            if la_enhanced_path.exists():
                config_path = la_enhanced_path
            else:
                config_path = Path(__file__).parent / "constraints" / "zoning_v12.yml"
        
        self.config = self._load_config(config_path)
        self.score_caps = self.config.get('score_caps', {})
        self.plausibility_floors = self.config.get('plausibility_floors', {})
        self.compatibility_matrix = self.config.get('compatibility_matrix', {})
        self.default_unknown = self.config.get('default_unknown', {})
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load zoning constraints configuration from YAML"""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Loaded zoning constraints from {config_path}")
            return config
        except Exception as e:
            logger.error(f"Failed to load zoning config from {config_path}: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Return default configuration if config file unavailable"""
        return {
            'score_caps': {},
            'plausibility_floors': {},
            'compatibility_matrix': {},
            'default_unknown': {
                'score_cap': 5.0,
                'plausibility_floor': 1.0,
                'compatible': False
            }
        }
    
    def get_score_cap(self, template: str, zoning: str) -> float:
        """
        Get maximum allowed score for template-zoning combination
        
        Args:
            template: Development template (retail, office, etc.)
            zoning: Zoning code (R1, C2, M1, etc.)
            
        Returns:
            Maximum score cap for this combination
        """
        template_caps = self.score_caps.get(template, {})
        return template_caps.get(zoning, self.default_unknown['score_cap'])
    
    def get_plausibility_floor(self, template: str) -> float:
        """
        Get minimum viable score for template
        
        Args:
            template: Development template
            
        Returns:
            Minimum plausible score for this template
        """
        return self.plausibility_floors.get(template, self.default_unknown['plausibility_floor'])
    
    def is_compatible(self, template: str, zoning: str) -> bool:
        """
        Check if template-zoning combination is compatible
        
        Args:
            template: Development template
            zoning: Zoning code
            
        Returns:
            True if combination is viable/compatible
        """
        template_compat = self.compatibility_matrix.get(template, {})
        return template_compat.get(zoning, self.default_unknown['compatible'])
    
    def apply_constraints(
        self, 
        raw_score: float, 
        template: str, 
        zoning: str
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Apply zoning constraints to raw score
        
        Args:
            raw_score: Unconstrained score from scoring engine
            template: Development template
            zoning: Property zoning code
            
        Returns:
            Tuple of (constrained_score, applied_constraints_dict)
        """
        applied_constraints = {
            'template': template,
            'zoning': zoning,
            'raw_score': round(raw_score, 1),
            'constraints_applied': []
        }
        
        constrained_score = raw_score
        
        # Check compatibility first
        if not self.is_compatible(template, zoning):
            applied_constraints['constraints_applied'].append({
                'type': 'compatibility',
                'reason': f'{template} not compatible with {zoning}',
                'action': 'score set to 0.0'
            })
            applied_constraints['final_score'] = 0.0
            applied_constraints['summary'] = "Applied 1 constraints: compatibility"
            return 0.0, applied_constraints
        
        # Apply score cap
        score_cap = self.get_score_cap(template, zoning)
        if constrained_score > score_cap:
            applied_constraints['constraints_applied'].append({
                'type': 'score_cap',
                'limit': score_cap,
                'original': round(constrained_score, 1),
                'action': f'capped at {score_cap}'
            })
            constrained_score = score_cap
        
        # Apply plausibility floor
        plausibility_floor = self.get_plausibility_floor(template)
        if constrained_score < plausibility_floor:
            applied_constraints['constraints_applied'].append({
                'type': 'plausibility_floor',
                'minimum': plausibility_floor,
                'original': round(constrained_score, 1),
                'action': f'raised to minimum {plausibility_floor}'
            })
            constrained_score = plausibility_floor
        
        applied_constraints['final_score'] = round(constrained_score, 1)
        
        # Add summary if constraints were applied
        if applied_constraints['constraints_applied']:
            constraint_types = [c['type'] for c in applied_constraints['constraints_applied']]
            applied_constraints['summary'] = f"Applied {len(constraint_types)} constraints: {', '.join(constraint_types)}"
        else:
            applied_constraints['summary'] = "No constraints applied"
        
        return round(constrained_score, 1), applied_constraints

def apply_zoning_constraints(
    raw_score: float,
    template: str, 
    zoning: str,
    config_path: Optional[str] = None
) -> Tuple[float, Dict[str, Any]]:
    """
    Convenience function to apply zoning constraints
    
    Args:
        raw_score: Unconstrained score
        template: Development template
        zoning: Property zoning code  
        config_path: Optional path to config file
        
    Returns:
        Tuple of (constrained_score, applied_constraints)
    """
    engine = ZoningConstraintsEngine(config_path)
    return engine.apply_constraints(raw_score, template, zoning)