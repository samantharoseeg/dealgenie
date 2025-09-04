"""
DealGenie Result Formatting Engine v1.2

Implements result ranking logic and output formatting for multi-template results.
Creates JSON payload structure with viable_uses array, primary recommendation, 
and alternatives list as specified in v1.2.
"""

import logging
import json
from typing import Dict, Any, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class ResultFormatter:
    """Handles result ranking and JSON payload formatting"""
    
    def __init__(self):
        """Initialize result formatter"""
        self.version = "1.2"
        
    def format_multi_template_result(
        self,
        scoring_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Format multi-template scoring result into v1.2 JSON payload structure
        
        Args:
            scoring_result: Result from MultiTemplateScorer.process_multi_template()
            
        Returns:
            Formatted JSON payload matching v1.2 specification
        """
        try:
            # Handle None scoring_result
            if scoring_result is None:
                return self._create_error_payload('unknown', 'unknown', 'Scoring result is None')
            
            # Extract basic information
            parcel_id = scoring_result.get('parcel_id', 'unknown')
            zoning = scoring_result.get('zoning', 'unknown')
            multi_triggered = scoring_result.get('multi_template_triggered', False)
            recommendations = scoring_result.get('recommendations', {})
            template_results = scoring_result.get('template_results', {})
            template_confidences = scoring_result.get('template_confidences', {})
            
            # Build viable_uses array
            viable_uses = self._build_viable_uses_array(
                template_results, template_confidences, recommendations
            )
            
            # Build primary recommendation (handle new business logic)
            analysis_type = recommendations.get('analysis_type', 'clear_ranking')
            primary_rec = recommendations.get('primary')
            
            if analysis_type in ['multiple_viable_options', 'statistical_tie']:
                # No primary recommendation for these cases
                primary_recommendation = None
            else:
                primary_recommendation = self._build_primary_recommendation(
                    primary_rec, template_results, template_confidences
                )
            
            # Build alternatives list
            alternatives = self._build_alternatives_list(
                recommendations, template_results, template_confidences
            )
            
            # Create main payload structure
            payload = {
                'version': self.version,
                'parcel_id': parcel_id,
                'zoning': zoning,
                'scoring_method': 'multi_template' if multi_triggered else 'single_template',
                'scored_at': scoring_result.get('processing_timestamp', datetime.now().isoformat()),
                
                # Core results
                'viable_uses': viable_uses,
                'primary_recommendation': primary_recommendation,
                'alternatives': alternatives,
                
                # Business logic results
                'analysis_type': analysis_type,
                'business_guidance': recommendations.get('business_guidance', ''),
                'viable_options': recommendations.get('viable_options', []),
                'tied_options': recommendations.get('tied_options', []),
                
                # Metadata
                'meta': {
                    'templates_evaluated': scoring_result.get('viable_templates_count', 0),
                    'templates_scored': scoring_result.get('scored_templates_count', 0),
                    'multi_template_triggered': multi_triggered,
                    'trigger_analysis': scoring_result.get('trigger_analysis', {}),
                    'compatibility_scores': scoring_result.get('compatibility_scores', {})
                }
            }
            
            return payload
            
        except Exception as e:
            import traceback
            logger.error(f"Error formatting multi-template result: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return self._create_error_payload(
                scoring_result.get('parcel_id', 'unknown') if scoring_result else 'unknown',
                scoring_result.get('zoning', 'unknown') if scoring_result else 'unknown',
                str(e)
            )
    
    def _build_viable_uses_array(
        self,
        template_results: Dict[str, Dict[str, Any]],
        template_confidences: Dict[str, Dict[str, Any]],
        recommendations: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Build viable_uses array with all viable templates ranked by score
        
        Args:
            template_results: Template scoring results
            template_confidences: Template confidence scores
            recommendations: Recommendation structure
            
        Returns:
            List of viable use dictionaries sorted by score descending
        """
        viable_uses = []
        
        # Filter to viable templates only
        viable_templates = {
            template: result for template, result in template_results.items()
            if result.get('viable', False)
        }
        
        # Sort by constrained score descending
        sorted_templates = sorted(
            viable_templates.items(),
            key=lambda x: x[1]['constrained_score'],
            reverse=True
        )
        
        for rank, (template, result) in enumerate(sorted_templates, 1):
            confidence_data = template_confidences.get(template, {})
            confidence_score = confidence_data.get('confidence', 0.0)
            confidence_analysis = confidence_data.get('analysis', {})
            
            # Determine recommendation level (handle None recommendations)
            rec_level = None
            if recommendations is not None:
                if recommendations.get('primary', {}) and recommendations.get('primary', {}).get('template') == template:
                    rec_level = 'primary'
                elif recommendations.get('secondary', {}) and recommendations.get('secondary', {}).get('template') == template:
                    rec_level = 'secondary'
                elif recommendations.get('tertiary', {}) and recommendations.get('tertiary', {}).get('template') == template:
                    rec_level = 'tertiary'
            
            viable_use = {
                'template': template,
                'rank': rank,
                'score': round(result['constrained_score'], 1),
                'raw_score': round(result['raw_score'], 1),
                'confidence': round(confidence_score, 3),
                'confidence_level': confidence_analysis.get('confidence_level', 'unknown'),
                'viable': True,
                'strong': result.get('strong', False),
                'recommendation_level': rec_level,
                'constraints_applied': result.get('constraints_applied', {}),
                'zoning_compatibility': self._get_zoning_compatibility(result, template)
            }
            
            viable_uses.append(viable_use)
        
        return viable_uses
    
    def _build_primary_recommendation(
        self,
        primary_rec: Optional[Dict[str, Any]],
        template_results: Dict[str, Dict[str, Any]],
        template_confidences: Dict[str, Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """
        Build primary recommendation with detailed information
        
        Args:
            primary_rec: Primary recommendation from scoring
            template_results: Template scoring results
            template_confidences: Template confidence data
            
        Returns:
            Formatted primary recommendation or None
        """
        if not primary_rec:
            return None
        
        template = primary_rec['template']
        template_result = template_results.get(template, {})
        confidence_data = template_confidences.get(template, {})
        
        return {
            'template': template,
            'score': round(primary_rec['score'], 1),
            'confidence': round(primary_rec.get('confidence', 0.0), 3),
            'confidence_level': confidence_data.get('analysis', {}).get('confidence_level', 'unknown'),
            'strong_recommendation': primary_rec.get('strong', False),
            'gap_to_next': round(primary_rec.get('gap_to_next', 0.0), 1),
            'reasoning': self._generate_primary_reasoning(primary_rec, template_result, confidence_data),
            'risk_factors': self._extract_risk_factors(template_result),
            'opportunities': self._extract_opportunities(template_result)
        }
    
    def _build_alternatives_list(
        self,
        recommendations: Dict[str, Any],
        template_results: Dict[str, Dict[str, Any]],
        template_confidences: Dict[str, Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Build alternatives list with secondary and tertiary recommendations
        
        Args:
            recommendations: Recommendation structure
            template_results: Template scoring results
            template_confidences: Template confidence data
            
        Returns:
            List of alternative recommendations
        """
        alternatives = []
        
        # Secondary recommendation
        secondary = recommendations.get('secondary')
        if secondary:
            template = secondary['template']
            confidence_data = template_confidences.get(template, {})
            
            alternatives.append({
                'level': 'secondary',
                'template': template,
                'score': round(secondary['score'], 1),
                'confidence': round(secondary.get('confidence', 0.0), 3),
                'confidence_level': confidence_data.get('analysis', {}).get('confidence_level', 'unknown'),
                'gap_from_primary': round(secondary.get('gap_from_primary', 0.0), 1),
                'gap_to_next': round(secondary.get('gap_to_next', 0.0), 1),
                'reasoning': f"Strong alternative with score gap of {secondary.get('gap_from_primary', 0.0):.1f} from primary"
            })
        
        # Tertiary recommendation
        tertiary = recommendations.get('tertiary')
        if tertiary:
            template = tertiary['template']
            confidence_data = template_confidences.get(template, {})
            
            alternatives.append({
                'level': 'tertiary',
                'template': template,
                'score': round(tertiary['score'], 1),
                'confidence': round(tertiary.get('confidence', 0.0), 3),
                'confidence_level': confidence_data.get('analysis', {}).get('confidence_level', 'unknown'),
                'gap_from_secondary': round(tertiary.get('gap_from_secondary', 0.0), 1),
                'reasoning': f"Third option with score gap of {tertiary.get('gap_from_secondary', 0.0):.1f} from secondary"
            })
        
        return alternatives
    
    def _get_zoning_compatibility(self, result: Dict[str, Any], template: str) -> Dict[str, Any]:
        """Extract zoning compatibility information"""
        constraints = result.get('constraints_applied', {})
        
        return {
            'compatible': constraints.get('constraints_applied', []) == [] or 
                         all(c['type'] != 'compatibility' for c in constraints.get('constraints_applied', [])),
            'score_cap': constraints.get('final_score', result.get('constrained_score', 0.0)),
            'constraints_summary': constraints.get('summary', 'No constraints applied')
        }
    
    def _generate_primary_reasoning(
        self,
        primary_rec: Dict[str, Any],
        template_result: Dict[str, Any],
        confidence_data: Dict[str, Any]
    ) -> str:
        """Generate reasoning text for primary recommendation"""
        template = primary_rec['template']
        score = primary_rec['score']
        gap = primary_rec.get('gap_to_next', 0.0)
        strong = primary_rec.get('strong', False)
        
        reasoning_parts = []
        
        # Score strength
        if strong:
            reasoning_parts.append(f"{template.title()} scores {score:.1f}/10 (strong recommendation)")
        else:
            reasoning_parts.append(f"{template.title()} scores {score:.1f}/10 (moderate recommendation)")
        
        # Gap analysis
        if gap > 2.0:
            reasoning_parts.append(f"clear {gap:.1f}-point lead over alternatives")
        elif gap > 1.0:
            reasoning_parts.append(f"{gap:.1f}-point advantage over next option")
        else:
            reasoning_parts.append(f"competitive with {gap:.1f}-point edge")
        
        # Confidence factors (handle None confidence_data)
        if confidence_data is None:
            confidence_data = {}
        confidence_analysis = confidence_data.get('analysis', {})
        components = confidence_analysis.get('components', {})
        
        if components.get('zoning_compatibility', {}).get('compatible', True):
            reasoning_parts.append("excellent zoning compatibility")
        
        if components.get('data_coverage', {}).get('confidence', 0.0) > 0.8:
            reasoning_parts.append("high data confidence")
        
        return ". ".join(reasoning_parts).capitalize() + "."
    
    def _extract_risk_factors(self, template_result: Dict[str, Any]) -> List[str]:
        """Extract risk factors from template scoring result"""
        risk_factors = []
        
        raw_result = template_result.get('raw_result', {})
        penalties = raw_result.get('penalties', {})
        
        # Extract penalty-based risks
        for penalty, value in penalties.items():
            if value > 0.5:
                risk_factors.append(f"{penalty.replace('_', ' ').title()}: -{value:.1f} points")
        
        # Constraint-based risks
        constraints = template_result.get('constraints_applied', {})
        for constraint in constraints.get('constraints_applied', []):
            if constraint['type'] == 'score_cap':
                risk_factors.append(f"Score capped due to zoning limitations")
            elif constraint['type'] == 'compatibility':
                risk_factors.append(f"Template-zoning incompatibility")
        
        return risk_factors[:3]  # Limit to top 3 risks
    
    def _extract_opportunities(self, template_result: Dict[str, Any]) -> List[str]:
        """Extract opportunities from template scoring result"""
        opportunities = []
        
        raw_result = template_result.get('raw_result', {})
        component_scores = raw_result.get('component_scores', {})
        
        # Identify strong components (>7.0)
        for component, score in component_scores.items():
            if score >= 7.0:
                opportunities.append(f"Strong {component.replace('_', ' ')}: {score:.1f}/10")
        
        # Template-specific opportunities
        template = template_result.get('template', '')
        score = template_result.get('constrained_score', 0.0)
        
        if score >= 8.0:
            opportunities.append(f"Excellent {template} development potential")
        elif score >= 6.0:
            opportunities.append(f"Good {template} development opportunity")
        
        return opportunities[:3]  # Limit to top 3 opportunities
    
    def _create_error_payload(self, parcel_id: str, zoning: str, error_msg: str) -> Dict[str, Any]:
        """Create error payload for failed formatting"""
        return {
            'version': self.version,
            'parcel_id': parcel_id,
            'zoning': zoning,
            'scoring_method': 'error',
            'scored_at': datetime.now().isoformat(),
            'viable_uses': [],
            'primary_recommendation': None,
            'alternatives': [],
            'error': error_msg,
            'meta': {
                'templates_evaluated': 0,
                'templates_scored': 0,
                'multi_template_triggered': False
            }
        }
    
    def format_batch_results(
        self,
        batch_results: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Format batch processing results into consolidated payload
        
        Args:
            batch_results: List of individual parcel results
            
        Returns:
            Consolidated batch results payload
        """
        formatted_results = []
        batch_stats = {
            'total_parcels': len(batch_results),
            'successful_scores': 0,
            'multi_template_triggered': 0,
            'average_viable_templates': 0,
            'processing_timestamp': datetime.now().isoformat()
        }
        
        viable_template_counts = []
        
        for result in batch_results:
            try:
                formatted_result = self.format_multi_template_result(result)
                formatted_results.append(formatted_result)
                
                # Update stats
                if 'error' not in formatted_result:
                    batch_stats['successful_scores'] += 1
                    
                if formatted_result.get('meta', {}).get('multi_template_triggered', False):
                    batch_stats['multi_template_triggered'] += 1
                    
                viable_count = formatted_result.get('meta', {}).get('templates_evaluated', 0)
                viable_template_counts.append(viable_count)
                
            except Exception as e:
                logger.error(f"Error formatting result for parcel {result.get('parcel_id', 'unknown')}: {e}")
                formatted_results.append(self._create_error_payload(
                    result.get('parcel_id', 'unknown'),
                    result.get('zoning', 'unknown'),
                    str(e)
                ))
        
        # Calculate average viable templates
        if viable_template_counts:
            batch_stats['average_viable_templates'] = round(
                sum(viable_template_counts) / len(viable_template_counts), 1
            )
        
        return {
            'version': self.version,
            'batch_stats': batch_stats,
            'results': formatted_results
        }

def format_multi_template_result(scoring_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convenience function to format multi-template scoring result
    
    Args:
        scoring_result: Result from MultiTemplateScorer.process_multi_template()
        
    Returns:
        Formatted JSON payload
    """
    formatter = ResultFormatter()
    return formatter.format_multi_template_result(scoring_result)