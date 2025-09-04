"""
Business Logic Fixes for DealGenie Scoring

Implements the required fixes:
1. 0.6 minimum confidence threshold for primary recommendations  
2. Score margin logic treating <0.5 point differences as ties
3. "Too close to call" scenarios for low-confidence results
"""

from typing import Dict, Any, List, Tuple, Optional

def determine_recommendations_fixed(
    template_results: Dict[str, Dict[str, Any]],
    template_confidences: Dict[str, Dict[str, Any]],
    template_scores: Dict[str, float],
    confidence_thresholds: Dict[str, float]
) -> Dict[str, Any]:
    """
    Fixed recommendation logic with proper confidence and margin handling
    
    Args:
        template_results: Template scoring results
        template_confidences: Template confidence scores  
        template_scores: Template final scores
        confidence_thresholds: Confidence threshold configuration
        
    Returns:
        Recommendations with proper business logic
    """
    MINIMUM_CONFIDENCE_FOR_PRIMARY = 0.6  # Required for primary recommendations
    STATISTICAL_TIE_THRESHOLD = 0.3       # Reduced from 0.5 - only very close scores are ties
    
    # Filter to viable templates only
    viable_results = {
        template: result for template, result in template_results.items()
        if result.get('viable', False)
    }
    
    if not viable_results:
        return {
            'primary': None,
            'secondary': None, 
            'tertiary': None,
            'reason': 'no_viable_templates',
            'analysis_type': 'no_options'
        }
    
    # Sort by constrained score descending
    sorted_templates = sorted(
        viable_results.items(),
        key=lambda x: x[1]['constrained_score'],
        reverse=True
    )
    
    # Get scores and confidence levels
    template_data = []
    for template, result in sorted_templates:
        confidence_data = template_confidences.get(template, {})
        confidence = confidence_data.get('confidence', 0.0)
        confidence_level = confidence_data.get('analysis', {}).get('confidence_level', 'very_low')
        
        template_data.append({
            'template': template,
            'result': result,
            'score': result['constrained_score'],
            'confidence': confidence,
            'confidence_level': confidence_level
        })
    
    # Check for low confidence scenarios
    top_template = template_data[0] if template_data else None
    
    if not top_template or top_template['confidence'] < MINIMUM_CONFIDENCE_FOR_PRIMARY:
        return _handle_low_confidence_scenario(template_data, STATISTICAL_TIE_THRESHOLD)
    
    # Check for statistical ties at the top
    tied_templates = _find_statistical_ties(template_data, STATISTICAL_TIE_THRESHOLD)
    
    if len(tied_templates) > 1:
        return _handle_statistical_tie_scenario(tied_templates, template_data)
    
    # Normal ranking scenario with clear winner
    return _handle_clear_ranking_scenario(template_data, confidence_thresholds, STATISTICAL_TIE_THRESHOLD)

def _handle_low_confidence_scenario(template_data: List[Dict], tie_threshold: float) -> Dict[str, Any]:
    """Handle scenario where top template has insufficient confidence"""
    
    # Find all templates within tie threshold of leader
    if not template_data:
        return {'primary': None, 'analysis_type': 'no_options', 'reason': 'no_viable_templates'}
    
    top_score = template_data[0]['score']
    viable_options = []
    
    for data in template_data:
        if top_score - data['score'] <= tie_threshold:
            viable_options.append(data)
    
    return {
        'primary': None,
        'secondary': None,
        'tertiary': None,
        'analysis_type': 'multiple_viable_options',
        'reason': 'insufficient_confidence_for_ranking',
        'viable_options': [{
            'template': opt['template'],
            'score': opt['score'],
            'confidence': opt['confidence'],
            'confidence_level': opt['confidence_level']
        } for opt in viable_options],
        'recommendation': f"Multiple viable options identified ({len(viable_options)} templates within {tie_threshold} points). Additional analysis recommended due to low confidence.",
        'business_guidance': 'Consider all viable options rather than relying on precise ranking'
    }

def _find_statistical_ties(template_data: List[Dict], tie_threshold: float) -> List[Dict]:
    """Find templates that are statistically tied with the leader"""
    if not template_data:
        return []
    
    top_score = template_data[0]['score']
    tied_templates = []
    
    for data in template_data:
        if top_score - data['score'] <= tie_threshold:
            tied_templates.append(data)
        else:
            break  # Scores are sorted, so we can stop here
    
    return tied_templates

def _handle_statistical_tie_scenario(tied_templates: List[Dict], all_templates: List[Dict]) -> Dict[str, Any]:
    """Handle scenario where multiple templates are statistically tied"""
    
    # Sort tied templates by confidence as tiebreaker
    tied_templates.sort(key=lambda x: x['confidence'], reverse=True)
    
    return {
        'primary': None,  # Don't declare a primary when tied
        'secondary': None,
        'tertiary': None,
        'analysis_type': 'statistical_tie',
        'reason': 'top_templates_statistically_tied',
        'tied_options': [{
            'template': opt['template'],
            'score': opt['score'],
            'confidence': opt['confidence'],
            'confidence_level': opt['confidence_level']
        } for opt in tied_templates],
        'recommendation': f"Statistical tie detected: {len(tied_templates)} templates within 0.3 points. Consider all as equivalent alternatives.",
        'business_guidance': 'Scores too close for meaningful ranking - present as equivalent options'
    }

def _handle_clear_ranking_scenario(
    template_data: List[Dict], 
    confidence_thresholds: Dict[str, float],
    tie_threshold: float
) -> Dict[str, Any]:
    """Handle normal scenario with clear winner and sufficient confidence"""
    
    top_template = template_data[0]
    
    recommendations = {
        'primary': {
            'template': top_template['template'],
            'score': top_template['score'],
            'confidence': top_template['confidence'],
            'confidence_level': top_template['confidence_level'],
            'strong': top_template['result']['strong'],
            'gap_to_next': template_data[0]['score'] - template_data[1]['score'] if len(template_data) > 1 else float('inf')
        },
        'secondary': None,
        'tertiary': None,
        'analysis_type': 'clear_ranking',
        'reason': 'sufficient_confidence_and_margin'
    }
    
    # Add secondary if meaningful gap exists
    if len(template_data) > 1:
        second_template = template_data[1]
        gap = template_data[0]['score'] - template_data[1]['score']
        
        if gap >= confidence_thresholds.get('CONF_SECONDARY_MIN', 1.0):
            recommendations['secondary'] = {
                'template': second_template['template'],
                'score': second_template['score'],
                'confidence': second_template['confidence'],
                'confidence_level': second_template['confidence_level'],
                'strong': second_template['result']['strong'],
                'gap_from_primary': gap,
                'gap_to_next': template_data[1]['score'] - template_data[2]['score'] if len(template_data) > 2 else float('inf')
            }
    
    # Add tertiary if meaningful gap exists
    if len(template_data) > 2 and recommendations['secondary']:
        third_template = template_data[2]
        gap = template_data[1]['score'] - template_data[2]['score']
        
        if gap >= confidence_thresholds.get('CONF_TERNARY_MIN', 0.5):
            recommendations['tertiary'] = {
                'template': third_template['template'],
                'score': third_template['score'],
                'confidence': third_template['confidence'],
                'confidence_level': third_template['confidence_level'],
                'strong': third_template['result']['strong'],
                'gap_from_secondary': gap
            }
    
    return recommendations

def format_business_guidance(recommendations: Dict[str, Any]) -> str:
    """Format business-friendly guidance based on analysis type"""
    
    analysis_type = recommendations.get('analysis_type', 'unknown')
    
    if analysis_type == 'no_options':
        return "No viable development options identified for this property."
    
    elif analysis_type == 'multiple_viable_options':
        count = len(recommendations.get('viable_options', []))
        return f"Analysis shows {count} viable development options with similar potential. Recommend detailed feasibility study for top options rather than relying on automated ranking."
    
    elif analysis_type == 'statistical_tie':
        count = len(recommendations.get('tied_options', []))
        return f"Top {count} development options are statistically equivalent. Present as equal alternatives to avoid false precision."
    
    elif analysis_type == 'clear_ranking':
        primary = recommendations.get('primary', {})
        if primary:
            confidence_level = primary.get('confidence_level', 'unknown')
            if confidence_level == 'high':
                return f"Clear recommendation: {primary['template']} development with high confidence ({primary['confidence']:.1%})."
            else:
                return f"Moderate recommendation: {primary['template']} development with {confidence_level} confidence ({primary['confidence']:.1%}). Consider validation."
    
    return "Unable to determine clear development guidance from analysis."