import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def calculate_penalties(features: Dict[str, Any], template: str) -> Dict[str, float]:
    """
    Calculate recalibrated negative scoring factors based on market reality validation.
    Penalties reduced to better reflect actual investment activity in challenging areas.
    
    Args:
        features: Property features dictionary
        template: Development template
    
    Returns:
        Dictionary of penalty factors and their values
    """
    penalties = {}
    
    # Flood zone penalty (reduced from 2.0 to 1.2)
    if features.get('flood_risk', False) or features.get('flood_zone', False):
        penalties['flood_zone'] = 1.2
    
    # High crime penalty (reduced max from 1.5 to 1.0)
    crime_factor = features.get('crime_factor', 1.0)
    if crime_factor > 1.5:
        penalties['high_crime'] = min(1.0, (crime_factor - 1.0) * 1.0)
    
    # Toxic sites penalty (reduced max from 3.0 to 2.0)
    toxic_sites = features.get('toxic_sites_nearby', 0)
    superfund_site = features.get('superfund_site_nearby', False)
    if toxic_sites > 0 or superfund_site:
        penalties['toxic_sites'] = min(2.0, 1.0 + (toxic_sites * 0.3))
    
    # Airport noise penalty (reduced max from 1.0 to 0.6)
    airport_noise = features.get('airport_noise_level', 0)  # dB levels
    if airport_noise > 65:  # FAA noise threshold
        penalties['airport_noise'] = min(0.6, (airport_noise - 65) / 25)
    elif features.get('near_airport', False):
        penalties['airport_noise'] = 0.3
    
    # Homeless concentration penalty (reduced max from 1.5 to 1.0)
    homeless_count = features.get('homeless_encampments_nearby', 0)
    homeless_density = features.get('homeless_population_density', 0)
    if homeless_count > 2 or homeless_density > 50:  # per sq mile
        penalties['homeless_concentration'] = min(1.0, 0.3 + (homeless_count * 0.2) + (homeless_density / 150))
    
    # Freeway proximity penalty (reduced from 1.0/0.5 to 0.6/0.3)
    freeway_distance = features.get('freeway_distance_ft', 5000)
    if freeway_distance < 500:
        penalties['freeway_noise'] = 0.6
    elif freeway_distance < 1000:
        penalties['freeway_noise'] = 0.3
    
    # Industrial pollution penalty (reduced max from 2.0 to 1.2)
    if template != 'industrial':
        industrial_proximity = features.get('industrial_facilities_nearby', 0)
        air_quality_index = features.get('air_quality_index', 50)
        if industrial_proximity > 3 or air_quality_index > 100:
            penalties['pollution'] = min(1.2, 0.3 + (industrial_proximity * 0.2) + max(0, (air_quality_index - 100) / 60))
    
    # Seismic risk penalty (reduced from 1.5/1.0 to 1.0/0.6)
    seismic_risk = features.get('seismic_risk_level', 'moderate')
    if seismic_risk == 'high':
        penalties['seismic_risk'] = 0.6
    elif seismic_risk == 'very_high':
        penalties['seismic_risk'] = 1.0
    
    # Infrastructure deficiencies (reduced max from 1.5 to 1.0)
    utility_issues = features.get('utility_deficiencies', [])
    if utility_issues:
        penalties['infrastructure'] = min(1.0, len(utility_issues) * 0.3)
    
    return penalties


def calculate_score(features: Dict[str, Any], template: str = 'multifamily') -> Dict[str, Any]:
    """
    Calculate investment score for a property based on features and template.
    
    Args:
        features: Dictionary containing property features
        template: Scoring template ('multifamily', 'commercial', 'residential')
    
    Returns:
        Dictionary with score, explanation, and recommendations
    """
    try:
        # Scoring weights by template
        weights = {
            'multifamily': {
                'zoning': 0.30,
                'lot_size': 0.20,
                'transit': 0.25,
                'demographics': 0.15,
                'market': 0.10
            },
            'commercial': {
                'zoning': 0.30,  # Increased from 0.25 - commercial needs zoning emphasis
                'lot_size': 0.15,
                'transit': 0.25,  # Reduced from 0.30 - less transit dependent
                'demographics': 0.20,
                'market': 0.10
            },
            'residential': {
                'zoning': 0.30,
                'lot_size': 0.25,
                'transit': 0.15,
                'demographics': 0.20,
                'market': 0.10
            },
            'industrial': {
                'zoning': 0.20,
                'lot_size': 0.35,
                'transit': 0.15,
                'demographics': 0.05,
                'market': 0.25
            },
            'retail': {
                'zoning': 0.30,  # Increased from 0.25 - retail needs proper zoning
                'lot_size': 0.15,
                'transit': 0.25,  # Reduced from 0.30 - less transit weight advantage
                'demographics': 0.20,  # REDUCED from 0.25 - main bias fix
                'market': 0.10   # Increased from 0.05 - retail sensitive to market
            },
            'mixed_use': {
                'zoning': 0.35,  # INCREASED from 0.25 - mixed-use needs strong zoning match
                'lot_size': 0.15,
                'transit': 0.30,  # Keep high for mixed-use transit orientation
                'demographics': 0.15,  # Reduced from 0.20 to differentiate from retail
                'market': 0.05   # Reduced from 0.10 - less market sensitive
            },
            'office': {
                'zoning': 0.30,  # Increased from 0.25 - office needs proper zoning
                'lot_size': 0.15,
                'transit': 0.30,  # Increased from 0.25 - office very transit dependent
                'demographics': 0.10,  # Reduced from 0.15 - less demographic dependent
                'market': 0.15   # Reduced from 0.20 - less market weight vs retail
            }
        }
        
        # Get template weights
        template_weights = weights.get(template, weights['multifamily'])
        
        # Calculate component scores
        scores = {}
        
        # Zoning score (0-10) - Template-specific scoring
        zoning = features.get('zoning', 'R1')
        
        if template == 'industrial':
            industrial_zones = ['M2', 'M1', 'MR1', 'MR2', 'M3']
            mixed_industrial = ['C2', 'C4', 'CM']
            if any(zone in zoning for zone in industrial_zones):
                scores['zoning'] = 9.0
            elif any(zone in zoning for zone in mixed_industrial):
                scores['zoning'] = 6.5
            else:
                scores['zoning'] = 3.0
                
        elif template == 'retail':
            prime_retail_zones = ['C2', 'C4', 'C1', 'C1.5']  # Removed CM from prime
            mixed_zones_for_retail = ['CM', 'RAS3', 'RAS4']    # CM moved to mixed category
            poor_retail_zones = ['R3', 'R4', 'R5']
            if any(zone in zoning for zone in prime_retail_zones):
                scores['zoning'] = 9.0
            elif any(zone in zoning for zone in mixed_zones_for_retail):
                scores['zoning'] = 7.0  # CM now gets 7.0 instead of 9.0 for retail
            elif any(zone in zoning for zone in poor_retail_zones):
                scores['zoning'] = 5.0
            else:
                scores['zoning'] = 4.0
                
        elif template == 'residential':
            # Residential template - strongly favors R1, RE, RS
            single_family_zones = ['R1', 'RE', 'RS', 'RA']
            medium_density_zones = ['R2', 'RD']
            high_density_zones = ['R3', 'R4', 'R5']
            commercial_zones = ['C1', 'C2', 'C4']
            
            if any(zone in zoning for zone in single_family_zones):
                scores['zoning'] = 10.0  # Perfect for residential
            elif any(zone in zoning for zone in medium_density_zones):
                scores['zoning'] = 7.5
            elif any(zone in zoning for zone in high_density_zones):
                scores['zoning'] = 5.0
            elif any(zone in zoning for zone in commercial_zones):
                scores['zoning'] = 3.0  # Poor for residential
            else:
                scores['zoning'] = 2.0
                
        elif template == 'multifamily':
            # Multifamily template - favors high-density residential
            high_density_zones = ['R5', 'R4', 'R3', 'RAS3', 'RAS4']
            mixed_use_zones = ['C2', 'C4', 'CM']
            medium_density_zones = ['R2', 'RD']
            low_density_zones = ['R1', 'RE', 'RS']
            
            if any(zone in zoning for zone in high_density_zones):
                scores['zoning'] = 10.0  # Perfect for multifamily
            elif any(zone in zoning for zone in mixed_use_zones):
                scores['zoning'] = 8.5
            elif any(zone in zoning for zone in medium_density_zones):
                scores['zoning'] = 6.5
            elif any(zone in zoning for zone in low_density_zones):
                scores['zoning'] = 3.0  # Poor for multifamily
            else:
                scores['zoning'] = 2.0
                
        elif template == 'commercial':
            # Commercial template - favors office/business zones
            office_zones = ['C1', 'C1.5', 'CM', 'P']
            mixed_commercial = ['C2', 'C4']
            high_density_residential = ['R4', 'R5', 'RAS3', 'RAS4']
            low_density_zones = ['R1', 'R2', 'R3']
            
            if any(zone in zoning for zone in office_zones):
                scores['zoning'] = 10.0  # Perfect for commercial
            elif any(zone in zoning for zone in mixed_commercial):
                scores['zoning'] = 8.0
            elif any(zone in zoning for zone in high_density_residential):
                scores['zoning'] = 6.0
            elif any(zone in zoning for zone in low_density_zones):
                scores['zoning'] = 3.0
            else:
                scores['zoning'] = 2.0
                
        elif template == 'mixed_use':
            # Mixed-use template - favors CM, CR, RAS zones
            perfect_mixed_zones = ['CM', 'CR', 'RAS3', 'RAS4']
            good_mixed_zones = ['C2', 'C4', 'R4', 'R5']
            moderate_zones = ['C1', 'R3', 'M1']
            poor_zones = ['R1', 'R2', 'M2', 'M3']
            
            if any(zone in zoning for zone in perfect_mixed_zones):
                scores['zoning'] = 10.0  # Perfect for mixed-use
            elif any(zone in zoning for zone in good_mixed_zones):
                scores['zoning'] = 8.0
            elif any(zone in zoning for zone in moderate_zones):
                scores['zoning'] = 5.5
            elif any(zone in zoning for zone in poor_zones):
                scores['zoning'] = 3.0
            else:
                scores['zoning'] = 2.0
                
        elif template == 'office':
            # Office template - favors C1, C2, LAX zones
            prime_office_zones = ['C1', 'C1.5', 'C2', 'LAX']
            good_office_zones = ['C4', 'CM', 'P']
            moderate_zones = ['R4', 'R5', 'RAS3', 'RAS4']
            poor_zones = ['R1', 'R2', 'R3', 'M1', 'M2']
            
            if any(zone in zoning for zone in prime_office_zones):
                scores['zoning'] = 10.0  # Perfect for office
            elif any(zone in zoning for zone in good_office_zones):
                scores['zoning'] = 8.0
            elif any(zone in zoning for zone in moderate_zones):
                scores['zoning'] = 5.5
            elif any(zone in zoning for zone in poor_zones):
                scores['zoning'] = 3.0
            else:
                scores['zoning'] = 2.0
                
        else:
            # Fallback for any other templates
            high_density_zones = ['R5', 'R4', 'R3', 'C2', 'C4', 'RAS3', 'RAS4']
            medium_density_zones = ['R2', 'RD', 'C1', 'C1.5']
            
            if any(zone in zoning for zone in high_density_zones):
                scores['zoning'] = 9.0
            elif any(zone in zoning for zone in medium_density_zones):
                scores['zoning'] = 7.0
            else:
                scores['zoning'] = 5.0
        
        # Lot size score (0-10)
        lot_size = features.get('lot_size_sqft', 5000)
        if lot_size >= 20000:
            scores['lot_size'] = 10.0
        elif lot_size >= 10000:
            scores['lot_size'] = 8.0
        elif lot_size >= 7500:
            scores['lot_size'] = 6.5
        elif lot_size >= 5000:
            scores['lot_size'] = 5.0
        else:
            scores['lot_size'] = 3.0
        
        # Transit score (0-10) - Template-adjusted
        transit_score = features.get('transit_score', 50)
        
        if template == 'industrial':
            # Industrial prefers freight/highway access over transit
            highway_access = features.get('highway_access', transit_score * 0.7)
            scores['transit'] = min(highway_access / 12, 10.0)
        elif template == 'retail':
            # Retail highly values customer accessibility
            scores['transit'] = min((transit_score * 1.2) / 10, 10.0)
        else:
            # Standard transit scoring
            scores['transit'] = min(transit_score / 10, 10.0)
        
        # Demographics score (0-10) with template-specific adjustments
        pop_density = features.get('population_density', 5000)
        income = features.get('median_income', 50000)
        crime_factor = features.get('crime_factor', 1.0)
        
        # Enhanced income scoring with stronger differentiation
        if income > 120000:  # Premium areas like Brentwood, Pacific Palisades
            base_demo_score = 9.5
        elif income > 90000:  # High-income areas
            base_demo_score = 8.5
        elif income > 75000 and pop_density > 10000:  # Dense, affluent
            base_demo_score = 9.0
        elif income > 60000 and pop_density > 7500:  # Good income + density
            base_demo_score = 7.5
        elif income > 45000 and pop_density > 5000:  # Moderate
            base_demo_score = 6.0
        elif income > 30000:  # Low-moderate
            base_demo_score = 4.5
        else:  # Very low income
            base_demo_score = 3.0
        
        # Adjust for template preferences and apply crime impact
        crime_penalty = 0
        
        if template in ['residential', 'retail']:
            # Residential and retail are very sensitive to crime
            if crime_factor > 1.8:
                crime_penalty = 2.5
            elif crime_factor > 1.5:
                crime_penalty = 2.0
            elif crime_factor > 1.2:
                crime_penalty = 1.5
            elif crime_factor > 0.8:
                crime_penalty = 0.5
        elif template in ['commercial', 'multifamily']:
            # Commercial and multifamily are moderately sensitive to crime
            if crime_factor > 1.8:
                crime_penalty = 1.5
            elif crime_factor > 1.5:
                crime_penalty = 1.0
            elif crime_factor > 1.2:
                crime_penalty = 0.5
        else:
            # Industrial is least sensitive to crime
            if crime_factor > 2.0:
                crime_penalty = 0.5
        
        # Apply crime penalty
        base_demo_score = max(1.0, base_demo_score - crime_penalty)
        
        if template == 'residential':
            # Residential values safety/low crime more than density
            if crime_factor < 0.5:  # Very safe areas
                base_demo_score = min(10.0, base_demo_score + 1.0)
            elif crime_factor < 0.7:  # Safe areas
                base_demo_score = min(10.0, base_demo_score + 0.5)
            # Lower density is ok for residential
            if pop_density < 8000 and income > 80000:
                base_demo_score = max(base_demo_score, 7.5)
        
        scores['demographics'] = base_demo_score
        
        # Market score (0-10)
        price_psf = features.get('price_per_sqft', 500)
        if price_psf < 400:
            scores['market'] = 8.0
        elif price_psf < 600:
            scores['market'] = 6.5
        elif price_psf < 800:
            scores['market'] = 5.0
        else:
            scores['market'] = 3.5
        
        # Calculate weighted score
        total_score = 0
        for component, weight in template_weights.items():
            total_score += scores.get(component, 5.0) * weight
        
        # Apply negative scoring factors (penalties)
        penalties = calculate_penalties(features, template)
        total_penalty = sum(penalties.values())
        
        # Apply penalties to base score
        total_score = max(0.0, total_score - total_penalty)
        
        # Round to 1 decimal place
        total_score = round(total_score, 1)
        
        # Generate explanation
        base_score = total_score + total_penalty
        explanation = f"Property scored {total_score}/10 for {template} development"
        
        if total_penalty > 0:
            explanation += f" (base score {base_score:.1f} minus {total_penalty:.1f} in penalties). "
        else:
            explanation += ". "
        
        # Add component analysis
        best_component = max(scores.items(), key=lambda x: x[1])
        worst_component = min(scores.items(), key=lambda x: x[1])
        
        explanation += f"Strong points: {best_component[0]} ({best_component[1]:.1f}/10). "
        explanation += f"Areas for improvement: {worst_component[0]} ({worst_component[1]:.1f}/10). "
        
        # Add penalty details
        if penalties:
            penalty_list = [f"{penalty.replace('_', ' ')}: -{value:.1f}" for penalty, value in penalties.items()]
            explanation += f"Risk factors: {', '.join(penalty_list)}. "
        
        # Add specific insights
        if scores['zoning'] >= 8:
            explanation += "Excellent zoning for high-density development. "
        if scores['transit'] >= 7:
            explanation += "Great transit accessibility enhances value. "
        if scores['lot_size'] >= 8:
            explanation += "Large lot size provides flexibility for development. "
        
        # Generate recommendations
        recommendations = []
        
        if total_score >= 8.0:
            recommendations.append("Excellent investment opportunity - proceed with detailed analysis")
            recommendations.append("Consider fast-track development to capitalize on market conditions")
        elif total_score >= 6.5:
            recommendations.append("Good investment potential with some optimization needed")
            recommendations.append("Review zoning variance possibilities for enhanced returns")
        elif total_score >= 5.0:
            recommendations.append("Moderate opportunity - careful analysis required")
            recommendations.append("Consider joint venture to mitigate risks")
        else:
            recommendations.append("Limited development potential in current state")
            recommendations.append("Monitor for market changes or rezoning opportunities")
        
        # Add specific recommendations based on scores
        if scores['transit'] < 5:
            recommendations.append("Limited transit access may affect rental/resale demand")
        if scores['lot_size'] < 5:
            recommendations.append("Small lot size limits development options")
        if scores['market'] > 7:
            recommendations.append("Favorable market pricing provides good entry point")
        
        return {
            'score': total_score,
            'base_score': round(base_score, 1),
            'total_penalties': round(total_penalty, 1),
            'penalties': penalties,
            'explanation': explanation,
            'recommendations': recommendations,
            'component_scores': scores,
            'template': template,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error calculating score: {e}")
        return {
            'score': 0.0,
            'explanation': f"Error calculating score: {str(e)}",
            'recommendations': ["Unable to generate recommendations due to error"],
            'error': str(e)
        }