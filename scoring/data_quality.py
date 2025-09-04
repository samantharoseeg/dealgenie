"""
Data Quality and Transparency Module

Provides data vintage tracking, coverage scoring, and limitation warnings
for all property analysis responses.
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class DataQualityMetrics:
    """Data quality metrics for a property analysis"""
    vintage: str                    # ISO timestamp of data sources
    coverage_score: float          # 0.0-1.0 coverage of required features
    missing_features: List[str]    # List of missing feature names
    stale_features: List[str]      # List of features with old data
    reliability_warnings: List[str] # Human-readable warnings
    accuracy_limitations: List[str] # Known accuracy limitations
    data_sources: Dict[str, str]   # Feature -> data source mapping

class DataQualityAssessment:
    """Assesses and reports data quality for property analyses"""
    
    # Required features for complete analysis
    REQUIRED_FEATURES = {
        'apn': 'Property identifier',
        'zoning': 'Zoning designation',
        'lot_size_sqft': 'Lot size in square feet',
        'transit_score': 'Transit accessibility score',
        'population_density': 'Population density per square mile',
        'median_income': 'Median household income',
        'price_per_sqft': 'Price per square foot'
    }
    
    # Optional features that improve accuracy
    OPTIONAL_FEATURES = {
        'building_age': 'Age of existing structures',
        'building_sqft': 'Existing building square footage',
        'building_stories': 'Number of building stories',
        'vacancy_rate': 'Local vacancy rate',
        'comparable_sales': 'Recent comparable sales',
        'permit_activity': 'Recent permit activity',
        'walkability_score': 'Walkability index',
        'crime_index': 'Local crime index',
        'school_rating': 'Local school ratings'
    }
    
    def __init__(self):
        """Initialize data quality assessment"""
        self.current_time = datetime.now(timezone.utc)
        
        # Data source vintage information (normally from config/database)
        self.data_sources = {
            'assessor_data': '2024-06-15T00:00:00Z',    # LA County Assessor
            'census_data': '2023-12-31T00:00:00Z',      # US Census
            'zoning_data': '2024-07-01T00:00:00Z',      # LA City Planning
            'market_data': '2024-05-30T00:00:00Z',      # Market pricing
            'transit_data': '2024-08-01T00:00:00Z',     # Metro/GTFS
            'demographics': '2023-12-31T00:00:00Z'      # ACS estimates
        }
        
        # Feature to data source mapping
        self.feature_sources = {
            'apn': 'assessor_data',
            'zoning': 'zoning_data', 
            'lot_size_sqft': 'assessor_data',
            'transit_score': 'transit_data',
            'population_density': 'demographics',
            'median_income': 'demographics',
            'price_per_sqft': 'market_data'
        }
    
    def assess_property_data_quality(self, property_data: Dict[str, Any]) -> DataQualityMetrics:
        """Assess data quality for a property analysis request"""
        
        # Calculate coverage scores
        coverage_score, missing_features = self._calculate_coverage(property_data)
        
        # Identify stale data
        stale_features = self._identify_stale_features(property_data)
        
        # Generate reliability warnings
        warnings = self._generate_reliability_warnings(
            property_data, missing_features, stale_features
        )
        
        # Add accuracy limitations
        limitations = self._get_accuracy_limitations(property_data)
        
        # Get most recent data vintage
        vintage = self._get_effective_vintage(property_data)
        
        # Map data sources
        sources = self._map_data_sources(property_data)
        
        return DataQualityMetrics(
            vintage=vintage,
            coverage_score=coverage_score,
            missing_features=missing_features,
            stale_features=stale_features,
            reliability_warnings=warnings,
            accuracy_limitations=limitations,
            data_sources=sources
        )
    
    def _calculate_coverage(self, property_data: Dict[str, Any]) -> Tuple[float, List[str]]:
        """Calculate feature coverage score and identify missing features"""
        
        present_required = 0
        missing_features = []
        
        # Check required features
        for feature, description in self.REQUIRED_FEATURES.items():
            if self._is_feature_present(property_data, feature):
                present_required += 1
            else:
                missing_features.append(f"{feature} ({description})")
        
        # Base coverage from required features (80% weight)
        required_coverage = present_required / len(self.REQUIRED_FEATURES)
        
        # Bonus coverage from optional features (20% weight)
        present_optional = 0
        for feature in self.OPTIONAL_FEATURES:
            if self._is_feature_present(property_data, feature):
                present_optional += 1
        
        optional_coverage = present_optional / len(self.OPTIONAL_FEATURES)
        
        # Combined coverage score
        coverage_score = (required_coverage * 0.8) + (optional_coverage * 0.2)
        
        return coverage_score, missing_features
    
    def _is_feature_present(self, data: Dict[str, Any], feature: str) -> bool:
        """Check if a feature is present and valid"""
        value = data.get(feature)
        
        if value is None:
            return False
        
        # Check for invalid values
        if isinstance(value, str) and value.strip() == '':
            return False
        
        if isinstance(value, (int, float)) and value <= 0 and feature != 'price_per_sqft':
            return False
        
        return True
    
    def _identify_stale_features(self, property_data: Dict[str, Any]) -> List[str]:
        """Identify features with stale data (>1 year old)"""
        stale_features = []
        stale_threshold_days = 365
        
        for feature in property_data:
            if feature in self.feature_sources:
                source = self.feature_sources[feature]
                if source in self.data_sources:
                    source_date = datetime.fromisoformat(
                        self.data_sources[source].replace('Z', '+00:00')
                    )
                    days_old = (self.current_time - source_date).days
                    
                    if days_old > stale_threshold_days:
                        stale_features.append(
                            f"{feature} ({days_old} days old)"
                        )
        
        return stale_features
    
    def _generate_reliability_warnings(
        self, 
        property_data: Dict[str, Any],
        missing_features: List[str],
        stale_features: List[str]
    ) -> List[str]:
        """Generate human-readable reliability warnings"""
        
        warnings = []
        
        # Coverage warnings
        if len(missing_features) > 2:
            warnings.append(
                f"Limited data coverage: {len(missing_features)} required features missing"
            )
        
        # Staleness warnings  
        if len(stale_features) > 0:
            warnings.append(
                f"Outdated data detected: {len(stale_features)} features may be stale"
            )
        
        # Zoning-specific warnings
        zoning = property_data.get('zoning', '')
        if zoning in ['C1', 'C2']:
            warnings.append(
                "C1/C2 zoning: Multiple viable templates possible, template selection reliability reduced"
            )
        
        if zoning in ['CM', 'CR']:
            warnings.append(
                "Mixed-use zoning: System prioritizes mixed-use template, verify market appropriateness"
            )
        
        # Income-based warnings
        income = property_data.get('median_income', 0)
        if income > 100000:
            warnings.append(
                "High-income area: System may bias toward retail template, validate with local market conditions"
            )
        
        # Market data warnings
        price_psf = property_data.get('price_per_sqft', 0)
        if price_psf == 0 or not price_psf:
            warnings.append(
                "No market pricing data: Scores based on demographic proxies only"
            )
        
        return warnings
    
    def _get_accuracy_limitations(self, property_data: Dict[str, Any]) -> List[str]:
        """Get known accuracy limitations for this property type/zone"""
        
        limitations = [
            "Template selection accuracy: 65-70% with free data sources",
            "System optimized for relative ranking, not absolute scores",
            "Requires analyst validation for investment decisions",
            "Performance may vary in rapidly changing neighborhoods"
        ]
        
        zoning = property_data.get('zoning', '')
        
        # Zoning-specific limitations
        if zoning in ['C1', 'C2']:
            limitations.append(
                "Commercial zones: Office vs retail vs commercial distinction limited by available features"
            )
        
        if zoning in ['CM', 'CR', 'RAS3', 'RAS4']:
            limitations.append(
                "Mixed-use zones: Template selection based on zoning compatibility, not market analysis"
            )
        
        # Income-based limitations
        income = property_data.get('median_income', 0)
        if income < 40000:
            limitations.append(
                "Low-income areas: Limited comparable data may affect accuracy"
            )
        
        # Geographic limitations
        limitations.append(
            "Geographic calibration: Limited to 4 broad tiers, micro-market variations not captured"
        )
        
        return limitations
    
    def _get_effective_vintage(self, property_data: Dict[str, Any]) -> str:
        """Get the effective vintage (oldest relevant data source)"""
        
        relevant_dates = []
        
        for feature in property_data:
            if feature in self.feature_sources:
                source = self.feature_sources[feature]
                if source in self.data_sources:
                    relevant_dates.append(self.data_sources[source])
        
        if relevant_dates:
            # Return the oldest date as the effective vintage
            oldest_date = min(relevant_dates)
            return oldest_date
        else:
            # Default to current time if no mapping available
            return self.current_time.isoformat()
    
    def _map_data_sources(self, property_data: Dict[str, Any]) -> Dict[str, str]:
        """Map each feature to its data source"""
        
        source_mapping = {}
        
        for feature in property_data:
            if feature in self.feature_sources:
                source = self.feature_sources[feature]
                source_mapping[feature] = source
            else:
                source_mapping[feature] = 'unknown'
        
        return source_mapping
    
    def format_for_api_response(self, metrics: DataQualityMetrics) -> Dict[str, Any]:
        """Format data quality metrics for API response"""
        
        return {
            'data_quality': {
                'vintage': metrics.vintage,
                'coverage_score': round(metrics.coverage_score, 3),
                'missing_features': metrics.missing_features,
                'stale_features': metrics.stale_features,
                'warnings': metrics.reliability_warnings,
                'limitations': metrics.accuracy_limitations,
                'data_sources': metrics.data_sources
            }
        }
    
    def format_for_html_report(self, metrics: DataQualityMetrics) -> str:
        """Format data quality information for HTML reports"""
        
        html = """
        <div class="data-quality-section">
            <h3>⚠️ Data Quality and Limitations</h3>
            
            <div class="quality-metrics">
                <p><strong>Data Vintage:</strong> {vintage}</p>
                <p><strong>Coverage Score:</strong> {coverage:.1%}</p>
            </div>
            
            {missing_section}
            {warnings_section}
            {limitations_section}
        </div>
        """.format(
            vintage=metrics.vintage[:10],  # Just the date part
            coverage=metrics.coverage_score,
            missing_section=self._format_missing_features_html(metrics.missing_features),
            warnings_section=self._format_warnings_html(metrics.reliability_warnings),
            limitations_section=self._format_limitations_html(metrics.accuracy_limitations)
        )
        
        return html
    
    def _format_missing_features_html(self, missing_features: List[str]) -> str:
        """Format missing features for HTML"""
        if not missing_features:
            return ""
        
        items = '\n'.join([f'<li>{feature}</li>' for feature in missing_features])
        return f"""
        <div class="missing-features">
            <h4>Missing Data:</h4>
            <ul>{items}</ul>
        </div>
        """
    
    def _format_warnings_html(self, warnings: List[str]) -> str:
        """Format warnings for HTML"""
        if not warnings:
            return ""
        
        items = '\n'.join([f'<li>{warning}</li>' for warning in warnings])
        return f"""
        <div class="reliability-warnings">
            <h4>Reliability Warnings:</h4>
            <ul>{items}</ul>
        </div>
        """
    
    def _format_limitations_html(self, limitations: List[str]) -> str:
        """Format limitations for HTML"""
        if not limitations:
            return ""
        
        items = '\n'.join([f'<li>{limitation}</li>' for limitation in limitations])
        return f"""
        <div class="accuracy-limitations">
            <h4>System Limitations:</h4>
            <ul>{items}</ul>
        </div>
        """

# Global instance for use across the application
data_quality_assessor = DataQualityAssessment()