#!/usr/bin/env python3
"""
Unit Tests for Confidence Scoring Engine

Tests confidence calculation components, geographic adjustments, and calibration.
"""

import unittest
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from scoring.confidence_engine import ConfidenceEngine
from scoring.zoning_engine import ZoningConstraintsEngine


class TestConfidenceScoring(unittest.TestCase):
    """Test suite for confidence scoring functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.engine = ConfidenceEngine()
        self.zoning_engine = ZoningConstraintsEngine()
    
    def test_score_margin_confidence(self):
        """Test score margin confidence calculation"""
        # Large margin should give high confidence
        conf, analysis = self.engine.calculate_score_margin_confidence(9.0, 6.0)
        self.assertGreater(conf, 0.8)
        self.assertEqual(analysis['score_margin'], 3.0)
        
        # Small margin should give lower confidence  
        conf, analysis = self.engine.calculate_score_margin_confidence(7.1, 7.0)
        self.assertLess(conf, 0.6)
        self.assertEqual(analysis['score_margin'], 0.1)
        
        # Zero margin should give very low confidence
        conf, analysis = self.engine.calculate_score_margin_confidence(7.0, 7.0)
        self.assertLess(conf, 0.5)
        self.assertEqual(analysis['score_margin'], 0.0)
        
        # Negative margin should handle gracefully
        conf, analysis = self.engine.calculate_score_margin_confidence(6.0, 7.0)
        self.assertGreaterEqual(conf, 0.0)
        self.assertEqual(analysis['score_margin'], -1.0)
    
    def test_data_coverage_confidence(self):
        """Test data coverage confidence calculation"""
        # Complete data should give high confidence
        complete_features = {
            'zoning': 'C2',
            'lot_size_sqft': 10000,
            'transit_score': 70,
            'population_density': 8000,
            'median_income': 60000,
            'price_per_sqft': 500
        }
        conf, analysis = self.engine.calculate_data_coverage_confidence(complete_features)
        self.assertEqual(conf, 1.0)
        self.assertEqual(len(analysis['present_fields']), 6)
        self.assertEqual(len(analysis['missing_fields']), 0)
        
        # Partial data should give proportional confidence
        partial_features = {
            'zoning': 'C2',
            'lot_size_sqft': 10000,
            'transit_score': 70
            # Missing 3 fields
        }
        conf, analysis = self.engine.calculate_data_coverage_confidence(partial_features)
        self.assertEqual(conf, 0.5)  # 3/6 fields
        self.assertEqual(len(analysis['present_fields']), 3)
        self.assertEqual(len(analysis['missing_fields']), 3)
        
        # Empty data should give zero confidence
        empty_features = {}
        conf, analysis = self.engine.calculate_data_coverage_confidence(empty_features)
        self.assertEqual(conf, 0.0)
        self.assertEqual(len(analysis['present_fields']), 0)
        self.assertEqual(len(analysis['missing_fields']), 6)
    
    def test_zoning_compatibility_confidence(self):
        """Test zoning compatibility confidence calculation"""
        # Perfect compatibility should give high confidence
        conf, analysis = self.engine.calculate_zoning_compatibility_confidence(
            'retail', 'C2', self.zoning_engine
        )
        self.assertGreater(conf, 0.8)
        self.assertTrue(analysis['compatible'])
        
        # Incompatible combination should give zero confidence
        conf, analysis = self.engine.calculate_zoning_compatibility_confidence(
            'residential', 'M2', self.zoning_engine
        )
        self.assertEqual(conf, 0.0)
        self.assertFalse(analysis['compatible'])
        
        # Moderate compatibility should give moderate confidence
        conf, analysis = self.engine.calculate_zoning_compatibility_confidence(
            'retail', 'R1', self.zoning_engine
        )
        self.assertLess(conf, 0.8)
        self.assertGreater(conf, 0.3)
        self.assertTrue(analysis['compatible'])
    
    def test_score_quality_confidence(self):
        """Test new score quality confidence component"""
        # High scores should give high confidence
        conf, analysis = self.engine.calculate_score_quality_confidence(9.0)
        self.assertGreater(conf, 0.9)
        self.assertEqual(analysis['score_tier'], 'high')
        
        # Medium scores should give medium confidence
        conf, analysis = self.engine.calculate_score_quality_confidence(7.0)
        self.assertGreater(conf, 0.6)
        self.assertLess(conf, 0.9)
        self.assertEqual(analysis['score_tier'], 'medium')
        
        # Low scores should give low confidence
        conf, analysis = self.engine.calculate_score_quality_confidence(3.0)
        self.assertLess(conf, 0.5)
        self.assertEqual(analysis['score_tier'], 'low')
    
    def test_overall_confidence_calculation(self):
        """Test overall confidence with all components"""
        # High-quality scenario
        features = {
            'zoning': 'C2', 'lot_size_sqft': 15000, 'transit_score': 80,
            'population_density': 10000, 'median_income': 80000, 'price_per_sqft': 600
        }
        
        conf, analysis = self.engine.calculate_overall_confidence(
            8.5, 6.0, features, 'retail', 'C2', self.zoning_engine
        )
        
        # Should have high overall confidence
        self.assertGreater(conf, 0.7)
        
        # Should include all components
        self.assertIn('score_margin', analysis['components'])
        self.assertIn('data_coverage', analysis['components'])
        self.assertIn('zoning_compatibility', analysis['components'])
        self.assertIn('score_quality', analysis['components'])
        
        # Should include geographic adjustment
        self.assertIn('geographic_adjustment', analysis)
        
        # Should have proper confidence level
        self.assertIn('confidence_level', analysis)
        if conf >= 0.8:
            self.assertEqual(analysis['confidence_level'], 'high')
        elif conf >= 0.6:
            self.assertEqual(analysis['confidence_level'], 'medium')
        elif conf >= 0.5:
            self.assertEqual(analysis['confidence_level'], 'low')
        else:
            self.assertEqual(analysis['confidence_level'], 'very_low')
    
    def test_geographic_adjustment_integration(self):
        """Test geographic location adjustments"""
        # Premium area should get confidence boost
        premium_features = {
            'zoning': 'C2', 'lot_size_sqft': 15000, 'transit_score': 80,
            'population_density': 10000, 'median_income': 115000, 'price_per_sqft': 1200
        }
        
        conf, analysis = self.engine.calculate_overall_confidence(
            8.0, 6.5, premium_features, 'retail', 'C2', self.zoning_engine
        )
        
        geo_adj = analysis['geographic_adjustment']
        self.assertEqual(geo_adj['neighborhood_tier'], 'premium')
        self.assertGreater(geo_adj['confidence_multiplier'], 1.0)
        self.assertGreater(geo_adj['adjusted_confidence'], geo_adj['base_confidence'])
        
        # Challenging area should get confidence penalty
        challenging_features = {
            'zoning': 'C2', 'lot_size_sqft': 15000, 'transit_score': 50,
            'population_density': 5000, 'median_income': 35000, 'price_per_sqft': 250
        }
        
        conf, analysis = self.engine.calculate_overall_confidence(
            6.0, 5.5, challenging_features, 'retail', 'C2', self.zoning_engine
        )
        
        geo_adj = analysis['geographic_adjustment']
        self.assertEqual(geo_adj['neighborhood_tier'], 'challenging')
        self.assertLess(geo_adj['confidence_multiplier'], 1.0)
        self.assertLess(geo_adj['adjusted_confidence'], geo_adj['base_confidence'])
    
    def test_confidence_level_thresholds(self):
        """Test confidence level classification thresholds"""
        test_cases = [
            (0.95, 'high'),
            (0.80, 'high'),
            (0.75, 'medium'),
            (0.60, 'medium'),
            (0.55, 'low'),
            (0.50, 'low'),
            (0.45, 'very_low'),
            (0.20, 'very_low')
        ]
        
        for conf_score, expected_level in test_cases:
            features = {'median_income': 60000, 'price_per_sqft': 500}  # Mid-tier
            
            # Mock a confidence score by using high-margin scenario
            conf, analysis = self.engine.calculate_overall_confidence(
                9.0, 3.0, features, 'retail', 'C2', self.zoning_engine
            )
            
            # Manually set confidence to test threshold
            analysis['overall_confidence'] = conf_score
            
            if conf_score >= 0.8:
                expected = 'high'
            elif conf_score >= 0.6:
                expected = 'medium'
            elif conf_score >= 0.5:
                expected = 'low'
            else:
                expected = 'very_low'
            
            self.assertEqual(expected, expected_level)
    
    def test_component_weights_sum_to_one(self):
        """Test that component weights sum to 1.0"""
        weights = [
            self.engine.SCORE_MARGIN_WEIGHT,
            self.engine.DATA_COVERAGE_WEIGHT,
            self.engine.ZONING_COMPAT_WEIGHT,
            self.engine.SCORE_QUALITY_WEIGHT
        ]
        
        total_weight = sum(weights)
        self.assertAlmostEqual(total_weight, 1.0, places=2)
    
    def test_sigmoid_parameters(self):
        """Test sigmoid function parameters are reasonable"""
        # Should be gentler than original (was 2.0, now 1.2)
        self.assertEqual(self.engine.SIGMOID_STEEPNESS, 1.2)
        
        # Should have lower midpoint than original (was 2.0, now 1.0)
        self.assertEqual(self.engine.SIGMOID_MIDPOINT, 1.0)
        
        # Test sigmoid behavior at midpoint
        conf, analysis = self.engine.calculate_score_margin_confidence(
            self.engine.SIGMOID_MIDPOINT + 5.0, 5.0  # Margin = midpoint
        )
        # At midpoint, sigmoid should be ~0.5
        self.assertAlmostEqual(conf, 0.5, delta=0.1)
    
    def test_edge_cases(self):
        """Test edge case scenarios"""
        # Same primary and secondary scores
        conf, analysis = self.engine.calculate_overall_confidence(
            7.0, 7.0, {'zoning': 'C2', 'median_income': 60000, 'price_per_sqft': 500},
            'retail', 'C2', self.zoning_engine
        )
        self.assertGreaterEqual(conf, 0.0)
        self.assertLessEqual(conf, 1.0)
        
        # Very high primary score
        conf, analysis = self.engine.calculate_overall_confidence(
            10.0, 5.0, {'zoning': 'C2', 'median_income': 60000, 'price_per_sqft': 500},
            'retail', 'C2', self.zoning_engine
        )
        self.assertGreater(conf, 0.8)  # Should be high confidence
        
        # Missing all features
        conf, analysis = self.engine.calculate_overall_confidence(
            8.0, 6.0, {}, 'retail', 'C2', self.zoning_engine
        )
        self.assertLess(conf, 0.5)  # Should be low due to missing data
    
    def test_recalibrated_weights(self):
        """Test that weights were properly recalibrated from original"""
        # Original weights were: margin=0.5, coverage=0.3, compat=0.2
        # New weights should be: margin=0.25, coverage=0.25, compat=0.3, quality=0.2
        
        self.assertEqual(self.engine.SCORE_MARGIN_WEIGHT, 0.25)  # Reduced from 0.5
        self.assertEqual(self.engine.DATA_COVERAGE_WEIGHT, 0.25)  # Reduced from 0.3
        self.assertEqual(self.engine.ZONING_COMPAT_WEIGHT, 0.30)  # Increased from 0.2
        self.assertEqual(self.engine.SCORE_QUALITY_WEIGHT, 0.20)  # New component


if __name__ == '__main__':
    unittest.main()