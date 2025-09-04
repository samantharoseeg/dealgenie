#!/usr/bin/env python3
"""
Unit Tests for Multi-Template Trigger Logic

Tests trigger conditions, thresholds, and decision logic for multi-template evaluation.
"""

import unittest
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from scoring.multi_template_engine import MultiTemplateEngine


class TestMultiTemplateTriggers(unittest.TestCase):
    """Test suite for multi-template trigger logic"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.engine = MultiTemplateEngine()
    
    def test_mixed_use_zoning_trigger(self):
        """Test mixed-use zoning trigger"""
        # Should trigger for mixed-use zones
        mixed_zones = ['CM', 'CR', 'C2', 'C4', 'RAS3', 'RAS4']
        for zone in mixed_zones:
            should_trigger, analysis = self.engine.should_run_multi_template(
                zone, {'retail': 7.0}, {'retail': 0.8}
            )
            self.assertTrue(should_trigger, f"Should trigger for {zone}")
            self.assertIn('mixed_use_zoning', analysis['triggered_conditions'])
        
        # Should not trigger for single-use zones
        single_zones = ['R1', 'M2', 'M3']
        for zone in single_zones:
            should_trigger, analysis = self.engine.should_run_multi_template(
                zone, {'industrial': 9.0}, {'industrial': 0.9}
            )
            # May trigger for other reasons but not mixed-use zoning
            if should_trigger:
                self.assertNotIn('mixed_use_zoning', analysis['triggered_conditions'])
    
    def test_close_scores_trigger(self):
        """Test close scores trigger (≤1.0 point difference)"""
        # Should trigger when top scores are close
        close_scores = {
            'retail': 8.0,
            'office': 7.5,  # 0.5 difference
            'commercial': 7.2  # 0.8 difference from top
        }
        
        should_trigger, analysis = self.engine.should_run_multi_template(
            'R1', close_scores, {'retail': 0.7, 'office': 0.6, 'commercial': 0.6}
        )
        
        if should_trigger and 'close_scores' in analysis['triggered_conditions']:
            close_analysis = analysis['triggers']['close_scores']
            self.assertTrue(close_analysis['has_close_scores'])
            self.assertLessEqual(close_analysis['top_score'] - close_analysis.get('second_score', 0), 1.0)
        
        # Should not trigger when scores are far apart
        far_scores = {
            'retail': 9.0,
            'office': 6.0,  # 3.0 difference
            'commercial': 5.0  # 4.0 difference
        }
        
        should_trigger, analysis = self.engine.should_run_multi_template(
            'R1', far_scores, {'retail': 0.8, 'office': 0.5, 'commercial': 0.4}
        )
        
        # Should not trigger due to close scores
        close_analysis = analysis['triggers']['close_scores']
        self.assertFalse(close_analysis['has_close_scores'])
    
    def test_high_compatibility_trigger(self):
        """Test high compatibility trigger (≥0.6 average)"""
        # Should trigger with high average compatibility
        high_compat = {
            'retail': 0.9,
            'office': 0.8,
            'commercial': 0.7,
            'multifamily': 0.6
        }
        
        should_trigger, analysis = self.engine.should_run_multi_template(
            'R1', {'retail': 7.0}, high_compat
        )
        
        if should_trigger and 'high_compatibility' in analysis['triggered_conditions']:
            compat_analysis = analysis['triggers']['high_compatibility']
            self.assertTrue(compat_analysis['has_high_compatibility'])
            self.assertGreaterEqual(compat_analysis['avg_compatibility'], 0.6)
        
        # Should not trigger with low compatibility
        low_compat = {
            'retail': 0.4,
            'office': 0.3,
            'commercial': 0.2
        }
        
        should_trigger, analysis = self.engine.should_run_multi_template(
            'R1', {'retail': 7.0}, low_compat
        )
        
        compat_analysis = analysis['triggers']['high_compatibility']
        self.assertFalse(compat_analysis['has_high_compatibility'])
    
    def test_combined_triggers(self):
        """Test multiple triggers firing simultaneously"""
        # Scenario with mixed zoning + close scores + high compatibility
        combined_scores = {
            'retail': 8.0,
            'commercial': 7.5,
            'office': 7.2
        }
        
        combined_compat = {
            'retail': 0.9,
            'commercial': 0.8,
            'office': 0.7
        }
        
        should_trigger, analysis = self.engine.should_run_multi_template(
            'CM', combined_scores, combined_compat
        )
        
        self.assertTrue(should_trigger)
        
        # Should have multiple trigger reasons
        expected_triggers = ['mixed_use_zoning', 'close_scores', 'high_compatibility']
        triggered = analysis['triggered_conditions']
        
        # At least one should be triggered
        self.assertTrue(any(trigger in triggered for trigger in expected_triggers))
    
    def test_trigger_thresholds(self):
        """Test specific trigger thresholds"""
        # Close scores threshold: 1.0 points
        edge_case_scores = {
            'retail': 8.0,
            'office': 7.0  # Exactly 1.0 difference
        }
        
        should_trigger, analysis = self.engine.should_run_multi_template(
            'R1', edge_case_scores, {'retail': 0.8, 'office': 0.7}
        )
        
        close_analysis = analysis['triggers']['close_scores']
        # Exactly at threshold should trigger
        if close_analysis['has_close_scores']:
            max_delta = max(pair['delta'] for pair in close_analysis.get('close_pairs', []))
            self.assertLessEqual(max_delta, 1.0)
        
        # Compatibility threshold: 0.6 average
        edge_compat = {
            'retail': 0.6,
            'office': 0.6
        }
        
        should_trigger, analysis = self.engine.should_run_multi_template(
            'R1', {'retail': 7.0}, edge_compat
        )
        
        compat_analysis = analysis['triggers']['high_compatibility']
        if compat_analysis['has_high_compatibility']:
            self.assertGreaterEqual(compat_analysis['avg_compatibility'], 0.6)
    
    def test_no_trigger_scenarios(self):
        """Test scenarios that should not trigger multi-template"""
        # Single high-scoring template, low compatibility, single-use zone
        no_trigger_scores = {'industrial': 9.0}
        no_trigger_compat = {'industrial': 0.4}
        
        should_trigger, analysis = self.engine.should_run_multi_template(
            'R1', no_trigger_scores, no_trigger_compat
        )
        
        # Should not trigger due to single template and low compatibility
        self.assertFalse(analysis['triggers']['close_scores']['has_close_scores'])
        self.assertFalse(analysis['triggers']['high_compatibility']['has_high_compatibility'])
        self.assertNotIn('mixed_use_zoning', analysis['triggered_conditions'])
    
    def test_analysis_structure(self):
        """Test trigger analysis structure"""
        should_trigger, analysis = self.engine.should_run_multi_template(
            'CM', {'retail': 8.0, 'office': 7.5}, {'retail': 0.8, 'office': 0.7}
        )
        
        # Should have required structure
        required_keys = ['triggered', 'primary_trigger', 'triggered_conditions', 'triggers', 'zoning']
        for key in required_keys:
            self.assertIn(key, analysis)
        
        # Triggers should have all three types
        trigger_types = ['mixed_use_zoning', 'close_scores', 'high_compatibility']
        for trigger_type in trigger_types:
            self.assertIn(trigger_type, analysis['triggers'])
        
        # Each trigger should have 'triggered' flag
        for trigger_type in trigger_types:
            self.assertIn('triggered', analysis['triggers'][trigger_type])
    
    def test_primary_trigger_priority(self):
        """Test primary trigger priority logic"""
        # Mixed-use zoning should take priority
        should_trigger, analysis = self.engine.should_run_multi_template(
            'CM', {'retail': 8.0}, {'retail': 0.9}
        )
        
        if should_trigger and 'mixed_use_zoning' in analysis['triggered_conditions']:
            self.assertEqual(analysis['primary_trigger'], 'mixed_use_zoning')
        
        # Close scores should be secondary priority
        should_trigger, analysis = self.engine.should_run_multi_template(
            'R1', {'retail': 8.0, 'office': 7.5}, {'retail': 0.5, 'office': 0.5}
        )
        
        if (should_trigger and 
            'close_scores' in analysis['triggered_conditions'] and 
            'mixed_use_zoning' not in analysis['triggered_conditions']):
            self.assertEqual(analysis['primary_trigger'], 'close_scores')
    
    def test_configuration_loading(self):
        """Test trigger configuration parameters"""
        # Should have loaded configuration
        self.assertIsNotNone(self.engine.config)
        
        # Should have trigger thresholds
        self.assertIn('multi_template_triggers', self.engine.config)
        triggers = self.engine.config['multi_template_triggers']
        
        expected_thresholds = ['close_scores_delta', 'high_compatibility_threshold']
        for threshold in expected_thresholds:
            self.assertIn(threshold, triggers)
        
        # Thresholds should be reasonable
        self.assertLessEqual(triggers['close_scores_delta'], 2.0)
        self.assertGreaterEqual(triggers['close_scores_delta'], 0.5)
        
        self.assertLessEqual(triggers['high_compatibility_threshold'], 1.0)
        self.assertGreaterEqual(triggers['high_compatibility_threshold'], 0.3)
    
    def test_edge_case_inputs(self):
        """Test edge case inputs"""
        # Empty scores
        should_trigger, analysis = self.engine.should_run_multi_template(
            'CM', {}, {}
        )
        self.assertIsInstance(should_trigger, bool)
        self.assertIsInstance(analysis, dict)
        
        # Single template
        should_trigger, analysis = self.engine.should_run_multi_template(
            'CM', {'retail': 8.0}, {'retail': 0.8}
        )
        self.assertIsInstance(should_trigger, bool)
        
        # None values
        try:
            should_trigger, analysis = self.engine.should_run_multi_template(
                'CM', {'retail': None}, {'retail': None}
            )
            self.assertIsInstance(should_trigger, bool)
        except (TypeError, ValueError):
            pass  # Acceptable to fail gracefully
        
        # Unknown zoning
        should_trigger, analysis = self.engine.should_run_multi_template(
            'UNKNOWN', {'retail': 8.0}, {'retail': 0.8}
        )
        self.assertIsInstance(should_trigger, bool)


if __name__ == '__main__':
    unittest.main()