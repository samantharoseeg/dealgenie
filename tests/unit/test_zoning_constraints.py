#!/usr/bin/env python3
"""
Unit Tests for Zoning Constraints Engine

Tests zoning constraints, compatibility matrix, score caps, and edge cases.
"""

import unittest
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from scoring.zoning_engine import ZoningConstraintsEngine


class TestZoningConstraints(unittest.TestCase):
    """Test suite for zoning constraints functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.engine = ZoningConstraintsEngine()
    
    def test_basic_compatibility_matrix(self):
        """Test basic compatibility checks"""
        # Known compatible combinations
        self.assertTrue(self.engine.is_compatible('retail', 'C2'))
        self.assertTrue(self.engine.is_compatible('office', 'C1'))
        self.assertTrue(self.engine.is_compatible('industrial', 'M1'))
        self.assertTrue(self.engine.is_compatible('residential', 'R1'))
        
        # Known incompatible combinations
        self.assertFalse(self.engine.is_compatible('residential', 'M2'))
        self.assertFalse(self.engine.is_compatible('retail', 'M3'))
        self.assertFalse(self.engine.is_compatible('multifamily', 'M2'))
    
    def test_mixed_use_compatibility(self):
        """Test mixed_use template compatibility"""
        # Mixed-use should work well with CM zoning
        self.assertTrue(self.engine.is_compatible('mixed_use', 'CM'))
        self.assertTrue(self.engine.is_compatible('mixed_use', 'C2'))
        self.assertTrue(self.engine.is_compatible('mixed_use', 'R3'))
        
        # Mixed-use should not work with single-family
        self.assertFalse(self.engine.is_compatible('mixed_use', 'R1'))
        
        # Mixed-use should not work with heavy industrial
        self.assertFalse(self.engine.is_compatible('mixed_use', 'M2'))
        self.assertFalse(self.engine.is_compatible('mixed_use', 'M3'))
    
    def test_la_specific_zones(self):
        """Test LA-specific zoning codes"""
        # LAX zone should work with office and commercial
        self.assertTrue(self.engine.is_compatible('office', 'LAX'))
        self.assertTrue(self.engine.is_compatible('commercial', 'LAX'))
        self.assertTrue(self.engine.is_compatible('industrial', 'LAX'))
        
        # LAX should not work well with residential
        self.assertFalse(self.engine.is_compatible('residential', 'LAX'))
        self.assertFalse(self.engine.is_compatible('multifamily', 'LAX'))
        
        # C4 should work with commercial uses
        self.assertTrue(self.engine.is_compatible('commercial', 'C4'))
        self.assertTrue(self.engine.is_compatible('office', 'C4'))
        self.assertTrue(self.engine.is_compatible('retail', 'C4'))
    
    def test_score_caps(self):
        """Test score cap retrieval"""
        # Test known score caps
        self.assertEqual(self.engine.get_score_cap('retail', 'C2'), 10.0)
        self.assertEqual(self.engine.get_score_cap('industrial', 'M2'), 10.0)
        self.assertEqual(self.engine.get_score_cap('residential', 'R1'), 10.0)
        
        # Test LA-specific caps
        self.assertEqual(self.engine.get_score_cap('office', 'LAX'), 9.0)
        self.assertEqual(self.engine.get_score_cap('mixed_use', 'CM'), 10.0)
        
        # Test lower caps for incompatible combinations
        self.assertLess(self.engine.get_score_cap('residential', 'M1'), 5.0)
        self.assertLess(self.engine.get_score_cap('retail', 'M2'), 5.0)
    
    def test_plausibility_floors(self):
        """Test plausibility floor enforcement"""
        floors = self.engine.plausibility_floors
        
        # All templates should have plausibility floors
        expected_templates = ['retail', 'office', 'multifamily', 'residential', 'commercial', 'industrial', 'mixed_use']
        for template in expected_templates:
            self.assertIn(template, floors)
            self.assertGreaterEqual(floors[template], 1.0)
            self.assertLessEqual(floors[template], 3.0)
    
    def test_apply_constraints(self):
        """Test constraint application logic"""
        # Test compatible combination - should not cap score
        score, constraints = self.engine.apply_constraints(8.5, 'retail', 'C2')
        self.assertEqual(score, 8.5)  # Should not be capped
        self.assertIn('final_score', constraints)
        
        # Test incompatible combination - should cap to 0
        score, constraints = self.engine.apply_constraints(8.5, 'residential', 'M2')
        self.assertEqual(score, 0.0)  # Should be capped to 0
        self.assertIn('summary', constraints)
        self.assertIn('compatibility', constraints['summary'])
        
        # Test score above cap - should be capped down
        score, constraints = self.engine.apply_constraints(12.0, 'retail', 'R1')
        cap = self.engine.get_score_cap('retail', 'R1')
        self.assertLessEqual(score, cap)
        
        # Test score below plausibility floor - should be raised to floor
        floor = self.engine.plausibility_floors.get('retail', 2.5)
        score, constraints = self.engine.apply_constraints(1.0, 'retail', 'C2')
        self.assertGreaterEqual(score, floor)
    
    def test_unknown_zoning_fallback(self):
        """Test handling of unknown zoning codes"""
        # Unknown zoning should use default values
        score, constraints = self.engine.apply_constraints(8.0, 'retail', 'UNKNOWN_ZONE')
        
        # Should use default cap (5.0)
        self.assertLessEqual(score, 5.0)
        
        # Should have compatibility issue
        self.assertIn('summary', constraints)
    
    def test_constraint_details(self):
        """Test constraint application details"""
        # Test constraint tracking
        score, constraints = self.engine.apply_constraints(9.0, 'office', 'R1')
        
        expected_keys = ['template', 'zoning', 'raw_score', 'final_score', 'constraints_applied', 'summary']
        for key in expected_keys:
            self.assertIn(key, constraints)
        
        # Should track what constraints were applied
        self.assertIsInstance(constraints['constraints_applied'], list)
    
    def test_edge_case_scores(self):
        """Test edge case score values"""
        # Test negative score
        score, constraints = self.engine.apply_constraints(-1.0, 'retail', 'C2')
        self.assertGreaterEqual(score, 0.0)
        
        # Test zero score
        score, constraints = self.engine.apply_constraints(0.0, 'retail', 'C2')
        floor = self.engine.plausibility_floors.get('retail', 2.5)
        self.assertGreaterEqual(score, floor)
        
        # Test very high score
        score, constraints = self.engine.apply_constraints(100.0, 'retail', 'C2')
        self.assertLessEqual(score, 10.0)  # Max cap should be 10
    
    def test_all_template_zone_combinations(self):
        """Test that all template-zone combinations are handled"""
        templates = ['retail', 'office', 'multifamily', 'residential', 'commercial', 'industrial', 'mixed_use']
        zones = ['R1', 'R2', 'R3', 'C1', 'C2', 'C4', 'CM', 'CR', 'M1', 'M2', 'LAX']
        
        for template in templates:
            for zone in zones:
                # Should not crash for any combination
                try:
                    compatible = self.engine.is_compatible(template, zone)
                    cap = self.engine.get_score_cap(template, zone)
                    score, constraints = self.engine.apply_constraints(7.0, template, zone)
                    
                    # Basic sanity checks
                    self.assertIsInstance(compatible, bool)
                    self.assertGreaterEqual(cap, 0.0)
                    self.assertLessEqual(cap, 10.0)
                    self.assertGreaterEqual(score, 0.0)
                    self.assertLessEqual(score, 10.0)
                    self.assertIsInstance(constraints, dict)
                    
                except Exception as e:
                    self.fail(f"Exception for {template}-{zone}: {e}")
    
    def test_configuration_loading(self):
        """Test configuration file loading"""
        # Should have loaded the LA-enhanced config
        self.assertIn('mixed_use', self.engine.score_caps)
        self.assertIn('LAX', self.engine.score_caps['office'])
        self.assertIn('C4', self.engine.score_caps['commercial'])
        
        # Should have all required sections
        self.assertIsNotNone(self.engine.score_caps)
        self.assertIsNotNone(self.engine.plausibility_floors)
        self.assertIsNotNone(self.engine.compatibility_matrix)
        self.assertIsNotNone(self.engine.default_unknown)


if __name__ == '__main__':
    unittest.main()