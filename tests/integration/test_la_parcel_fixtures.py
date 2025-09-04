#!/usr/bin/env python3
"""
Integration Test Fixtures for Specific LA Parcels

Tests Griffith Park C1, Arts District M1, and Venice Boardwalk retail parcels
with expected behaviors, score ranges, and constraint applications.
"""

import unittest
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from api.multi_template_api import create_app
from scoring.multi_template_scorer import MultiTemplateScorer


class TestLAParcelFixtures(unittest.TestCase):
    """Integration tests for specific LA parcel scenarios"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.app = create_app({'TESTING': True})
        self.client = self.app.test_client()
        self.scorer = MultiTemplateScorer()
    
    def test_griffith_park_c1_parcel(self):
        """Test Griffith Park C1 zoning parcel behavior"""
        # Griffith Park area: Low density, premium location, limited commercial
        griffith_park_property = {
            'apn': 'GRIFFITH-PARK-C1-TEST',
            'zoning': 'C1',  # Light commercial
            'lot_size_sqft': 12000,  # Moderate size
            'transit_score': 45,     # Limited transit near park
            'population_density': 2500,  # Low density area
            'median_income': 85000,  # Upper-middle class area
            'price_per_sqft': 750,   # Premium location pricing
            'latitude': 34.1369,     # Griffith Park coordinates
            'longitude': -118.2494
        }
        
        print("\n=== GRIFFITH PARK C1 PARCEL TEST ===")
        
        # Test via API
        response = self.client.post('/v2/score_multi', json=griffith_park_property)
        self.assertEqual(response.status_code, 200)
        
        data = response.get_json()
        
        # Expected behavior assertions
        self.assertIn('viable_uses', data)
        self.assertIn('primary_recommendation', data)
        self.assertIn('meta', data)
        
        viable_uses = data['viable_uses']
        self.assertGreater(len(viable_uses), 0, "Should have viable uses")
        
        # C1 zoning expectations
        template_names = [use['template'] for use in viable_uses]
        
        # C1 should favor office and commercial
        self.assertIn('office', template_names, "C1 should be viable for office")
        self.assertIn('commercial', template_names, "C1 should be viable for commercial")
        
        # Should not favor industrial (incompatible with park area)
        if 'industrial' in template_names:
            industrial_use = next(use for use in viable_uses if use['template'] == 'industrial')
            self.assertLess(industrial_use['score'], 6.0, "Industrial should score low near park")
        
        # Score range expectations (premium location but limited commercial intensity)
        top_score = viable_uses[0]['score']
        self.assertGreaterEqual(top_score, 6.0, "Should score reasonably for premium area")
        self.assertLessEqual(top_score, 8.5, "Should not score too high for C1 limitations")
        
        # Geographic premium should be applied
        meta = data['meta']
        self.assertTrue(meta['multi_template_triggered'], "Should trigger multi-template")
        
        print(f"Top template: {viable_uses[0]['template']} (score: {viable_uses[0]['score']:.1f})")
        print(f"Viable templates: {len(viable_uses)}")
        print(f"Analysis type: {data.get('analysis_type', 'unknown')}")
        
        # Constraint verification
        for use in viable_uses:
            constraints = use.get('constraints_applied', {})
            self.assertIn('template', constraints, "Should have constraint tracking")
    
    def test_arts_district_m1_parcel(self):
        """Test Arts District M1 zoning parcel behavior"""
        # Arts District: Industrial heritage, gentrifying, mixed-use potential
        arts_district_property = {
            'apn': 'ARTS-DISTRICT-M1-TEST',
            'zoning': 'M1',  # Light industrial
            'lot_size_sqft': 25000,  # Large industrial lot
            'transit_score': 75,     # Good transit access (Metro)
            'population_density': 4500,  # Medium density, gentrifying
            'median_income': 65000,  # Rising income due to gentrification
            'price_per_sqft': 450,   # Moderate pricing, transitional area
            'latitude': 34.0419,     # Arts District coordinates
            'longitude': -118.2297
        }
        
        print("\n=== ARTS DISTRICT M1 PARCEL TEST ===")
        
        response = self.client.post('/v2/score_multi', json=arts_district_property)
        self.assertEqual(response.status_code, 200)
        
        data = response.get_json()
        viable_uses = data['viable_uses']
        
        # M1 zoning expectations
        template_names = [use['template'] for use in viable_uses]
        
        # Industrial should be top or near-top
        self.assertIn('industrial', template_names, "M1 should be viable for industrial")
        industrial_use = next(use for use in viable_uses if use['template'] == 'industrial')
        self.assertGreaterEqual(industrial_use['score'], 6.5, "Industrial should score well in M1")
        
        # Mixed-use should also be viable (Arts District trend)
        self.assertIn('mixed_use', template_names, "M1 should be viable for mixed-use in Arts District")
        
        # Office might be viable (loft conversions)
        if 'office' in template_names:
            office_use = next(use for use in viable_uses if use['template'] == 'office')
            self.assertGreaterEqual(office_use['score'], 5.0, "Office should have moderate viability")
        
        # Retail should score lower (not primary use for M1)
        if 'retail' in template_names:
            retail_use = next(use for use in viable_uses if use['template'] == 'retail')
            self.assertLess(retail_use['score'], 7.0, "Retail should not dominate M1 zone")
        
        # Score expectations for transitional area
        top_score = viable_uses[0]['score']
        self.assertGreaterEqual(top_score, 6.0, "Should score well for industrial/mixed use")
        self.assertLessEqual(top_score, 9.0, "Should reflect M1 limitations")
        
        print(f"Top template: {viable_uses[0]['template']} (score: {viable_uses[0]['score']:.1f})")
        print(f"Industrial score: {industrial_use['score']:.1f}")
        print(f"Templates: {template_names}")
        
        # Zoning boundary consistency
        constraints_applied = industrial_use.get('constraints_applied', {})
        self.assertGreater(industrial_use['score'], 0, "Industrial should not be constrained out")
    
    def test_venice_boardwalk_retail_parcel(self):
        """Test Venice Boardwalk retail parcel behavior"""
        # Venice Boardwalk: High tourist traffic, retail-friendly, beach premium
        venice_boardwalk_property = {
            'apn': 'VENICE-BOARDWALK-RETAIL-TEST',
            'zoning': 'C2',  # Commercial - good for retail
            'lot_size_sqft': 8000,   # Smaller beachfront lot
            'transit_score': 60,     # Moderate transit
            'population_density': 12000,  # High density beach area
            'median_income': 75000,  # Mix of income levels
            'price_per_sqft': 850,   # Beach premium pricing
            'latitude': 34.0195,     # Venice Beach coordinates
            'longitude': -118.4912,
            'beach_proximity': True,  # Special beach factor
            'tourist_traffic': 'high'  # High foot traffic
        }
        
        print("\n=== VENICE BOARDWALK RETAIL PARCEL TEST ===")
        
        response = self.client.post('/v2/score_multi', json=venice_boardwalk_property)
        self.assertEqual(response.status_code, 200)
        
        data = response.get_json()
        viable_uses = data['viable_uses']
        
        # Retail expectations for boardwalk location
        template_names = [use['template'] for use in viable_uses]
        
        # Retail should be top-scoring or very competitive
        self.assertIn('retail', template_names, "C2 should be viable for retail")
        retail_use = next(use for use in viable_uses if use['template'] == 'retail')
        
        # Retail should score very well on Venice Boardwalk
        self.assertGreaterEqual(retail_use['score'], 7.5, "Retail should score high on boardwalk")
        
        # Commercial/office should also be viable
        self.assertIn('commercial', template_names, "C2 should support commercial")
        
        # Mixed-use should be competitive (live-work beach lifestyle)
        if 'mixed_use' in template_names:
            mixed_use = next(use for use in viable_uses if use['template'] == 'mixed_use')
            self.assertGreaterEqual(mixed_use['score'], 6.0, "Mixed-use viable near beach")
        
        # Industrial should score poorly (incompatible with beach/tourist area)
        if 'industrial' in template_names:
            industrial_use = next(use for use in viable_uses if use['template'] == 'industrial')
            self.assertLess(industrial_use['score'], 5.0, "Industrial should score low at beach")
        
        # Beach premium should boost scores
        top_score = viable_uses[0]['score']
        self.assertGreaterEqual(top_score, 8.0, "Beach location should achieve high scores")
        
        # Check for primary recommendation
        primary = data.get('primary_recommendation')
        if primary:
            self.assertIn(primary['template'], ['retail', 'commercial', 'mixed_use'], 
                         "Primary should be appropriate for beach retail")
            self.assertGreater(primary['confidence'], 0.6, "Should have reasonable confidence")
        
        print(f"Top template: {viable_uses[0]['template']} (score: {viable_uses[0]['score']:.1f})")
        print(f"Retail score: {retail_use['score']:.1f}")
        print(f"Primary recommendation: {primary['template'] if primary else 'None'}")
        
        # Geographic premium verification
        if 'geographic_adjustment' in viable_uses[0].get('constraints_applied', {}):
            geo_adj = viable_uses[0]['constraints_applied']['geographic_adjustment']
            self.assertIn('neighborhood_tier', geo_adj, "Should have geographic tier")
    
    def test_zoning_boundary_consistency(self):
        """Test consistency across similar zoning types"""
        # Test that similar properties in same zone type get consistent treatment
        base_c2_property = {
            'zoning': 'C2',
            'lot_size_sqft': 10000,
            'transit_score': 70,
            'population_density': 8000,
            'median_income': 65000,
            'price_per_sqft': 500
        }
        
        # Create variations with small differences
        properties = []
        for i in range(3):
            prop = base_c2_property.copy()
            prop['apn'] = f'C2-CONSISTENCY-TEST-{i}'
            prop['lot_size_sqft'] += i * 500  # Small variations
            prop['transit_score'] += i * 2
            properties.append(prop)
        
        print("\n=== ZONING BOUNDARY CONSISTENCY TEST ===")
        
        results = []
        for prop in properties:
            response = self.client.post('/v2/score_multi', json=prop)
            self.assertEqual(response.status_code, 200)
            data = response.get_json()
            results.append(data)
        
        # All should have similar viable templates
        viable_templates = [set(use['template'] for use in result['viable_uses']) for result in results]
        
        # Should have significant overlap in viable templates
        common_templates = viable_templates[0]
        for templates in viable_templates[1:]:
            overlap = len(common_templates & templates) / len(common_templates | templates)
            self.assertGreater(overlap, 0.6, "Should have consistent template viability")
        
        # Top scores should be within reasonable range
        top_scores = [result['viable_uses'][0]['score'] for result in results]
        score_range = max(top_scores) - min(top_scores)
        self.assertLess(score_range, 1.5, "Similar properties should have similar scores")
        
        print(f"Score range across similar properties: {score_range:.2f}")
        print(f"Templates consistency: {len(common_templates)} common templates")
    
    def test_constraint_application_verification(self):
        """Test that constraints are properly applied and documented"""
        # Test properties that should trigger different constraints
        test_cases = [
            {
                'name': 'Incompatible Combination',
                'property': {
                    'apn': 'CONSTRAINT-INCOMPATIBLE',
                    'zoning': 'M2',  # Heavy industrial
                    'lot_size_sqft': 30000,
                    'median_income': 50000,
                    'price_per_sqft': 300
                },
                'template': 'residential',  # Should be incompatible
                'expected_constraint': 'compatibility'
            },
            {
                'name': 'Score Cap Applied',
                'property': {
                    'apn': 'CONSTRAINT-CAP',
                    'zoning': 'R1',  # Single family
                    'lot_size_sqft': 8000,
                    'median_income': 80000,
                    'price_per_sqft': 700
                },
                'template': 'commercial',  # Should be capped
                'expected_constraint': 'score_cap'
            }
        ]
        
        print("\n=== CONSTRAINT APPLICATION VERIFICATION ===")
        
        for case in test_cases:
            print(f"\nTesting: {case['name']}")
            
            response = self.client.post('/v2/score_multi', json=case['property'])
            self.assertEqual(response.status_code, 200)
            
            data = response.get_json()
            viable_uses = data['viable_uses']
            
            # Find the specific template if it exists
            template_use = None
            for use in viable_uses:
                if use['template'] == case['template']:
                    template_use = use
                    break
            
            if template_use:
                constraints = template_use.get('constraints_applied', {})
                self.assertIn('constraints_applied', constraints, "Should track constraints")
                
                if case['expected_constraint'] == 'compatibility':
                    # Should have very low or zero score due to incompatibility
                    self.assertLess(template_use['score'], 1.0, "Incompatible should score very low")
                
                elif case['expected_constraint'] == 'score_cap':
                    # Should be below a reasonable cap
                    self.assertLess(template_use['score'], 8.0, "Should be capped")
                
                print(f"  {case['template']}: Score={template_use['score']:.1f}")
                print(f"  Constraints: {constraints.get('summary', 'None')}")
            else:
                print(f"  {case['template']}: Not viable (correctly excluded)")
    
    def test_expected_score_ranges(self):
        """Test that parcels fall within expected score ranges"""
        test_scenarios = [
            {
                'name': 'Premium Commercial',
                'property': {
                    'apn': 'PREMIUM-COMMERCIAL',
                    'zoning': 'C2',
                    'lot_size_sqft': 15000,
                    'transit_score': 85,
                    'median_income': 100000,
                    'price_per_sqft': 1000
                },
                'expected_range': (8.0, 10.0),
                'expected_templates': ['retail', 'commercial', 'office']
            },
            {
                'name': 'Challenging Industrial',
                'property': {
                    'apn': 'CHALLENGING-INDUSTRIAL',
                    'zoning': 'M1',
                    'lot_size_sqft': 20000,
                    'transit_score': 30,
                    'median_income': 35000,
                    'price_per_sqft': 200
                },
                'expected_range': (5.0, 7.5),
                'expected_templates': ['industrial']
            }
        ]
        
        print("\n=== EXPECTED SCORE RANGES TEST ===")
        
        for scenario in test_scenarios:
            print(f"\nTesting: {scenario['name']}")
            
            response = self.client.post('/v2/score_multi', json=scenario['property'])
            self.assertEqual(response.status_code, 200)
            
            data = response.get_json()
            viable_uses = data['viable_uses']
            
            # Check top score is in expected range
            top_score = viable_uses[0]['score']
            min_score, max_score = scenario['expected_range']
            
            self.assertGreaterEqual(top_score, min_score, 
                                  f"Top score {top_score:.1f} below expected minimum {min_score}")
            self.assertLessEqual(top_score, max_score,
                                f"Top score {top_score:.1f} above expected maximum {max_score}")
            
            # Check expected templates are present
            template_names = [use['template'] for use in viable_uses]
            for expected_template in scenario['expected_templates']:
                self.assertIn(expected_template, template_names,
                             f"Expected template {expected_template} not found")
            
            print(f"  Score range: {top_score:.1f} (expected: {min_score}-{max_score})")
            print(f"  Top templates: {template_names[:3]}")


if __name__ == '__main__':
    unittest.main()