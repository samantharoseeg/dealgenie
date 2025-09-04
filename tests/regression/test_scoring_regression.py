#!/usr/bin/env python3
"""
Regression Tests for Scoring Quality

Ensures mixed-use mis-selection ≤5%, retail never beats industrial on M-zones in premium areas,
and geographic score distribution remains realistic.
"""

import unittest
import sys
import statistics
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from api.multi_template_api import create_app
from scoring.multi_template_scorer import MultiTemplateScorer


class TestScoringRegression(unittest.TestCase):
    """Regression tests to prevent scoring quality degradation"""
    
    def setUp(self):
        """Set up regression test fixtures"""
        self.app = create_app({'TESTING': True})
        self.client = self.app.test_client()
        self.scorer = MultiTemplateScorer()
        
        # Regression thresholds
        self.MAX_MIXED_USE_MIS_SELECTION = 0.05  # 5%
        self.MIN_GEOGRAPHIC_SEPARATION = 1.0     # Points between tiers
        self.MIN_INDUSTRIAL_M_ZONE_ADVANTAGE = 0.5  # Points over retail
    
    def test_mixed_use_selection_accuracy(self):
        """Test that mixed-use template is selected appropriately (≤5% mis-selection)"""
        print("\n=== MIXED-USE SELECTION ACCURACY REGRESSION TEST ===")
        
        # Properties that SHOULD favor mixed-use
        mixed_use_favorable = [
            {
                'apn': 'MU-FAVOR-1', 'zoning': 'CM',
                'lot_size_sqft': 15000, 'transit_score': 75,
                'population_density': 8000, 'median_income': 60000, 'price_per_sqft': 500
            },
            {
                'apn': 'MU-FAVOR-2', 'zoning': 'CR', 
                'lot_size_sqft': 12000, 'transit_score': 70,
                'population_density': 9000, 'median_income': 65000, 'price_per_sqft': 550
            },
            {
                'apn': 'MU-FAVOR-3', 'zoning': 'RAS3',
                'lot_size_sqft': 10000, 'transit_score': 65,
                'population_density': 7500, 'median_income': 55000, 'price_per_sqft': 450
            },
            {
                'apn': 'MU-FAVOR-4', 'zoning': 'C2',  # Can work for mixed-use
                'lot_size_sqft': 18000, 'transit_score': 80,
                'population_density': 8500, 'median_income': 70000, 'price_per_sqft': 600
            }
        ]
        
        # Properties that should NOT favor mixed-use
        mixed_use_unfavorable = [
            {
                'apn': 'MU-UNFAVOR-1', 'zoning': 'R1',  # Single-family
                'lot_size_sqft': 8000, 'transit_score': 40,
                'population_density': 3000, 'median_income': 80000, 'price_per_sqft': 700
            },
            {
                'apn': 'MU-UNFAVOR-2', 'zoning': 'M2',  # Heavy industrial
                'lot_size_sqft': 40000, 'transit_score': 30,
                'population_density': 1000, 'median_income': 40000, 'price_per_sqft': 150
            },
            {
                'apn': 'MU-UNFAVOR-3', 'zoning': 'M3',  # Heavy industrial
                'lot_size_sqft': 50000, 'transit_score': 25,
                'population_density': 800, 'median_income': 35000, 'price_per_sqft': 120
            }
        ]
        
        # Test favorable cases
        favorable_correct = 0
        for prop in mixed_use_favorable:
            response = self.client.post('/v2/score_multi', json=prop)
            data = response.get_json()
            
            viable_uses = data.get('viable_uses', [])
            template_names = [use['template'] for use in viable_uses]
            
            # Mixed-use should be in top 3 or have competitive score
            mixed_use_rank = None
            mixed_use_score = None
            
            for i, use in enumerate(viable_uses):
                if use['template'] == 'mixed_use':
                    mixed_use_rank = i + 1
                    mixed_use_score = use['score']
                    break
            
            # Consider it correct if mixed-use is in top 3 or within 1 point of leader
            if mixed_use_rank and mixed_use_rank <= 3:
                favorable_correct += 1
            elif mixed_use_score and viable_uses[0]['score'] - mixed_use_score <= 1.0:
                favorable_correct += 1
            
            print(f"  {prop['zoning']}: Mixed-use rank #{mixed_use_rank}, score {mixed_use_score:.1f if mixed_use_score else 'N/A'}")
        
        # Test unfavorable cases
        unfavorable_correct = 0
        for prop in mixed_use_unfavorable:
            response = self.client.post('/v2/score_multi', json=prop)
            data = response.get_json()
            
            viable_uses = data.get('viable_uses', [])
            template_names = [use['template'] for use in viable_uses]
            
            # Mixed-use should NOT be top choice or have very low rank
            mixed_use_rank = None
            for i, use in enumerate(viable_uses):
                if use['template'] == 'mixed_use':
                    mixed_use_rank = i + 1
                    break
            
            # Consider correct if mixed-use not in top 2 or not viable at all
            if mixed_use_rank is None or mixed_use_rank > 2:
                unfavorable_correct += 1
            
            print(f"  {prop['zoning']}: Mixed-use rank #{mixed_use_rank if mixed_use_rank else 'Not viable'}")
        
        # Calculate accuracy
        total_cases = len(mixed_use_favorable) + len(mixed_use_unfavorable)
        total_correct = favorable_correct + unfavorable_correct
        accuracy = total_correct / total_cases
        mis_selection_rate = 1 - accuracy
        
        print(f"\nMixed-use selection accuracy: {accuracy:.1%}")
        print(f"Mis-selection rate: {mis_selection_rate:.1%}")
        print(f"Target: ≤{self.MAX_MIXED_USE_MIS_SELECTION:.1%} mis-selection")
        
        # Regression check
        self.assertLessEqual(mis_selection_rate, self.MAX_MIXED_USE_MIS_SELECTION,
                            f"Mixed-use mis-selection {mis_selection_rate:.1%} exceeds {self.MAX_MIXED_USE_MIS_SELECTION:.1%} threshold")
        
        print("✅ Mixed-use selection accuracy PASSED")
    
    def test_industrial_m_zone_dominance(self):
        """Test that retail never beats industrial on M-zones in premium areas"""
        print("\n=== INDUSTRIAL M-ZONE DOMINANCE REGRESSION TEST ===")
        
        # Premium area M-zone properties where industrial should dominate
        premium_m_zone_properties = [
            {
                'apn': 'PREMIUM-M1-1', 'zoning': 'M1',
                'lot_size_sqft': 25000, 'transit_score': 60,
                'population_density': 3000, 'median_income': 90000, 'price_per_sqft': 800
            },
            {
                'apn': 'PREMIUM-M1-2', 'zoning': 'M1', 
                'lot_size_sqft': 30000, 'transit_score': 55,
                'population_density': 2500, 'median_income': 95000, 'price_per_sqft': 750
            },
            {
                'apn': 'PREMIUM-M2-1', 'zoning': 'M2',
                'lot_size_sqft': 40000, 'transit_score': 45,
                'population_density': 2000, 'median_income': 85000, 'price_per_sqft': 600
            },
            {
                'apn': 'PREMIUM-CM-1', 'zoning': 'CM',  # Mixed but should favor industrial
                'lot_size_sqft': 35000, 'transit_score': 50,
                'population_density': 4000, 'median_income': 80000, 'price_per_sqft': 650
            }
        ]
        
        violations = []
        industrial_advantages = []
        
        for prop in premium_m_zone_properties:
            print(f"\nTesting: {prop['apn']} ({prop['zoning']})")
            
            response = self.client.post('/v2/score_multi', json=prop)
            data = response.get_json()
            
            viable_uses = data.get('viable_uses', [])
            
            # Find industrial and retail scores
            industrial_score = None
            retail_score = None
            
            for use in viable_uses:
                if use['template'] == 'industrial':
                    industrial_score = use['score']
                elif use['template'] == 'retail':
                    retail_score = use['score']
            
            print(f"  Industrial: {industrial_score:.1f if industrial_score else 'N/A'}")
            print(f"  Retail: {retail_score:.1f if retail_score else 'N/A'}")
            
            # Check for violations
            if industrial_score and retail_score:
                if retail_score > industrial_score:
                    violations.append({
                        'property': prop['apn'],
                        'zoning': prop['zoning'],
                        'retail_score': retail_score,
                        'industrial_score': industrial_score,
                        'advantage': retail_score - industrial_score
                    })
                    print(f"  ❌ VIOLATION: Retail beats industrial by {retail_score - industrial_score:.1f}")
                else:
                    advantage = industrial_score - retail_score
                    industrial_advantages.append(advantage)
                    print(f"  ✅ Industrial dominates by {advantage:.1f} points")
            elif industrial_score and not retail_score:
                print(f"  ✅ Industrial viable, retail not viable (good)")
            elif not industrial_score:
                print(f"  ⚠️  Industrial not viable (unexpected for M-zone)")
        
        # Report violations
        violation_count = len(violations)
        total_cases = len(premium_m_zone_properties)
        violation_rate = violation_count / total_cases
        
        print(f"\n=== RESULTS ===")
        print(f"Properties tested: {total_cases}")
        print(f"Violations (retail > industrial): {violation_count}")
        print(f"Violation rate: {violation_rate:.1%}")
        print(f"Target: 0% violations")
        
        if industrial_advantages:
            avg_advantage = statistics.mean(industrial_advantages)
            min_advantage = min(industrial_advantages)
            print(f"Average industrial advantage: {avg_advantage:.1f} points")
            print(f"Minimum industrial advantage: {min_advantage:.1f} points")
        
        # List violations if any
        if violations:
            print("\nVIOLATIONS DETECTED:")
            for v in violations:
                print(f"  {v['property']} ({v['zoning']}): Retail {v['retail_score']:.1f} > Industrial {v['industrial_score']:.1f}")
        
        # Regression assertion
        self.assertEqual(violation_count, 0, 
                        f"{violation_count} violations found - retail should never beat industrial in premium M-zones")
        
        if industrial_advantages:
            self.assertGreater(min(industrial_advantages), 0,
                              "Industrial should always have positive advantage over retail in M-zones")
        
        print("✅ Industrial M-zone dominance PASSED")
    
    def test_geographic_score_distribution(self):
        """Test that geographic score distribution remains realistic"""
        print("\n=== GEOGRAPHIC SCORE DISTRIBUTION REGRESSION TEST ===")
        
        # Define neighborhood tiers with expected score ranges
        neighborhood_tiers = {
            'premium': {
                'properties': [
                    {'median_income': 115000, 'price_per_sqft': 1200, 'name': 'Beverly Hills'},
                    {'median_income': 120000, 'price_per_sqft': 1500, 'name': 'Bel Air'},
                    {'median_income': 110000, 'price_per_sqft': 1100, 'name': 'Manhattan Beach'}
                ],
                'expected_range': (8.5, 10.0)
            },
            'high': {
                'properties': [
                    {'median_income': 90000, 'price_per_sqft': 850, 'name': 'West LA'},
                    {'median_income': 85000, 'price_per_sqft': 750, 'name': 'Santa Monica'},
                    {'median_income': 80000, 'price_per_sqft': 650, 'name': 'Culver City'}
                ],
                'expected_range': (7.5, 9.0)
            },
            'mid': {
                'properties': [
                    {'median_income': 55000, 'price_per_sqft': 450, 'name': 'Mid-City'},
                    {'median_income': 60000, 'price_per_sqft': 500, 'name': 'Hollywood'},
                    {'median_income': 65000, 'price_per_sqft': 550, 'name': 'Silver Lake'}
                ],
                'expected_range': (6.0, 8.0)
            },
            'challenging': {
                'properties': [
                    {'median_income': 35000, 'price_per_sqft': 250, 'name': 'South LA'},
                    {'median_income': 30000, 'price_per_sqft': 200, 'name': 'Watts'},
                    {'median_income': 38000, 'price_per_sqft': 280, 'name': 'East LA'}
                ],
                'expected_range': (5.0, 7.0)
            }
        }
        
        # Test each tier
        tier_scores = {}
        
        for tier_name, tier_data in neighborhood_tiers.items():
            print(f"\nTesting {tier_name.upper()} tier:")
            tier_scores[tier_name] = []
            
            for prop_data in tier_data['properties']:
                # Standard C2 commercial property template
                property_features = {
                    'apn': f'GEO-DIST-{prop_data["name"].upper()}',
                    'zoning': 'C2',
                    'lot_size_sqft': 10000,
                    'transit_score': 70,
                    'population_density': 8000,
                    'median_income': prop_data['median_income'],
                    'price_per_sqft': prop_data['price_per_sqft']
                }
                
                response = self.client.post('/v2/score_multi', json=property_features)
                data = response.get_json()
                
                # Get top score
                viable_uses = data.get('viable_uses', [])
                if viable_uses:
                    top_score = viable_uses[0]['score']
                    tier_scores[tier_name].append(top_score)
                    
                    print(f"  {prop_data['name']}: {top_score:.1f}")
                else:
                    print(f"  {prop_data['name']}: No viable uses (ERROR)")
        
        # Calculate tier averages
        tier_averages = {}
        for tier_name, scores in tier_scores.items():
            if scores:
                tier_averages[tier_name] = statistics.mean(scores)
            else:
                tier_averages[tier_name] = 0
        
        print(f"\n=== TIER AVERAGES ===")
        for tier_name, avg_score in tier_averages.items():
            expected_range = neighborhood_tiers[tier_name]['expected_range']
            print(f"{tier_name}: {avg_score:.1f} (expected: {expected_range[0]:.1f}-{expected_range[1]:.1f})")
        
        # Check tier separation
        separations = []
        tier_order = ['premium', 'high', 'mid', 'challenging']
        
        for i in range(len(tier_order) - 1):
            higher_tier = tier_order[i]
            lower_tier = tier_order[i + 1]
            
            if higher_tier in tier_averages and lower_tier in tier_averages:
                separation = tier_averages[higher_tier] - tier_averages[lower_tier]
                separations.append(separation)
                print(f"{higher_tier} vs {lower_tier}: {separation:.1f} point separation")
        
        # Verify each tier is within expected range
        range_violations = []
        for tier_name, scores in tier_scores.items():
            if scores:
                expected_range = neighborhood_tiers[tier_name]['expected_range']
                avg_score = tier_averages[tier_name]
                
                if avg_score < expected_range[0] or avg_score > expected_range[1]:
                    range_violations.append({
                        'tier': tier_name,
                        'actual': avg_score,
                        'expected_range': expected_range
                    })
        
        # Check minimum separation between tiers
        if separations:
            min_separation = min(separations)
            avg_separation = statistics.mean(separations)
            
            print(f"\nMinimum tier separation: {min_separation:.1f} points")
            print(f"Average tier separation: {avg_separation:.1f} points")
            print(f"Target minimum: {self.MIN_GEOGRAPHIC_SEPARATION} points")
            
            # Regression assertions
            self.assertGreater(min_separation, self.MIN_GEOGRAPHIC_SEPARATION * 0.8,  # Allow 20% tolerance
                              f"Minimum separation {min_separation:.1f} below threshold")
        
        # Check range compliance
        self.assertEqual(len(range_violations), 0,
                        f"Tier range violations: {range_violations}")
        
        # Check tier ordering is correct
        if len(tier_averages) == 4:
            premium_avg = tier_averages.get('premium', 0)
            high_avg = tier_averages.get('high', 0)
            mid_avg = tier_averages.get('mid', 0)
            challenging_avg = tier_averages.get('challenging', 0)
            
            self.assertGreater(premium_avg, high_avg, "Premium should score higher than high tier")
            self.assertGreater(high_avg, mid_avg, "High should score higher than mid tier")
            self.assertGreater(mid_avg, challenging_avg, "Mid should score higher than challenging tier")
        
        print("✅ Geographic score distribution PASSED")
        
        return tier_averages
    
    def test_confidence_score_correlation(self):
        """Test that confidence scores correlate properly with score quality"""
        print("\n=== CONFIDENCE SCORE CORRELATION REGRESSION TEST ===")
        
        # Test properties with varying score quality
        test_scenarios = [
            {
                'name': 'High Score High Confidence',
                'property': {
                    'apn': 'CONF-HIGH', 'zoning': 'C2',
                    'lot_size_sqft': 15000, 'transit_score': 85,
                    'population_density': 10000, 'median_income': 100000, 'price_per_sqft': 1000
                },
                'expected_confidence_min': 0.8
            },
            {
                'name': 'Medium Score Medium Confidence',
                'property': {
                    'apn': 'CONF-MED', 'zoning': 'C2',
                    'lot_size_sqft': 10000, 'transit_score': 65,
                    'population_density': 7000, 'median_income': 60000, 'price_per_sqft': 500
                },
                'expected_confidence_range': (0.5, 0.8)
            },
            {
                'name': 'Low Score Low Confidence',
                'property': {
                    'apn': 'CONF-LOW', 'zoning': 'C2',
                    'lot_size_sqft': 8000, 'transit_score': 40,
                    'population_density': 4000, 'median_income': 35000, 'price_per_sqft': 250
                },
                'expected_confidence_max': 0.7
            }
        ]
        
        correlation_violations = []
        
        for scenario in test_scenarios:
            print(f"\nTesting: {scenario['name']}")
            
            response = self.client.post('/v2/score_multi', json=scenario['property'])
            data = response.get_json()
            
            viable_uses = data.get('viable_uses', [])
            if viable_uses:
                top_use = viable_uses[0]
                score = top_use['score']
                confidence = top_use['confidence']
                
                print(f"  Score: {score:.1f}, Confidence: {confidence:.3f}")
                
                # Check confidence expectations
                if 'expected_confidence_min' in scenario:
                    if confidence < scenario['expected_confidence_min']:
                        correlation_violations.append({
                            'scenario': scenario['name'],
                            'issue': 'confidence_too_low',
                            'score': score,
                            'confidence': confidence,
                            'expected_min': scenario['expected_confidence_min']
                        })
                
                elif 'expected_confidence_max' in scenario:
                    if confidence > scenario['expected_confidence_max']:
                        correlation_violations.append({
                            'scenario': scenario['name'],
                            'issue': 'confidence_too_high',
                            'score': score,
                            'confidence': confidence,
                            'expected_max': scenario['expected_confidence_max']
                        })
                
                elif 'expected_confidence_range' in scenario:
                    min_conf, max_conf = scenario['expected_confidence_range']
                    if confidence < min_conf or confidence > max_conf:
                        correlation_violations.append({
                            'scenario': scenario['name'],
                            'issue': 'confidence_out_of_range',
                            'score': score,
                            'confidence': confidence,
                            'expected_range': (min_conf, max_conf)
                        })
        
        # Report violations
        if correlation_violations:
            print("\n❌ CONFIDENCE CORRELATION VIOLATIONS:")
            for v in correlation_violations:
                print(f"  {v['scenario']}: {v['issue']}")
                print(f"    Score: {v['score']:.1f}, Confidence: {v['confidence']:.3f}")
        else:
            print("\n✅ All confidence correlations correct")
        
        # Regression assertion
        self.assertEqual(len(correlation_violations), 0,
                        f"Confidence correlation violations detected: {correlation_violations}")
        
        print("✅ Confidence score correlation PASSED")
    
    def test_zoning_constraint_consistency(self):
        """Test that zoning constraints are applied consistently"""
        print("\n=== ZONING CONSTRAINT CONSISTENCY REGRESSION TEST ===")
        
        # Test known incompatible combinations
        incompatible_tests = [
            {'template': 'residential', 'zoning': 'M2', 'name': 'Residential in Heavy Industrial'},
            {'template': 'retail', 'zoning': 'M3', 'name': 'Retail in Heavy Manufacturing'},
            {'template': 'multifamily', 'zoning': 'M2', 'name': 'Multifamily in Heavy Industrial'}
        ]
        
        inconsistencies = []
        
        for test_case in incompatible_tests:
            print(f"\nTesting: {test_case['name']}")
            
            # Create test property
            property_data = {
                'apn': f'CONSTRAINT-{test_case["template"].upper()}-{test_case["zoning"]}',
                'zoning': test_case['zoning'],
                'lot_size_sqft': 20000,
                'transit_score': 50,
                'population_density': 5000,
                'median_income': 50000,
                'price_per_sqft': 300
            }
            
            response = self.client.post('/v2/score_multi', json=property_data)
            data = response.get_json()
            
            viable_uses = data.get('viable_uses', [])
            template_names = [use['template'] for use in viable_uses]
            
            # Check if incompatible template appears with high score
            if test_case['template'] in template_names:
                template_use = next(use for use in viable_uses if use['template'] == test_case['template'])
                score = template_use['score']
                
                print(f"  {test_case['template']} score: {score:.1f}")
                
                # Should have very low score due to incompatibility
                if score > 2.0:
                    inconsistencies.append({
                        'template': test_case['template'],
                        'zoning': test_case['zoning'],
                        'score': score,
                        'issue': 'incompatible_combination_scored_too_high'
                    })
                    print(f"  ❌ Incompatible combination scored too high: {score:.1f}")
                else:
                    print(f"  ✅ Correctly constrained to {score:.1f}")
            else:
                print(f"  ✅ {test_case['template']} correctly excluded from viable templates")
        
        # Report inconsistencies
        if inconsistencies:
            print("\n❌ CONSTRAINT INCONSISTENCIES:")
            for inc in inconsistencies:
                print(f"  {inc['template']} in {inc['zoning']}: {inc['issue']} (score: {inc['score']:.1f})")
        
        # Regression assertion
        self.assertEqual(len(inconsistencies), 0,
                        f"Zoning constraint inconsistencies detected: {inconsistencies}")
        
        print("✅ Zoning constraint consistency PASSED")


if __name__ == '__main__':
    unittest.main(verbosity=2)