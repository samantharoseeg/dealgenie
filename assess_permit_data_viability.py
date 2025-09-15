#!/usr/bin/env python3
"""
ASSESS PERMIT DATA DENSITY AND BUSINESS VALUE
Real-world testing to determine viability for property scoring
"""

import requests
import json
import csv
import time
import random
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

class PermitDataViabilityAssessor:
    def __init__(self):
        self.base_url = "https://data.lacity.org/resource/pi9x-tg5x.json"
        self.app_token = "lmUNVajT2wIHnzFI2x3HGEt5H"
        self.test_results = []
        
    def test_real_la_development_properties(self) -> Dict[str, Any]:
        """
        TASK 1: Sample Real LA Properties with Known Permits
        Test addresses from recent development projects and known construction
        """
        print(f"🏗️ TESTING REAL LA DEVELOPMENT PROPERTIES")
        print("="*50)
        
        # Properties likely to have permits based on recent development activity
        known_development_addresses = [
            # Downtown LA Major Developments
            {"address": "1100 Wilshire Blvd", "type": "commercial", "reason": "Major office development"},
            {"address": "900 Wilshire Blvd", "type": "mixed-use", "reason": "High-rise residential/retail"},
            {"address": "1050 S Hill St", "type": "residential", "reason": "Luxury apartment complex"},
            {"address": "800 S Hope St", "type": "commercial", "reason": "Office tower renovation"},
            
            # Hollywood Development Corridor
            {"address": "6250 Hollywood Blvd", "type": "mixed-use", "reason": "Hollywood Walk of Fame area development"},
            {"address": "1750 N Highland Ave", "type": "commercial", "reason": "Hollywood & Highland area"},
            {"address": "6801 Hollywood Blvd", "type": "mixed-use", "reason": "Major entertainment complex"},
            
            # West LA High-Value Areas
            {"address": "10250 Santa Monica Blvd", "type": "commercial", "reason": "Century City adjacency"},
            {"address": "11601 Wilshire Blvd", "type": "commercial", "reason": "Westwood commercial corridor"},
            {"address": "1901 Avenue of the Stars", "type": "commercial", "reason": "Century City towers"},
            
            # Arts District & Emerging Areas
            {"address": "777 S Alameda St", "type": "mixed-use", "reason": "Arts District redevelopment"},
            {"address": "1855 Industrial St", "type": "mixed-use", "reason": "Arts District conversions"},
            {"address": "700 S Flower St", "type": "commercial", "reason": "Financial District"},
            
            # Transit-Oriented Development
            {"address": "3250 Wilshire Blvd", "type": "mixed-use", "reason": "Metro Purple Line station area"},
            {"address": "5250 Lankershim Blvd", "type": "mixed-use", "reason": "North Hollywood Metro area"},
            
            # Historic Renovation Projects
            {"address": "304 S Broadway", "type": "mixed-use", "reason": "Historic Broadway theater district"},
            {"address": "650 S Spring St", "type": "mixed-use", "reason": "Historic core adaptive reuse"},
            
            # Major Institutional Projects
            {"address": "1200 Getty Center Dr", "type": "institutional", "reason": "Getty Center area"},
            {"address": "10833 Le Conte Ave", "type": "institutional", "reason": "UCLA Medical Center area"}
        ]
        
        development_results = []
        successful_permit_finds = 0
        
        for i, prop in enumerate(known_development_addresses, 1):
            address = prop["address"]
            prop_type = prop["type"]
            reason = prop["reason"]
            
            print(f"\n🧪 Testing {i}: {address}")
            print(f"   Type: {prop_type}")
            print(f"   Reason: {reason}")
            
            # Test multiple search strategies for each address
            search_strategies = [
                # Strategy 1: Exact address match
                {"strategy": "exact", "params": {
                    '$where': f"upper(primary_address) like '%{address.split()[0]}%{address.split()[1]}%'",
                    '$limit': '20'
                }},
                # Strategy 2: Street name only
                {"strategy": "street", "params": {
                    '$where': f"upper(primary_address) like '%{' '.join(address.split()[1:])}%'",
                    '$limit': '20'
                }},
                # Strategy 3: Address number range
                {"strategy": "range", "params": {
                    '$where': f"primary_address like '{address.split()[0][:2]}%' AND upper(primary_address) like '%{address.split()[1]}%'",
                    '$limit': '20'
                }}
            ]
            
            best_result = None
            max_permits = 0
            
            for strategy in search_strategies:
                try:
                    response = requests.get(
                        self.base_url,
                        params=strategy["params"],
                        headers={"X-App-Token": self.app_token},
                        timeout=15
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        permits_found = len(data)
                        
                        if permits_found > max_permits:
                            max_permits = permits_found
                            best_result = {
                                'address': address,
                                'property_type': prop_type,
                                'reason': reason,
                                'strategy': strategy["strategy"],
                                'permits_found': permits_found,
                                'sample_permits': data[:3] if data else [],
                                'success': True
                            }
                    
                    time.sleep(0.3)  # Rate limiting
                    
                except Exception as e:
                    print(f"   ❌ Strategy {strategy['strategy']} failed: {e}")
            
            if best_result:
                development_results.append(best_result)
                if best_result['permits_found'] > 0:
                    successful_permit_finds += 1
                    print(f"   ✅ Found {best_result['permits_found']} permits (strategy: {best_result['strategy']})")
                    
                    # Show sample permit details
                    if best_result['sample_permits']:
                        sample = best_result['sample_permits'][0]
                        print(f"      Sample: {sample.get('primary_address', 'N/A')}")
                        print(f"      Type: {sample.get('permit_type', 'N/A')}")
                        print(f"      Date: {sample.get('issue_date', 'N/A')}")
                else:
                    print(f"   🔍 No permits found despite multiple strategies")
            else:
                print(f"   ❌ All strategies failed")
        
        hit_rate = (successful_permit_finds / len(known_development_addresses)) * 100
        
        print(f"\n📊 DEVELOPMENT PROPERTY PERMIT ANALYSIS:")
        print(f"Properties tested: {len(known_development_addresses)}")
        print(f"Properties with permits: {successful_permit_finds}")
        print(f"Hit rate: {hit_rate:.1f}%")
        
        return {
            'total_tested': len(known_development_addresses),
            'successful_finds': successful_permit_finds,
            'hit_rate': hit_rate,
            'detailed_results': development_results
        }
    
    def statistical_permit_density_sampling(self) -> Dict[str, Any]:
        """
        TASK 2: Statistical Sampling of Permit Density
        Test random LA addresses across different property types
        """
        print(f"\n📈 STATISTICAL PERMIT DENSITY SAMPLING")
        print("="*45)
        
        # Generate systematic sampling across LA geography and property types
        test_samples = []
        
        # Residential samples
        residential_streets = [
            "Main St", "Spring St", "Broadway", "Hill St", "Olive St",
            "Flower St", "Hope St", "Grand Ave", "Figueroa St", "Vermont Ave",
            "Western Ave", "Normandie Ave", "Crenshaw Blvd", "La Brea Ave", "Fairfax Ave"
        ]
        
        # Commercial corridors
        commercial_streets = [
            "Wilshire Blvd", "Hollywood Blvd", "Sunset Blvd", "Santa Monica Blvd",
            "Melrose Ave", "Beverly Blvd", "Olympic Blvd", "Pico Blvd", "Washington Blvd"
        ]
        
        # Generate random addresses
        for street in residential_streets:
            for i in range(3):  # 3 samples per street
                address_num = random.randint(100, 9999)
                direction = random.choice(["N", "S", "E", "W", ""])
                full_address = f"{address_num} {direction} {street}".strip()
                test_samples.append({
                    'address': full_address,
                    'category': 'residential',
                    'street_type': street.split()[-1]
                })
        
        for street in commercial_streets:
            for i in range(2):  # 2 samples per commercial street
                address_num = random.randint(1000, 9999)
                direction = random.choice(["N", "S", "E", "W", ""])
                full_address = f"{address_num} {direction} {street}".strip()
                test_samples.append({
                    'address': full_address,
                    'category': 'commercial',
                    'street_type': street.split()[-1]
                })
        
        # Shuffle for random testing order
        random.shuffle(test_samples)
        test_samples = test_samples[:50]  # Limit to 50 for practical testing
        
        print(f"Testing {len(test_samples)} random LA addresses...")
        
        density_results = []
        category_stats = {'residential': {'total': 0, 'with_permits': 0}, 
                         'commercial': {'total': 0, 'with_permits': 0}}
        
        for i, sample in enumerate(test_samples, 1):
            address = sample['address']
            category = sample['category']
            
            print(f"🧪 Sample {i}: {address} ({category})")
            
            try:
                # Clean search
                clean_addr = address.replace(',', '').replace('  ', ' ')
                params = {
                    '$where': f"upper(primary_address) like '%{clean_addr.upper()}%'",
                    '$limit': '10',
                    '$order': 'issue_date DESC'
                }
                
                response = requests.get(
                    self.base_url,
                    params=params,
                    headers={"X-App-Token": self.app_token},
                    timeout=10
                )
                
                if response.status_code == 200:
                    data = response.json()
                    permits_found = len(data)
                    
                    result = {
                        'address': address,
                        'category': category,
                        'street_type': sample['street_type'],
                        'permits_found': permits_found,
                        'has_permits': permits_found > 0,
                        'recent_permit_count': sum(1 for p in data if '2024' in str(p.get('issue_date', '')) or '2025' in str(p.get('issue_date', '')))
                    }
                    
                    density_results.append(result)
                    category_stats[category]['total'] += 1
                    if permits_found > 0:
                        category_stats[category]['with_permits'] += 1
                        print(f"   ✅ {permits_found} permits found")
                    else:
                        print(f"   🔍 No permits")
                
                time.sleep(0.4)  # Rate limiting
                
            except Exception as e:
                print(f"   ❌ Error: {e}")
        
        # Calculate density statistics
        total_tested = len(density_results)
        total_with_permits = sum(1 for r in density_results if r['has_permits'])
        overall_density = (total_with_permits / total_tested * 100) if total_tested > 0 else 0
        
        residential_density = (category_stats['residential']['with_permits'] / 
                             category_stats['residential']['total'] * 100) if category_stats['residential']['total'] > 0 else 0
        commercial_density = (category_stats['commercial']['with_permits'] / 
                            category_stats['commercial']['total'] * 100) if category_stats['commercial']['total'] > 0 else 0
        
        print(f"\n📊 PERMIT DENSITY ANALYSIS:")
        print(f"Total addresses tested: {total_tested}")
        print(f"Addresses with permits: {total_with_permits}")
        print(f"Overall permit density: {overall_density:.1f}%")
        print(f"Residential density: {residential_density:.1f}%")
        print(f"Commercial density: {commercial_density:.1f}%")
        
        return {
            'total_tested': total_tested,
            'total_with_permits': total_with_permits,
            'overall_density': overall_density,
            'residential_density': residential_density,
            'commercial_density': commercial_density,
            'category_breakdown': category_stats,
            'detailed_results': density_results
        }
    
    def analyze_permit_patterns(self) -> Dict[str, Any]:
        """
        TASK 3: Analyze Permit Data Patterns
        Study permit types, dates, and geographic distribution
        """
        print(f"\n🔍 ANALYZING PERMIT DATA PATTERNS")
        print("="*40)
        
        # Test broad queries to understand permit patterns
        pattern_queries = [
            {
                'name': 'Recent permits (2024-2025)',
                'params': {
                    '$where': "issue_date >= '2024-01-01T00:00:00.000'",
                    '$limit': '100',
                    '$order': 'issue_date DESC'
                }
            },
            {
                'name': 'Building permits only',
                'params': {
                    '$where': "upper(permit_type) like '%BLDG%'",
                    '$limit': '100',
                    '$order': 'issue_date DESC'
                }
            },
            {
                'name': 'High-value areas (Wilshire)',
                'params': {
                    '$where': "upper(primary_address) like '%WILSHIRE%'",
                    '$limit': '50',
                    '$order': 'issue_date DESC'
                }
            },
            {
                'name': 'Hollywood development',
                'params': {
                    '$where': "upper(primary_address) like '%HOLLYWOOD%'",
                    '$limit': '50',
                    '$order': 'issue_date DESC'
                }
            }
        ]
        
        pattern_results = {}
        
        for query in pattern_queries:
            print(f"\n🔍 Analyzing: {query['name']}")
            
            try:
                response = requests.get(
                    self.base_url,
                    params=query['params'],
                    headers={"X-App-Token": self.app_token},
                    timeout=15
                )
                
                if response.status_code == 200:
                    data = response.json()
                    permits_found = len(data)
                    
                    if permits_found > 0:
                        # Analyze permit types
                        permit_types = {}
                        neighborhoods = {}
                        date_range = {'earliest': None, 'latest': None}
                        
                        for permit in data:
                            # Count permit types
                            permit_type = permit.get('permit_type', 'Unknown')
                            permit_types[permit_type] = permit_types.get(permit_type, 0) + 1
                            
                            # Count neighborhoods (using address patterns)
                            address = permit.get('primary_address', '')
                            if 'HOLLYWOOD' in address.upper():
                                neighborhoods['Hollywood'] = neighborhoods.get('Hollywood', 0) + 1
                            elif 'WILSHIRE' in address.upper():
                                neighborhoods['Wilshire Corridor'] = neighborhoods.get('Wilshire Corridor', 0) + 1
                            elif 'SUNSET' in address.upper():
                                neighborhoods['Sunset Strip Area'] = neighborhoods.get('Sunset Strip Area', 0) + 1
                            elif 'DOWNTOWN' in address.upper() or any(st in address.upper() for st in ['SPRING', 'MAIN', 'BROADWAY']):
                                neighborhoods['Downtown'] = neighborhoods.get('Downtown', 0) + 1
                            else:
                                neighborhoods['Other'] = neighborhoods.get('Other', 0) + 1
                            
                            # Track date range
                            issue_date = permit.get('issue_date', '')
                            if issue_date:
                                if not date_range['earliest'] or issue_date < date_range['earliest']:
                                    date_range['earliest'] = issue_date
                                if not date_range['latest'] or issue_date > date_range['latest']:
                                    date_range['latest'] = issue_date
                        
                        # Show top results
                        top_permit_types = sorted(permit_types.items(), key=lambda x: x[1], reverse=True)[:5]
                        top_neighborhoods = sorted(neighborhoods.items(), key=lambda x: x[1], reverse=True)[:3]
                        
                        print(f"   ✅ Found {permits_found} permits")
                        print(f"   📋 Top permit types:")
                        for ptype, count in top_permit_types:
                            print(f"      {ptype}: {count}")
                        print(f"   📍 Top areas:")
                        for area, count in top_neighborhoods:
                            print(f"      {area}: {count}")
                        print(f"   📅 Date range: {date_range['earliest']} to {date_range['latest']}")
                        
                        pattern_results[query['name']] = {
                            'total_permits': permits_found,
                            'permit_types': permit_types,
                            'neighborhoods': neighborhoods,
                            'date_range': date_range
                        }
                    else:
                        print(f"   🔍 No permits found")
                
                time.sleep(0.5)
                
            except Exception as e:
                print(f"   ❌ Error: {e}")
        
        return pattern_results
    
    def assess_scoring_impact(self, development_results: Dict, density_results: Dict) -> Dict[str, Any]:
        """
        TASK 5: Scoring Impact Analysis
        Evaluate how permit data affects property scoring
        """
        print(f"\n🎯 SCORING IMPACT ANALYSIS")
        print("="*35)
        
        # Simulate scoring with and without permit data
        scoring_scenarios = []
        
        # Mock property scoring function
        def calculate_base_score(property_type: str, location: str) -> float:
            """Simulate base property score without permits"""
            base_scores = {
                'commercial': 75.0,
                'mixed-use': 70.0,
                'residential': 65.0,
                'institutional': 60.0
            }
            
            # Location adjustments
            location_bonuses = {
                'hollywood': 5.0,
                'wilshire': 8.0,
                'downtown': 6.0,
                'sunset': 4.0
            }
            
            base = base_scores.get(property_type, 65.0)
            
            for area, bonus in location_bonuses.items():
                if area in location.lower():
                    base += bonus
                    break
            
            return min(base, 100.0)
        
        def calculate_permit_bonus(permits_found: int, recent_permits: int = 0) -> float:
            """Simulate permit-based scoring bonus"""
            if permits_found == 0:
                return 0.0
            
            # Base permit bonus
            permit_bonus = min(permits_found * 2.0, 10.0)  # Max 10 points
            
            # Recent activity bonus
            if recent_permits > 0:
                permit_bonus += min(recent_permits * 3.0, 8.0)  # Max 8 additional points
            
            return permit_bonus
        
        # Analyze scoring impact from development results
        if 'detailed_results' in development_results:
            for result in development_results['detailed_results']:
                address = result['address']
                prop_type = result['property_type']
                permits_found = result['permits_found']
                
                base_score = calculate_base_score(prop_type, address)
                permit_bonus = calculate_permit_bonus(permits_found)
                final_score = min(base_score + permit_bonus, 100.0)
                
                impact = (permit_bonus / base_score * 100) if base_score > 0 else 0
                
                scoring_scenarios.append({
                    'address': address,
                    'property_type': prop_type,
                    'base_score': base_score,
                    'permit_bonus': permit_bonus,
                    'final_score': final_score,
                    'permit_impact_percent': impact,
                    'permits_found': permits_found
                })
        
        # Calculate scoring statistics
        total_scenarios = len(scoring_scenarios)
        scenarios_with_permits = sum(1 for s in scoring_scenarios if s['permits_found'] > 0)
        avg_permit_impact = sum(s['permit_impact_percent'] for s in scoring_scenarios if s['permits_found'] > 0) / scenarios_with_permits if scenarios_with_permits > 0 else 0
        max_permit_impact = max(s['permit_impact_percent'] for s in scoring_scenarios) if scoring_scenarios else 0
        
        print(f"📊 SCORING IMPACT METRICS:")
        print(f"Properties analyzed: {total_scenarios}")
        print(f"Properties with permit data: {scenarios_with_permits}")
        print(f"Average permit score impact: {avg_permit_impact:.1f}%")
        print(f"Maximum permit score impact: {max_permit_impact:.1f}%")
        
        # Show examples
        print(f"\n📋 SCORING EXAMPLES:")
        high_impact_scenarios = sorted(scoring_scenarios, key=lambda x: x['permit_impact_percent'], reverse=True)[:3]
        for scenario in high_impact_scenarios:
            print(f"   {scenario['address']}")
            print(f"     Base score: {scenario['base_score']:.1f}")
            print(f"     With permits: {scenario['final_score']:.1f} (+{scenario['permit_bonus']:.1f})")
            print(f"     Impact: {scenario['permit_impact_percent']:.1f}%")
            print()
        
        return {
            'total_scenarios': total_scenarios,
            'scenarios_with_permits': scenarios_with_permits,
            'permit_data_coverage': (scenarios_with_permits / total_scenarios * 100) if total_scenarios > 0 else 0,
            'avg_permit_impact': avg_permit_impact,
            'max_permit_impact': max_permit_impact,
            'detailed_scenarios': scoring_scenarios
        }
    
    def run_comprehensive_assessment(self) -> Dict[str, Any]:
        """Run complete permit data viability assessment"""
        print(f"🔍 COMPREHENSIVE PERMIT DATA VIABILITY ASSESSMENT")
        print("="*60)
        print(f"Starting assessment at: {datetime.now().isoformat()}")
        print()
        
        # Task 1: Test real development properties
        development_results = self.test_real_la_development_properties()
        
        # Task 2: Statistical sampling
        density_results = self.statistical_permit_density_sampling()
        
        # Task 3: Pattern analysis
        pattern_results = self.analyze_permit_patterns()
        
        # Task 5: Scoring impact
        scoring_results = self.assess_scoring_impact(development_results, density_results)
        
        # Comprehensive recommendations
        print(f"\n🎯 COMPREHENSIVE ASSESSMENT RESULTS")
        print("="*45)
        
        overall_viability = "HIGH" if density_results['overall_density'] > 20 else "MEDIUM" if density_results['overall_density'] > 10 else "LOW"
        
        print(f"Overall permit data density: {density_results['overall_density']:.1f}%")
        print(f"Development property hit rate: {development_results['hit_rate']:.1f}%")
        print(f"Scoring impact coverage: {scoring_results['permit_data_coverage']:.1f}%")
        print(f"Average scoring impact: {scoring_results['avg_permit_impact']:.1f}%")
        print(f"Viability rating: {overall_viability}")
        
        # Business recommendations
        print(f"\n💡 BUSINESS RECOMMENDATIONS:")
        if overall_viability == "HIGH":
            print("✅ Permit data is viable for property scoring")
            print("✅ Include permits as a significant scoring factor")
            print("✅ Consider permit activity as development indicator")
        elif overall_viability == "MEDIUM":
            print("⚠️ Permit data has limited coverage but some value")
            print("⚠️ Use permits as supplementary scoring factor only")
            print("⚠️ Focus on high-activity areas where permits are more common")
        else:
            print("❌ Permit data too sparse for reliable scoring")
            print("❌ Consider alternative development indicators")
            print("❌ Use permits for validation rather than primary scoring")
        
        return {
            'development_results': development_results,
            'density_results': density_results,
            'pattern_results': pattern_results,
            'scoring_results': scoring_results,
            'viability_rating': overall_viability,
            'assessment_timestamp': datetime.now().isoformat()
        }

def main():
    """
    ASSESS PERMIT DATA DENSITY AND BUSINESS VALUE
    """
    print(f"🏗️ LA PERMIT DATA VIABILITY ASSESSMENT")
    print("="*50)
    print("Determining real-world business value of permit data")
    print()
    
    assessor = PermitDataViabilityAssessor()
    results = assessor.run_comprehensive_assessment()
    
    # Save comprehensive results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_filename = f"permit_data_viability_assessment_{timestamp}.json"
    
    with open(results_filename, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n📁 ASSESSMENT COMPLETE:")
    print(f"Results saved to: {results_filename}")
    
    return results

if __name__ == "__main__":
    main()