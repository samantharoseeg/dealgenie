#!/usr/bin/env python3
"""
FOCUSED PERMIT DATA ANALYSIS - HIGH-VALUE PROPERTIES
Test specific known development areas for permit availability
"""

import requests
import json
import time
from datetime import datetime
from typing import Dict, List, Any

class FocusedPermitAnalyzer:
    def __init__(self):
        self.base_url = "https://data.lacity.org/resource/pi9x-tg5x.json"
        self.app_token = "lmUNVajT2wIHnzFI2x3HGEt5H"
        
    def test_known_development_areas(self) -> Dict[str, Any]:
        """Test areas with known recent development activity"""
        print("🏗️ TESTING KNOWN DEVELOPMENT AREAS")
        print("="*40)
        
        # Focus on areas with confirmed development activity
        development_zones = [
            {
                'name': 'Downtown Financial District',
                'search_terms': ['FIGUEROA', 'FLOWER', 'HOPE', 'GRAND'],
                'expected_activity': 'HIGH'
            },
            {
                'name': 'Hollywood Entertainment District', 
                'search_terms': ['HOLLYWOOD BLVD', 'VINE', 'HIGHLAND'],
                'expected_activity': 'HIGH'
            },
            {
                'name': 'Wilshire Corridor',
                'search_terms': ['WILSHIRE'],
                'expected_activity': 'MEDIUM'
            },
            {
                'name': 'Arts District',
                'search_terms': ['ALAMEDA', 'TRACTION', 'HEWITT'],
                'expected_activity': 'MEDIUM'
            },
            {
                'name': 'Century City Adjacent',
                'search_terms': ['AVENUE OF THE STARS', 'CONSTELLATION'],
                'expected_activity': 'HIGH'
            }
        ]
        
        zone_results = {}
        
        for zone in development_zones:
            print(f"\n🔍 Testing: {zone['name']}")
            zone_permits = 0
            zone_details = []
            
            for term in zone['search_terms']:
                try:
                    params = {
                        '$where': f"upper(primary_address) like '%{term}%'",
                        '$limit': '20',
                        '$order': 'issue_date DESC'
                    }
                    
                    response = requests.get(
                        self.base_url,
                        params=params,
                        headers={"X-App-Token": self.app_token},
                        timeout=15
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        permits_found = len(data)
                        zone_permits += permits_found
                        
                        if permits_found > 0:
                            print(f"   {term}: {permits_found} permits")
                            # Get recent permits (2024-2025)
                            recent_permits = [p for p in data if '2024' in str(p.get('issue_date', '')) or '2025' in str(p.get('issue_date', ''))]
                            if recent_permits:
                                sample = recent_permits[0]
                                zone_details.append({
                                    'address': sample.get('primary_address', ''),
                                    'permit_type': sample.get('permit_type', ''),
                                    'issue_date': sample.get('issue_date', ''),
                                    'status': sample.get('status_desc', '')
                                })
                        else:
                            print(f"   {term}: No permits")
                    else:
                        print(f"   {term}: API error {response.status_code}")
                    
                    time.sleep(0.5)  # Rate limiting
                    
                except Exception as e:
                    print(f"   {term}: Error - {e}")
            
            zone_results[zone['name']] = {
                'total_permits': zone_permits,
                'expected_activity': zone['expected_activity'],
                'sample_permits': zone_details[:3],
                'meets_expectations': zone_permits > 10 if zone['expected_activity'] == 'HIGH' else zone_permits > 5
            }
            
            print(f"   📊 Total permits in {zone['name']}: {zone_permits}")
        
        return zone_results
    
    def test_permit_type_distribution(self) -> Dict[str, Any]:
        """Analyze what types of permits are most common"""
        print(f"\n📋 PERMIT TYPE DISTRIBUTION ANALYSIS")
        print("="*40)
        
        # Get recent permits to analyze types
        try:
            params = {
                '$where': "issue_date >= '2024-01-01T00:00:00.000'",
                '$limit': '100',
                '$order': 'issue_date DESC'
            }
            
            response = requests.get(
                self.base_url,
                params=params,
                headers={"X-App-Token": self.app_token},
                timeout=20
            )
            
            if response.status_code == 200:
                data = response.json()
                print(f"Analyzing {len(data)} recent permits...")
                
                # Count permit types
                permit_types = {}
                neighborhoods = {}
                recent_activity = {'2025': 0, '2024': 0}
                
                for permit in data:
                    # Permit types
                    ptype = permit.get('permit_type', 'Unknown')
                    permit_types[ptype] = permit_types.get(ptype, 0) + 1
                    
                    # Geographic distribution
                    address = permit.get('primary_address', '').upper()
                    if any(area in address for area in ['HOLLYWOOD', 'VINE', 'HIGHLAND']):
                        neighborhoods['Hollywood'] = neighborhoods.get('Hollywood', 0) + 1
                    elif any(st in address for st in ['WILSHIRE', 'MIRACLE MILE']):
                        neighborhoods['Wilshire Corridor'] = neighborhoods.get('Wilshire Corridor', 0) + 1
                    elif any(st in address for st in ['FIGUEROA', 'FLOWER', 'HOPE', 'SPRING', 'MAIN', 'BROADWAY']):
                        neighborhoods['Downtown'] = neighborhoods.get('Downtown', 0) + 1
                    else:
                        neighborhoods['Other Areas'] = neighborhoods.get('Other Areas', 0) + 1
                    
                    # Year distribution
                    issue_date = permit.get('issue_date', '')
                    if '2025' in issue_date:
                        recent_activity['2025'] += 1
                    elif '2024' in issue_date:
                        recent_activity['2024'] += 1
                
                # Show results
                print(f"\n📊 TOP PERMIT TYPES:")
                top_types = sorted(permit_types.items(), key=lambda x: x[1], reverse=True)[:5]
                for ptype, count in top_types:
                    percentage = (count / len(data)) * 100
                    print(f"   {ptype}: {count} ({percentage:.1f}%)")
                
                print(f"\n📍 GEOGRAPHIC DISTRIBUTION:")
                for area, count in sorted(neighborhoods.items(), key=lambda x: x[1], reverse=True):
                    percentage = (count / len(data)) * 100
                    print(f"   {area}: {count} ({percentage:.1f}%)")
                
                print(f"\n📅 RECENT ACTIVITY:")
                print(f"   2025 permits: {recent_activity['2025']}")
                print(f"   2024 permits: {recent_activity['2024']}")
                
                return {
                    'total_analyzed': len(data),
                    'permit_types': permit_types,
                    'neighborhoods': neighborhoods,
                    'recent_activity': recent_activity,
                    'top_permit_types': top_types
                }
            else:
                print(f"❌ API Error: {response.status_code}")
                return {'error': f'API returned {response.status_code}'}
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return {'error': str(e)}
    
    def assess_business_viability(self, zone_results: Dict, type_results: Dict) -> Dict[str, Any]:
        """Assess business viability of permit data for property scoring"""
        print(f"\n🎯 BUSINESS VIABILITY ASSESSMENT")
        print("="*35)
        
        # Calculate viability metrics
        total_zones = len(zone_results)
        zones_meeting_expectations = sum(1 for z in zone_results.values() if z.get('meets_expectations', False))
        zone_success_rate = (zones_meeting_expectations / total_zones * 100) if total_zones > 0 else 0
        
        total_permits_found = sum(z.get('total_permits', 0) for z in zone_results.values())
        avg_permits_per_zone = total_permits_found / total_zones if total_zones > 0 else 0
        
        # Analyze permit data currency
        if 'recent_activity' in type_results:
            recent_permits = type_results['recent_activity'].get('2024', 0) + type_results['recent_activity'].get('2025', 0)
            data_currency = "HIGH" if recent_permits > 50 else "MEDIUM" if recent_permits > 20 else "LOW"
        else:
            recent_permits = 0
            data_currency = "UNKNOWN"
        
        # Determine overall viability
        if zone_success_rate >= 60 and avg_permits_per_zone >= 15 and data_currency in ["HIGH", "MEDIUM"]:
            viability = "HIGH"
            recommendation = "VIABLE"
        elif zone_success_rate >= 40 and avg_permits_per_zone >= 8:
            viability = "MEDIUM" 
            recommendation = "LIMITED_USE"
        else:
            viability = "LOW"
            recommendation = "NOT_VIABLE"
        
        print(f"📊 VIABILITY METRICS:")
        print(f"Zones meeting expectations: {zones_meeting_expectations}/{total_zones} ({zone_success_rate:.1f}%)")
        print(f"Average permits per zone: {avg_permits_per_zone:.1f}")
        print(f"Data currency: {data_currency}")
        print(f"Recent permits available: {recent_permits}")
        print(f"Overall viability: {viability}")
        print(f"Recommendation: {recommendation}")
        
        # Business implications
        print(f"\n💼 BUSINESS IMPLICATIONS:")
        if recommendation == "VIABLE":
            print("✅ Permit data can be integrated as scoring factor")
            print("✅ Focus on high-activity zones for maximum impact")
            print("✅ Recent permit activity indicates good data currency")
            scoring_weight = "15-20%"
        elif recommendation == "LIMITED_USE":
            print("⚠️ Permit data useful but limited coverage")
            print("⚠️ Use as supplementary factor, not primary")
            print("⚠️ Focus on specific high-activity corridors only")
            scoring_weight = "5-10%"
        else:
            print("❌ Permit data too sparse for reliable scoring")
            print("❌ Consider alternative development indicators")
            print("❌ May create bias toward certain areas")
            scoring_weight = "0%"
        
        print(f"Recommended scoring weight: {scoring_weight}")
        
        return {
            'viability_rating': viability,
            'recommendation': recommendation,
            'zone_success_rate': zone_success_rate,
            'avg_permits_per_zone': avg_permits_per_zone,
            'data_currency': data_currency,
            'recent_permits': recent_permits,
            'scoring_weight': scoring_weight,
            'business_implications': {
                'viable': recommendation == "VIABLE",
                'limited_use': recommendation == "LIMITED_USE",
                'not_viable': recommendation == "NOT_VIABLE"
            }
        }

def main():
    """Run focused permit data analysis"""
    print("🏗️ FOCUSED PERMIT DATA ANALYSIS")
    print("="*40)
    print("Testing high-value development areas")
    print()
    
    analyzer = FocusedPermitAnalyzer()
    
    # Test known development zones
    zone_results = analyzer.test_known_development_areas()
    
    # Analyze permit type distribution  
    type_results = analyzer.test_permit_type_distribution()
    
    # Assess business viability
    viability_results = analyzer.assess_business_viability(zone_results, type_results)
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_filename = f"focused_permit_analysis_{timestamp}.json"
    
    comprehensive_results = {
        'zone_analysis': zone_results,
        'permit_type_analysis': type_results,
        'viability_assessment': viability_results,
        'analysis_timestamp': datetime.now().isoformat()
    }
    
    with open(results_filename, 'w') as f:
        json.dump(comprehensive_results, f, indent=2, default=str)
    
    print(f"\n📁 ANALYSIS COMPLETE")
    print(f"Results saved to: {results_filename}")
    
    return comprehensive_results

if __name__ == "__main__":
    main()