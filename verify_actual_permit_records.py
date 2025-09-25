#!/usr/bin/env python3
"""
VERIFY PERMIT DATA QUALITY - SHOW ACTUAL RECORDS
Examine real permit records to assess investment value
"""

import requests
import json
import time
from datetime import datetime
from typing import Dict, List, Any

class PermitRecordVerifier:
    def __init__(self):
        self.base_url = "https://data.lacity.org/resource/pi9x-tg5x.json"
        self.app_token = "lmUNVajT2wIHnzFI2x3HGEt5H"
        
    def show_actual_permit_records(self) -> List[Dict]:
        """
        TASK 1: Show Actual Permit Records Found
        Display real permit records with all details
        """
        print("📋 SHOWING ACTUAL PERMIT RECORDS")
        print("="*45)
        
        # Get recent permits with full details
        try:
            params = {
                '$where': "issue_date >= '2024-01-01T00:00:00.000'",
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
                permits = response.json()
                print(f"Retrieved {len(permits)} actual permit records:")
                print()
                
                displayed_permits = []
                
                for i, permit in enumerate(permits[:15], 1):
                    print(f"🏗️ PERMIT {i}:")
                    print(f"   Permit Number: {permit.get('permit_nbr', 'N/A')}")
                    print(f"   Address: {permit.get('primary_address', 'N/A')}")
                    print(f"   Permit Type: {permit.get('permit_type', 'N/A')}")
                    print(f"   Sub Type: {permit.get('permit_sub_type', 'N/A')}")
                    print(f"   Work Description: {permit.get('use_desc', 'N/A')}")
                    print(f"   Issue Date: {permit.get('issue_date', 'N/A')}")
                    print(f"   Status: {permit.get('status_desc', 'N/A')}")
                    print(f"   Valuation: ${permit.get('valuation', 'N/A')}")
                    print(f"   APN: {permit.get('apn', 'N/A')}")
                    print(f"   Zone: {permit.get('zone', 'N/A')}")
                    print(f"   Council District: {permit.get('cd', 'N/A')}")
                    print()
                    
                    # Store for analysis
                    displayed_permits.append({
                        'permit_nbr': permit.get('permit_nbr', ''),
                        'address': permit.get('primary_address', ''),
                        'permit_type': permit.get('permit_type', ''),
                        'permit_sub_type': permit.get('permit_sub_type', ''),
                        'work_description': permit.get('use_desc', ''),
                        'valuation': permit.get('valuation', ''),
                        'issue_date': permit.get('issue_date', ''),
                        'status': permit.get('status_desc', ''),
                        'zone': permit.get('zone', ''),
                        'council_district': permit.get('cd', '')
                    })
                
                return displayed_permits
            else:
                print(f"❌ API Error: {response.status_code}")
                return []
                
        except Exception as e:
            print(f"❌ Error retrieving permits: {e}")
            return []
    
    def categorize_permits_by_investment_value(self, permits: List[Dict]) -> Dict[str, Any]:
        """
        TASK 2: Categorize Permit Types by Investment Relevance
        """
        print("💰 CATEGORIZING PERMITS BY INVESTMENT VALUE")
        print("="*50)
        
        categories = {
            'major_development': [],
            'minor_residential': [],
            'commercial_retail': [],
            'infrastructure_only': [],
            'unknown': []
        }
        
        for permit in permits:
            permit_type = permit.get('permit_type', '').upper()
            sub_type = permit.get('permit_sub_type', '').upper()
            work_desc = permit.get('work_description', '').upper()
            valuation = permit.get('valuation', '')
            
            # Try to parse valuation
            try:
                val_amount = float(valuation) if valuation and valuation != 'N/A' else 0
            except:
                val_amount = 0
            
            # Categorization logic
            if any(keyword in permit_type for keyword in ['NEW', 'ADDITION']) and val_amount > 500000:
                categories['major_development'].append(permit)
            elif 'NEW' in permit_type or 'ADDITION' in permit_type or val_amount > 100000:
                if any(keyword in sub_type for keyword in ['APARTMENT', 'COMMERCIAL', 'OFFICE', 'RETAIL']):
                    categories['commercial_retail'].append(permit)
                else:
                    categories['major_development'].append(permit)
            elif any(keyword in permit_type for keyword in ['POOL', 'SPA']) or val_amount < 50000:
                categories['minor_residential'].append(permit)
            elif any(keyword in permit_type for keyword in ['ELECTRICAL', 'PLUMBING', 'MECHANICAL']):
                categories['infrastructure_only'].append(permit)
            else:
                categories['unknown'].append(permit)
        
        # Display categorization results
        total_permits = len(permits)
        print(f"📊 PERMIT CATEGORIZATION RESULTS:")
        print(f"Total permits analyzed: {total_permits}")
        print()
        
        for category, permit_list in categories.items():
            count = len(permit_list)
            percentage = (count / total_permits * 100) if total_permits > 0 else 0
            category_name = category.replace('_', ' ').title()
            
            print(f"🏷️ {category_name}: {count} permits ({percentage:.1f}%)")
            
            if permit_list:
                print(f"   Examples:")
                for permit in permit_list[:3]:  # Show up to 3 examples
                    val_str = f"${permit.get('valuation', 'N/A')}" if permit.get('valuation', 'N/A') != 'N/A' else 'No valuation'
                    print(f"   - {permit.get('address', 'N/A')}: {permit.get('permit_type', 'N/A')} ({val_str})")
            print()
        
        # Determine majority category
        majority_category = max(categories.keys(), key=lambda k: len(categories[k]))
        majority_count = len(categories[majority_category])
        majority_percentage = (majority_count / total_permits * 100) if total_permits > 0 else 0
        
        print(f"📈 MAJORITY CATEGORY: {majority_category.replace('_', ' ').title()}")
        print(f"   {majority_count}/{total_permits} permits ({majority_percentage:.1f}%)")
        
        return {
            'categories': categories,
            'total_permits': total_permits,
            'majority_category': majority_category,
            'majority_percentage': majority_percentage
        }
    
    def test_realistic_investment_properties(self) -> Dict[str, Any]:
        """
        TASK 3: Test Realistic Property Investment Scenarios
        """
        print("🏠 TESTING REALISTIC INVESTMENT PROPERTIES")
        print("="*45)
        
        # Typical investment property addresses (not major development zones)
        realistic_properties = [
            # Small apartment buildings
            {"address": "1234 Vermont Ave", "type": "small_apartment", "area": "residential"},
            {"address": "5678 Western Ave", "type": "small_apartment", "area": "residential"}, 
            {"address": "2345 Crenshaw Blvd", "type": "small_apartment", "area": "residential"},
            
            # Retail spaces in neighborhood centers
            {"address": "3456 Beverly Blvd", "type": "retail_space", "area": "commercial"},
            {"address": "4567 Melrose Ave", "type": "retail_space", "area": "commercial"},
            {"address": "6789 Pico Blvd", "type": "retail_space", "area": "commercial"},
            
            # Single family homes in investment areas
            {"address": "1357 6th St", "type": "single_family", "area": "residential"},
            {"address": "2468 8th St", "type": "single_family", "area": "residential"},
            {"address": "3579 Olympic Blvd", "type": "single_family", "area": "residential"},
            
            # Mixed-use buildings
            {"address": "4680 Sunset Blvd", "type": "mixed_use", "area": "mixed"},
            {"address": "5791 Santa Monica Blvd", "type": "mixed_use", "area": "mixed"}
        ]
        
        print(f"Testing {len(realistic_properties)} realistic investment properties...")
        print()
        
        results = []
        properties_with_permits = 0
        
        for i, prop in enumerate(realistic_properties, 1):
            address = prop["address"]
            prop_type = prop["type"]
            area = prop["area"]
            
            print(f"🧪 Testing {i}: {address} ({prop_type})")
            
            try:
                # Search for permits at this address
                params = {
                    '$where': f"upper(primary_address) like '%{address.split()[1].upper()}%' AND primary_address like '{address.split()[0]}%'",
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
                    permits = response.json()
                    permits_found = len(permits)
                    
                    if permits_found > 0:
                        properties_with_permits += 1
                        print(f"   ✅ {permits_found} permits found")
                        
                        # Show permit details
                        for permit in permits[:2]:  # Show first 2
                            print(f"      - {permit.get('primary_address', 'N/A')}: {permit.get('permit_type', 'N/A')}")
                    else:
                        print(f"   🔍 No permits found")
                    
                    results.append({
                        'address': address,
                        'property_type': prop_type,
                        'area': area,
                        'permits_found': permits_found,
                        'has_permits': permits_found > 0
                    })
                else:
                    print(f"   ❌ API error: {response.status_code}")
                
                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                print(f"   ❌ Error: {e}")
        
        hit_rate = (properties_with_permits / len(realistic_properties) * 100) if realistic_properties else 0
        
        print(f"\n📊 REALISTIC PROPERTY RESULTS:")
        print(f"Properties tested: {len(realistic_properties)}")
        print(f"Properties with permits: {properties_with_permits}")
        print(f"Hit rate: {hit_rate:.1f}%")
        
        return {
            'total_tested': len(realistic_properties),
            'properties_with_permits': properties_with_permits,
            'hit_rate': hit_rate,
            'detailed_results': results
        }
    
    def validate_development_zone_claims(self) -> Dict[str, Any]:
        """
        TASK 4: Validate Development Zone Claims
        Show actual addresses and permits found in claimed development zones
        """
        print("🔍 VALIDATING DEVELOPMENT ZONE CLAIMS")
        print("="*42)
        
        # Test specific addresses in claimed development zones
        development_addresses = [
            # Downtown Financial District - specific addresses
            {"zone": "Downtown Financial District", "address": "555 S Flower St", "expected": "high-value"},
            {"zone": "Downtown Financial District", "address": "400 S Hope St", "expected": "high-value"},  
            {"zone": "Downtown Financial District", "address": "1111 S Figueroa St", "expected": "high-value"},
            
            # Hollywood Entertainment - specific addresses
            {"zone": "Hollywood Entertainment", "address": "6801 Hollywood Blvd", "expected": "entertainment"},
            {"zone": "Hollywood Entertainment", "address": "1750 N Highland Ave", "expected": "entertainment"},
            
            # Century City Adjacent - specific addresses  
            {"zone": "Century City Adjacent", "address": "1950 Avenue of the Stars", "expected": "corporate"}
        ]
        
        validation_results = []
        
        for test in development_addresses:
            zone = test["zone"]
            address = test["address"]
            expected = test["expected"]
            
            print(f"\n🎯 Validating: {address} ({zone})")
            
            try:
                # Search for exact address
                address_parts = address.split()
                street_name = ' '.join(address_parts[1:])
                
                params = {
                    '$where': f"upper(primary_address) like '%{street_name.upper()}%'",
                    '$limit': '15',
                    '$order': 'issue_date DESC'
                }
                
                response = requests.get(
                    self.base_url,
                    params=params,
                    headers={"X-App-Token": self.app_token},
                    timeout=15
                )
                
                if response.status_code == 200:
                    permits = response.json()
                    permits_found = len(permits)
                    
                    print(f"   📋 Found {permits_found} permits on {street_name}")
                    
                    if permits_found > 0:
                        # Show actual permits found
                        actual_permits = []
                        print(f"   🏗️ ACTUAL PERMITS FOUND:")
                        
                        for i, permit in enumerate(permits[:5], 1):
                            permit_info = {
                                'address': permit.get('primary_address', 'N/A'),
                                'permit_type': permit.get('permit_type', 'N/A'),
                                'sub_type': permit.get('permit_sub_type', 'N/A'),
                                'valuation': permit.get('valuation', 'N/A'),
                                'issue_date': permit.get('issue_date', 'N/A'),
                                'status': permit.get('status_desc', 'N/A')
                            }
                            actual_permits.append(permit_info)
                            
                            val_str = f"${permit_info['valuation']}" if permit_info['valuation'] != 'N/A' else 'No valuation'
                            print(f"      {i}. {permit_info['address']}")
                            print(f"         Type: {permit_info['permit_type']}")
                            print(f"         Subtype: {permit_info['sub_type']}")
                            print(f"         Value: {val_str}")
                            print(f"         Date: {permit_info['issue_date']}")
                            print(f"         Status: {permit_info['status']}")
                            print()
                        
                        validation_results.append({
                            'zone': zone,
                            'address': address,
                            'expected': expected,
                            'permits_found': permits_found,
                            'actual_permits': actual_permits,
                            'validated': True
                        })
                    else:
                        print(f"   ❌ No permits found for claimed development area")
                        validation_results.append({
                            'zone': zone,
                            'address': address,
                            'expected': expected,
                            'permits_found': 0,
                            'actual_permits': [],
                            'validated': False
                        })
                else:
                    print(f"   ❌ API error: {response.status_code}")
                
                time.sleep(0.7)
                
            except Exception as e:
                print(f"   ❌ Error: {e}")
        
        # Summary of validation
        validated_zones = sum(1 for r in validation_results if r['validated'])
        total_zones = len(validation_results)
        validation_rate = (validated_zones / total_zones * 100) if total_zones > 0 else 0
        
        print(f"\n📊 DEVELOPMENT ZONE VALIDATION:")
        print(f"Zones tested: {total_zones}")
        print(f"Zones with permits: {validated_zones}")
        print(f"Validation rate: {validation_rate:.1f}%")
        
        return {
            'validation_results': validation_results,
            'validation_rate': validation_rate,
            'zones_validated': validated_zones,
            'total_zones_tested': total_zones
        }

def main():
    """Verify actual permit data quality and investment value"""
    print("🔍 PERMIT DATA QUALITY VERIFICATION")
    print("="*45)
    print("Examining actual permit records for investment value")
    print()
    
    verifier = PermitRecordVerifier()
    
    # Task 1: Show actual permit records
    actual_permits = verifier.show_actual_permit_records()
    
    # Task 2: Categorize by investment value
    if actual_permits:
        categorization = verifier.categorize_permits_by_investment_value(actual_permits)
    
    # Task 3: Test realistic investment properties
    realistic_results = verifier.test_realistic_investment_properties()
    
    # Task 4: Validate development zone claims
    validation_results = verifier.validate_development_zone_claims()
    
    # Final assessment
    print(f"\n🎯 FINAL PERMIT DATA ASSESSMENT")
    print("="*40)
    
    if actual_permits and 'majority_category' in categorization:
        print(f"Majority permit type: {categorization['majority_category'].replace('_', ' ').title()}")
        print(f"Realistic property hit rate: {realistic_results['hit_rate']:.1f}%")
        print(f"Development zone validation: {validation_results['validation_rate']:.1f}%")
        
        # Overall recommendation
        if (categorization['majority_category'] in ['major_development', 'commercial_retail'] and 
            realistic_results['hit_rate'] > 20):
            print(f"\n✅ RECOMMENDATION: Permit data has HIGH investment value")
        elif realistic_results['hit_rate'] > 10:
            print(f"\n⚠️ RECOMMENDATION: Permit data has MODERATE investment value")
        else:
            print(f"\n❌ RECOMMENDATION: Permit data has LOW investment value")
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_filename = f"permit_quality_verification_{timestamp}.json"
    
    comprehensive_results = {
        'actual_permits': actual_permits,
        'categorization': categorization if actual_permits else {},
        'realistic_property_results': realistic_results,
        'development_zone_validation': validation_results,
        'verification_timestamp': datetime.now().isoformat()
    }
    
    with open(results_filename, 'w') as f:
        json.dump(comprehensive_results, f, indent=2, default=str)
    
    print(f"\n📁 VERIFICATION COMPLETE")
    print(f"Results saved to: {results_filename}")
    
    return comprehensive_results

if __name__ == "__main__":
    main()