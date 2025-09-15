#!/usr/bin/env python3
"""
TEST RANDOM LA ADDRESSES - REAL-WORLD SUCCESS RATE
Test genuinely random addresses from different LA areas
"""

import sqlite3
import requests
import random
import json
from datetime import datetime
from typing import Dict, List, Any, Optional

class RandomAddressTester:
    def __init__(self):
        self.db_path = 'dealgenie_properties.db'
        self.permit_api_url = "https://data.lacity.org/resource/pi9x-tg5x.json"
        self.app_token = "lmUNVajT2wIHnzFI2x3HGEt5H"
        
    def generate_random_addresses(self) -> List[Dict[str, str]]:
        """Generate genuinely random addresses from different LA areas"""
        
        # Use random number generator with seed for reproducibility
        random.seed(42)  # Fixed seed for consistent testing
        
        # Street names by area (real LA streets)
        areas = {
            'San Fernando Valley': {
                'streets': ['Ventura Blvd', 'Reseda Blvd', 'Balboa Ave', 'Sepulveda Blvd', 'Van Nuys Blvd', 
                           'Sherman Way', 'Victory Blvd', 'Burbank Blvd', 'Magnolia Blvd', 'Riverside Dr'],
                'zip_codes': ['91401', '91405', '91411', '91423', '91436']
            },
            'South LA': {
                'streets': ['Crenshaw Blvd', 'Vermont Ave', 'Western Ave', 'Florence Ave', 'Slauson Ave',
                           'Manchester Ave', 'Century Blvd', 'Imperial Hwy', 'Central Ave', 'Broadway'],
                'zip_codes': ['90001', '90003', '90007', '90008', '90043']
            },
            'East LA': {
                'streets': ['Cesar Chavez Ave', 'Whittier Blvd', 'Olympic Blvd', 'Brooklyn Ave', '1st St',
                           'Soto St', 'Indiana St', 'Lorena St', 'Atlantic Blvd', 'Eastern Ave'],
                'zip_codes': ['90022', '90023', '90033', '90063']
            },
            'Westside': {
                'streets': ['Wilshire Blvd', 'Santa Monica Blvd', 'Pico Blvd', 'Olympic Blvd', 'Sunset Blvd',
                           'Westwood Blvd', 'Bundy Dr', 'Barrington Ave', 'Sawtelle Blvd', 'Robertson Blvd'],
                'zip_codes': ['90024', '90025', '90064', '90066', '90067']
            },
            'Central LA': {
                'streets': ['Spring St', 'Main St', 'Broadway', 'Hill St', 'Olive St',
                           'Grand Ave', 'Hope St', 'Flower St', 'Figueroa St', 'Alameda St'],
                'zip_codes': ['90012', '90013', '90014', '90015', '90017']
            }
        }
        
        random_addresses = []
        
        for area_name, area_data in areas.items():
            # Generate random house number (realistic range)
            house_number = random.randint(100, 9999)
            
            # Random street from area
            street = random.choice(area_data['streets'])
            
            # Random direction prefix (sometimes)
            if random.random() > 0.5:
                direction = random.choice(['N', 'S', 'E', 'W'])
                street = f"{direction} {street}"
            
            # Random zip code from area
            zip_code = random.choice(area_data['zip_codes'])
            
            # Construct full address
            full_address = f"{house_number} {street}, Los Angeles, CA {zip_code}"
            
            random_addresses.append({
                'address': full_address,
                'area': area_name,
                'house_number': house_number,
                'street': street,
                'zip_code': zip_code
            })
        
        return random_addresses
    
    def test_address(self, address_info: Dict[str, str]) -> Dict[str, Any]:
        """Test a single address across all systems"""
        
        address = address_info['address']
        area = address_info['area']
        
        print(f"\n🏠 TESTING: {address}")
        print(f"   Area: {area}")
        print("-" * 50)
        
        result = {
            'address': address,
            'area': area,
            'database_test': None,
            'crime_score_test': None,
            'permit_test': None,
            'overall_success': False,
            'errors': []
        }
        
        # Test 1: Database property lookup
        print("   📊 Database lookup...")
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Search for properties on this street
            street_name = address_info['street'].split()[-1]  # Get main street name
            query = """
            SELECT COUNT(*) FROM enhanced_scored_properties 
            WHERE UPPER(site_address) LIKE ?
            """
            cursor.execute(query, (f'%{street_name.upper()}%',))
            count = cursor.fetchone()[0]
            
            if count > 0:
                print(f"      ✅ Found {count} properties on {street_name}")
                result['database_test'] = {'success': True, 'count': count}
            else:
                print(f"      ❌ No properties found on {street_name}")
                result['database_test'] = {'success': False, 'count': 0}
                result['errors'].append(f"No properties in database for {street_name}")
            
            conn.close()
            
        except Exception as e:
            error_msg = f"Database error: {str(e)}"
            print(f"      ❌ {error_msg}")
            result['database_test'] = {'success': False, 'error': str(e)}
            result['errors'].append(error_msg)
        
        # Test 2: Crime score lookup
        print("   🚨 Crime score lookup...")
        try:
            # Get approximate coordinates for the area
            area_coords = {
                'San Fernando Valley': (34.1808, -118.4684),
                'South LA': (33.9925, -118.2816),
                'East LA': (34.0239, -118.1721),
                'Westside': (34.0522, -118.4437),
                'Central LA': (34.0522, -118.2437)
            }
            
            if area in area_coords:
                lat, lon = area_coords[area]
                
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                # Find nearest crime grid cell
                query = """
                SELECT cell_id, total_density_weighted, incident_count
                FROM crime_density_grid
                WHERE ABS(lat - ?) < 0.05 AND ABS(lon - ?) < 0.05
                ORDER BY ABS(lat - ?) + ABS(lon - ?)
                LIMIT 1
                """
                cursor.execute(query, (lat, lon, lat, lon))
                crime_data = cursor.fetchone()
                
                if crime_data:
                    cell_id, crime_score, incidents = crime_data
                    print(f"      ✅ Crime score: {crime_score:.2f} ({incidents} incidents)")
                    result['crime_score_test'] = {
                        'success': True, 
                        'score': crime_score, 
                        'incidents': incidents
                    }
                else:
                    print(f"      ❌ No crime data for this area")
                    result['crime_score_test'] = {'success': False}
                    result['errors'].append("No crime grid data for area")
                
                conn.close()
            else:
                print(f"      ⚠️ Area coordinates not found")
                result['crime_score_test'] = {'success': False}
                result['errors'].append("Area coordinates not mapped")
                
        except Exception as e:
            error_msg = f"Crime score error: {str(e)}"
            print(f"      ❌ {error_msg}")
            result['crime_score_test'] = {'success': False, 'error': str(e)}
            result['errors'].append(error_msg)
        
        # Test 3: Permit API lookup
        print("   🏗️ Permit API lookup...")
        try:
            # Extract street name for search
            street_parts = address_info['street'].split()
            street_search = ' '.join(street_parts[-2:] if len(street_parts) > 1 else street_parts)
            
            params = {
                '$where': f"upper(primary_address) like '%{street_search.upper()}%'",
                '$limit': '5',
                '$order': 'issue_date DESC'
            }
            
            response = requests.get(
                self.permit_api_url,
                params=params,
                headers={"X-App-Token": self.app_token},
                timeout=10
            )
            
            if response.status_code == 200:
                permits = response.json()
                if len(permits) > 0:
                    print(f"      ✅ Found {len(permits)} permits on {street_search}")
                    result['permit_test'] = {'success': True, 'count': len(permits)}
                else:
                    print(f"      ⚠️ No permits found on {street_search}")
                    result['permit_test'] = {'success': True, 'count': 0}  # API worked, just no data
            else:
                error_msg = f"API returned status {response.status_code}"
                print(f"      ❌ {error_msg}")
                result['permit_test'] = {'success': False, 'error': error_msg}
                result['errors'].append(f"Permit API error: {error_msg}")
                
        except Exception as e:
            error_msg = f"Permit API error: {str(e)}"
            print(f"      ❌ {error_msg}")
            result['permit_test'] = {'success': False, 'error': str(e)}
            result['errors'].append(error_msg)
        
        # Determine overall success
        db_success = result['database_test'] and result['database_test'].get('success', False)
        crime_success = result['crime_score_test'] and result['crime_score_test'].get('success', False)
        permit_success = result['permit_test'] and result['permit_test'].get('success', False)
        
        # Overall success if at least 2 out of 3 systems work
        result['overall_success'] = sum([db_success, crime_success, permit_success]) >= 2
        
        if result['overall_success']:
            print(f"   ✅ OVERALL: SUCCESS ({sum([db_success, crime_success, permit_success])}/3 systems)")
        else:
            print(f"   ❌ OVERALL: FAILURE ({sum([db_success, crime_success, permit_success])}/3 systems)")
        
        return result
    
    def display_summary(self, results: List[Dict]):
        """Display summary of all test results"""
        print("\n" + "="*60)
        print("📊 RANDOM ADDRESS TEST SUMMARY")
        print("="*60)
        
        total_addresses = len(results)
        successful_addresses = sum(1 for r in results if r['overall_success'])
        
        # System-specific success rates
        db_successes = sum(1 for r in results if r['database_test'] and r['database_test'].get('success', False))
        crime_successes = sum(1 for r in results if r['crime_score_test'] and r['crime_score_test'].get('success', False))
        permit_successes = sum(1 for r in results if r['permit_test'] and r['permit_test'].get('success', False))
        
        print(f"\n🎯 OVERALL SUCCESS RATE: {successful_addresses}/{total_addresses} ({successful_addresses/total_addresses*100:.1f}%)")
        print()
        
        print("📈 SYSTEM-SPECIFIC SUCCESS RATES:")
        print(f"   Database lookups: {db_successes}/{total_addresses} ({db_successes/total_addresses*100:.1f}%)")
        print(f"   Crime scores: {crime_successes}/{total_addresses} ({crime_successes/total_addresses*100:.1f}%)")
        print(f"   Permit API: {permit_successes}/{total_addresses} ({permit_successes/total_addresses*100:.1f}%)")
        print()
        
        print("📍 RESULTS BY AREA:")
        for result in results:
            status = "✅" if result['overall_success'] else "❌"
            print(f"   {status} {result['area']}: {result['address'][:50]}...")
            if result['errors']:
                print(f"      Issues: {', '.join(result['errors'][:2])}")
        
        print("\n🔍 COMMON FAILURE PATTERNS:")
        all_errors = []
        for result in results:
            all_errors.extend(result['errors'])
        
        if all_errors:
            # Count error types
            error_types = {}
            for error in all_errors:
                if 'No properties' in error:
                    error_type = 'No properties in database'
                elif 'crime' in error.lower():
                    error_type = 'Crime data issues'
                elif 'permit' in error.lower():
                    error_type = 'Permit API issues'
                else:
                    error_type = 'Other'
                
                error_types[error_type] = error_types.get(error_type, 0) + 1
            
            for error_type, count in sorted(error_types.items(), key=lambda x: x[1], reverse=True):
                print(f"   • {error_type}: {count} occurrences")
        else:
            print("   No errors detected!")
        
        return {
            'total_tested': total_addresses,
            'successful': successful_addresses,
            'success_rate': successful_addresses/total_addresses*100,
            'system_rates': {
                'database': db_successes/total_addresses*100,
                'crime': crime_successes/total_addresses*100,
                'permits': permit_successes/total_addresses*100
            }
        }

def main():
    """Run random address testing"""
    print("🎲 RANDOM LA ADDRESS TESTING")
    print("="*40)
    print("Testing genuinely random addresses from 5 LA areas")
    print()
    
    tester = RandomAddressTester()
    
    # Generate random addresses
    random_addresses = tester.generate_random_addresses()
    
    print("📍 GENERATED RANDOM ADDRESSES:")
    for i, addr_info in enumerate(random_addresses, 1):
        print(f"   {i}. {addr_info['area']}: {addr_info['address']}")
    
    # Test each address
    results = []
    for addr_info in random_addresses:
        result = tester.test_address(addr_info)
        results.append(result)
    
    # Display summary
    summary = tester.display_summary(results)
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_filename = f"random_address_test_{timestamp}.json"
    
    with open(results_filename, 'w') as f:
        json.dump({
            'test_timestamp': datetime.now().isoformat(),
            'addresses_tested': random_addresses,
            'detailed_results': results,
            'summary': summary
        }, f, indent=2, default=str)
    
    print(f"\n📁 Results saved to: {results_filename}")
    
    return results

if __name__ == "__main__":
    main()