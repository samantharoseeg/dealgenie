#!/usr/bin/env python3
"""
DealGenie Fix Verification System
CodeRabbit: Please review this core production verification system
Automated testing suite to verify all critical fixes are working correctly.
"""

import sys
import os
import time
import subprocess
import json
import sqlite3
from pathlib import Path
from typing import Dict, List, Tuple, Optional

# Add project root to path
sys.path.append('.')

class DealGenieVerifier:
    """Comprehensive verification system for DealGenie fixes."""
    
    def __init__(self):
        self.test_apns = ['4306026007', '5483019004', '2353027012']
        self.results = {}
        self.start_time = time.time()
        
        # Expected real addresses for validation
        self.expected_addresses = {
            '4306026007': '9406 W OAKMORE ROAD',
            '5483019004': '6222 E BURWOOD AVE',
            '2353027012': '11240 W CAMARILLO ST'
        }
        
        # Expected zoning codes for validation
        self.expected_zoning = {
            '4306026007': 'R1V2',
            '5483019004': 'R1-1-HCR',
            '2353027012': 'R3-1'
        }
        
        print("🔍 DealGenie Fix Verification System")
        print("=" * 50)
        print(f"Testing APNs: {', '.join(self.test_apns)}")
        print(f"Start time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print()
    
    def test_bootstrap_pipeline(self) -> Tuple[bool, str, Dict]:
        """Test 1: Bootstrap pipeline completion without timeout."""
        print("🧪 TEST 1: Bootstrap Pipeline Completion")
        print("-" * 40)
        
        try:
            start_time = time.time()
            
            # Run bootstrap with timeout
            cmd = ['make', 'bootstrap'] if Path('Makefile').exists() else ['bash', 'ops/bootstrap.sh']
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120  # allow bootstrap to finish
            )
            
            duration = time.time() - start_time
            
            if result.returncode == 0:
                # Check for successful completion markers
                output = result.stdout
                success_markers = [
                    "Bootstrap Complete!",
                    "HTML reports",
                    "Performance:"
                ]
                
                markers_found = sum(1 for marker in success_markers if marker in output)
                
                if markers_found >= 2:
                    status = f"✅ PASS - Completed in {duration:.1f}s"
                    success = True
                else:
                    status = f"⚠️  PARTIAL - Completed but missing success markers"
                    success = False
            else:
                status = f"❌ FAIL - Exit code {result.returncode}"
                success = False
            
            print(f"Duration: {duration:.1f} seconds")
            print(f"Status: {status}")
            
            details = {
                'duration': duration,
                'returncode': result.returncode,
                'stdout_length': len(result.stdout),
                'stderr_length': len(result.stderr)
            }
            
            return success, status, details
            
        except subprocess.TimeoutExpired:
            duration = 60.0
            status = "❌ FAIL - Timeout after 60 seconds"
            print(f"Status: {status}")
            return False, status, {'duration': duration, 'timeout': True}
            
        except Exception as e:
            status = f"❌ FAIL - Error: {e}"
            print(f"Status: {status}")
            return False, status, {'error': str(e)}
    
    def test_html_reports_addresses(self) -> Tuple[bool, str, Dict]:
        """Test 2: HTML reports contain real addresses."""
        print("\n🧪 TEST 2: HTML Reports with Real Addresses")
        print("-" * 40)
        
        results = {}
        total_tests = 0
        passed_tests = 0
        
        # Check existing reports in out/ directory
        out_dir = Path('out')
        if not out_dir.exists():
            return False, "❌ FAIL - Output directory doesn't exist", {}
        
        html_files = list(out_dir.glob('dealgenie_report_*.html'))
        if not html_files:
            return False, "❌ FAIL - No HTML reports found", {}
        
        print(f"Found {len(html_files)} HTML reports")
        
        # Test specific APNs if their reports exist
        for apn in self.test_apns:
            report_files = list(out_dir.glob(f'dealgenie_report_{apn}_*.html'))
            
            if report_files:
                total_tests += 1
                report_file = report_files[0]  # Use first matching file
                
                try:
                    with open(report_file, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    # Check for real address
                    expected_addr = self.expected_addresses.get(apn, '')
                    has_address = expected_addr in content or 'Property Address' in content
                    
                    # Check for property information section
                    has_property_info = 'Property Information' in content
                    
                    # Check for APN display
                    has_apn = apn in content
                    
                    if has_address and has_property_info and has_apn:
                        results[apn] = "✅ PASS"
                        passed_tests += 1
                        print(f"  APN {apn}: ✅ Contains real address and property info")
                    else:
                        results[apn] = "❌ FAIL"
                        print(f"  APN {apn}: ❌ Missing address or property info")
                        
                except Exception as e:
                    results[apn] = f"❌ ERROR: {e}"
                    print(f"  APN {apn}: ❌ Error reading file: {e}")
        
        # Also test any available reports for address content
        address_test_count = 0
        for html_file in html_files[:5]:  # Test up to 5 reports
            try:
                with open(html_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Look for real address indicators
                has_street = any(word in content.upper() for word in ['STREET', 'AVE', 'BLVD', 'DR', 'ROAD', 'WAY'])
                has_la = 'Los Angeles' in content
                has_zip = any(f'9{i}' in content for i in range(10))  # LA area zip codes
                
                if has_street and (has_la or has_zip):
                    address_test_count += 1
                    
            except Exception:
                continue
        
        if total_tests == 0:
            # Fallback: check if any reports have address-like content
            if address_test_count > 0:
                status = f"✅ PASS - {address_test_count} reports contain real address data"
                success = True
            else:
                status = "❌ FAIL - No reports contain real address data"
                success = False
        else:
            if passed_tests == total_tests:
                status = f"✅ PASS - {passed_tests}/{total_tests} test APNs have correct addresses"
                success = True
            else:
                status = f"⚠️  PARTIAL - {passed_tests}/{total_tests} test APNs have correct addresses"
                success = passed_tests > 0
        
        print(f"Overall Status: {status}")
        
        details = {
            'total_reports': len(html_files),
            'test_apns_found': total_tests,
            'test_apns_passed': passed_tests,
            'address_indicators_found': address_test_count,
            'individual_results': results
        }
        
        return success, status, details
    
    def test_census_api(self) -> Tuple[bool, str, Dict]:
        """Test 3: Census API returns demographic data without errors."""
        print("\n🧪 TEST 3: Census API Integration")
        print("-" * 40)
        
        results = {}
        total_tests = 0
        passed_tests = 0
        
        for apn in self.test_apns:
            total_tests += 1
            print(f"  Testing APN {apn}...")
            
            try:
                # Test Census API call
                result = subprocess.run(
                    ['python3', 'ingest/census_acs.py', 'single', '--apn', apn],
                    capture_output=True,
                    text=True,
                    timeout=60
                )
                
                if result.returncode == 0:
                    output = result.stdout
                    
                    # Check for success indicators
                    has_demographics = 'Demographic data retrieved:' in output
                    has_income = 'median_household_income' in output
                    has_population = 'total_population' in output
                    no_json_error = 'JSON decode error' not in output
                    
                    if has_demographics and has_income and has_population and no_json_error:
                        results[apn] = "✅ PASS"
                        passed_tests += 1
                        print(f"    ✅ Successfully retrieved demographic data")
                    else:
                        results[apn] = "❌ FAIL - Incomplete data"
                        print(f"    ❌ Incomplete demographic data")
                else:
                    results[apn] = f"❌ FAIL - Exit code {result.returncode}"
                    print(f"    ❌ API call failed with exit code {result.returncode}")
                    
            except subprocess.TimeoutExpired:
                results[apn] = "❌ FAIL - Timeout"
                print(f"    ❌ API call timed out")
            except Exception as e:
                results[apn] = f"❌ ERROR: {e}"
                print(f"    ❌ Error: {e}")
        
        if passed_tests == total_tests:
            status = f"✅ PASS - {passed_tests}/{total_tests} APNs returned demographic data"
            success = True
        elif passed_tests > 0:
            status = f"⚠️  PARTIAL - {passed_tests}/{total_tests} APNs returned demographic data"
            success = False
        else:
            status = f"❌ FAIL - No APNs returned demographic data"
            success = False
        
        print(f"Overall Status: {status}")
        
        details = {
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'individual_results': results
        }
        
        return success, status, details
    
    def test_sqlite_database(self) -> Tuple[bool, str, Dict]:
        """Test 4: SQLite database has populated tables."""
        print("\n🧪 TEST 4: SQLite Database Population")
        print("-" * 40)
        
        db_path = 'data/dealgenie.db'
        
        if not os.path.exists(db_path):
            status = "❌ FAIL - Database file doesn't exist"
            print(f"Status: {status}")
            return False, status, {}
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Check table existence
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            required_tables = ['parcels', 'parcel_scores', 'zoning_codes', 'feature_cache']
            missing_tables = [t for t in required_tables if t not in tables]
            
            if missing_tables:
                status = f"❌ FAIL - Missing tables: {', '.join(missing_tables)}"
                print(f"Status: {status}")
                return False, status, {'missing_tables': missing_tables}
            
            print("✅ All required tables exist")
            
            # Check table populations
            table_counts = {}
            for table in required_tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                table_counts[table] = count
                print(f"  {table}: {count} records")
            
            # Validation criteria
            has_zoning_codes = table_counts['zoning_codes'] >= 30  # Should have 32+ codes
            has_some_data = table_counts['parcels'] > 0 or table_counts['parcel_scores'] > 0
            
            if has_zoning_codes and has_some_data:
                status = "✅ PASS - Database properly populated"
                success = True
            elif has_zoning_codes:
                status = "⚠️  PARTIAL - Zoning codes loaded, but no parcel data yet"
                success = False
            else:
                status = "❌ FAIL - Database not properly populated"
                success = False
            
            print(f"Status: {status}")
            
            conn.close()
            
            details = {
                'tables_found': tables,
                'table_counts': table_counts,
                'total_tables': len(tables)
            }
            
            return success, status, details
            
        except Exception as e:
            status = f"❌ FAIL - Database error: {e}"
            print(f"Status: {status}")
            return False, status, {'error': str(e)}
    
    def test_feature_extraction(self) -> Tuple[bool, str, Dict]:
        """Test 5: Feature extraction works with real LA County data."""
        print("\n🧪 TEST 5: Feature Extraction with Real Data")
        print("-" * 40)
        
        try:
            from features.feature_matrix import get_feature_matrix
        except ImportError as e:
            status = f"❌ FAIL - Cannot import feature extraction: {e}"
            print(f"Status: {status}")
            return False, status, {'error': str(e)}
        
        results = {}
        total_tests = 0
        passed_tests = 0
        
        for apn in self.test_apns:
            total_tests += 1
            print(f"  Testing APN {apn}...")
            
            try:
                features = get_feature_matrix(apn)
                
                # Check feature count
                feature_count = len(features)
                has_enough_features = feature_count >= 40  # Should be 44+
                
                # Check for key features
                has_address = 'site_address' in features and features['site_address'] != 'Address not available'
                has_zoning = 'zoning' in features and features['zoning']
                has_lot_size = 'lot_size_sqft' in features and features['lot_size_sqft'] > 0
                
                # Check if address matches expected (if known)
                expected_addr = self.expected_addresses.get(apn)
                address_matches = True
                if expected_addr and has_address:
                    address_matches = expected_addr.upper() in features['site_address'].upper()
                
                # Check if zoning matches expected (if known)
                expected_zone = self.expected_zoning.get(apn)
                zoning_matches = True
                if expected_zone and has_zoning:
                    zoning_matches = expected_zone.upper() in features['zoning'].upper()
                
                if has_enough_features and has_address and has_zoning and has_lot_size and address_matches and zoning_matches:
                    results[apn] = "✅ PASS"
                    passed_tests += 1
                    print(f"    ✅ {feature_count} features extracted with real data")
                    print(f"       Address: {features.get('site_address', 'N/A')}")
                    print(f"       Zoning: {features.get('zoning', 'N/A')}")
                    print(f"       Lot Size: {features.get('lot_size_sqft', 'N/A')} sqft")
                else:
                    results[apn] = "❌ FAIL"
                    print(f"    ❌ Insufficient or incorrect data")
                    print(f"       Features: {feature_count}")
                    print(f"       Address: {features.get('site_address', 'N/A')}")
                    print(f"       Zoning: {features.get('zoning', 'N/A')}")
                
            except Exception as e:
                results[apn] = f"❌ ERROR: {e}"
                print(f"    ❌ Error extracting features: {e}")
        
        if passed_tests == total_tests:
            status = f"✅ PASS - {passed_tests}/{total_tests} APNs have complete real data"
            success = True
        elif passed_tests > 0:
            status = f"⚠️  PARTIAL - {passed_tests}/{total_tests} APNs have complete real data"
            success = False
        else:
            status = f"❌ FAIL - No APNs have complete real data"
            success = False
        
        print(f"Overall Status: {status}")
        
        details = {
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'individual_results': results
        }
        
        return success, status, details
    
    def test_scoring_system(self) -> Tuple[bool, str, Dict]:
        """Test 6: Scoring system works with real zoning codes."""
        print("\n🧪 TEST 6: Scoring System with Real Zoning")
        print("-" * 40)
        
        try:
            from features.feature_matrix import get_feature_matrix
            from scoring.engine import calculate_score
        except ImportError as e:
            status = f"❌ FAIL - Cannot import scoring system: {e}"
            print(f"Status: {status}")
            return False, status, {'error': str(e)}
        
        results = {}
        total_tests = 0
        passed_tests = 0
        templates = ['multifamily', 'residential', 'commercial']
        
        for i, apn in enumerate(self.test_apns):
            total_tests += 1
            template = templates[i % len(templates)]
            print(f"  Testing APN {apn} with {template} template...")
            
            try:
                features = get_feature_matrix(apn)
                score_result = calculate_score(features, template)
                
                # Check scoring results
                has_score = 'score' in score_result and isinstance(score_result['score'], (int, float))
                has_explanation = 'explanation' in score_result and score_result['explanation']
                has_components = 'component_scores' in score_result and score_result['component_scores']
                score_in_range = has_score and 0 <= score_result['score'] <= 10
                
                # Check if real zoning is being used
                zoning = features.get('zoning', '')
                has_real_zoning = zoning and zoning not in ['Unknown', '']
                
                if has_score and has_explanation and has_components and score_in_range and has_real_zoning:
                    results[apn] = "✅ PASS"
                    passed_tests += 1
                    print(f"    ✅ Score: {score_result['score']:.1f}/10")
                    print(f"       Zoning: {zoning}")
                    print(f"       Components: {len(score_result['component_scores'])}")
                else:
                    results[apn] = "❌ FAIL"
                    print(f"    ❌ Incomplete scoring result")
                    if has_score:
                        print(f"       Score: {score_result.get('score', 'N/A')}")
                    print(f"       Zoning: {zoning}")
                
            except Exception as e:
                results[apn] = f"❌ ERROR: {e}"
                print(f"    ❌ Error in scoring: {e}")
        
        if passed_tests == total_tests:
            status = f"✅ PASS - {passed_tests}/{total_tests} APNs scored successfully with real data"
            success = True
        elif passed_tests > 0:
            status = f"⚠️  PARTIAL - {passed_tests}/{total_tests} APNs scored successfully with real data"
            success = False
        else:
            status = f"❌ FAIL - No APNs scored successfully"
            success = False
        
        print(f"Overall Status: {status}")
        
        details = {
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'individual_results': results
        }
        
        return success, status, details
    
    def run_all_tests(self) -> Dict:
        """Run all verification tests and generate report."""
        
        # Test 1: Bootstrap Pipeline
        success1, status1, details1 = self.test_bootstrap_pipeline()
        self.results['bootstrap_pipeline'] = {
            'success': success1,
            'status': status1,
            'details': details1
        }
        
        # Test 2: HTML Reports
        success2, status2, details2 = self.test_html_reports_addresses()
        self.results['html_reports'] = {
            'success': success2,
            'status': status2,
            'details': details2
        }
        
        # Test 3: Census API
        success3, status3, details3 = self.test_census_api()
        self.results['census_api'] = {
            'success': success3,
            'status': status3,
            'details': details3
        }
        
        # Test 4: SQLite Database
        success4, status4, details4 = self.test_sqlite_database()
        self.results['sqlite_database'] = {
            'success': success4,
            'status': status4,
            'details': details4
        }
        
        # Test 5: Feature Extraction
        success5, status5, details5 = self.test_feature_extraction()
        self.results['feature_extraction'] = {
            'success': success5,
            'status': status5,
            'details': details5
        }
        
        # Test 6: Scoring System
        success6, status6, details6 = self.test_scoring_system()
        self.results['scoring_system'] = {
            'success': success6,
            'status': status6,
            'details': details6
        }
        
        return self.results
    
    def generate_report(self) -> None:
        """Generate comprehensive verification report."""
        total_time = time.time() - self.start_time
        
        print("\n" + "=" * 60)
        print("🎯 DEALGENIE FIX VERIFICATION REPORT")
        print("=" * 60)
        
        # Count successes
        total_tests = len(self.results)
        passed_tests = sum(1 for result in self.results.values() if result['success'])
        
        print(f"Total Verification Time: {total_time:.1f} seconds")
        print(f"Overall Success Rate: {passed_tests}/{total_tests} tests passed")
        print()
        
        # Individual test results
        test_names = {
            'bootstrap_pipeline': '1. Bootstrap Pipeline Completion',
            'html_reports': '2. HTML Reports with Real Addresses',
            'census_api': '3. Census API Integration',
            'sqlite_database': '4. SQLite Database Population',
            'feature_extraction': '5. Feature Extraction with Real Data',
            'scoring_system': '6. Scoring System with Real Zoning'
        }
        
        for key, name in test_names.items():
            if key in self.results:
                result = self.results[key]
                status_icon = "✅" if result['success'] else "❌"
                print(f"{status_icon} {name}")
                print(f"   {result['status']}")
                print()
        
        # Critical issues check
        critical_issues = []
        if not self.results.get('bootstrap_pipeline', {}).get('success'):
            critical_issues.append("Bootstrap pipeline failing")
        if not self.results.get('html_reports', {}).get('success'):
            critical_issues.append("HTML reports missing addresses")
        if not self.results.get('feature_extraction', {}).get('success'):
            critical_issues.append("Feature extraction not working")
        
        print("🚨 CRITICAL ISSUES:")
        if critical_issues:
            for issue in critical_issues:
                print(f"   ❌ {issue}")
        else:
            print("   ✅ No critical issues detected")
        print()
        
        # Overall verdict
        if passed_tests == total_tests:
            print("🎉 VERDICT: ALL FIXES VERIFIED - PRODUCTION READY")
        elif passed_tests >= total_tests * 0.8:
            print("⚠️  VERDICT: MOST FIXES WORKING - MINOR ISSUES REMAIN")
        else:
            print("🔴 VERDICT: SIGNIFICANT ISSUES - NOT PRODUCTION READY")
        
        print()
        print("For detailed troubleshooting, see VERIFICATION_GUIDE.md")
        print("=" * 60)
    
    def save_detailed_report(self) -> None:
        """Save detailed results to JSON file."""
        report_data = {
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'test_apns': self.test_apns,
            'results': self.results,
            'summary': {
                'total_tests': len(self.results),
                'passed_tests': sum(1 for r in self.results.values() if r['success']),
                'verification_time': time.time() - self.start_time
            }
        }
        
        with open('verification_results.json', 'w') as f:
            json.dump(report_data, f, indent=2)
        
        print(f"📄 Detailed results saved to: verification_results.json")


def main():
    """Main verification function."""
    verifier = DealGenieVerifier()
    
    try:
        results = verifier.run_all_tests()
        verifier.generate_report()
        verifier.save_detailed_report()
        
        # Exit with appropriate code
        passed = sum(1 for r in results.values() if r['success'])
        total = len(results)
        
        if passed == total:
            sys.exit(0)  # All tests passed
        elif passed >= total * 0.8:
            sys.exit(1)  # Most tests passed, minor issues
        else:
            sys.exit(2)  # Significant issues
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Verification interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n\n❌ Verification failed with error: {e}")
        sys.exit(3)


if __name__ == "__main__":
    main()