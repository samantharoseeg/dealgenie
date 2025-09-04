#!/usr/bin/env python3
"""
DealGenie Performance Regression Test Suite

Ensures performance thresholds are maintained across code changes.
Fails if throughput drops >10% from established baselines.

Baseline Performance (measured on 12-core ARM, Python 3.13.7):
- Feature extraction: 15.9 parcels/second (median)
- Single property scoring: 0.1ms (median)  
- End-to-end pipeline: 31.4 operations/second

Regression Thresholds (10% tolerance):
- Feature extraction must be > 14.3 parcels/second
- Single property scoring must be < 0.11ms (p95)
- Pipeline throughput must be > 28.3 operations/second
"""

import sys
import time
import statistics
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from features.csv_feature_matrix import CSVFeatureMatrix
from scoring.engine import calculate_score

class PerformanceRegressionTest:
    """Performance regression testing with hard failure thresholds."""
    
    # Baseline thresholds (with 10% tolerance)
    MIN_FEATURE_EXTRACTION_THROUGHPUT = 14.3  # parcels/second
    MAX_SINGLE_SCORING_TIME = 0.00011  # seconds (0.11ms)
    MIN_PIPELINE_THROUGHPUT = 28.3  # operations/second
    
    def __init__(self):
        self.test_apns = [
            "4306026007",  # Beverly Hills area
            "5483019004",  # Hollywood area 
            "2353027012",  # Valley area
            "4255016008",  # West LA
            "2224032011",  # San Fernando
        ]
        
    def test_feature_extraction_performance(self) -> bool:
        """Test feature extraction meets minimum throughput threshold."""
        print("🧪 Testing Feature Extraction Performance...")
        
        feature_matrix = CSVFeatureMatrix()
        times = []
        
        # Run 3 iterations for reliability
        for iteration in range(3):
            start_time = time.perf_counter()
            
            successful_extractions = 0
            for apn in self.test_apns:
                features = feature_matrix.get_feature_matrix(apn)
                if features and len(features) >= 40:  # Expect 44+ features
                    successful_extractions += 1
            
            end_time = time.perf_counter()
            iteration_time = end_time - start_time
            times.append(iteration_time)
        
        median_time = statistics.median(times)
        throughput = len(self.test_apns) / median_time
        
        passed = throughput >= self.MIN_FEATURE_EXTRACTION_THROUGHPUT
        status = "✅ PASS" if passed else "❌ FAIL"
        
        print(f"   Measured throughput: {throughput:.1f} parcels/second")
        print(f"   Required minimum: {self.MIN_FEATURE_EXTRACTION_THROUGHPUT} parcels/second")
        print(f"   Result: {status}")
        
        if not passed:
            print(f"   ⚠️  Performance regression detected: {throughput:.1f} < {self.MIN_FEATURE_EXTRACTION_THROUGHPUT}")
            
        return passed
    
    def test_single_scoring_performance(self) -> bool:
        """Test single property scoring meets maximum latency threshold."""
        print("\n🎯 Testing Single Property Scoring Performance...")
        
        feature_matrix = CSVFeatureMatrix()
        features = feature_matrix.get_feature_matrix(self.test_apns[0])
        
        if not features:
            print(f"   ❌ FAIL: Could not extract features for test APN {self.test_apns[0]}")
            return False
        
        times = []
        
        # Run 10 iterations for precise timing
        for _ in range(10):
            start_time = time.perf_counter()
            result = calculate_score(features, "multifamily")
            end_time = time.perf_counter()
            
            if result and result.get("overall_score"):
                times.append(end_time - start_time)
        
        if not times:
            print("   ❌ FAIL: No successful scoring operations")
            return False
            
        # Use 95th percentile for consistency with benchmark
        p95_time = sorted(times)[int(0.95 * len(times))]
        
        passed = p95_time <= self.MAX_SINGLE_SCORING_TIME
        status = "✅ PASS" if passed else "❌ FAIL"
        
        print(f"   Measured latency (p95): {p95_time*1000:.2f}ms")
        print(f"   Required maximum: {self.MAX_SINGLE_SCORING_TIME*1000:.2f}ms")
        print(f"   Result: {status}")
        
        if not passed:
            print(f"   ⚠️  Performance regression detected: {p95_time*1000:.2f}ms > {self.MAX_SINGLE_SCORING_TIME*1000:.2f}ms")
            
        return passed
    
    def test_pipeline_performance(self) -> bool:
        """Test end-to-end pipeline meets minimum throughput threshold."""
        print("\n🏗️ Testing Pipeline Performance...")
        
        feature_matrix = CSVFeatureMatrix()
        times = []
        
        # Run 3 iterations for reliability
        for iteration in range(3):
            start_time = time.perf_counter()
            
            successful_operations = 0
            for apn in self.test_apns:
                features = feature_matrix.get_feature_matrix(apn)
                if features:
                    # Test 2 templates per APN (like comprehensive benchmark)
                    for template in ["multifamily", "commercial"]:
                        result = calculate_score(features, template)
                        if result and result.get("overall_score"):
                            successful_operations += 1
            
            end_time = time.perf_counter()
            times.append(end_time - start_time)
        
        median_time = statistics.median(times)
        expected_operations = len(self.test_apns) * 2  # 2 templates per APN
        throughput = expected_operations / median_time
        
        passed = throughput >= self.MIN_PIPELINE_THROUGHPUT
        status = "✅ PASS" if passed else "❌ FAIL"
        
        print(f"   Measured throughput: {throughput:.1f} operations/second")
        print(f"   Required minimum: {self.MIN_PIPELINE_THROUGHPUT} operations/second")
        print(f"   Result: {status}")
        
        if not passed:
            print(f"   ⚠️  Performance regression detected: {throughput:.1f} < {self.MIN_PIPELINE_THROUGHPUT}")
            
        return passed
    
    def run_all_tests(self) -> bool:
        """Run all performance regression tests."""
        print("🚀 DealGenie Performance Regression Test Suite")
        print("=" * 55)
        print("Testing against established baselines with 10% tolerance...")
        print()
        
        results = []
        
        # Run all tests
        results.append(self.test_feature_extraction_performance())
        results.append(self.test_single_scoring_performance())
        results.append(self.test_pipeline_performance())
        
        # Summary
        passed_count = sum(results)
        total_count = len(results)
        
        print("\n" + "=" * 55)
        print("📋 Performance Regression Test Results")
        print("=" * 55)
        
        if all(results):
            print("✅ ALL TESTS PASSED - No performance regressions detected")
            print(f"   {passed_count}/{total_count} performance thresholds met")
            return True
        else:
            print("❌ PERFORMANCE REGRESSION DETECTED")  
            print(f"   {passed_count}/{total_count} performance thresholds met")
            print("\n🚨 Action Required:")
            print("   - Review recent code changes for performance impact")
            print("   - Consider optimizing slow operations")
            print("   - Update thresholds if intentional architectural changes")
            return False

def main():
    """Run performance regression tests and exit with appropriate code."""
    test_suite = PerformanceRegressionTest()
    success = test_suite.run_all_tests()
    
    # Exit with error code if tests failed (for CI/CD integration)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()