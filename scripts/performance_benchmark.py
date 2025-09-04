#!/usr/bin/env python3
"""
Performance benchmark script to test DealGenie scoring throughput on real LA County data.
Measures parcels/second performance and identifies bottlenecks.
"""

import time
import subprocess
import statistics
import json
import pandas as pd
import sys
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import random

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

class DealGeniePerformanceBenchmark:
    def __init__(self, csv_path: str = "scraper/la_parcels_complete_merged.csv"):
        self.csv_path = csv_path
        self.results = {}
        self.apns = []
        
    def load_test_apns(self, sample_size: int = 1000):
        """Load APNs for testing."""
        print(f"Loading {sample_size} test APNs from {self.csv_path}...")
        
        # Read sample of data
        df = pd.read_csv(self.csv_path, nrows=sample_size * 2)  # Get extra in case of duplicates
        
        # Get unique APNs
        self.apns = df['apn'].drop_duplicates().tolist()[:sample_size]
        
        print(f"Loaded {len(self.apns)} unique APNs for testing")
        return len(self.apns)
    
    def score_single_apn(self, apn: str, template: str = "multifamily") -> dict:
        """Score a single APN and measure performance."""
        start_time = time.time()
        
        try:
            # Run the scoring command
            cmd = ["python", "cli/dg_score.py", "score", "--template", template, "--apn", str(apn)]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            end_time = time.time()
            duration = end_time - start_time
            
            success = result.returncode == 0
            
            return {
                "apn": apn,
                "template": template,
                "duration": duration,
                "success": success,
                "stdout": result.stdout if success else "",
                "stderr": result.stderr if not success else "",
                "returncode": result.returncode
            }
            
        except subprocess.TimeoutExpired:
            return {
                "apn": apn,
                "template": template,
                "duration": 30.0,  # Timeout duration
                "success": False,
                "stdout": "",
                "stderr": "Timeout after 30 seconds",
                "returncode": -1
            }
        except Exception as e:
            return {
                "apn": apn,
                "template": template,
                "duration": time.time() - start_time,
                "success": False,
                "stdout": "",
                "stderr": str(e),
                "returncode": -2
            }
    
    def benchmark_sequential(self, test_apns: list, template: str = "multifamily") -> dict:
        """Benchmark sequential processing."""
        print(f"\n🔄 Running sequential benchmark with {len(test_apns)} APNs...")
        
        start_time = time.time()
        results = []
        
        for i, apn in enumerate(test_apns, 1):
            print(f"  Processing APN {i}/{len(test_apns)}: {apn}")
            result = self.score_single_apn(apn, template)
            results.append(result)
            
            if i % 10 == 0:  # Progress update every 10 APNs
                elapsed = time.time() - start_time
                rate = i / elapsed
                print(f"    Progress: {i}/{len(test_apns)} APNs, Rate: {rate:.2f} parcels/sec")
        
        total_time = time.time() - start_time
        successful_results = [r for r in results if r['success']]
        
        return {
            "method": "sequential",
            "total_apns": len(test_apns),
            "successful_apns": len(successful_results),
            "failed_apns": len(test_apns) - len(successful_results),
            "total_time": total_time,
            "avg_time_per_apn": total_time / len(test_apns),
            "parcels_per_second": len(test_apns) / total_time,
            "successful_parcels_per_second": len(successful_results) / total_time,
            "success_rate": len(successful_results) / len(test_apns),
            "individual_times": [r['duration'] for r in successful_results],
            "template": template,
            "detailed_results": results
        }
    
    def benchmark_parallel(self, test_apns: list, template: str = "multifamily", max_workers: int = 4) -> dict:
        """Benchmark parallel processing."""
        print(f"\n⚡ Running parallel benchmark with {len(test_apns)} APNs (workers: {max_workers})...")
        
        start_time = time.time()
        results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_apn = {executor.submit(self.score_single_apn, apn, template): apn 
                           for apn in test_apns}
            
            # Collect results as they complete
            for i, future in enumerate(as_completed(future_to_apn), 1):
                result = future.result()
                results.append(result)
                
                if i % 10 == 0:  # Progress update every 10 completions
                    elapsed = time.time() - start_time
                    rate = i / elapsed
                    print(f"    Progress: {i}/{len(test_apns)} APNs, Rate: {rate:.2f} parcels/sec")
        
        total_time = time.time() - start_time
        successful_results = [r for r in results if r['success']]
        
        return {
            "method": "parallel",
            "max_workers": max_workers,
            "total_apns": len(test_apns),
            "successful_apns": len(successful_results),
            "failed_apns": len(test_apns) - len(successful_results),
            "total_time": total_time,
            "avg_time_per_apn": total_time / len(test_apns),
            "parcels_per_second": len(test_apns) / total_time,
            "successful_parcels_per_second": len(successful_results) / total_time,
            "success_rate": len(successful_results) / len(test_apns),
            "individual_times": [r['duration'] for r in successful_results],
            "template": template,
            "detailed_results": results
        }
    
    def analyze_performance(self, benchmark_result: dict) -> dict:
        """Analyze performance characteristics."""
        times = benchmark_result['individual_times']
        
        if not times:
            return {"error": "No successful timings to analyze"}
        
        analysis = {
            "timing_stats": {
                "mean": statistics.mean(times),
                "median": statistics.median(times),
                "min": min(times),
                "max": max(times),
                "std_dev": statistics.stdev(times) if len(times) > 1 else 0,
            },
            "performance_metrics": {
                "target_rate": 300,  # parcels/sec target
                "actual_rate": benchmark_result['successful_parcels_per_second'],
                "target_met": benchmark_result['successful_parcels_per_second'] >= 300,
                "efficiency": benchmark_result['success_rate'],
            },
            "bottleneck_analysis": {
                "slow_parcels_count": len([t for t in times if t > 5.0]),  # > 5 seconds
                "very_slow_parcels_count": len([t for t in times if t > 10.0]),  # > 10 seconds
                "timeout_count": len([r for r in benchmark_result['detailed_results'] 
                                    if not r['success'] and 'Timeout' in r['stderr']]),
            }
        }
        
        # Performance assessment
        rate = benchmark_result['successful_parcels_per_second']
        if rate >= 300:
            analysis['assessment'] = "✅ EXCELLENT - Meets production target (300+ parcels/sec)"
        elif rate >= 200:
            analysis['assessment'] = "🟡 GOOD - Close to target, minor optimization needed"
        elif rate >= 100:
            analysis['assessment'] = "🟠 MODERATE - Significant optimization required"
        else:
            analysis['assessment'] = "🔴 POOR - Major performance issues, requires investigation"
        
        return analysis
    
    def run_comprehensive_benchmark(self, sample_sizes: list = [50, 100, 250, 500, 1000]):
        """Run comprehensive benchmark with different sample sizes."""
        print("="*80)
        print("🚀 DEALGENIE COMPREHENSIVE PERFORMANCE BENCHMARK")
        print("="*80)
        
        # Load APNs
        max_size = max(sample_sizes)
        loaded_count = self.load_test_apns(max_size)
        
        if loaded_count < max_size:
            print(f"⚠️  Warning: Only {loaded_count} APNs available, adjusting sample sizes")
            sample_sizes = [s for s in sample_sizes if s <= loaded_count]
        
        all_results = {}
        
        for size in sample_sizes:
            print(f"\n{'='*60}")
            print(f"📊 TESTING WITH {size} PARCELS")
            print(f"{'='*60}")
            
            test_apns = random.sample(self.apns, size)
            
            # Sequential benchmark
            seq_result = self.benchmark_sequential(test_apns, template="multifamily")
            seq_analysis = self.analyze_performance(seq_result)
            
            # Parallel benchmark (if more than 10 APNs)
            par_result = None
            par_analysis = None
            if size >= 10:
                par_result = self.benchmark_parallel(test_apns, template="multifamily", max_workers=4)
                par_analysis = self.analyze_performance(par_result)
            
            all_results[f"sample_{size}"] = {
                "sequential": {
                    "results": seq_result,
                    "analysis": seq_analysis
                },
                "parallel": {
                    "results": par_result,
                    "analysis": par_analysis
                } if par_result else None
            }
            
            # Print results
            self.print_benchmark_summary(size, seq_result, seq_analysis, par_result, par_analysis)
        
        # Save results
        timestamp = int(time.time())
        results_file = f"performance_benchmark_results_{timestamp}.json"
        
        with open(results_file, 'w') as f:
            json.dump(all_results, f, indent=2, default=str)
        
        print(f"\n💾 Results saved to: {results_file}")
        
        # Final summary
        self.print_final_summary(all_results)
        
        return all_results
    
    def print_benchmark_summary(self, size: int, seq_result: dict, seq_analysis: dict, 
                              par_result: dict = None, par_analysis: dict = None):
        """Print summary of benchmark results."""
        print(f"\n📈 RESULTS FOR {size} PARCELS:")
        print(f"   Sequential: {seq_result['successful_parcels_per_second']:.1f} parcels/sec "
              f"({seq_result['success_rate']*100:.1f}% success)")
        
        if par_result:
            print(f"   Parallel:   {par_result['successful_parcels_per_second']:.1f} parcels/sec "
                  f"({par_result['success_rate']*100:.1f}% success)")
        
        print(f"   Assessment: {seq_analysis['assessment']}")
    
    def print_final_summary(self, all_results: dict):
        """Print final performance summary."""
        print(f"\n{'='*80}")
        print("🎯 FINAL PERFORMANCE SUMMARY")
        print("="*80)
        
        # Find best performance
        best_rate = 0
        best_config = ""
        
        for size, results in all_results.items():
            seq_rate = results['sequential']['results']['successful_parcels_per_second']
            if seq_rate > best_rate:
                best_rate = seq_rate
                best_config = f"{size.replace('sample_', '')} parcels (sequential)"
            
            if results['parallel'] and results['parallel']['results']:
                par_rate = results['parallel']['results']['successful_parcels_per_second']
                if par_rate > best_rate:
                    best_rate = par_rate
                    best_config = f"{size.replace('sample_', '')} parcels (parallel)"
        
        print(f"🏆 Best Performance: {best_rate:.1f} parcels/sec with {best_config}")
        print(f"🎯 Target: 300 parcels/sec")
        print(f"📊 Status: {'✅ TARGET MET' if best_rate >= 300 else '🔴 OPTIMIZATION NEEDED'}")
        
        print(f"\n💡 Recommendations:")
        if best_rate >= 300:
            print("   • Current performance meets production requirements")
            print("   • Consider load testing with larger datasets")
        elif best_rate >= 200:
            print("   • Performance is close to target")
            print("   • Consider parallel processing optimization")
            print("   • Review slow parcels for common patterns")
        else:
            print("   • Major performance optimization required")
            print("   • Investigate scoring algorithm bottlenecks")
            print("   • Consider caching frequently accessed data")
            print("   • Profile memory usage and I/O operations")

if __name__ == "__main__":
    # Check if CSV file exists
    csv_path = "scraper/la_parcels_complete_merged.csv"
    if not os.path.exists(csv_path):
        print(f"❌ Error: CSV file not found at {csv_path}")
        print("Please ensure the LA County parcel data is available")
        sys.exit(1)
    
    # Initialize benchmark
    benchmark = DealGeniePerformanceBenchmark(csv_path)
    
    # Run comprehensive benchmark
    sample_sizes = [25, 50, 100, 250, 500]  # Reasonable sizes for testing
    results = benchmark.run_comprehensive_benchmark(sample_sizes)
    
    print(f"\n🎉 Benchmark completed!")
    print(f"📁 Check the JSON results file for detailed analysis")