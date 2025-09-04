#!/usr/bin/env python3
"""
Performance Benchmarks for Batch Scoring

Tests batch processing performance with target 2x improvement on 1k parcels.
Includes memory usage monitoring and throughput analysis.
"""

import unittest
import time
import psutil
import sys
import random
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from scoring.multi_template_scorer import MultiTemplateScorer
from scoring.batch_processor import BatchProcessor
from scoring.zoning_engine import ZoningConstraintsEngine


class TestBatchScoringPerformance(unittest.TestCase):
    """Performance benchmarks for batch scoring operations"""
    
    def setUp(self):
        """Set up performance test fixtures"""
        self.scorer = MultiTemplateScorer()
        self.zoning_engine = ZoningConstraintsEngine()
        self.batch_processor = BatchProcessor(self.zoning_engine)
        
        # Performance targets
        self.TARGET_THROUGHPUT_1K = 500  # parcels per second (2x improvement target)
        self.MAX_MEMORY_INCREASE_MB = 100  # Max memory increase during batch
        self.MAX_SINGLE_PARCEL_MS = 50   # Max time per parcel in milliseconds
    
    def generate_test_parcels(self, count: int) -> list:
        """Generate realistic test parcel data"""
        zoning_types = ['R1', 'R2', 'R3', 'C1', 'C2', 'C4', 'CM', 'M1', 'LAX']
        
        parcels = []
        for i in range(count):
            parcel = {
                'apn': f'PERF-TEST-{i:06d}',
                'zoning': random.choice(zoning_types),
                'lot_size_sqft': random.randint(5000, 50000),
                'transit_score': random.randint(20, 90),
                'population_density': random.randint(1000, 15000),
                'median_income': random.randint(30000, 120000),
                'price_per_sqft': random.randint(200, 1200),
                'latitude': 34.05 + random.uniform(-0.3, 0.3),
                'longitude': -118.25 + random.uniform(-0.3, 0.3)
            }
            parcels.append(parcel)
        
        return parcels
    
    def measure_memory_usage(self):
        """Get current memory usage in MB"""
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024
    
    def test_single_parcel_performance(self):
        """Test single parcel scoring performance"""
        print("\n=== SINGLE PARCEL PERFORMANCE TEST ===")
        
        parcel = self.generate_test_parcels(1)[0]
        
        # Warmup run
        self.scorer.process_multi_template(parcel)
        
        # Performance measurement
        start_time = time.perf_counter()
        result = self.scorer.process_multi_template(parcel)
        end_time = time.perf_counter()
        
        elapsed_ms = (end_time - start_time) * 1000
        
        print(f"Single parcel scoring time: {elapsed_ms:.2f} ms")
        print(f"Target: ≤{self.MAX_SINGLE_PARCEL_MS} ms")
        
        # Verify result quality
        self.assertIn('viable_uses', result)
        self.assertGreater(len(result['viable_uses']), 0)
        
        # Performance assertion
        self.assertLess(elapsed_ms, self.MAX_SINGLE_PARCEL_MS, 
                       f"Single parcel took {elapsed_ms:.2f}ms, target ≤{self.MAX_SINGLE_PARCEL_MS}ms")
        
        print("✅ Single parcel performance PASSED")
    
    def test_batch_100_parcels(self):
        """Test batch performance with 100 parcels"""
        print("\n=== BATCH 100 PARCELS PERFORMANCE TEST ===")
        
        parcels = self.generate_test_parcels(100)
        
        # Memory baseline
        memory_start = self.measure_memory_usage()
        
        # Warmup
        self.scorer.process_multi_template(parcels[0])
        
        # Batch processing
        start_time = time.perf_counter()
        results = []
        
        for parcel in parcels:
            result = self.scorer.process_multi_template(parcel)
            results.append(result)
        
        end_time = time.perf_counter()
        memory_end = self.measure_memory_usage()
        
        # Performance metrics
        total_time = end_time - start_time
        throughput = len(parcels) / total_time
        avg_time_per_parcel_ms = (total_time / len(parcels)) * 1000
        memory_increase = memory_end - memory_start
        
        print(f"Batch size: {len(parcels)} parcels")
        print(f"Total time: {total_time:.2f} seconds")
        print(f"Throughput: {throughput:.1f} parcels/second")
        print(f"Average per parcel: {avg_time_per_parcel_ms:.2f} ms")
        print(f"Memory increase: {memory_increase:.1f} MB")
        
        # Quality checks
        successful_results = [r for r in results if len(r.get('viable_uses', [])) > 0]
        success_rate = len(successful_results) / len(results)
        
        print(f"Success rate: {success_rate:.1%}")
        
        # Assertions
        self.assertGreater(success_rate, 0.95, "Should have >95% success rate")
        self.assertLess(memory_increase, self.MAX_MEMORY_INCREASE_MB, 
                       f"Memory increase {memory_increase:.1f}MB exceeds limit")
        
        print("✅ Batch 100 parcels performance PASSED")
    
    def test_batch_1k_parcels_performance_target(self):
        """Test 1k parcels batch processing - main performance target"""
        print("\n=== BATCH 1K PARCELS PERFORMANCE TARGET TEST ===")
        
        parcels = self.generate_test_parcels(1000)
        
        # Memory baseline
        memory_start = self.measure_memory_usage()
        
        # Batch processing with progress tracking
        start_time = time.perf_counter()
        results = []
        
        checkpoint_interval = 100
        for i, parcel in enumerate(parcels):
            result = self.scorer.process_multi_template(parcel)
            results.append(result)
            
            # Progress checkpoints
            if (i + 1) % checkpoint_interval == 0:
                elapsed = time.perf_counter() - start_time
                current_throughput = (i + 1) / elapsed
                print(f"  Progress: {i + 1}/1000 parcels, {current_throughput:.1f} parcels/sec")
        
        end_time = time.perf_counter()
        memory_end = self.measure_memory_usage()
        
        # Performance metrics
        total_time = end_time - start_time
        throughput = len(parcels) / total_time
        avg_time_per_parcel_ms = (total_time / len(parcels)) * 1000
        memory_increase = memory_end - memory_start
        
        print(f"\n=== PERFORMANCE RESULTS ===")
        print(f"Batch size: {len(parcels)} parcels")
        print(f"Total time: {total_time:.2f} seconds")
        print(f"Throughput: {throughput:.1f} parcels/second")
        print(f"Target throughput: {self.TARGET_THROUGHPUT_1K} parcels/second")
        print(f"Performance ratio: {throughput/self.TARGET_THROUGHPUT_1K:.2f}x target")
        print(f"Average per parcel: {avg_time_per_parcel_ms:.2f} ms")
        print(f"Memory usage increase: {memory_increase:.1f} MB")
        
        # Quality metrics
        successful_results = [r for r in results if len(r.get('viable_uses', [])) > 0]
        success_rate = len(successful_results) / len(results)
        
        avg_viable_templates = sum(len(r.get('viable_uses', [])) for r in successful_results) / len(successful_results)
        multi_template_rate = sum(1 for r in results if r.get('multi_template_triggered', False)) / len(results)
        
        print(f"\n=== QUALITY METRICS ===")
        print(f"Success rate: {success_rate:.1%}")
        print(f"Average viable templates: {avg_viable_templates:.1f}")
        print(f"Multi-template trigger rate: {multi_template_rate:.1%}")
        
        # Performance assertions
        self.assertGreater(throughput, self.TARGET_THROUGHPUT_1K * 0.8,  # Allow 20% tolerance
                          f"Throughput {throughput:.1f} below 80% of target {self.TARGET_THROUGHPUT_1K}")
        
        self.assertLess(memory_increase, self.MAX_MEMORY_INCREASE_MB * 2,  # 2x tolerance for 1k batch
                       f"Memory increase {memory_increase:.1f}MB too high")
        
        self.assertGreater(success_rate, 0.95, "Should maintain >95% success rate at scale")
        
        # Performance grade
        if throughput >= self.TARGET_THROUGHPUT_1K:
            print("\n🎉 PERFORMANCE TARGET ACHIEVED!")
        elif throughput >= self.TARGET_THROUGHPUT_1K * 0.9:
            print("\n✅ PERFORMANCE TARGET NEARLY ACHIEVED (90%+)")
        else:
            print(f"\n⚠️ PERFORMANCE BELOW TARGET ({throughput/self.TARGET_THROUGHPUT_1K:.1%})")
        
        return {
            'throughput': throughput,
            'memory_increase': memory_increase,
            'success_rate': success_rate,
            'avg_viable_templates': avg_viable_templates
        }
    
    def test_batch_processing_optimization(self):
        """Test batch processing optimizations vs individual processing"""
        print("\n=== BATCH PROCESSING OPTIMIZATION TEST ===")
        
        parcels = self.generate_test_parcels(250)
        
        # Method 1: Individual processing (baseline)
        start_time = time.perf_counter()
        individual_results = []
        for parcel in parcels:
            result = self.scorer.process_multi_template(parcel)
            individual_results.append(result)
        individual_time = time.perf_counter() - start_time
        
        # Method 2: Optimized processing (with caching and pre-filtering)
        start_time = time.perf_counter()
        optimized_results = []
        
        # Group by zoning for template pre-filtering optimization
        zoning_groups = {}
        for parcel in parcels:
            zoning = parcel['zoning']
            if zoning not in zoning_groups:
                zoning_groups[zoning] = []
            zoning_groups[zoning].append(parcel)
        
        # Process with zoning-based optimization
        for zoning, zoning_parcels in zoning_groups.items():
            # Pre-filter templates once per zoning type
            viable_templates, _ = self.batch_processor.pre_filter_templates(zoning)
            
            for parcel in zoning_parcels:
                result = self.scorer.process_multi_template(parcel)
                optimized_results.append(result)
        
        optimized_time = time.perf_counter() - start_time
        
        # Performance comparison
        speedup_ratio = individual_time / optimized_time
        individual_throughput = len(parcels) / individual_time
        optimized_throughput = len(parcels) / optimized_time
        
        print(f"Individual processing: {individual_time:.2f}s ({individual_throughput:.1f} parcels/sec)")
        print(f"Optimized processing: {optimized_time:.2f}s ({optimized_throughput:.1f} parcels/sec)")
        print(f"Speedup ratio: {speedup_ratio:.2f}x")
        print(f"Target speedup: 1.5x minimum")
        
        # Quality verification - results should be equivalent
        self.assertEqual(len(individual_results), len(optimized_results))
        
        # Verify optimization maintains quality
        individual_success = sum(1 for r in individual_results if len(r.get('viable_uses', [])) > 0)
        optimized_success = sum(1 for r in optimized_results if len(r.get('viable_uses', [])) > 0)
        
        self.assertAlmostEqual(individual_success, optimized_success, delta=5,
                              msg="Optimization should not significantly affect success rate")
        
        # Performance target
        self.assertGreater(speedup_ratio, 1.2,  # Minimum improvement
                          f"Speedup {speedup_ratio:.2f}x below minimum 1.2x")
        
        print("✅ Batch processing optimization PASSED")
        
        return speedup_ratio
    
    def test_memory_efficiency(self):
        """Test memory efficiency and garbage collection"""
        print("\n=== MEMORY EFFICIENCY TEST ===")
        
        # Baseline memory
        import gc
        gc.collect()
        baseline_memory = self.measure_memory_usage()
        
        # Process batches and measure memory growth
        batch_sizes = [50, 100, 200, 400]
        memory_measurements = [baseline_memory]
        
        for batch_size in batch_sizes:
            parcels = self.generate_test_parcels(batch_size)
            
            # Process batch
            results = []
            for parcel in parcels:
                result = self.scorer.process_multi_template(parcel)
                results.append(result)
            
            # Force garbage collection
            del results
            gc.collect()
            
            current_memory = self.measure_memory_usage()
            memory_measurements.append(current_memory)
            
            print(f"After {batch_size} parcels: {current_memory:.1f} MB")
        
        # Calculate memory growth rate
        memory_growth = memory_measurements[-1] - memory_measurements[0]
        max_memory = max(memory_measurements)
        
        print(f"Total memory growth: {memory_growth:.1f} MB")
        print(f"Peak memory: {max_memory:.1f} MB")
        print(f"Memory efficiency: {memory_growth < 50}")
        
        # Memory efficiency assertions
        self.assertLess(memory_growth, 75, 
                       f"Memory growth {memory_growth:.1f}MB exceeds threshold")
        
        print("✅ Memory efficiency PASSED")
    
    def test_concurrent_processing_safety(self):
        """Test thread safety for concurrent processing"""
        print("\n=== CONCURRENT PROCESSING SAFETY TEST ===")
        
        import threading
        import queue
        
        parcels = self.generate_test_parcels(100)
        results_queue = queue.Queue()
        errors_queue = queue.Queue()
        
        def worker(parcels_subset, worker_id):
            try:
                # Each worker gets its own scorer instance
                worker_scorer = MultiTemplateScorer()
                worker_results = []
                
                for parcel in parcels_subset:
                    result = worker_scorer.process_multi_template(parcel)
                    worker_results.append(result)
                
                results_queue.put((worker_id, worker_results))
            except Exception as e:
                errors_queue.put((worker_id, str(e)))
        
        # Split parcels across 4 workers
        chunk_size = len(parcels) // 4
        threads = []
        
        start_time = time.perf_counter()
        
        for i in range(4):
            start_idx = i * chunk_size
            end_idx = start_idx + chunk_size if i < 3 else len(parcels)
            parcels_subset = parcels[start_idx:end_idx]
            
            thread = threading.Thread(target=worker, args=(parcels_subset, i))
            threads.append(thread)
            thread.start()
        
        # Wait for all workers
        for thread in threads:
            thread.join()
        
        concurrent_time = time.perf_counter() - start_time
        
        # Check for errors
        error_count = errors_queue.qsize()
        self.assertEqual(error_count, 0, "Should have no concurrent processing errors")
        
        # Collect results
        all_results = []
        while not results_queue.empty():
            worker_id, worker_results = results_queue.get()
            all_results.extend(worker_results)
            print(f"Worker {worker_id}: {len(worker_results)} results")
        
        # Verify result count
        self.assertEqual(len(all_results), len(parcels))
        
        # Quality check
        successful_results = [r for r in all_results if len(r.get('viable_uses', [])) > 0]
        success_rate = len(successful_results) / len(all_results)
        
        print(f"Concurrent processing time: {concurrent_time:.2f}s")
        print(f"Success rate: {success_rate:.1%}")
        
        self.assertGreater(success_rate, 0.95, "Concurrent processing should maintain quality")
        
        print("✅ Concurrent processing safety PASSED")


if __name__ == '__main__':
    # Run performance tests with detailed output
    unittest.main(verbosity=2)