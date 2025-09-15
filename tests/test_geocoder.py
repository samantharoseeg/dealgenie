#!/usr/bin/env python3
"""
Comprehensive tests for DealGenie Hierarchical Geocoding Service
Tests all components including caching, rate limiting, and batch processing.
"""

import asyncio
import unittest
import sys
import os
from unittest.mock import patch, MagicMock, AsyncMock
import json
import time

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from geocoding.geocoder import (
    HierarchicalGeocoder, NominatimGeocoder, GoogleGeocoder,
    GeocodeResult, GeocodeStatus, GeocodeProvider, GeocodeCache,
    CircuitBreaker, RateLimiter, geocode_address, geocode_addresses
)

class TestGeocodeResult(unittest.TestCase):
    """Test GeocodeResult dataclass functionality."""
    
    def test_geocode_result_creation(self):
        """Test creating GeocodeResult with various parameters."""
        result = GeocodeResult(
            latitude=34.0522,
            longitude=-118.2437,
            formatted_address="Los Angeles, CA",
            confidence_score=0.95,
            provider=GeocodeProvider.GOOGLE,
            status=GeocodeStatus.SUCCESS
        )
        
        self.assertEqual(result.latitude, 34.0522)
        self.assertEqual(result.longitude, -118.2437)
        self.assertEqual(result.confidence_score, 0.95)
        self.assertEqual(result.provider, GeocodeProvider.GOOGLE)
        self.assertEqual(result.status, GeocodeStatus.SUCCESS)
    
    def test_geocode_result_to_dict(self):
        """Test conversion to dictionary."""
        result = GeocodeResult(
            latitude=34.0522,
            longitude=-118.2437,
            provider=GeocodeProvider.NOMINATIM,
            status=GeocodeStatus.SUCCESS
        )
        
        result_dict = result.to_dict()
        
        self.assertIsInstance(result_dict, dict)
        self.assertEqual(result_dict['latitude'], 34.0522)
        self.assertEqual(result_dict['longitude'], -118.2437)
        self.assertEqual(result_dict['provider'], 'nominatim')
        self.assertEqual(result_dict['status'], 'success')

class TestCircuitBreaker(unittest.TestCase):
    """Test circuit breaker pattern implementation."""
    
    def test_circuit_breaker_closed_state(self):
        """Test circuit breaker in closed state."""
        cb = CircuitBreaker(failure_threshold=3, timeout_seconds=1)
        
        self.assertTrue(cb.call_allowed())
        self.assertEqual(cb.state, 'closed')
    
    def test_circuit_breaker_failure_tracking(self):
        """Test failure tracking and state transitions."""
        cb = CircuitBreaker(failure_threshold=2, timeout_seconds=1)
        
        # Record failures
        cb.record_failure()
        self.assertEqual(cb.state, 'closed')
        self.assertTrue(cb.call_allowed())
        
        cb.record_failure()
        self.assertEqual(cb.state, 'open')
        self.assertFalse(cb.call_allowed())
    
    def test_circuit_breaker_recovery(self):
        """Test circuit breaker recovery after timeout."""
        cb = CircuitBreaker(failure_threshold=1, timeout_seconds=0.1)
        
        # Trigger circuit open
        cb.record_failure()
        self.assertFalse(cb.call_allowed())
        
        # Wait for timeout
        time.sleep(0.2)
        
        # Should be half-open now
        self.assertTrue(cb.call_allowed())
        
        # Record success to close circuit
        cb.record_success()
        self.assertEqual(cb.state, 'closed')

class TestRateLimiter(unittest.TestCase):
    """Test token bucket rate limiter."""
    
    def test_rate_limiter_basic_operation(self):
        """Test basic rate limiter functionality."""
        limiter = RateLimiter(requests_per_second=2.0, burst_size=5)
        
        # Should allow initial burst
        for _ in range(5):
            self.assertTrue(limiter.acquire())
        
        # Should reject next request
        self.assertFalse(limiter.acquire())
    
    def test_rate_limiter_token_replenishment(self):
        """Test token replenishment over time."""
        limiter = RateLimiter(requests_per_second=10.0, burst_size=2)
        
        # Use up tokens
        self.assertTrue(limiter.acquire())
        self.assertTrue(limiter.acquire())
        self.assertFalse(limiter.acquire())
        
        # Wait for token replenishment
        time.sleep(0.2)  # Should add 2 tokens
        self.assertTrue(limiter.acquire())

class TestGeocodeCache(unittest.TestCase):
    """Test Redis-based geocoding cache."""
    
    def setUp(self):
        """Set up test cache with mocked Redis."""
        self.cache = GeocodeCache()
        # Mock Redis client
        self.cache.redis_client = MagicMock()
    
    def test_cache_key_generation(self):
        """Test cache key generation."""
        key1 = self.cache._make_key("123 Main St, LA, CA")
        key2 = self.cache._make_key("123 main st, la, ca")
        key3 = self.cache._make_key("456 Oak Ave, LA, CA")
        
        # Same address should generate same key (case insensitive)
        self.assertEqual(key1, key2)
        
        # Different addresses should generate different keys
        self.assertNotEqual(key1, key3)
        
        # Keys should have proper format
        self.assertTrue(key1.startswith("geocode:"))
    
    def test_cache_get_miss(self):
        """Test cache miss."""
        self.cache.redis_client.get.return_value = None
        
        result = self.cache.get("123 Main St")
        self.assertIsNone(result)
    
    def test_cache_get_hit(self):
        """Test cache hit."""
        cached_data = {
            'latitude': 34.0522,
            'longitude': -118.2437,
            'status': 'success',
            'provider': 'nominatim'
        }
        
        self.cache.redis_client.get.return_value = json.dumps(cached_data)
        
        result = self.cache.get("123 Main St")
        
        self.assertIsNotNone(result)
        self.assertTrue(result.cached)
        self.assertEqual(result.provider, GeocodeProvider.CACHE)
        self.assertEqual(result.latitude, 34.0522)
    
    def test_cache_set(self):
        """Test setting cache values."""
        result = GeocodeResult(
            latitude=34.0522,
            longitude=-118.2437,
            status=GeocodeStatus.SUCCESS,
            provider=GeocodeProvider.NOMINATIM
        )
        
        self.cache.set("123 Main St", result)
        
        # Should call setex on redis client
        self.cache.redis_client.setex.assert_called_once()

class TestNominatimGeocoder(unittest.IsolatedAsyncioTestCase):
    """Test Nominatim geocoding service."""
    
    def setUp(self):
        """Set up Nominatim geocoder."""
        self.geocoder = NominatimGeocoder()
    
    async def test_nominatim_successful_geocoding(self):
        """Test successful Nominatim geocoding."""
        mock_response = [{
            'lat': '34.0522265',
            'lon': '-118.2436596',
            'display_name': 'Los Angeles, CA, USA',
            'importance': 0.8,
            'type': 'city',
            'address': {
                'city': 'Los Angeles',
                'state': 'California',
                'country': 'United States'
            }
        }]
        
        with patch('aiohttp.ClientSession') as mock_session:
            mock_response_obj = AsyncMock()
            mock_response_obj.status = 200
            mock_response_obj.json = AsyncMock(return_value=mock_response)
            
            mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value = mock_response_obj
            
            result = await self.geocoder.geocode("Los Angeles, CA")
            
            self.assertEqual(result.status, GeocodeStatus.SUCCESS)
            self.assertEqual(result.provider, GeocodeProvider.NOMINATIM)
            self.assertAlmostEqual(result.latitude, 34.0522265)
            self.assertAlmostEqual(result.longitude, -118.2436596)
            self.assertGreater(result.confidence_score, 0)
    
    async def test_nominatim_no_results(self):
        """Test Nominatim with no results."""
        with patch('aiohttp.ClientSession') as mock_session:
            mock_response_obj = AsyncMock()
            mock_response_obj.status = 200
            mock_response_obj.json = AsyncMock(return_value=[])
            
            mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value = mock_response_obj
            
            result = await self.geocoder.geocode("Invalid Address")
            
            self.assertEqual(result.status, GeocodeStatus.FAILED)
            self.assertEqual(result.provider, GeocodeProvider.NOMINATIM)
    
    async def test_nominatim_rate_limited(self):
        """Test Nominatim rate limiting response."""
        with patch('aiohttp.ClientSession') as mock_session:
            mock_response_obj = AsyncMock()
            mock_response_obj.status = 429
            
            mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value = mock_response_obj
            
            result = await self.geocoder.geocode("Los Angeles, CA")
            
            self.assertEqual(result.status, GeocodeStatus.RATE_LIMITED)
            self.assertEqual(result.provider, GeocodeProvider.NOMINATIM)

class TestGoogleGeocoder(unittest.IsolatedAsyncioTestCase):
    """Test Google Maps geocoding service."""
    
    def setUp(self):
        """Set up Google geocoder."""
        self.geocoder = GoogleGeocoder("test_api_key")
    
    async def test_google_successful_geocoding(self):
        """Test successful Google geocoding."""
        mock_response = {
            'status': 'OK',
            'results': [{
                'formatted_address': 'Los Angeles, CA, USA',
                'geometry': {
                    'location': {'lat': 34.0522265, 'lng': -118.2436596},
                    'location_type': 'ROOFTOP'
                },
                'address_components': [
                    {'short_name': 'Los Angeles', 'types': ['locality']},
                    {'short_name': 'CA', 'types': ['administrative_area_level_1']},
                    {'short_name': 'US', 'types': ['country']}
                ]
            }]
        }
        
        with patch('aiohttp.ClientSession') as mock_session:
            mock_response_obj = AsyncMock()
            mock_response_obj.status = 200
            mock_response_obj.json = AsyncMock(return_value=mock_response)
            
            mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value = mock_response_obj
            
            result = await self.geocoder.geocode("Los Angeles, CA")
            
            self.assertEqual(result.status, GeocodeStatus.SUCCESS)
            self.assertEqual(result.provider, GeocodeProvider.GOOGLE)
            self.assertEqual(result.latitude, 34.0522265)
            self.assertEqual(result.longitude, -118.2436596)
            self.assertGreater(result.confidence_score, 0.9)  # ROOFTOP should have high confidence
    
    async def test_google_quota_exceeded(self):
        """Test Google quota exceeded response."""
        mock_response = {'status': 'OVER_QUERY_LIMIT'}
        
        with patch('aiohttp.ClientSession') as mock_session:
            mock_response_obj = AsyncMock()
            mock_response_obj.status = 200
            mock_response_obj.json = AsyncMock(return_value=mock_response)
            
            mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value = mock_response_obj
            
            result = await self.geocoder.geocode("Los Angeles, CA")
            
            self.assertEqual(result.status, GeocodeStatus.QUOTA_EXCEEDED)
            self.assertEqual(result.provider, GeocodeProvider.GOOGLE)

class TestHierarchicalGeocoder(unittest.IsolatedAsyncioTestCase):
    """Test main hierarchical geocoding service."""
    
    def setUp(self):
        """Set up hierarchical geocoder."""
        self.geocoder = HierarchicalGeocoder(google_api_key="test_key")
    
    async def test_cache_hit(self):
        """Test geocoding with cache hit."""
        cached_result = GeocodeResult(
            latitude=34.0522,
            longitude=-118.2437,
            status=GeocodeStatus.SUCCESS,
            provider=GeocodeProvider.CACHE,
            cached=True
        )
        
        with patch.object(self.geocoder.cache, 'get', return_value=cached_result):
            result = await self.geocoder.geocode("123 Main St")
            
            self.assertEqual(result.status, GeocodeStatus.SUCCESS)
            self.assertEqual(result.provider, GeocodeProvider.CACHE)
            self.assertTrue(result.cached)
            self.assertEqual(self.geocoder.stats['cache_hits'], 1)
    
    async def test_nominatim_fallback_to_google(self):
        """Test fallback from Nominatim to Google."""
        # Mock cache miss
        with patch.object(self.geocoder.cache, 'get', return_value=None):
            # Mock Nominatim failure
            with patch.object(self.geocoder.nominatim, 'geocode', 
                            return_value=GeocodeResult(status=GeocodeStatus.FAILED)):
                # Mock Google success
                google_result = GeocodeResult(
                    latitude=34.0522,
                    longitude=-118.2437,
                    status=GeocodeStatus.SUCCESS,
                    provider=GeocodeProvider.GOOGLE
                )
                
                with patch.object(self.geocoder.google, 'geocode', 
                                return_value=google_result):
                    result = await self.geocoder.geocode("123 Main St")
                    
                    self.assertEqual(result.status, GeocodeStatus.SUCCESS)
                    self.assertEqual(result.provider, GeocodeProvider.GOOGLE)
    
    async def test_batch_geocoding(self):
        """Test batch geocoding functionality."""
        addresses = [
            "123 Main St, LA, CA",
            "456 Oak Ave, LA, CA",
            "789 Pine Rd, LA, CA"
        ]
        
        # Mock individual geocoding results
        mock_results = [
            GeocodeResult(latitude=34.01, longitude=-118.24, status=GeocodeStatus.SUCCESS),
            GeocodeResult(latitude=34.02, longitude=-118.25, status=GeocodeStatus.SUCCESS),
            GeocodeResult(status=GeocodeStatus.FAILED)
        ]
        
        with patch.object(self.geocoder, 'geocode', side_effect=mock_results):
            results = await self.geocoder.geocode_batch(addresses, batch_size=2)
            
            self.assertEqual(len(results), 3)
            self.assertEqual(results[0].status, GeocodeStatus.SUCCESS)
            self.assertEqual(results[1].status, GeocodeStatus.SUCCESS)
            self.assertEqual(results[2].status, GeocodeStatus.FAILED)
    
    def test_get_stats(self):
        """Test statistics collection."""
        # Simulate some operations
        self.geocoder.stats['total_requests'] = 100
        self.geocoder.stats['cache_hits'] = 30
        self.geocoder.stats['nominatim_success'] = 50
        self.geocoder.stats['google_success'] = 15
        self.geocoder.stats['failures'] = 5
        
        stats = self.geocoder.get_stats()
        
        self.assertEqual(stats['cache_hit_rate'], 0.3)
        self.assertEqual(stats['success_rate'], 0.65)  # (50 + 15) / 100
        self.assertIn('nominatim_circuit_breaker_state', stats)
        self.assertIn('google_circuit_breaker_state', stats)

class TestSyncWrappers(unittest.TestCase):
    """Test synchronous wrapper functions."""
    
    def test_geocode_address_sync(self):
        """Test synchronous geocode_address wrapper."""
        with patch('geocoding.geocoder.HierarchicalGeocoder') as mock_geocoder_class:
            mock_geocoder = MagicMock()
            mock_geocoder_class.return_value = mock_geocoder
            
            # Mock the async method
            async def mock_geocode(addr):
                return GeocodeResult(
                    latitude=34.0522,
                    longitude=-118.2437,
                    status=GeocodeStatus.SUCCESS
                )
            
            mock_geocoder.geocode = mock_geocode
            
            result = geocode_address("123 Main St")
            
            self.assertEqual(result.status, GeocodeStatus.SUCCESS)
            self.assertEqual(result.latitude, 34.0522)
    
    def test_geocode_addresses_sync(self):
        """Test synchronous geocode_addresses wrapper."""
        addresses = ["123 Main St", "456 Oak Ave"]
        
        with patch('geocoding.geocoder.HierarchicalGeocoder') as mock_geocoder_class:
            mock_geocoder = MagicMock()
            mock_geocoder_class.return_value = mock_geocoder
            
            # Mock the async batch method
            async def mock_batch_geocode(addrs, batch_size=10):
                return [
                    GeocodeResult(latitude=34.01, longitude=-118.24, status=GeocodeStatus.SUCCESS),
                    GeocodeResult(latitude=34.02, longitude=-118.25, status=GeocodeStatus.SUCCESS)
                ]
            
            mock_geocoder.geocode_batch = mock_batch_geocode
            
            results = geocode_addresses(addresses)
            
            self.assertEqual(len(results), 2)
            self.assertEqual(results[0].status, GeocodeStatus.SUCCESS)
            self.assertEqual(results[1].status, GeocodeStatus.SUCCESS)

class TestIntegration(unittest.IsolatedAsyncioTestCase):
    """Integration tests for complete geocoding workflow."""
    
    async def test_full_workflow(self):
        """Test complete geocoding workflow."""
        geocoder = HierarchicalGeocoder()
        
        # Mock all components
        with patch.object(geocoder.cache, 'get', return_value=None):
            with patch.object(geocoder.cache, 'set'):
                with patch.object(geocoder.nominatim, 'geocode') as mock_nominatim:
                    mock_nominatim.return_value = GeocodeResult(
                        latitude=34.0522,
                        longitude=-118.2437,
                        status=GeocodeStatus.SUCCESS,
                        provider=GeocodeProvider.NOMINATIM,
                        confidence_score=0.85
                    )
                    
                    result = await geocoder.geocode("123 Main St, Los Angeles, CA")
                    
                    self.assertEqual(result.status, GeocodeStatus.SUCCESS)
                    self.assertEqual(result.provider, GeocodeProvider.NOMINATIM)
                    self.assertEqual(result.confidence_score, 0.85)
                    
                    # Check that cache was attempted
                    geocoder.cache.get.assert_called_once()
                    geocoder.cache.set.assert_called_once()

def run_performance_tests():
    """Run performance tests (not part of unittest suite)."""
    print("\n🚀 Performance Tests")
    print("=" * 30)
    
    async def test_batch_performance():
        geocoder = HierarchicalGeocoder()
        
        # Mock fast geocoding
        async def fast_mock_geocode(address):
            await asyncio.sleep(0.001)  # 1ms mock response time
            return GeocodeResult(
                latitude=34.0522,
                longitude=-118.2437,
                status=GeocodeStatus.SUCCESS,
                provider=GeocodeProvider.NOMINATIM
            )
        
        with patch.object(geocoder, 'geocode', side_effect=fast_mock_geocode):
            addresses = [f"123 Main St #{i}, LA, CA" for i in range(100)]
            
            start_time = time.time()
            results = await geocoder.geocode_batch(addresses, batch_size=20, max_concurrent=10)
            end_time = time.time()
            
            total_time = end_time - start_time
            print(f"Batch geocoded {len(addresses)} addresses in {total_time:.2f}s")
            print(f"Average: {total_time/len(addresses)*1000:.1f}ms per address")
            print(f"Throughput: {len(addresses)/total_time:.0f} addresses/second")
    
    asyncio.run(test_batch_performance())

if __name__ == '__main__':
    # Run unit tests
    unittest.main(verbosity=2, exit=False)
    
    # Run performance tests
    run_performance_tests()