#!/usr/bin/env python3
"""
DealGenie Hierarchical Geocoding Service
Implements robust geocoding with multiple providers, caching, rate limiting,
and batch processing for Los Angeles real estate data.
"""

import asyncio
import hashlib
import json
import logging
import time
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple, Union, Any
from enum import Enum
import threading
from datetime import datetime, timedelta

# HTTP and async imports
import aiohttp
import requests
from urllib.parse import urlencode, quote

# Redis for caching
try:
    import redis
    import redis.asyncio as aioredis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logging.warning("Redis not available. Install with: pip install redis")

class GeocodeProvider(Enum):
    """Geocoding service providers."""
    NOMINATIM = "nominatim"
    GOOGLE = "google"
    CACHE = "cache"

class GeocodeStatus(Enum):
    """Geocoding operation status."""
    SUCCESS = "success"
    PARTIAL = "partial" 
    FAILED = "failed"
    RATE_LIMITED = "rate_limited"
    QUOTA_EXCEEDED = "quota_exceeded"
    CIRCUIT_OPEN = "circuit_open"

@dataclass
class GeocodeResult:
    """Structured geocoding result with quality metrics."""
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    formatted_address: Optional[str] = None
    confidence_score: float = 0.0
    provider: Optional[GeocodeProvider] = None
    status: GeocodeStatus = GeocodeStatus.FAILED
    
    # Address components
    street_number: Optional[str] = None
    street_name: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None
    
    # Quality metrics
    precision: Optional[str] = None  # rooftop, interpolated, approximate
    match_type: Optional[str] = None  # exact, partial, fuzzy
    
    # Metadata
    response_time_ms: float = 0.0
    cached: bool = False
    timestamp: Optional[datetime] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization."""
        result = asdict(self)
        if self.timestamp:
            result['timestamp'] = self.timestamp.isoformat()
        if self.provider:
            result['provider'] = self.provider.value
        result['status'] = self.status.value
        return result

class CircuitBreaker:
    """Circuit breaker pattern for geocoding services."""
    
    def __init__(self, failure_threshold: int = 5, timeout_seconds: int = 60):
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'closed'  # closed, open, half-open
        self._lock = threading.Lock()
    
    def call_allowed(self) -> bool:
        """Check if calls are allowed through the circuit breaker."""
        with self._lock:
            if self.state == 'closed':
                return True
            elif self.state == 'open':
                if self.last_failure_time and \
                   time.time() - self.last_failure_time > self.timeout_seconds:
                    self.state = 'half-open'
                    return True
                return False
            else:  # half-open
                return True
    
    def record_success(self):
        """Record successful operation."""
        with self._lock:
            self.failure_count = 0
            self.state = 'closed'
    
    def record_failure(self):
        """Record failed operation."""
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = 'open'

class RateLimiter:
    """Token bucket rate limiter for API calls."""
    
    def __init__(self, requests_per_second: float = 1.0, burst_size: int = 5):
        self.rate = requests_per_second
        self.burst_size = burst_size
        self.tokens = burst_size
        self.last_update = time.time()
        self._lock = threading.Lock()
    
    def acquire(self, tokens: int = 1) -> bool:
        """Attempt to acquire tokens for API call."""
        with self._lock:
            now = time.time()
            elapsed = now - self.last_update
            
            # Add tokens based on elapsed time
            self.tokens = min(self.burst_size, 
                            self.tokens + elapsed * self.rate)
            self.last_update = now
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False
    
    def time_until_available(self) -> float:
        """Time in seconds until tokens are available."""
        with self._lock:
            if self.tokens >= 1:
                return 0.0
            return (1 - self.tokens) / self.rate

class GeocodeCache:
    """Redis-based geocoding cache with TTL."""
    
    def __init__(self, redis_url: str = "redis://localhost:6379", 
                 ttl_seconds: int = 7 * 24 * 3600):  # 1 week default
        self.ttl_seconds = ttl_seconds
        self.redis_client = None
        
        if REDIS_AVAILABLE:
            try:
                self.redis_client = redis.from_url(redis_url, decode_responses=True)
                # Test connection
                self.redis_client.ping()
                logging.info("Connected to Redis cache")
            except Exception as e:
                logging.warning(f"Redis connection failed: {e}")
                self.redis_client = None
        else:
            logging.warning("Redis not available for caching")
    
    def _make_key(self, address: str) -> str:
        """Generate cache key from address."""
        normalized = address.lower().strip()
        hash_obj = hashlib.md5(normalized.encode('utf-8'))
        return f"geocode:{hash_obj.hexdigest()}"
    
    def get(self, address: str) -> Optional[GeocodeResult]:
        """Retrieve cached geocoding result."""
        if not self.redis_client:
            return None
            
        try:
            key = self._make_key(address)
            cached_data = self.redis_client.get(key)
            
            if cached_data:
                data = json.loads(cached_data)
                result = GeocodeResult(**data)
                result.cached = True
                result.provider = GeocodeProvider.CACHE
                return result
        except Exception as e:
            logging.error(f"Cache get error: {e}")
        
        return None
    
    def set(self, address: str, result: GeocodeResult):
        """Store geocoding result in cache."""
        if not self.redis_client or result.status != GeocodeStatus.SUCCESS:
            return
            
        try:
            key = self._make_key(address)
            # Don't cache the cached flag
            cache_result = GeocodeResult(**asdict(result))
            cache_result.cached = False
            cache_result.timestamp = datetime.now()
            
            data = json.dumps(cache_result.to_dict())
            self.redis_client.setex(key, self.ttl_seconds, data)
        except Exception as e:
            logging.error(f"Cache set error: {e}")

class NominatimGeocoder:
    """OpenStreetMap Nominatim geocoding service."""
    
    def __init__(self, user_agent: str = "DealGenie/1.0"):
        self.base_url = "https://nominatim.openstreetmap.org/search"
        self.user_agent = user_agent
        self.rate_limiter = RateLimiter(requests_per_second=1.0)  # Nominatim limit
        self.circuit_breaker = CircuitBreaker(failure_threshold=3)
        
    async def geocode(self, address: str) -> GeocodeResult:
        """Geocode address using Nominatim."""
        if not self.circuit_breaker.call_allowed():
            return GeocodeResult(
                status=GeocodeStatus.CIRCUIT_OPEN,
                provider=GeocodeProvider.NOMINATIM
            )
        
        if not self.rate_limiter.acquire():
            return GeocodeResult(
                status=GeocodeStatus.RATE_LIMITED,
                provider=GeocodeProvider.NOMINATIM
            )
        
        start_time = time.time()
        
        try:
            params = {
                'q': address,
                'format': 'json',
                'limit': 1,
                'addressdetails': 1,
                'bounded': 1,
                'viewbox': '-118.9,33.7,-117.6,34.3',  # LA County bounds
                'countrycodes': 'us'
            }
            
            headers = {
                'User-Agent': self.user_agent
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.base_url}?{urlencode(params)}", 
                                     headers=headers) as response:
                    
                    response_time = (time.time() - start_time) * 1000
                    
                    if response.status == 429:
                        self.circuit_breaker.record_failure()
                        return GeocodeResult(
                            status=GeocodeStatus.RATE_LIMITED,
                            provider=GeocodeProvider.NOMINATIM,
                            response_time_ms=response_time
                        )
                    
                    if response.status != 200:
                        self.circuit_breaker.record_failure()
                        return GeocodeResult(
                            status=GeocodeStatus.FAILED,
                            provider=GeocodeProvider.NOMINATIM,
                            response_time_ms=response_time
                        )
                    
                    data = await response.json()
                    
                    if not data:
                        return GeocodeResult(
                            status=GeocodeStatus.FAILED,
                            provider=GeocodeProvider.NOMINATIM,
                            response_time_ms=response_time
                        )
                    
                    result = self._parse_nominatim_response(data[0])
                    result.response_time_ms = response_time
                    result.provider = GeocodeProvider.NOMINATIM
                    
                    self.circuit_breaker.record_success()
                    return result
                    
        except asyncio.TimeoutError:
            self.circuit_breaker.record_failure()
            return GeocodeResult(
                status=GeocodeStatus.FAILED,
                provider=GeocodeProvider.NOMINATIM,
                response_time_ms=(time.time() - start_time) * 1000
            )
        except Exception as e:
            self.circuit_breaker.record_failure()
            logging.error(f"Nominatim geocoding error: {e}")
            return GeocodeResult(
                status=GeocodeStatus.FAILED,
                provider=GeocodeProvider.NOMINATIM,
                response_time_ms=(time.time() - start_time) * 1000
            )
    
    def _parse_nominatim_response(self, data: Dict) -> GeocodeResult:
        """Parse Nominatim response into GeocodeResult."""
        address_parts = data.get('address', {})
        
        # Calculate confidence based on importance and type
        importance = float(data.get('importance', 0))
        place_type = data.get('type', '')
        
        confidence = min(importance * 10, 1.0)  # Scale importance to 0-1
        if place_type in ['house', 'building']:
            confidence = max(confidence, 0.9)
        elif place_type in ['residential', 'commercial']:
            confidence = max(confidence, 0.8)
        
        return GeocodeResult(
            latitude=float(data['lat']),
            longitude=float(data['lon']),
            formatted_address=data.get('display_name'),
            confidence_score=confidence,
            status=GeocodeStatus.SUCCESS,
            street_number=address_parts.get('house_number'),
            street_name=address_parts.get('road'),
            city=address_parts.get('city') or address_parts.get('town'),
            state=address_parts.get('state'),
            postal_code=address_parts.get('postcode'),
            country=address_parts.get('country'),
            precision=self._determine_precision(data),
            match_type='exact' if confidence > 0.8 else 'partial'
        )
    
    def _determine_precision(self, data: Dict) -> str:
        """Determine precision level from Nominatim response."""
        place_type = data.get('type', '')
        if place_type in ['house', 'building']:
            return 'rooftop'
        elif place_type in ['residential', 'commercial']:
            return 'interpolated'
        else:
            return 'approximate'

class GoogleGeocoder:
    """Google Maps Geocoding API service."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://maps.googleapis.com/maps/api/geocode/json"
        self.rate_limiter = RateLimiter(requests_per_second=50.0)  # Google limit
        self.circuit_breaker = CircuitBreaker(failure_threshold=5)
        self.daily_quota = 0  # Track daily usage
        self.quota_limit = 25000  # Adjust based on plan
        
    async def geocode(self, address: str) -> GeocodeResult:
        """Geocode address using Google Maps API."""
        if not self.circuit_breaker.call_allowed():
            return GeocodeResult(
                status=GeocodeStatus.CIRCUIT_OPEN,
                provider=GeocodeProvider.GOOGLE
            )
        
        if self.daily_quota >= self.quota_limit:
            return GeocodeResult(
                status=GeocodeStatus.QUOTA_EXCEEDED,
                provider=GeocodeProvider.GOOGLE
            )
        
        if not self.rate_limiter.acquire():
            return GeocodeResult(
                status=GeocodeStatus.RATE_LIMITED,
                provider=GeocodeProvider.GOOGLE
            )
        
        start_time = time.time()
        
        try:
            params = {
                'address': address,
                'key': self.api_key,
                'bounds': '33.7,-118.9|34.3,-117.6',  # LA County bounds
                'region': 'us'
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.base_url}?{urlencode(params)}") as response:
                    
                    response_time = (time.time() - start_time) * 1000
                    self.daily_quota += 1
                    
                    if response.status == 429:
                        self.circuit_breaker.record_failure()
                        return GeocodeResult(
                            status=GeocodeStatus.RATE_LIMITED,
                            provider=GeocodeProvider.GOOGLE,
                            response_time_ms=response_time
                        )
                    
                    if response.status != 200:
                        self.circuit_breaker.record_failure()
                        return GeocodeResult(
                            status=GeocodeStatus.FAILED,
                            provider=GeocodeProvider.GOOGLE,
                            response_time_ms=response_time
                        )
                    
                    data = await response.json()
                    
                    if data['status'] == 'OVER_QUERY_LIMIT':
                        return GeocodeResult(
                            status=GeocodeStatus.QUOTA_EXCEEDED,
                            provider=GeocodeProvider.GOOGLE,
                            response_time_ms=response_time
                        )
                    
                    if data['status'] != 'OK' or not data.get('results'):
                        return GeocodeResult(
                            status=GeocodeStatus.FAILED,
                            provider=GeocodeProvider.GOOGLE,
                            response_time_ms=response_time
                        )
                    
                    result = self._parse_google_response(data['results'][0])
                    result.response_time_ms = response_time
                    result.provider = GeocodeProvider.GOOGLE
                    
                    self.circuit_breaker.record_success()
                    return result
                    
        except asyncio.TimeoutError:
            self.circuit_breaker.record_failure()
            return GeocodeResult(
                status=GeocodeStatus.FAILED,
                provider=GeocodeProvider.GOOGLE,
                response_time_ms=(time.time() - start_time) * 1000
            )
        except Exception as e:
            self.circuit_breaker.record_failure()
            logging.error(f"Google geocoding error: {e}")
            return GeocodeResult(
                status=GeocodeStatus.FAILED,
                provider=GeocodeProvider.GOOGLE,
                response_time_ms=(time.time() - start_time) * 1000
            )
    
    def _parse_google_response(self, data: Dict) -> GeocodeResult:
        """Parse Google response into GeocodeResult."""
        geometry = data['geometry']
        location = geometry['location']
        
        # Extract address components
        components = {}
        for comp in data.get('address_components', []):
            for comp_type in comp['types']:
                components[comp_type] = comp['short_name']
        
        # Determine confidence from geometry location_type
        location_type = geometry.get('location_type', 'APPROXIMATE')
        confidence_map = {
            'ROOFTOP': 0.95,
            'RANGE_INTERPOLATED': 0.85,
            'GEOMETRIC_CENTER': 0.75,
            'APPROXIMATE': 0.65
        }
        confidence = confidence_map.get(location_type, 0.5)
        
        return GeocodeResult(
            latitude=location['lat'],
            longitude=location['lng'],
            formatted_address=data['formatted_address'],
            confidence_score=confidence,
            status=GeocodeStatus.SUCCESS,
            street_number=components.get('street_number'),
            street_name=components.get('route'),
            city=components.get('locality'),
            state=components.get('administrative_area_level_1'),
            postal_code=components.get('postal_code'),
            country=components.get('country'),
            precision=location_type.lower().replace('_', ' '),
            match_type='exact' if confidence > 0.8 else 'partial'
        )

class HierarchicalGeocoder:
    """
    Main geocoding service with hierarchical provider fallback,
    caching, rate limiting, and batch processing.
    """
    
    def __init__(self, google_api_key: Optional[str] = None,
                 redis_url: str = "redis://localhost:6379",
                 user_agent: str = "DealGenie/1.0"):
        
        self.logger = logging.getLogger(__name__)
        self.cache = GeocodeCache(redis_url)
        self.nominatim = NominatimGeocoder(user_agent)
        self.google = GoogleGeocoder(google_api_key) if google_api_key else None
        
        # Statistics
        self.stats = {
            'total_requests': 0,
            'cache_hits': 0,
            'nominatim_success': 0,
            'google_success': 0,
            'failures': 0
        }
    
    async def geocode(self, address: str, 
                     use_cache: bool = True,
                     max_retries: int = 2) -> GeocodeResult:
        """
        Geocode a single address with hierarchical fallback.
        
        Flow: Cache → Nominatim → Google (if available)
        """
        if not address or not address.strip():
            return GeocodeResult(status=GeocodeStatus.FAILED)
        
        self.stats['total_requests'] += 1
        address = address.strip()
        
        # Try cache first
        if use_cache:
            cached_result = self.cache.get(address)
            if cached_result:
                self.stats['cache_hits'] += 1
                return cached_result
        
        # Try Nominatim first (free)
        for attempt in range(max_retries + 1):
            result = await self.nominatim.geocode(address)
            
            if result.status == GeocodeStatus.SUCCESS:
                self.stats['nominatim_success'] += 1
                if use_cache:
                    self.cache.set(address, result)
                return result
            
            if result.status == GeocodeStatus.RATE_LIMITED and attempt < max_retries:
                wait_time = self.nominatim.rate_limiter.time_until_available()
                await asyncio.sleep(wait_time)
                continue
            
            break
        
        # Fallback to Google if available
        if self.google:
            for attempt in range(max_retries + 1):
                result = await self.google.geocode(address)
                
                if result.status == GeocodeStatus.SUCCESS:
                    self.stats['google_success'] += 1
                    if use_cache:
                        self.cache.set(address, result)
                    return result
                
                if result.status == GeocodeStatus.RATE_LIMITED and attempt < max_retries:
                    wait_time = self.google.rate_limiter.time_until_available()
                    await asyncio.sleep(wait_time)
                    continue
                
                break
        
        # All providers failed
        self.stats['failures'] += 1
        return GeocodeResult(status=GeocodeStatus.FAILED)
    
    async def geocode_batch(self, addresses: List[str],
                           batch_size: int = 10,
                           max_concurrent: int = 5,
                           use_cache: bool = True) -> List[GeocodeResult]:
        """
        Geocode multiple addresses efficiently with batching and concurrency control.
        """
        if not addresses:
            return []
        
        self.logger.info(f"Starting batch geocoding of {len(addresses)} addresses")
        
        results = []
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def geocode_with_semaphore(addr: str) -> GeocodeResult:
            async with semaphore:
                return await self.geocode(addr, use_cache=use_cache)
        
        # Process in batches to manage memory and rate limits
        for i in range(0, len(addresses), batch_size):
            batch = addresses[i:i + batch_size]
            
            # Create tasks for this batch
            tasks = [geocode_with_semaphore(addr) for addr in batch]
            
            # Execute batch
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Handle results and exceptions
            for j, result in enumerate(batch_results):
                if isinstance(result, Exception):
                    self.logger.error(f"Error geocoding '{batch[j]}': {result}")
                    results.append(GeocodeResult(status=GeocodeStatus.FAILED))
                else:
                    results.append(result)
            
            # Progress logging
            completed = len(results)
            self.logger.info(f"Completed {completed}/{len(addresses)} geocodes")
            
            # Brief pause between batches to respect rate limits
            if i + batch_size < len(addresses):
                await asyncio.sleep(0.5)
        
        success_count = sum(1 for r in results if r.status == GeocodeStatus.SUCCESS)
        self.logger.info(f"Batch geocoding complete: {success_count}/{len(addresses)} successful")
        
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get geocoding service statistics."""
        total = self.stats['total_requests']
        if total == 0:
            return self.stats
        
        return {
            **self.stats,
            'cache_hit_rate': self.stats['cache_hits'] / total,
            'success_rate': (self.stats['nominatim_success'] + self.stats['google_success']) / total,
            'nominatim_circuit_breaker_state': self.nominatim.circuit_breaker.state,
            'google_circuit_breaker_state': self.google.circuit_breaker.state if self.google else None,
            'google_daily_quota_used': self.google.daily_quota if self.google else 0
        }
    
    def reset_stats(self):
        """Reset statistics counters."""
        self.stats = {
            'total_requests': 0,
            'cache_hits': 0,
            'nominatim_success': 0,
            'google_success': 0,
            'failures': 0
        }

# Convenience functions for synchronous usage
def geocode_address(address: str, google_api_key: Optional[str] = None) -> GeocodeResult:
    """Synchronous geocoding wrapper."""
    geocoder = HierarchicalGeocoder(google_api_key=google_api_key)
    
    async def _geocode():
        return await geocoder.geocode(address)
    
    return asyncio.run(_geocode())

def geocode_addresses(addresses: List[str], 
                     google_api_key: Optional[str] = None,
                     batch_size: int = 10) -> List[GeocodeResult]:
    """Synchronous batch geocoding wrapper."""
    geocoder = HierarchicalGeocoder(google_api_key=google_api_key)
    
    async def _batch_geocode():
        return await geocoder.geocode_batch(addresses, batch_size=batch_size)
    
    return asyncio.run(_batch_geocode())

def demo():
    """Demonstration of geocoding capabilities."""
    print("🌍 DealGenie Hierarchical Geocoding Demo")
    print("=" * 50)
    
    # Sample LA addresses
    test_addresses = [
        "1234 N Highland Ave, Los Angeles, CA 90028",
        "6801 Hollywood Blvd, Hollywood, CA 90028",
        "123 N Beverly Dr, Beverly Hills, CA 90210",
        "1 World Trade Center Dr, Long Beach, CA 90831"
    ]
    
    async def run_demo():
        geocoder = HierarchicalGeocoder()
        
        print(f"\n📍 Single Address Geocoding:")
        result = await geocoder.geocode(test_addresses[0])
        
        print(f"Address: {test_addresses[0]}")
        print(f"Coordinates: ({result.latitude}, {result.longitude})")
        print(f"Provider: {result.provider.value if result.provider else 'None'}")
        print(f"Confidence: {result.confidence_score:.3f}")
        print(f"Status: {result.status.value}")
        print(f"Response Time: {result.response_time_ms:.1f}ms")
        
        print(f"\n📍 Batch Geocoding:")
        results = await geocoder.geocode_batch(test_addresses[:3], batch_size=2)
        
        for i, (addr, result) in enumerate(zip(test_addresses[:3], results)):
            print(f"{i+1}. {addr}")
            if result.status == GeocodeStatus.SUCCESS:
                print(f"   → ({result.latitude:.6f}, {result.longitude:.6f})")
                print(f"   → {result.provider.value} - {result.confidence_score:.3f}")
            else:
                print(f"   → Failed: {result.status.value}")
        
        print(f"\n📊 Statistics:")
        stats = geocoder.get_stats()
        for key, value in stats.items():
            if isinstance(value, float):
                print(f"  {key}: {value:.3f}")
            else:
                print(f"  {key}: {value}")
    
    asyncio.run(run_demo())

if __name__ == "__main__":
    demo()