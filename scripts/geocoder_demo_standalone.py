#!/usr/bin/env python3
"""
DealGenie Geocoding Service Demo - Standalone Version
Demonstrates the core geocoding concepts and architecture.
"""

import time
import hashlib
import json
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any
from enum import Enum
import threading

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
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization."""
        result = asdict(self)
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

def demo():
    """Main demo function."""
    print("🌍 DealGenie Hierarchical Geocoding Service")
    print("=" * 55)
    print("Core Components Demo (No External Dependencies)")
    
    # Test Circuit Breaker
    print("\n🔧 Circuit Breaker Pattern:")
    print("-" * 27)
    cb = CircuitBreaker(failure_threshold=2, timeout_seconds=1)
    
    print(f"Initial state: {cb.state}")
    print(f"Calls allowed: {cb.call_allowed()}")
    
    print("\nSimulating failures...")
    cb.record_failure()
    print(f"After 1 failure: state={cb.state}, allowed={cb.call_allowed()}")
    
    cb.record_failure()
    print(f"After 2 failures: state={cb.state}, allowed={cb.call_allowed()}")
    
    print("✅ Circuit opened to protect system")
    
    # Test Rate Limiter
    print("\n⏱️ Token Bucket Rate Limiter:")
    print("-" * 28)
    limiter = RateLimiter(requests_per_second=2.0, burst_size=3)
    
    print("Testing burst capacity (3 tokens available):")
    for i in range(5):
        allowed = limiter.acquire()
        status = "✅ Allowed" if allowed else "❌ Rate limited"
        print(f"  Request {i+1}: {status}")
    
    print(f"Time until next token: {limiter.time_until_available():.2f}s")
    
    # Test GeocodeResult
    print("\n📍 Geocoding Result Structure:")
    print("-" * 31)
    
    # Simulate successful geocoding result
    result = GeocodeResult(
        latitude=34.0522265,
        longitude=-118.2436596,
        formatted_address="Los Angeles, CA, USA",
        confidence_score=0.92,
        provider=GeocodeProvider.GOOGLE,
        status=GeocodeStatus.SUCCESS,
        street_name="Main Street",
        city="Los Angeles",
        state="CA",
        postal_code="90012",
        precision="rooftop",
        match_type="exact",
        response_time_ms=143.7,
        cached=False
    )
    
    print(f"📍 Coordinates: ({result.latitude}, {result.longitude})")
    print(f"🏠 Address: {result.formatted_address}")
    print(f"🎯 Confidence: {result.confidence_score:.3f}")
    print(f"🔧 Provider: {result.provider.value}")
    print(f"📏 Precision: {result.precision}")
    print(f"⚡ Response: {result.response_time_ms}ms")
    print(f"💾 Cached: {result.cached}")
    
    # Show data structure
    result_dict = result.to_dict()
    print(f"\n📊 Data structure has {len(result_dict)} fields")
    
    # Hierarchical Strategy
    print("\n🏗️ Hierarchical Geocoding Strategy:")
    print("-" * 37)
    print("1. 🔍 Redis Cache Check")
    print("   • Instant response if cached")
    print("   • 1 week TTL default")
    print("   • MD5 hashed address keys")
    
    print("\n2. 🆓 Nominatim (OpenStreetMap)")
    print("   • Free service, rate limited")
    print("   • 1 req/sec limit")
    print("   • Good for bulk processing")
    print("   • Circuit breaker protection")
    
    print("\n3. 💰 Google Maps API")
    print("   • High accuracy fallback")
    print("   • 50 req/sec limit")  
    print("   • Daily quota tracking")
    print("   • Premium precision levels")
    
    # Quality Metrics
    print("\n📈 Quality Assessment:")
    print("-" * 21)
    
    quality_examples = [
        ("Rooftop", 0.95, "Exact building location"),
        ("Interpolated", 0.85, "Between known addresses"),
        ("Approximate", 0.65, "General area/street level"),
        ("Partial", 0.45, "City/ZIP level match")
    ]
    
    for precision, confidence, description in quality_examples:
        print(f"  {precision:<12} {confidence:.2f} - {description}")
    
    # Batch Processing
    print("\n⚡ Batch Processing Features:")
    print("-" * 28)
    print("• Configurable batch sizes")
    print("• Concurrent processing with semaphores")
    print("• Automatic rate limit coordination")
    print("• Progress tracking and error recovery")
    print("• Memory-efficient streaming")
    
    # Sample batch results
    addresses = [
        "1234 N Highland Ave, Los Angeles, CA",
        "6801 Hollywood Blvd, Hollywood, CA", 
        "123 N Beverly Dr, Beverly Hills, CA"
    ]
    
    print(f"\n📋 Sample Batch (3 addresses):")
    for i, addr in enumerate(addresses, 1):
        lat = 34.0522 + i * 0.01
        lng = -118.2437 - i * 0.01
        provider = ["cache", "nominatim", "google"][i-1]
        print(f"  {i}. {addr[:30]}...")
        print(f"     → ({lat:.4f}, {lng:.4f}) via {provider}")
    
    # Statistics
    print("\n📊 Service Statistics:")
    print("-" * 20)
    
    stats = {
        'total_requests': 15780,
        'cache_hits': 5234,
        'nominatim_success': 8932,
        'google_success': 1456,
        'failures': 158
    }
    
    cache_hit_rate = stats['cache_hits'] / stats['total_requests']
    success_rate = (stats['nominatim_success'] + stats['google_success']) / stats['total_requests']
    
    print(f"Total requests: {stats['total_requests']:,}")
    print(f"Cache hit rate: {cache_hit_rate:.1%}")
    print(f"Success rate: {success_rate:.1%}")
    print(f"Provider breakdown:")
    print(f"  • Cache: {stats['cache_hits']:,} ({stats['cache_hits']/stats['total_requests']:.1%})")
    print(f"  • Nominatim: {stats['nominatim_success']:,} ({stats['nominatim_success']/stats['total_requests']:.1%})")
    print(f"  • Google: {stats['google_success']:,} ({stats['google_success']/stats['total_requests']:.1%})")
    
    # Production Deployment
    print("\n🚀 Production Deployment:")
    print("-" * 24)
    print("📋 Dependencies:")
    print("  pip install aiohttp requests redis")
    
    print("\n⚙️ Configuration:")
    print("  export REDIS_URL='redis://localhost:6379'")
    print("  export GOOGLE_GEOCODING_API_KEY='your-key-here'")
    
    print("\n🏗️ Integration Example:")
    print("```python")
    print("from src.geocoding import HierarchicalGeocoder")
    print("")
    print("geocoder = HierarchicalGeocoder(google_api_key='key')")
    print("result = await geocoder.geocode('123 Main St, LA, CA')")
    print("print(f'Coordinates: ({result.latitude}, {result.longitude})')")
    print("```")
    
    print("\n✅ Geocoding Service Ready for Integration!")
    print("   Perfect for DealGenie's real estate data pipeline")

if __name__ == "__main__":
    demo()