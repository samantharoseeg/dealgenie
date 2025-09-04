# DealGenie Hierarchical Geocoding Service

## Overview

The DealGenie Geocoding Service provides robust, production-ready geocoding with hierarchical provider fallback, Redis caching, rate limiting, circuit breaker protection, and efficient batch processing. Designed specifically for Los Angeles real estate data processing.

## Features

### 🌍 **Hierarchical Provider Strategy**
- **Primary**: Nominatim (OpenStreetMap) - Free, rate-limited
- **Fallback**: Google Maps API - High accuracy, quota-limited  
- **Cache**: Redis - Instant response, 1-week TTL

### 🛡️ **Reliability Patterns**
- **Circuit Breakers**: Protect against service failures
- **Rate Limiting**: Token bucket algorithm prevents quota exhaustion
- **Retry Logic**: Exponential backoff with configurable attempts
- **Quota Management**: Daily usage tracking and limits

### ⚡ **Performance Features**
- **Batch Processing**: Concurrent geocoding with semaphore control
- **Redis Caching**: Sub-millisecond cache hits with MD5 key hashing
- **Async Architecture**: Non-blocking I/O for maximum throughput
- **Memory Efficient**: Streaming batch processing

### 📊 **Quality Metrics**
- **Confidence Scores**: 0.0-1.0 based on precision and match type
- **Precision Levels**: Rooftop, interpolated, approximate
- **Response Tracking**: Provider attribution and timing
- **Statistics**: Hit rates, success rates, error tracking

## Installation

```bash
# Core dependencies
pip install aiohttp requests redis

# Optional: For development and testing
pip install pytest pytest-asyncio
```

### Redis Setup

```bash
# Install Redis
brew install redis  # macOS
sudo apt-get install redis-server  # Ubuntu

# Start Redis server
redis-server

# Test connection
redis-cli ping  # Should return PONG
```

## Quick Start

### Basic Usage

```python
import asyncio
from geocoding.geocoder import HierarchicalGeocoder

async def main():
    # Initialize with optional Google API key
    geocoder = HierarchicalGeocoder(
        google_api_key="your-api-key-here",  # Optional
        redis_url="redis://localhost:6379"
    )
    
    # Geocode single address
    result = await geocoder.geocode("1234 N Highland Ave, Los Angeles, CA")
    
    print(f"Coordinates: ({result.latitude}, {result.longitude})")
    print(f"Confidence: {result.confidence_score:.3f}")
    print(f"Provider: {result.provider.value}")

asyncio.run(main())
```

### Batch Geocoding

```python
async def batch_example():
    geocoder = HierarchicalGeocoder()
    
    addresses = [
        "6801 Hollywood Blvd, Hollywood, CA",
        "123 N Beverly Dr, Beverly Hills, CA", 
        "1 World Trade Center Dr, Long Beach, CA"
    ]
    
    results = await geocoder.geocode_batch(
        addresses,
        batch_size=10,           # Process 10 at a time
        max_concurrent=5,        # Max 5 simultaneous requests
        use_cache=True          # Use Redis cache
    )
    
    for addr, result in zip(addresses, results):
        if result.status.value == "success":
            print(f"{addr} → ({result.latitude:.6f}, {result.longitude:.6f})")
        else:
            print(f"{addr} → Failed: {result.status.value}")

asyncio.run(batch_example())
```

### Synchronous Wrappers

```python
from geocoding.geocoder import geocode_address, geocode_addresses

# Single address
result = geocode_address("123 Main St, Los Angeles, CA")

# Multiple addresses  
results = geocode_addresses([
    "123 Main St, LA, CA",
    "456 Oak Ave, LA, CA"
], batch_size=5)
```

## API Reference

### HierarchicalGeocoder

#### Constructor

```python
HierarchicalGeocoder(
    google_api_key: Optional[str] = None,
    redis_url: str = "redis://localhost:6379", 
    user_agent: str = "DealGenie/1.0"
)
```

#### Methods

**`async geocode(address: str, use_cache: bool = True, max_retries: int = 2) -> GeocodeResult`**

Geocode single address with hierarchical fallback.

**`async geocode_batch(addresses: List[str], batch_size: int = 10, max_concurrent: int = 5, use_cache: bool = True) -> List[GeocodeResult]`**

Batch geocode multiple addresses efficiently.

**`get_stats() -> Dict[str, Any]`**

Get service statistics including hit rates and provider usage.

**`reset_stats()`**

Reset statistics counters.

### GeocodeResult

#### Properties

```python
@dataclass
class GeocodeResult:
    # Coordinates
    latitude: Optional[float]
    longitude: Optional[float]
    
    # Address components
    formatted_address: Optional[str]
    street_number: Optional[str]
    street_name: Optional[str]
    city: Optional[str]
    state: Optional[str]
    postal_code: Optional[str]
    country: Optional[str]
    
    # Quality metrics
    confidence_score: float         # 0.0 - 1.0
    provider: GeocodeProvider       # CACHE, NOMINATIM, GOOGLE
    status: GeocodeStatus          # SUCCESS, FAILED, etc.
    precision: Optional[str]        # rooftop, interpolated, approximate
    match_type: Optional[str]       # exact, partial, fuzzy
    
    # Metadata
    response_time_ms: float
    cached: bool
    timestamp: Optional[datetime]
```

#### Methods

**`to_dict() -> Dict`**

Convert result to dictionary for serialization.

### Enums

#### GeocodeStatus
- `SUCCESS`: Geocoding successful
- `PARTIAL`: Partial/low-confidence result
- `FAILED`: Geocoding failed
- `RATE_LIMITED`: Hit provider rate limit
- `QUOTA_EXCEEDED`: Exceeded daily quota
- `CIRCUIT_OPEN`: Circuit breaker protection active

#### GeocodeProvider
- `CACHE`: Result from Redis cache
- `NOMINATIM`: OpenStreetMap Nominatim service
- `GOOGLE`: Google Maps Geocoding API

## Configuration

### Environment Variables

```bash
# Redis connection
export REDIS_URL="redis://localhost:6379"

# Google Maps API (optional but recommended)
export GOOGLE_GEOCODING_API_KEY="your-api-key-here"

# Optional: Custom user agent
export GEOCODING_USER_AGENT="YourApp/1.0"
```

### Rate Limiting

Default rate limits (configurable):
- **Nominatim**: 1 request/second, burst of 3
- **Google**: 50 requests/second, burst of 10

### Cache Configuration

- **TTL**: 7 days default
- **Key Format**: `geocode:{md5_hash}`
- **Storage**: JSON serialized GeocodeResult

## Quality Assessment

### Confidence Scoring

| Precision Level | Confidence Range | Description |
|----------------|------------------|-------------|
| Rooftop        | 0.90 - 1.00     | Exact building location |
| Interpolated   | 0.80 - 0.89     | Between known addresses |
| Approximate    | 0.60 - 0.79     | Street/area level |
| Partial        | 0.40 - 0.59     | City/ZIP level |

### Provider Characteristics

| Provider   | Accuracy | Speed | Cost | Rate Limit |
|-----------|----------|-------|------|------------|
| Cache     | N/A      | <1ms  | Free | None       |
| Nominatim | Good     | ~200ms| Free | 1/sec      |
| Google    | Excellent| ~150ms| Paid | 50/sec     |

## Monitoring & Statistics

### Available Metrics

```python
stats = geocoder.get_stats()
print(stats)

# Output:
{
    'total_requests': 1000,
    'cache_hits': 300,
    'nominatim_success': 500,
    'google_success': 150,
    'failures': 50,
    'cache_hit_rate': 0.30,
    'success_rate': 0.95,
    'nominatim_circuit_breaker_state': 'closed',
    'google_circuit_breaker_state': 'closed',
    'google_daily_quota_used': 150
}
```

### Circuit Breaker States

- **Closed**: Normal operation
- **Open**: Too many failures, requests blocked
- **Half-Open**: Testing if service recovered

## Error Handling

### Common Issues

1. **Redis Connection Failed**
   ```python
   # Service continues without cache
   # Check Redis server status
   redis-cli ping
   ```

2. **Rate Limit Exceeded**
   ```python
   # Circuit breaker opens temporarily
   # Wait for rate limit reset
   if result.status == GeocodeStatus.RATE_LIMITED:
       await asyncio.sleep(limiter.time_until_available())
   ```

3. **No Results Found**
   ```python
   if result.status == GeocodeStatus.FAILED:
       print("Address could not be geocoded")
   ```

### Retry Strategy

- **Max Retries**: 2 per provider
- **Backoff**: Exponential with jitter
- **Circuit Breaker**: Opens after 5 failures

## Performance Optimization

### Batch Processing Tips

```python
# Optimal batch configuration for LA addresses
results = await geocoder.geocode_batch(
    addresses,
    batch_size=20,        # Balance memory vs throughput
    max_concurrent=10     # Don't overwhelm services
)
```

### Memory Management

- Process large datasets in chunks
- Use streaming for very large batches
- Monitor memory usage during processing

### Caching Strategy

- **Cache Hit Rate**: Aim for >30% for repeated addresses
- **TTL**: Balance freshness vs performance (7 days default)
- **Key Distribution**: MD5 ensures even distribution

## Integration with DealGenie

### ETL Pipeline Usage

```python
from geocoding.geocoder import HierarchicalGeocoder
from normalization.address_parser import AddressParser

async def process_properties(raw_addresses):
    parser = AddressParser()
    geocoder = HierarchicalGeocoder(google_api_key=api_key)
    
    # Step 1: Parse and normalize addresses
    normalized = [parser.parse(addr).to_usps_format() for addr in raw_addresses]
    
    # Step 2: Batch geocode
    results = await geocoder.geocode_batch(normalized, batch_size=50)
    
    # Step 3: Extract coordinates for database
    return [{
        'original_address': orig,
        'normalized_address': norm, 
        'latitude': result.latitude,
        'longitude': result.longitude,
        'confidence': result.confidence_score,
        'geocoding_provider': result.provider.value
    } for orig, norm, result in zip(raw_addresses, normalized, results)]
```

### Database Integration

```sql
-- Add geocoding results to properties table
ALTER TABLE properties ADD COLUMN latitude DECIMAL(10, 8);
ALTER TABLE properties ADD COLUMN longitude DECIMAL(11, 8);
ALTER TABLE properties ADD COLUMN geocoding_confidence DECIMAL(3, 2);
ALTER TABLE properties ADD COLUMN geocoding_provider VARCHAR(20);
ALTER TABLE properties ADD COLUMN geocoded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Create spatial index for geographic queries
CREATE INDEX idx_properties_location ON properties(latitude, longitude);
```

## Testing

### Unit Tests

```bash
# Run all geocoding tests
python -m pytest tests/test_geocoder.py -v

# Run specific test
python -m pytest tests/test_geocoder.py::TestHierarchicalGeocoder -v
```

### Integration Testing

```python
# Test with real services (requires API keys)
async def integration_test():
    geocoder = HierarchicalGeocoder(google_api_key="real-key")
    
    test_addresses = [
        "1234 N Highland Ave, Los Angeles, CA 90028",
        "6801 Hollywood Blvd, Hollywood, CA 90028"
    ]
    
    results = await geocoder.geocode_batch(test_addresses)
    
    for result in results:
        assert result.status == GeocodeStatus.SUCCESS
        assert result.latitude is not None
        assert result.longitude is not None
        assert result.confidence_score > 0.5
```

## Deployment

### Production Checklist

- [ ] Redis server configured and running
- [ ] Google Maps API key configured (if using)
- [ ] Rate limits configured for your usage
- [ ] Monitoring and logging set up
- [ ] Error alerting configured
- [ ] Cache TTL optimized for your use case

### Docker Configuration

```dockerfile
# Add to your Dockerfile
RUN pip install aiohttp requests redis

# Redis service
services:
  redis:
    image: redis:alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
```

### Scaling Considerations

- **Horizontal**: Multiple instances share Redis cache
- **Rate Limits**: Distribute across API keys if needed
- **Geographic**: Consider regional Nominatim instances
- **Monitoring**: Track quotas and circuit breaker states

## Troubleshooting

### Common Issues

1. **Slow Performance**
   - Check Redis connection latency
   - Verify rate limiting isn't too restrictive
   - Monitor circuit breaker states

2. **Low Success Rate**
   - Verify address quality/formatting
   - Check API key validity and quotas
   - Review geographic bounds configuration

3. **Memory Usage**
   - Reduce batch sizes
   - Implement streaming for large datasets
   - Monitor Redis memory usage

### Debug Mode

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Enables detailed request/response logging
geocoder = HierarchicalGeocoder()
```

## Roadmap

### Future Enhancements

- [ ] Additional providers (HERE, MapBox)
- [ ] Machine learning confidence scoring
- [ ] Address validation integration
- [ ] Geographical clustering optimization
- [ ] Real-time monitoring dashboard
- [ ] Automatic API key rotation

---

*The DealGenie Geocoding Service provides enterprise-grade geocoding capabilities specifically designed for real estate data processing workflows.*