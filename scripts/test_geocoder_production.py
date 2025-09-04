#!/usr/bin/env python3
"""
Production-ready test script for the DealGenie Geocoding Service.
This shows the exact interface you would use with real API calls.

Requirements:
    pip install aiohttp requests redis

Usage:
    export GOOGLE_GEOCODING_API_KEY="your-api-key"  # Optional
    export REDIS_URL="redis://localhost:6379"       # Optional
    python test_geocoder_production.py
"""

import asyncio
import os

async def test_geocoder():
    """Test the production geocoding service."""
    
    try:
        # Import the real geocoder
        from src.geocoding.geocoder import HierarchicalGeocoder
        
        print("🌍 DealGenie Hierarchical Geocoding Service - Production Test")
        print("=" * 65)
        
        # Initialize with optional configuration
        google_api_key = os.getenv('GOOGLE_GEOCODING_API_KEY')
        redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
        
        geocoder = HierarchicalGeocoder(
            google_api_key=google_api_key,
            redis_url=redis_url
        )
        
        if google_api_key:
            print("✅ Google API key configured")
        else:
            print("ℹ️  No Google API key - using Nominatim only")
        
        print(f"📍 Single Address Test:")
        print("-" * 23)
        
        # Test single address
        result = await geocoder.geocode("1600 Amphitheatre Parkway, Mountain View, CA")
        
        print(f"Address: {result.formatted_address}")
        print(f"Coordinates: ({result.latitude}, {result.longitude})")
        print(f"Confidence: {result.confidence_score:.3f}")  # Note: real API uses confidence_score
        print(f"Provider: {result.provider.value if result.provider else 'None'}")
        print(f"Status: {result.status.value}")
        print(f"Response time: {result.response_time_ms:.1f}ms")
        print(f"Precision: {result.precision}")
        print(f"Cached: {result.cached}")
        
        print(f"\\n📋 Batch Processing Test:")
        print("-" * 25)
        
        # Test batch processing  
        addresses = [
            "123 N Beverly Dr, Beverly Hills, CA",
            "6801 Hollywood Blvd, Hollywood, CA", 
            "1 World Trade Center Dr, Long Beach, CA",
            "Invalid address xyz123"
        ]
        
        results = await geocoder.geocode_batch(
            addresses, 
            batch_size=2,        # Process 2 at a time
            max_concurrent=2     # Max 2 simultaneous requests
        )
        
        print(f"\\n📊 Results:")
        for addr, result in zip(addresses, results):
            status_icon = "✅" if result.status.value == "success" else "❌"
            provider_info = f"via {result.provider.value}" if result.provider else "failed"
            
            print(f"{status_icon} {addr}")
            print(f"    Confidence: {result.confidence_score:.2f} {provider_info}")
            
            if result.latitude and result.longitude:
                print(f"    Location: ({result.latitude:.6f}, {result.longitude:.6f})")
                if result.precision:
                    print(f"    Precision: {result.precision}")
            
            print(f"    Response: {result.response_time_ms:.1f}ms")
            print()
        
        # Show service statistics
        print(f"📈 Service Statistics:")
        print("-" * 20)
        stats = geocoder.get_stats()
        
        print(f"Total requests: {stats['total_requests']}")
        print(f"Cache hit rate: {stats.get('cache_hit_rate', 0):.1%}")
        print(f"Success rate: {stats.get('success_rate', 0):.1%}")
        
        if 'nominatim_circuit_breaker_state' in stats:
            print(f"Nominatim circuit: {stats['nominatim_circuit_breaker_state']}")
        
        if 'google_circuit_breaker_state' in stats:
            print(f"Google circuit: {stats['google_circuit_breaker_state']}")
        
        if 'google_daily_quota_used' in stats:
            print(f"Google quota used: {stats['google_daily_quota_used']}")
        
        print(f"\\n✅ Production test completed!")
        
    except ImportError as e:
        print("❌ Cannot import geocoding service - missing dependencies")
        print("📋 Install required packages:")
        print("   pip install aiohttp requests redis")
        print("\\n🔧 Or run the mock version:")
        print("   python scripts/test_geocoder_simple.py")
        
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        print("\\nℹ️  This might be due to:")
        print("   • Redis server not running")
        print("   • Network connectivity issues") 
        print("   • API rate limits")
        print("\\n🔧 Try the mock version for development:")
        print("   python scripts/test_geocoder_simple.py")

async def test_with_real_addresses():
    """Test with real LA addresses for DealGenie."""
    
    try:
        from src.geocoding.geocoder import geocode_addresses  # Sync wrapper
        
        print("\\n🏠 DealGenie Real Estate Address Test:")
        print("-" * 38)
        
        # Real LA real estate addresses
        la_addresses = [
            "1234 N Highland Ave, Los Angeles, CA 90028",
            "6801 Hollywood Blvd, Hollywood, CA 90028",
            "123 N Beverly Dr, Beverly Hills, CA 90210", 
            "555 Wilshire Blvd, Los Angeles, CA 90036",
            "1 World Trade Center Dr, Long Beach, CA 90831"
        ]
        
        print(f"Processing {len(la_addresses)} LA real estate addresses...")
        
        # Use synchronous wrapper for convenience
        results = geocode_addresses(
            la_addresses,
            google_api_key=os.getenv('GOOGLE_GEOCODING_API_KEY'),
            batch_size=3
        )
        
        print(f"\\n📍 Geocoded Properties:")
        for i, (addr, result) in enumerate(zip(la_addresses, results), 1):
            print(f"{i}. {addr}")
            
            if result.status.value == "success":
                print(f"   ✅ ({result.latitude:.6f}, {result.longitude:.6f})")
                print(f"   📏 {result.precision} precision, {result.confidence_score:.3f} confidence")
                print(f"   🔧 via {result.provider.value}")
            else:
                print(f"   ❌ Failed to geocode")
            print()
        
        # Calculate success metrics
        successful = [r for r in results if r.status.value == "success"]
        print(f"📊 Success rate: {len(successful)}/{len(results)} ({len(successful)/len(results)*100:.1f}%)")
        
        if successful:
            avg_confidence = sum(r.confidence_score for r in successful) / len(successful)
            print(f"📈 Average confidence: {avg_confidence:.3f}")
            
            # Show geographic bounds  
            lats = [r.latitude for r in successful]
            lngs = [r.longitude for r in successful]
            
            print(f"🗺️  Geographic bounds:")
            print(f"   Latitude: {min(lats):.4f} to {max(lats):.4f}")  
            print(f"   Longitude: {min(lngs):.4f} to {max(lngs):.4f}")
        
    except ImportError:
        print("\\n📋 Real address test requires full installation")
        
if __name__ == "__main__":
    asyncio.run(test_geocoder())
    asyncio.run(test_with_real_addresses())