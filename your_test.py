import asyncio
import sys
import os
sys.path.insert(0, 'src')

from geocoding.geocoder import HierarchicalGeocoder

async def test_geocoder():
    geocoder = HierarchicalGeocoder()
    
    # Test single address
    result = await geocoder.geocode("1600 Amphitheatre Parkway, Mountain View, CA")
    print(f"Address: {result.formatted_address}")
    print(f"Coordinates: ({result.latitude}, {result.longitude})")
    print(f"Confidence: {result.confidence_score}")  # Note: adjusted from your original
    print(f"Provider: {result.provider}")
    
    # Test batch processing
    addresses = [
        "123 N Beverly Dr, Beverly Hills, CA",
        "6801 Hollywood Blvd, Hollywood, CA", 
        "1 World Trade Center, Long Beach, CA",
        "Invalid address xyz123"
    ]
    
    results = await geocoder.geocode_batch(addresses)  # Note: method name adjusted
    for addr, result in zip(addresses, results):
        print(f"{addr} -> {result.confidence_score:.2f} confidence")

# Run the test
asyncio.run(test_geocoder())