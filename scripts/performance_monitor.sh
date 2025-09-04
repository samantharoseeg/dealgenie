#!/bin/bash
# scripts/performance_monitor.sh

echo "=== Performance Monitor ==="
python3 -c "
import time
import asyncio
from src.normalization.address_parser import AddressParser
from src.geocoding.geocoder import HierarchicalGeocoder

async def monitor():
    parser = AddressParser()
    geocoder = HierarchicalGeocoder()
    
    # Test 10 addresses
    addresses = ['123 Main St, Los Angeles, CA'] * 10
    
    start = time.time()
    for addr in addresses:
        parser.parse(addr)
    print(f'Address parsing: {len(addresses)/(time.time()-start):.0f} ops/sec')
    
    start = time.time()
    result = await geocoder.geocode('123 Main St, Los Angeles, CA')
    print(f'Geocoding: {1/(time.time()-start):.1f} ops/sec')
    
    stats = geocoder.get_stats()
    print(f'Cache hit rate: {stats.get(\"cache_hit_rate\", 0):.1%}')

asyncio.run(monitor())
"