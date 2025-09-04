#!/bin/bash
# scripts/daily_health_check.sh

set -euo pipefail
echo "=== DealGenie Day 1 Health Check ==="
echo "Date: $(date)"

# Test database connectivity
echo "Testing database..."
sqlite3 ./data/dealgenie.db "SELECT COUNT(*) FROM etl_audit;" > /dev/null && echo "✓ Database OK" || echo "✗ Database FAIL"

# Test address parser
echo "Testing address parser..."
python3 -c "import sys; sys.path.insert(0, 'src'); from normalization.address_parser import AddressParser; p=AddressParser(); r=p.parse('123 Main St'); print('✓ Address parser OK' if r.confidence_score > 0.5 else '✗ Address parser FAIL')"

# Test geocoder
echo "Testing geocoder..."
python3 -c "import sys, asyncio; sys.path.insert(0, 'src'); from geocoding.geocoder import HierarchicalGeocoder, GeocodeStatus; async def _t(): return await HierarchicalGeocoder().geocode('123 Main St, LA, CA'); sys.exit(0 if asyncio.run(_t()).status == GeocodeStatus.SUCCESS else 1)" > /dev/null && echo "✓ Geocoder OK" || echo "✗ Geocoder FAIL"

# Test Redis if available
redis-cli ping > /dev/null 2>&1 && echo "✓ Redis cache OK" || echo "! Redis cache not available (optional)"

echo "=== Health Check Complete ==="