#!/bin/bash
# scripts/daily_health_check.sh

echo "=== DealGenie Day 1 Health Check ==="
echo "Date: $(date)"

# Test database connectivity
echo "Testing database..."
sqlite3 ./data/dealgenie.db "SELECT COUNT(*) FROM etl_audit;" > /dev/null && echo "✓ Database OK" || echo "✗ Database FAIL"

# Test address parser
echo "Testing address parser..."
python3 -c "from src.normalization.address_parser import AddressParser; p=AddressParser(); r=p.parse('123 Main St'); print('✓ Address parser OK' if r.confidence_score > 0.5 else '✗ Address parser FAIL')"

# Test geocoder
echo "Testing geocoder..."
python3 -c "import asyncio; from src.geocoding.geocoder import HierarchicalGeocoder; asyncio.run(HierarchicalGeocoder().geocode('123 Main St, LA, CA'))" > /dev/null && echo "✓ Geocoder OK" || echo "✗ Geocoder FAIL"

# Test Redis if available
redis-cli ping > /dev/null 2>&1 && echo "✓ Redis cache OK" || echo "! Redis cache not available (optional)"

echo "=== Health Check Complete ==="