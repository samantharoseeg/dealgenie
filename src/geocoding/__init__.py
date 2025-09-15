"""
DealGenie Geocoding Package

Provides hierarchical geocoding with multiple providers, caching, rate limiting,
and batch processing for Los Angeles real estate data.
"""

from .geocoder import (
    HierarchicalGeocoder,
    GeocodeResult,
    GeocodeStatus,
    GeocodeProvider,
    geocode_address,
    geocode_addresses
)

__all__ = [
    'HierarchicalGeocoder',
    'GeocodeResult', 
    'GeocodeStatus',
    'GeocodeProvider',
    'geocode_address',
    'geocode_addresses'
]
__version__ = '1.0.0'