"""
DealGenie Address Normalization Package

Provides address parsing, normalization, and fuzzy matching capabilities
for Los Angeles real estate data processing.
"""

from .address_parser import AddressParser, ParsedAddress, FuzzyMatcher

__all__ = ['AddressParser', 'ParsedAddress', 'FuzzyMatcher']
__version__ = '1.0.0'