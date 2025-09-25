#!/usr/bin/env python3
"""
DealGenie Address Parser Demo Script
Demonstrates the capabilities of the address normalization system.
"""

import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from normalization.address_parser import AddressParser, FuzzyMatcher

def main():
    """Main demo function."""
    print("🏠 DealGenie Address Parser Demonstration")
    print("=" * 60)
    
    parser = AddressParser()
    
    # Test cases covering various LA address formats
    test_addresses = [
        "1234 N Highland Ave, Los Angeles, CA 90028",
        "456 SUNSET BLVD APT 3B, HOLLYWOOD CA 90028",
        "789 w 3rd street, los angeles ca",
        "321 S FIGUEROA ST STE 1500 LOS ANGELES CA 90071", 
        "555 WILSHIRE BLVD FL 39, LOS ANGELES, CA 90036",
        "12345 VENTURA BLVD STUDIO CITY CA 91604",
        "678 1st Street, Los Angeles, CA",
        "999 42nd Avenue, LA CA",
        "123 O'Malley St, Los Angeles CA 90210"
    ]
    
    print("\n📍 Address Parsing Examples:")
    print("-" * 40)
    
    parsed_addresses = []
    for i, addr in enumerate(test_addresses, 1):
        print(f"\n{i}. Input: {addr}")
        parsed = parser.parse(addr)
        parsed_addresses.append(parsed)
        
        print(f"   House Number: {parsed.house_number}")
        print(f"   Street Name: {parsed.street_name}")
        print(f"   Street Suffix: {parsed.street_suffix}")
        print(f"   City: {parsed.city}")
        print(f"   State: {parsed.state}")
        print(f"   ZIP: {parsed.postal_code}")
        print(f"   Confidence: {parsed.confidence_score:.2f}")
        print(f"   Method: {parsed.parsing_method}")
        
        usps_format = parsed.to_usps_format()
        print(f"   USPS Format: {usps_format.replace(chr(10), ' / ')}")
    
    # Fuzzy matching demonstration
    print(f"\n🔍 Fuzzy Matching Demonstration:")
    print("-" * 40)
    
    # Compare similar addresses
    addr1 = parser.parse("1234 N Highland Ave, Los Angeles, CA")
    addr2 = parser.parse("1234 NORTH HIGHLAND AVENUE, LA, CA")
    
    similarity = FuzzyMatcher.address_similarity(addr1, addr2)
    print(f"\nAddress 1: {addr1.to_usps_format().replace(chr(10), ' / ')}")
    print(f"Address 2: {addr2.to_usps_format().replace(chr(10), ' / ')}")
    print(f"Similarity Score: {similarity:.3f}")
    
    # Find best match example
    target = parser.parse("1234 Highland Avenue, Los Angeles")
    candidates = [
        parser.parse("5678 Sunset Blvd, Hollywood CA"),
        parser.parse("1234 HILAND AVE, LOS ANGELES"),  # Typo
        parser.parse("1234 N Highland Ave, LA CA"),    # Close match
        parser.parse("9999 Wilshire Blvd, Beverly Hills CA")
    ]
    
    print(f"\n🎯 Finding Best Match:")
    print(f"Target: {target.to_usps_format().replace(chr(10), ' / ')}")
    print(f"\nCandidates:")
    for i, candidate in enumerate(candidates, 1):
        score = FuzzyMatcher.address_similarity(target, candidate)
        print(f"  {i}. {candidate.to_usps_format().replace(chr(10), ' / ')} (Score: {score:.3f})")
    
    best_match = FuzzyMatcher.find_best_match(target, candidates, threshold=0.7)
    if best_match:
        match, score = best_match
        print(f"\n✅ Best Match: {match.to_usps_format().replace(chr(10), ' / ')} (Score: {score:.3f})")
    else:
        print(f"\n❌ No match found above threshold")
    
    # Distance algorithm comparison
    print(f"\n📏 Distance Algorithm Comparison:")
    print("-" * 40)
    
    str1 = "HIGHLAND"
    str2 = "HILAND"
    
    levenshtein = FuzzyMatcher.levenshtein_distance(str1, str2)
    jaro_winkler = FuzzyMatcher.jaro_winkler_similarity(str1, str2)
    sequence = FuzzyMatcher.sequence_similarity(str1, str2)
    
    print(f"String 1: {str1}")
    print(f"String 2: {str2}")
    print(f"Levenshtein Distance: {levenshtein}")
    print(f"Jaro-Winkler Similarity: {jaro_winkler:.3f}")
    print(f"Sequence Similarity: {sequence:.3f}")
    
    # Summary statistics
    print(f"\n📊 Parsing Statistics:")
    print("-" * 40)
    
    total_parsed = len(parsed_addresses)
    successful_parses = sum(1 for addr in parsed_addresses if addr.confidence_score > 0.5)
    high_confidence = sum(1 for addr in parsed_addresses if addr.confidence_score > 0.8)
    
    print(f"Total addresses parsed: {total_parsed}")
    print(f"Successful parses (>50% confidence): {successful_parses}")
    print(f"High confidence parses (>80% confidence): {high_confidence}")
    print(f"Success rate: {successful_parses/total_parsed*100:.1f}%")
    
    # Method breakdown
    methods = {}
    for addr in parsed_addresses:
        methods[addr.parsing_method] = methods.get(addr.parsing_method, 0) + 1
    
    print(f"\nParsing method breakdown:")
    for method, count in methods.items():
        print(f"  {method}: {count} ({count/total_parsed*100:.1f}%)")
    
    print(f"\n✅ Demo completed successfully!")
    print(f"The address parser is ready for integration with DealGenie's ETL pipeline.")

if __name__ == "__main__":
    main()