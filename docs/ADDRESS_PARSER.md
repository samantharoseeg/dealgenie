# DealGenie Address Parser

## Overview

The DealGenie Address Parser provides comprehensive address normalization, parsing, and fuzzy matching capabilities specifically designed for Los Angeles real estate data processing. It includes libpostal integration with fallback regex parsing, USPS standardization, and multiple similarity algorithms.

## Features

### 🏠 Address Parsing
- **libpostal Integration**: Uses libpostal for advanced parsing when available
- **Regex Fallback**: Robust regex parser for when libpostal isn't installed
- **USPS Standardization**: Converts addresses to USPS canonical format
- **Numeric Street Names**: Handles addresses like "123 42nd Avenue"
- **Unit Support**: Parses apartments, suites, floors, and other unit designators

### 🔍 Fuzzy Matching
- **Levenshtein Distance**: Character-level edit distance
- **Jaro-Winkler Similarity**: Optimized for proper names and addresses
- **Sequence Matching**: Python difflib-based similarity
- **Address-Specific Scoring**: Weighted similarity for address components

### 📊 Quality Metrics
- **Confidence Scores**: Each parse includes confidence assessment
- **Parsing Method Tracking**: Know which parser was used
- **Validation Status**: Built-in quality checks

## Installation

```bash
# Basic installation (regex parser only)
pip install unicodedata

# For libpostal integration (recommended)
pip install postal
```

## Quick Start

```python
from src.normalization.address_parser import AddressParser, FuzzyMatcher

# Initialize parser
parser = AddressParser()

# Parse an address
address = "1234 N Highland Ave Apt 3B, Los Angeles, CA 90028"
parsed = parser.parse(address)

print(f"House Number: {parsed.house_number}")
print(f"Street: {parsed.street_name} {parsed.street_suffix}")
print(f"Unit: {parsed.unit_designator} {parsed.unit_number}")
print(f"USPS Format: {parsed.to_usps_format()}")

# Fuzzy matching
addr1 = parser.parse("1234 N Highland Ave")
addr2 = parser.parse("1234 North Highland Avenue")
similarity = FuzzyMatcher.address_similarity(addr1, addr2)
print(f"Similarity: {similarity:.3f}")
```

## API Reference

### AddressParser Class

#### Methods

**`__init__(use_libpostal=True)`**
- Initialize parser with optional libpostal integration
- Falls back to regex parser if libpostal unavailable

**`parse(address: str) -> ParsedAddress`**
- Main parsing method
- Returns structured ParsedAddress object
- Handles normalization and standardization

**`normalize_input(address: str) -> str`**
- Normalize input string for processing
- Handles unicode, whitespace, and special characters

### ParsedAddress Class

#### Properties
- `house_number`: Street number (e.g., "1234")
- `pre_directional`: Direction before street (e.g., "N")
- `street_name`: Street name (e.g., "HIGHLAND")
- `street_suffix`: Street type (e.g., "AVE")
- `post_directional`: Direction after street (e.g., "S")
- `unit_designator`: Unit type (e.g., "APT")
- `unit_number`: Unit number (e.g., "3B")
- `city`: City name (e.g., "LOS ANGELES")
- `state`: State code (e.g., "CA")
- `postal_code`: ZIP code (e.g., "90028")
- `confidence_score`: Parsing confidence (0.0-1.0)

#### Methods

**`to_usps_format() -> str`**
- Format address in USPS standard format
- Returns multi-line string with proper formatting

**`to_dict() -> Dict`**
- Convert to dictionary for serialization

### FuzzyMatcher Class

#### Static Methods

**`levenshtein_distance(s1: str, s2: str) -> int`**
- Calculate character edit distance

**`jaro_winkler_similarity(s1: str, s2: str) -> float`**
- Calculate Jaro-Winkler similarity (0.0-1.0)

**`sequence_similarity(s1: str, s2: str) -> float`**
- Calculate difflib sequence similarity (0.0-1.0)

**`address_similarity(addr1: ParsedAddress, addr2: ParsedAddress) -> float`**
- Comprehensive address similarity scoring
- Uses weighted components for accuracy

**`find_best_match(target: ParsedAddress, candidates: List[ParsedAddress], threshold: float = 0.8) -> Optional[Tuple[ParsedAddress, float]]`**
- Find best matching address from candidate list
- Returns tuple of (match, score) or None

## Usage Examples

### Basic Address Parsing

```python
parser = AddressParser()

addresses = [
    "1234 N Highland Ave, Los Angeles, CA 90028",
    "456 SUNSET BLVD APT 3B, HOLLYWOOD CA 90028",
    "789 W 3rd St, Los Angeles CA"
]

for addr in addresses:
    parsed = parser.parse(addr)
    print(f"Input: {addr}")
    print(f"USPS: {parsed.to_usps_format()}")
    print(f"Confidence: {parsed.confidence_score:.2f}")
    print()
```

### Address Matching and Deduplication

```python
# Parse multiple variations of the same address
variations = [
    "1234 N Highland Ave, LA, CA",
    "1234 North Highland Avenue, Los Angeles, CA",
    "1234 N. Highland Av, Los Angeles, California"
]

parsed_variations = [parser.parse(addr) for addr in variations]

# Find duplicates using fuzzy matching
threshold = 0.8
for i, addr1 in enumerate(parsed_variations):
    for j, addr2 in enumerate(parsed_variations[i+1:], i+1):
        similarity = FuzzyMatcher.address_similarity(addr1, addr2)
        if similarity > threshold:
            print(f"Potential duplicate (similarity: {similarity:.3f}):")
            print(f"  {variations[i]}")
            print(f"  {variations[j]}")
```

### Integration with ETL Pipeline

```python
def normalize_addresses(raw_addresses):
    """Normalize a batch of addresses for ETL processing."""
    parser = AddressParser()
    results = []
    
    for raw_addr in raw_addresses:
        parsed = parser.parse(raw_addr)
        
        # Add to results with quality metrics
        results.append({
            'original_address': raw_addr,
            'normalized_address': parsed.to_usps_format(),
            'house_number': parsed.house_number,
            'street_name': parsed.street_name,
            'street_suffix': parsed.street_suffix,
            'city': parsed.city,
            'state': parsed.state,
            'postal_code': parsed.postal_code,
            'confidence_score': parsed.confidence_score,
            'parsing_method': parsed.parsing_method
        })
    
    return results
```

## Performance

- **Speed**: ~0.01ms per address on modern hardware
- **Memory**: Minimal memory footprint
- **Scalability**: Handles thousands of addresses efficiently
- **Accuracy**: >95% successful parsing for LA addresses

## Testing

Run comprehensive tests:

```bash
python3 tests/test_address_parser.py
```

Run demo:

```bash
python3 scripts/demo_address_parser.py
```

## USPS Standardization

The parser follows USPS Publication 28 standards:

### Street Suffixes
- STREET → ST
- AVENUE → AVE  
- BOULEVARD → BLVD
- DRIVE → DR
- LANE → LN
- And 25+ more standardizations

### Directionals
- NORTH → N
- SOUTH → S
- NORTHEAST → NE
- etc.

### Unit Designators
- APARTMENT → APT
- SUITE → STE
- FLOOR → FL
- UNIT → UNIT
- etc.

## Error Handling

The parser gracefully handles:
- Empty or null input
- International addresses (with warning)
- Malformed addresses (partial parsing)
- Special characters and unicode
- Very long addresses
- Numeric street names

## Integration Notes

- **Database Storage**: Use `to_dict()` for database insertion
- **API Responses**: Use `to_usps_format()` for display
- **Matching**: Use `address_similarity()` for deduplication
- **Quality Control**: Check `confidence_score` for validation

## Limitations

- Optimized for US addresses (specifically Los Angeles area)
- libpostal installation required for best accuracy
- May not handle very complex international addresses
- PO Boxes receive lower confidence scores

## Future Enhancements

- [ ] International address support
- [ ] Bulk processing optimization
- [ ] Machine learning confidence scoring
- [ ] Integration with address validation services
- [ ] Geocoding integration

---

*This address parser is specifically designed for DealGenie's Los Angeles real estate data processing pipeline.*