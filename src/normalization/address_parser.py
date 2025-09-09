#!/usr/bin/env python3
"""
DealGenie Address Parser
Integrates libpostal for address parsing/normalization with USPS canonical formatting
and fuzzy matching capabilities for Los Angeles real estate data processing.
"""

import re
import logging
from typing import Dict, List, Tuple, Optional, Union
from dataclasses import dataclass, asdict
from difflib import SequenceMatcher
import unicodedata

try:
    import postal
    from postal.expand import expand_address
    from postal.parser import parse_address
    POSTAL_AVAILABLE = True
except ImportError:
    POSTAL_AVAILABLE = False
    logging.warning("libpostal not available. Install with: pip install postal")

# USPS Standard Abbreviations
USPS_STREET_SUFFIXES = {
    'avenue': 'AVE', 'ave': 'AVE', 'av': 'AVE',
    'boulevard': 'BLVD', 'blvd': 'BLVD', 'boul': 'BLVD', 'boulv': 'BLVD',
    'circle': 'CIR', 'cir': 'CIR', 'circ': 'CIR', 'circl': 'CIR',
    'court': 'CT', 'ct': 'CT', 'crt': 'CT',
    'drive': 'DR', 'dr': 'DR', 'drv': 'DR', 'driv': 'DR',
    'lane': 'LN', 'ln': 'LN', 'la': 'LN',
    'place': 'PL', 'pl': 'PL', 'plc': 'PL',
    'road': 'RD', 'rd': 'RD',
    'street': 'ST', 'st': 'ST', 'str': 'ST', 'strt': 'ST',
    'terrace': 'TER', 'ter': 'TER', 'terr': 'TER',
    'trail': 'TRL', 'trl': 'TRL', 'tr': 'TRL',
    'way': 'WAY', 'wy': 'WAY',
    'plaza': 'PLZ', 'plz': 'PLZ', 'plza': 'PLZ',
    'parkway': 'PKWY', 'pkwy': 'PKWY', 'parkwy': 'PKWY', 'pkw': 'PKWY'
}

USPS_DIRECTIONALS = {
    'north': 'N', 'n': 'N', 'no': 'N',
    'south': 'S', 's': 'S', 'so': 'S',
    'east': 'E', 'e': 'E', 'ea': 'E',
    'west': 'W', 'w': 'W', 'we': 'W',
    'northeast': 'NE', 'ne': 'NE', 'n.e.': 'NE',
    'northwest': 'NW', 'nw': 'NW', 'n.w.': 'NW',
    'southeast': 'SE', 'se': 'SE', 's.e.': 'SE',
    'southwest': 'SW', 'sw': 'SW', 's.w.': 'SW'
}

USPS_UNIT_DESIGNATORS = {
    'apartment': 'APT', 'apt': 'APT', 'ap': 'APT',
    'building': 'BLDG', 'bldg': 'BLDG', 'bld': 'BLDG',
    'floor': 'FL', 'fl': 'FL', 'flr': 'FL',
    'suite': 'STE', 'ste': 'STE', 'su': 'STE',
    'unit': 'UNIT', 'un': 'UNIT', 'unt': 'UNIT',
    'room': 'RM', 'rm': 'RM',
    'office': 'OFC', 'ofc': 'OFC', 'off': 'OFC',
    'penthouse': 'PH', 'ph': 'PH'
}

@dataclass
class ParsedAddress:
    """Structured address components with USPS standardization."""
    house_number: Optional[str] = None
    pre_directional: Optional[str] = None
    street_name: Optional[str] = None
    street_suffix: Optional[str] = None
    post_directional: Optional[str] = None
    unit_designator: Optional[str] = None
    unit_number: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    
    # Quality metrics
    confidence_score: float = 1.0
    normalized_input: Optional[str] = None
    parsing_method: str = 'unknown'
    
    def to_usps_format(self) -> str:
        """Format address in USPS standard format."""
        parts = []
        
        if self.house_number:
            parts.append(self.house_number)
        if self.pre_directional:
            parts.append(self.pre_directional)
        if self.street_name:
            parts.append(self.street_name.upper())
        if self.street_suffix:
            parts.append(self.street_suffix)
        if self.post_directional:
            parts.append(self.post_directional)
        if self.unit_designator and self.unit_number:
            parts.append(f"{self.unit_designator} {self.unit_number}")
        
        street_line = ' '.join(parts)
        
        address_lines = [street_line]
        if self.city and self.state:
            city_state = f"{self.city.upper()}, {self.state.upper()}"
            if self.postal_code:
                city_state += f" {self.postal_code}"
            address_lines.append(city_state)
        
        return '\n'.join(address_lines)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return asdict(self)

class AddressParser:
    """
    Advanced address parser with libpostal integration and USPS standardization.
    """
    
    def __init__(self, use_libpostal: bool = True):
        self.use_libpostal = use_libpostal and POSTAL_AVAILABLE
        self.logger = logging.getLogger(__name__)
        
        if self.use_libpostal:
            self.logger.info("libpostal integration enabled")
        else:
            self.logger.warning("Using fallback regex parser - libpostal not available")
    
    def normalize_input(self, address: str) -> str:
        """
        Normalize input address string for processing.
        """
        if not address:
            return ""
        
        # Unicode normalization
        address = unicodedata.normalize('NFKD', address)
        
        # Remove excessive whitespace
        address = re.sub(r'\s+', ' ', address.strip())
        
        # Common cleanup patterns for LA addresses
        address = re.sub(r'[^\w\s\-\#\.\/]', ' ', address)  # Keep basic punctuation
        address = re.sub(r'\.{2,}', '.', address)  # Multiple periods to single
        address = re.sub(r'\s+', ' ', address.strip())  # Final whitespace cleanup
        
        return address
    
    def parse(self, address: str) -> ParsedAddress:
        """
        Parse address using libpostal or fallback regex parser.
        """
        if not address or not address.strip():
            return ParsedAddress(confidence_score=0.0, parsing_method='empty_input')
        
        normalized = self.normalize_input(address)
        
        if self.use_libpostal:
            return self._parse_with_libpostal(normalized)
        else:
            return self._parse_with_regex(normalized)
    
    def _parse_with_libpostal(self, address: str) -> ParsedAddress:
        """Parse using libpostal library."""
        try:
            parsed = parse_address(address)
            components = {label: value for value, label in parsed}
            
            result = ParsedAddress(
                house_number=components.get('house_number'),
                pre_directional=self._standardize_directional(components.get('road')),
                street_name=self._extract_street_name(components.get('road')),
                street_suffix=self._standardize_street_suffix(components.get('road')),
                post_directional=None,  # libpostal typically doesn't separate this
                unit_designator=self._extract_unit_designator(components.get('unit')),
                unit_number=self._extract_unit_number(components.get('unit')),
                city=components.get('city'),
                state=self._standardize_state(components.get('state')),
                postal_code=components.get('postcode'),
                confidence_score=self._calculate_confidence(components),
                normalized_input=address,
                parsing_method='libpostal'
            )
            
            return self._post_process(result)
            
        except Exception as e:
            self.logger.warning(f"libpostal parsing failed: {e}, falling back to regex")
            return self._parse_with_regex(address)
    
    def _parse_with_regex(self, address: str) -> ParsedAddress:
        """Fallback regex-based parser for when libpostal is unavailable."""
        
        # Try enhanced pattern first (handles # units and multi-word cities)
        result = self._try_enhanced_regex(address)
        if result and result.confidence_score > 0.6:
            return result
        
        # Fallback to original pattern
        return self._try_basic_regex(address)
    
    def _try_enhanced_regex(self, address: str) -> ParsedAddress:
        """Enhanced parsing for complex LA addresses with step-by-step extraction."""
        
        # Start with empty result
        result = ParsedAddress(
            normalized_input=address,
            parsing_method='enhanced_regex',
            confidence_score=0.3
        )
        
        addr_upper = address.upper()
        remaining = addr_upper
        
        # 1. Extract house number
        house_match = re.match(r'^\s*(\d+[A-Z]?(?:\-\d+)?)\s+', remaining)
        if house_match:
            result.house_number = house_match.group(1)
            remaining = remaining[house_match.end():]
            result.confidence_score += 0.2
        
        # 2. Extract pre-directional
        pre_dir_match = re.match(r'^(N|S|E|W|NE|NW|SE|SW|NORTH|SOUTH|EAST|WEST|NORTHEAST|NORTHWEST|SOUTHEAST|SOUTHWEST)\.?\s+', remaining)
        if pre_dir_match:
            result.pre_directional = self._standardize_directional(pre_dir_match.group(1))
            remaining = remaining[pre_dir_match.end():]
            result.confidence_score += 0.1
        
        # 3. Extract unit with # notation
        unit_hash_match = re.search(r'#\s*([A-Z0-9\-]+)', remaining)
        if unit_hash_match:
            result.unit_designator = "UNIT"
            result.unit_number = unit_hash_match.group(1)
            remaining = re.sub(r'#\s*[A-Z0-9\-]+', '', remaining)
            result.confidence_score += 0.1
        
        # 4. Extract postal code
        zip_match = re.search(r'\b(\d{5}(?:\-\d{4})?)\s*$', remaining)
        if zip_match:
            result.postal_code = zip_match.group(1)
            remaining = remaining[:zip_match.start()].strip()
            result.confidence_score += 0.1
        
        # 5. Extract state
        state_match = re.search(r',?\s*(CA|CALIFORNIA)\s*$', remaining)
        if state_match:
            result.state = 'CA'
            remaining = remaining[:state_match.start()].strip()
            result.confidence_score += 0.05
        
        # 6. Extract city (everything after last comma, but before state)
        city_match = re.search(r',\s*([A-Z][A-Z\s]*[A-Z]|[A-Z])\s*$', remaining)
        if city_match:
            result.city = self._clean_city(city_match.group(1))
            remaining = remaining[:city_match.start()].strip()
            result.confidence_score += 0.1
        
        # 7. Extract street suffix
        suffix_match = re.search(r'\b(ST|AVE|BLVD|DR|LN|CT|PL|RD|WAY|CIR|TER|TRL|PKWY|PLZ|STREET|AVENUE|BOULEVARD|DRIVE|LANE|COURT|PLACE|ROAD|CIRCLE|TERRACE|TRAIL|PARKWAY|PLAZA)\.?\s*$', remaining)
        if suffix_match:
            result.street_suffix = self._standardize_street_suffix(suffix_match.group(1))
            remaining = remaining[:suffix_match.start()].strip()
            result.confidence_score += 0.15
        
        # 8. Extract post-directional
        post_dir_match = re.search(r'\b(N|S|E|W|NE|NW|SE|SW|NORTH|SOUTH|EAST|WEST|NORTHEAST|NORTHWEST|SOUTHEAST|SOUTHWEST)\.?\s*$', remaining)
        if post_dir_match:
            result.post_directional = self._standardize_directional(post_dir_match.group(1))
            remaining = remaining[:post_dir_match.start()].strip()
            result.confidence_score += 0.05
        
        # 9. Extract unit designation (if not already found with #)
        if not result.unit_number:
            unit_match = re.search(r'\b(APT|APARTMENT|STE|SUITE|UNIT|BLDG|BUILDING|FL|FLOOR|RM|ROOM)\.?\s+([A-Z0-9\-]+)\s*$', remaining)
            if unit_match:
                result.unit_designator = self._standardize_unit_designator(unit_match.group(1))
                result.unit_number = unit_match.group(2)
                remaining = remaining[:unit_match.start()].strip()
                result.confidence_score += 0.1
        
        # 10. What's left should be street name
        if remaining:
            result.street_name = self._clean_street_name(remaining)
            result.confidence_score += 0.15
        
        return self._post_process(result)
    
    def _try_basic_regex(self, address: str) -> ParsedAddress:
        """Basic regex pattern for standard addresses."""
        
        # Enhanced regex pattern for LA addresses
        pattern = re.compile(
            r'^\s*'
            r'(?P<house_number>\d+[A-Z]?(?:\-\d+)?)\s*'
            r'(?P<pre_dir>(?:N|S|E|W|NE|NW|SE|SW|NORTH|SOUTH|EAST|WEST|NORTHEAST|NORTHWEST|SOUTHEAST|SOUTHWEST)\.?\s+)?'
            r'(?P<street_name>(?:[A-Z0-9][A-Z0-9\s\-\.\']*?))\s+'
            r'(?P<street_suffix>(?:ST|AVE|BLVD|DR|LN|CT|PL|RD|WAY|CIR|TER|TRL|PKWY|PLZ|STREET|AVENUE|BOULEVARD|DRIVE|LANE|COURT|PLACE|ROAD|CIRCLE|TERRACE|TRAIL|PARKWAY|PLAZA)\.?)\s*'
            r'(?P<post_dir>(?:N|S|E|W|NE|NW|SE|SW|NORTH|SOUTH|EAST|WEST|NORTHEAST|NORTHWEST|SOUTHEAST|SOUTHWEST)\.?\s*)?'
            r'(?:(?P<unit_designator>(?:APT|APARTMENT|STE|SUITE|UNIT|BLDG|BUILDING|FL|FLOOR|RM|ROOM)\.?\s*)?'
            r'(?P<unit_number>[A-Z0-9\-]+)\s*)?'
            r'(?:,?\s*(?P<city>[A-Z\s]+?)\s*,?\s*)?'
            r'(?P<state>CA|CALIFORNIA)?\s*'
            r'(?P<postal_code>\d{5}(?:\-\d{4})?)?'
            r'\s*$',
            re.IGNORECASE
        )
        
        match = pattern.match(address.upper())
        
        if match:
            groups = match.groupdict()
            
            result = ParsedAddress(
                house_number=groups.get('house_number'),
                pre_directional=self._standardize_directional(groups.get('pre_dir')),
                street_name=self._clean_street_name(groups.get('street_name')),
                street_suffix=self._standardize_street_suffix(groups.get('street_suffix')),
                post_directional=self._standardize_directional(groups.get('post_dir')),
                unit_designator=self._standardize_unit_designator(groups.get('unit_designator')),
                unit_number=groups.get('unit_number'),
                city=self._clean_city(groups.get('city')),
                state=self._standardize_state(groups.get('state')),
                postal_code=groups.get('postal_code'),
                confidence_score=0.85,  # High confidence for regex match
                normalized_input=address,
                parsing_method='regex'
            )
        else:
            # Partial parsing for difficult addresses
            result = self._partial_parse(address)
        
        return self._post_process(result)
    
    def _partial_parse(self, address: str) -> ParsedAddress:
        """Attempt partial parsing when full regex fails."""
        result = ParsedAddress(
            normalized_input=address,
            parsing_method='partial_regex',
            confidence_score=0.3
        )
        
        # Try to extract house number
        house_match = re.search(r'\b(\d+[A-Z]?(?:\-\d+)?)\b', address)
        if house_match:
            result.house_number = house_match.group(1)
            result.confidence_score += 0.2
        
        # Try to extract postal code
        zip_match = re.search(r'\b(\d{5}(?:\-\d{4})?)\b', address)
        if zip_match:
            result.postal_code = zip_match.group(1)
            result.confidence_score += 0.1
        
        # Special handling for street names without clear suffixes
        # Look for patterns like "AVE OF THE STARS"
        if not result.house_number:
            street_patterns = [
                r'\b(AVE OF THE STARS)\b',
                r'\b([A-Z\s]+ OF THE [A-Z\s]+)\b',
                r'\b([A-Z\s]+ BOULEVARD)\b',
                r'\b([A-Z\s]+ AVENUE)\b',
                r'\b([A-Z\s]+ STREET)\b'
            ]
            
            for pattern in street_patterns:
                match = re.search(pattern, address.upper())
                if match:
                    street_full = match.group(1)
                    # Extract suffix if present
                    suffix_match = re.search(r'\b(BOULEVARD|AVENUE|STREET|BLVD|AVE|ST)$', street_full)
                    if suffix_match:
                        suffix = self._standardize_street_suffix(suffix_match.group(1))
                        street_name = street_full[:street_full.rfind(suffix_match.group(1))].strip()
                        result.street_name = street_name
                        result.street_suffix = suffix
                    else:
                        result.street_name = street_full
                    
                    result.confidence_score += 0.3
                    break
        
        return result
    
    def _standardize_directional(self, directional: Optional[str]) -> Optional[str]:
        """Standardize directional to USPS format."""
        if not directional:
            return None
        
        directional = directional.lower().strip().rstrip('.')
        return USPS_DIRECTIONALS.get(directional, directional.upper())
    
    def _standardize_street_suffix(self, street_part: Optional[str]) -> Optional[str]:
        """Extract and standardize street suffix."""
        if not street_part:
            return None
        
        # Look for suffix at end of street part
        words = street_part.lower().split()
        if not words:
            return None
        
        last_word = words[-1].rstrip('.')
        return USPS_STREET_SUFFIXES.get(last_word, last_word.upper() if last_word else None)
    
    def _extract_street_name(self, road_part: Optional[str]) -> Optional[str]:
        """Extract street name, removing directionals and suffixes."""
        if not road_part:
            return None
        
        words = road_part.split()
        if not words:
            return None
        
        # Remove directionals from beginning and end
        start_idx = 0
        end_idx = len(words)
        
        if words[0].lower().rstrip('.') in USPS_DIRECTIONALS:
            start_idx = 1
        
        if len(words) > 1 and words[-1].lower().rstrip('.') in USPS_STREET_SUFFIXES:
            end_idx -= 1
        
        if len(words) > 2 and words[-1].lower().rstrip('.') in USPS_DIRECTIONALS:
            end_idx -= 1
        
        street_words = words[start_idx:end_idx]
        return ' '.join(street_words).upper() if street_words else None
    
    def _standardize_unit_designator(self, unit_part: Optional[str]) -> Optional[str]:
        """Standardize unit designator."""
        if not unit_part:
            return None
        
        unit_part = unit_part.lower().strip().rstrip('.')
        return USPS_UNIT_DESIGNATORS.get(unit_part, unit_part.upper())
    
    def _extract_unit_designator(self, unit_full: Optional[str]) -> Optional[str]:
        """Extract unit designator from full unit string."""
        if not unit_full:
            return None
        
        words = unit_full.split()
        if not words:
            return None
        
        first_word = words[0].lower().rstrip('.')
        return USPS_UNIT_DESIGNATORS.get(first_word, first_word.upper())
    
    def _extract_unit_number(self, unit_full: Optional[str]) -> Optional[str]:
        """Extract unit number from full unit string."""
        if not unit_full:
            return None
        
        words = unit_full.split()
        if len(words) > 1:
            return ' '.join(words[1:])
        elif len(words) == 1 and re.match(r'^[A-Z0-9\-]+$', words[0].upper()):
            # If it's just a unit number without designator
            return words[0].upper()
        
        return None
    
    def _clean_street_name(self, street_name: Optional[str]) -> Optional[str]:
        """Clean and standardize street name."""
        if not street_name:
            return None
        
        # Remove extra spaces and standardize case
        return re.sub(r'\s+', ' ', street_name.strip().title())
    
    def _clean_city(self, city: Optional[str]) -> Optional[str]:
        """Clean and standardize city name."""
        if not city:
            return None
        
        city = city.strip().title()
        
        # Common LA area city standardizations
        city_mappings = {
            'La': 'Los Angeles',
            'L.A.': 'Los Angeles',
            'L A': 'Los Angeles',
            'Hollywood': 'Hollywood',
            'Beverly Hills': 'Beverly Hills', 
            'Long Beach': 'Long Beach',
            'Santa Monica': 'Santa Monica',
            'Hills': 'Beverly Hills',  # Common truncation
            'Beach': 'Long Beach',    # Common truncation
            'Angeles': 'Los Angeles'  # Common truncation
        }
        
        return city_mappings.get(city, city)
    
    def _standardize_state(self, state: Optional[str]) -> Optional[str]:
        """Standardize state to 2-letter code."""
        if not state:
            return None
        
        state = state.upper().strip()
        if state in ['CALIFORNIA', 'CALIF', 'CALIF.']:
            return 'CA'
        
        return state
    
    def _calculate_confidence(self, components: Dict) -> float:
        """Calculate confidence score based on parsing completeness."""
        score = 0.5  # Base score
        
        # Required components
        if components.get('house_number'):
            score += 0.2
        if components.get('road'):
            score += 0.2
        
        # Optional but valuable components
        if components.get('city'):
            score += 0.05
        if components.get('state'):
            score += 0.05
        if components.get('postcode'):
            score += 0.05
        
        return min(score, 1.0)
    
    def _post_process(self, result: ParsedAddress) -> ParsedAddress:
        """Post-process parsed address for consistency."""
        
        # Default to Los Angeles, CA if city/state missing but looks like LA address
        if not result.city and result.house_number and result.street_name:
            result.city = 'LOS ANGELES'
            result.state = 'CA'
        
        # Ensure state is CA for LA addresses
        if result.city and 'LOS ANGELES' in result.city.upper() and not result.state:
            result.state = 'CA'
        
        return result

class FuzzyMatcher:
    """
    Fuzzy matching utilities for address comparison using multiple algorithms.
    """
    
    @staticmethod
    def levenshtein_distance(s1: str, s2: str) -> int:
        """Calculate Levenshtein distance between two strings."""
        if len(s1) < len(s2):
            return FuzzyMatcher.levenshtein_distance(s2, s1)
        
        if len(s2) == 0:
            return len(s1)
        
        previous_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        
        return previous_row[-1]
    
    @staticmethod
    def jaro_winkler_similarity(s1: str, s2: str, prefix_scaling: float = 0.1) -> float:
        """Calculate Jaro-Winkler similarity between two strings."""
        def jaro_similarity(s1: str, s2: str) -> float:
            if not s1 or not s2:
                return 0.0
            
            if s1 == s2:
                return 1.0
            
            match_distance = (max(len(s1), len(s2)) // 2) - 1
            match_distance = max(0, match_distance)
            
            s1_matches = [False] * len(s1)
            s2_matches = [False] * len(s2)
            
            matches = 0
            transpositions = 0
            
            # Identify matches
            for i in range(len(s1)):
                start = max(0, i - match_distance)
                end = min(i + match_distance + 1, len(s2))
                
                for j in range(start, end):
                    if s2_matches[j] or s1[i] != s2[j]:
                        continue
                    s1_matches[i] = True
                    s2_matches[j] = True
                    matches += 1
                    break
            
            if matches == 0:
                return 0.0
            
            # Count transpositions
            k = 0
            for i in range(len(s1)):
                if not s1_matches[i]:
                    continue
                while not s2_matches[k]:
                    k += 1
                if s1[i] != s2[k]:
                    transpositions += 1
                k += 1
            
            jaro = (matches / len(s1) + matches / len(s2) + 
                   (matches - transpositions/2) / matches) / 3.0
            
            return jaro
        
        jaro = jaro_similarity(s1, s2)
        
        # Calculate common prefix (up to 4 characters)
        prefix_length = 0
        for i in range(min(len(s1), len(s2), 4)):
            if s1[i] == s2[i]:
                prefix_length += 1
            else:
                break
        
        return jaro + (prefix_length * prefix_scaling * (1 - jaro))
    
    @staticmethod
    def sequence_similarity(s1: str, s2: str) -> float:
        """Calculate sequence similarity using difflib."""
        return SequenceMatcher(None, s1, s2).ratio()
    
    @staticmethod
    def address_similarity(addr1: ParsedAddress, addr2: ParsedAddress, 
                         weights: Optional[Dict[str, float]] = None) -> float:
        """
        Calculate comprehensive similarity between two parsed addresses.
        """
        if weights is None:
            weights = {
                'house_number': 0.3,
                'street_name': 0.4,
                'street_suffix': 0.1,
                'city': 0.1,
                'postal_code': 0.1
            }
        
        total_score = 0.0
        total_weight = 0.0
        
        # Compare each component
        for field, weight in weights.items():
            val1 = getattr(addr1, field)
            val2 = getattr(addr2, field)
            
            if val1 is None or val2 is None:
                continue
            
            if val1 == val2:
                score = 1.0
            else:
                # Use Jaro-Winkler for string similarity
                score = FuzzyMatcher.jaro_winkler_similarity(
                    str(val1).upper(), str(val2).upper()
                )
            
            total_score += score * weight
            total_weight += weight
        
        return total_score / total_weight if total_weight > 0 else 0.0
    
    @staticmethod
    def find_best_match(target: ParsedAddress, candidates: List[ParsedAddress], 
                       threshold: float = 0.8) -> Optional[Tuple[ParsedAddress, float]]:
        """
        Find the best matching address from a list of candidates.
        """
        best_match = None
        best_score = 0.0
        
        for candidate in candidates:
            score = FuzzyMatcher.address_similarity(target, candidate)
            if score > best_score and score >= threshold:
                best_score = score
                best_match = candidate
        
        return (best_match, best_score) if best_match else None

def demo():
    """Demonstration of address parser capabilities."""
    print("🏠 DealGenie Address Parser Demo")
    print("=" * 50)
    
    parser = AddressParser()
    
    test_addresses = [
        "1234 N Highland Ave, Los Angeles, CA 90028",
        "456 SUNSET BLVD APT 3B, HOLLYWOOD CA 90028",
        "789 w 3rd street, los angeles ca",
        "321 S FIGUEROA ST STE 1500 LOS ANGELES CA 90071",
        "555 WILSHIRE BLVD FL 39, LOS ANGELES, CA 90036",
        "12345 VENTURA BLVD # 678 STUDIO CITY CA 91604"
    ]
    
    for addr in test_addresses:
        print(f"\n📍 Input: {addr}")
        parsed = parser.parse(addr)
        print(f"   Method: {parsed.parsing_method}")
        print(f"   Confidence: {parsed.confidence_score:.2f}")
        print(f"   USPS Format:")
        for line in parsed.to_usps_format().split('\n'):
            print(f"     {line}")
    
    # Fuzzy matching demo
    print(f"\n🔍 Fuzzy Matching Demo")
    print("=" * 30)
    
    addr1 = parser.parse("1234 N Highland Ave, Los Angeles, CA")
    addr2 = parser.parse("1234 NORTH HIGHLAND AVENUE, LA, CA")
    
    similarity = FuzzyMatcher.address_similarity(addr1, addr2)
    print(f"Similarity: {similarity:.3f}")
    
    print(f"Address 1: {addr1.to_usps_format().replace(chr(10), ' / ')}")
    print(f"Address 2: {addr2.to_usps_format().replace(chr(10), ' / ')}")

if __name__ == "__main__":
    demo()