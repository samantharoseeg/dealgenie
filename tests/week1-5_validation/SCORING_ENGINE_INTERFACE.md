# SCORING ENGINE INTERFACE DOCUMENTATION
## Week 1-5 Component Integration Guide

**File:** `/Users/samanthagrant/Desktop/dealgenie/scoring/engine.py`
**Function:** `calculate_score()`
**Status:** ✅ EXISTS AND READY

---

## Function Signature

```python
def calculate_score(features: Dict[str, Any], template: str = 'multifamily') -> Dict[str, Any]:
    """
    Calculate investment score for a property based on features and template.

    Args:
        features: Dictionary containing property features
        template: Scoring template ('multifamily', 'commercial', 'residential',
                                    'industrial', 'retail', 'mixed_use', 'office')

    Returns:
        Dictionary with score, explanation, and recommendations
    """
```

---

## Input Requirements

### Required Fields

**Minimum to run scoring:**
```python
features = {
    'zoning': 'R1',           # Zoning code (string)
    'lot_size_sqft': 5000.0   # Lot size in square feet (float/int)
}
```

### Optional Fields (Enhance Scoring)

```python
features = {
    # REQUIRED
    'zoning': 'R1',                    # Zoning code
    'lot_size_sqft': 5000.0,           # Lot size

    # OPTIONAL - Crime Data
    'crime_factor': 1.0,               # Crime factor (0.5=very safe, 2.0=high crime)

    # OPTIONAL - Location/Transit
    'transit_distance': 0.5,           # Distance to transit (miles)
    'walkability_score': 75.0,         # Walkability (0-100)

    # OPTIONAL - Financial
    'assessed_value': 500000.0,        # Total assessed value
    'land_value': 300000.0,            # Land value only
    'improvement_value': 200000.0,     # Improvement value

    # OPTIONAL - Demographics
    'median_income': 75000.0,          # Median household income
    'population_density': 5000.0,      # People per sq mile

    # OPTIONAL - Risk Factors (Boolean)
    'flood_risk': False,               # In flood zone
    'flood_zone': False,               # Alternative flood flag
    'superfund_site_nearby': False,    # Near toxic site
    'near_airport': False,             # Airport proximity

    # OPTIONAL - Risk Factors (Numeric)
    'toxic_sites_nearby': 0,           # Count of toxic sites
    'airport_noise_level': 50,         # dB levels
    'homeless_encampments_nearby': 0,  # Count of encampments
    'homeless_population_density': 0,  # Per sq mile
    'freeway_distance_ft': 5000,       # Distance to freeway
    'industrial_facilities_nearby': 0, # Count of facilities
    'air_quality_index': 50,           # AQI value

    # OPTIONAL - Infrastructure
    'seismic_risk_level': 'moderate',  # 'low', 'moderate', 'high', 'very_high'
    'utility_deficiencies': []         # List of utility issues
}
```

---

## Mapping Database Fields to Scoring Engine

### From properties_unified to features dict

```python
import psycopg2

conn = psycopg2.connect(
    host='localhost',
    database='dealgenie_production',
    user='samanthagrant',
    port=5432
)
cursor = conn.cursor()

# Get property from database
cursor.execute("""
    SELECT
        apn,
        zoning_code,
        lot_size_sqft,
        land_value,
        improvement_value,
        crime_score,
        data_quality_score,
        walkability_score,
        transit_accessibility_bonus
    FROM properties_unified
    WHERE apn = %s;
""", ('4306026007',))

row = cursor.fetchone()
apn, zoning, lot_size, land_val, imp_val, crime, quality, walk, transit = row

# Build features dict for scoring engine
features = {
    # REQUIRED
    'zoning': zoning,                           # From zoning_code
    'lot_size_sqft': float(lot_size),          # From lot_size_sqft

    # OPTIONAL - Financial
    'land_value': float(land_val) if land_val else None,
    'improvement_value': float(imp_val) if imp_val else None,
    'assessed_value': (float(land_val) + float(imp_val)) if (land_val and imp_val) else None,

    # OPTIONAL - Enhanced Week 1-5 Data
    'crime_factor': (100 - crime) / 100 if crime else 1.0,  # Convert crime_score to factor
    'walkability_score': walk if walk else None,
    # transit_accessibility_bonus not directly used (engine calculates from distance)
}

# Import scoring engine
import sys
sys.path.insert(0, '/Users/samanthagrant/Desktop/dealgenie')
from scoring.engine import calculate_score

# Score the property
result = calculate_score(features, template='multifamily')
```

---

## Field Conversions

### Crime Score → Crime Factor

**Database:** `crime_score` (0-100 scale, higher = safer)
**Engine:** `crime_factor` (0.5-2.0 scale, 1.0 = average, higher = more crime)

```python
# Convert database crime_score to engine crime_factor
if crime_score is not None:
    crime_factor = (100 - crime_score) / 100
    # crime_score 85 (safe) → crime_factor 0.15 (very safe)
    # crime_score 50 (average) → crime_factor 0.50
    # crime_score 20 (high crime) → crime_factor 0.80
else:
    crime_factor = 1.0  # Default (average)
```

### Zoning Code

**Database:** `zoning_code` (string) - e.g., "R1", "R1-1-O", "C2"
**Engine:** `zoning` (string) - Same format

```python
# Direct mapping - no conversion needed
features['zoning'] = zoning_code
```

### Lot Size

**Database:** `lot_size_sqft` (NUMERIC/Decimal)
**Engine:** `lot_size_sqft` (float)

```python
# Convert Decimal to float
from decimal import Decimal

features['lot_size_sqft'] = float(lot_size_sqft) if lot_size_sqft else 0.0
```

---

## Output Structure

```python
result = calculate_score(features, template='multifamily')

# Result dictionary structure (ACTUAL):
{
    'score': 7.5,                      # Overall score (0-10)

    'base_score': 8.5,                 # Score before penalties

    'total_penalties': 1.0,            # Sum of all penalties

    'penalties': {                     # Applied penalties
        'high_crime': 0.5,
        'flood_zone': 1.2
    },

    'explanation': "Property scored 7.5/10 for multifamily development...",

    'recommendations': [               # List of recommendation strings
        "Strong zoning for multifamily development",
        "Good transit accessibility enhances value",
        "Monitor crime trends in area"
    ],

    'component_scores': {              # Individual component scores
        'zoning': 8.0,
        'lot_size': 7.5,
        'transit': 6.5,
        'demographics': 8.5,
        'market': 7.0
    },

    'template': 'multifamily',         # Template used

    'timestamp': '2025-10-06T17:43:19.796383'  # ISO 8601 timestamp
}

# NOTE: The engine does NOT return 'grade' or 'investment_recommendation'
# You can calculate grade from score:
# A+ (9.0+), A (8.5+), B+ (7.5+), B (7.0+), C (6.0+), D (<6.0)
```

---

## Template Types (7 Available)

| Template | Use Case | Zoning Preferences | Key Factors |
|----------|----------|-------------------|-------------|
| `multifamily` | Apartments, condos | R3, R4, R5, C2 | Zoning 30%, Transit 25%, Lot 20% |
| `residential` | Single-family homes | R1, RE, RS | Zoning 30%, Lot 25%, Demographics 20% |
| `commercial` | Office, retail, mixed | C1, C2, C4, CM | Zoning 30%, Transit 25%, Demographics 20% |
| `industrial` | Warehouses, manufacturing | M1, M2, M3 | Lot 35%, Market 25%, Zoning 20% |
| `retail` | Shopping, restaurants | C2, C4, C1 | Zoning 30%, Transit 25%, Demographics 20% |
| `mixed_use` | Combined uses | CM, CR, RAS3/4 | Zoning 35%, Transit 30%, Lot 15% |
| `office` | Office buildings | C1, C2, LAX | Transit 30%, Zoning 30%, Market 15% |

---

## Component Scoring Logic

### Zoning Score (0-10)

**Template-specific zone preferences:**

```python
# Example for multifamily template:
if 'R5' in zoning or 'R4' in zoning or 'R3' in zoning:
    zoning_score = 10.0    # Perfect for multifamily
elif 'C2' in zoning or 'C4' in zoning:
    zoning_score = 8.5     # Good for multifamily
elif 'R2' in zoning or 'RD' in zoning:
    zoning_score = 6.5     # Moderate
elif 'R1' in zoning:
    zoning_score = 3.0     # Poor for multifamily
else:
    zoning_score = 2.0     # Default
```

### Lot Size Score (0-10)

**Size-based scoring (line 305):**

```python
lot_size = features.get('lot_size_sqft', 5000)

# Minimum thresholds vary by template
# Generally:
# - Very small (<2000): 2.0
# - Small (2000-5000): 5.0
# - Medium (5000-10000): 7.0
# - Large (10000-20000): 8.5
# - Very large (>20000): 10.0
```

### Crime Impact (lines 334-387)

**Crime factor penalties/bonuses:**

```python
crime_factor = features.get('crime_factor', 1.0)

# For residential template:
if crime_factor > 1.8:      # High crime
    crime_penalty = -2.0
elif crime_factor > 1.5:    # Elevated crime
    crime_penalty = -1.0
elif crime_factor > 1.2:    # Moderate crime
    crime_penalty = -0.5
elif crime_factor < 0.5:    # Very safe
    crime_bonus = +1.0
elif crime_factor < 0.7:    # Safe
    crime_bonus = +0.5
```

---

## Penalty System

### Penalty Calculation (lines 34-102)

**Risk factors that reduce scores:**

```python
penalties = calculate_penalties(features, template)

# Possible penalties (all in score points):
{
    'flood_zone': 1.2,                  # If flood_risk or flood_zone = True
    'high_crime': 1.0,                  # If crime_factor > 1.5
    'toxic_sites': 2.0,                 # If toxic_sites_nearby > 0
    'airport_noise': 0.6,               # If airport_noise_level > 65 dB
    'homeless_concentration': 1.0,      # If homeless_encampments_nearby > 2
    'freeway_noise': 0.6,               # If freeway_distance_ft < 500
    'pollution': 1.2,                   # If air_quality_index > 100
    'seismic_risk': 1.0,                # If seismic_risk_level = 'very_high'
    'infrastructure': 1.0               # If utility_deficiencies exist
}

# Final score calculation:
total_score = base_score - sum(penalties.values())
total_score = max(0.0, total_score)  # Floor at 0
```

---

## Usage Examples

### Example 1: Minimum Required Fields

```python
from scoring.engine import calculate_score

# Minimal property
features = {
    'zoning': 'R3',
    'lot_size_sqft': 7500.0
}

result = calculate_score(features, template='multifamily')
print(f"Score: {result['score']:.1f}/10")
# Output: Score: 7.5/10 (approximate - depends on defaults)
```

### Example 2: Enhanced with Crime Data

```python
# Property with enhanced data
features = {
    'zoning': 'R3',
    'lot_size_sqft': 7500.0,
    'crime_factor': 0.85,  # From database: crime_score = 85 → crime_factor = 0.15
    'assessed_value': 650000.0
}

result = calculate_score(features, template='multifamily')
print(f"Score: {result['score']:.1f}/10")
print(f"Penalties: {result['penalties']}")
```

### Example 3: Full Featured

```python
# Property with all available data
features = {
    # Required
    'zoning': 'R4',
    'lot_size_sqft': 12000.0,

    # Financial
    'assessed_value': 850000.0,
    'land_value': 500000.0,
    'improvement_value': 350000.0,

    # Enhanced data
    'crime_factor': 0.60,        # Safe area
    'walkability_score': 82.0,   # Highly walkable

    # Risk factors
    'flood_zone': False,
    'freeway_distance_ft': 2000,
    'air_quality_index': 45
}

result = calculate_score(features, template='multifamily')
print(f"Score: {result['score']:.1f}/10")
print(f"Template: {result['template']}")
print(f"Recommendations: {result['recommendations']}")
```

---

## Integration with properties_unified

### Complete Example

```python
import psycopg2
import sys
sys.path.insert(0, '/Users/samanthagrant/Desktop/dealgenie')
from scoring.engine import calculate_score
from decimal import Decimal

def score_property_from_database(apn: str, template: str = 'multifamily'):
    """
    Fetch property from database and score it
    """
    conn = psycopg2.connect(
        host='localhost',
        database='dealgenie_production',
        user='samanthagrant',
        port=5432
    )
    cursor = conn.cursor()

    # Fetch property
    cursor.execute("""
        SELECT
            apn,
            zoning_code,
            lot_size_sqft,
            land_value,
            improvement_value,
            crime_score,
            data_quality_score,
            property_type,
            data_source
        FROM properties_unified
        WHERE apn = %s;
    """, (apn,))

    row = cursor.fetchone()
    if not row:
        return {'error': 'Property not found'}

    apn, zoning, lot_size, land_val, imp_val, crime, quality, prop_type, source = row

    # Convert Decimal to float
    def to_float(val):
        return float(val) if isinstance(val, Decimal) and val is not None else val

    # Build features
    features = {
        'zoning': zoning,
        'lot_size_sqft': to_float(lot_size),
    }

    # Add optional fields if available
    if land_val:
        features['land_value'] = to_float(land_val)
    if imp_val:
        features['improvement_value'] = to_float(imp_val)
    if land_val and imp_val:
        features['assessed_value'] = to_float(land_val) + to_float(imp_val)

    # Add enhanced data if available
    if crime is not None:
        # Convert crime_score (0-100, higher=safer) to crime_factor (lower=safer)
        features['crime_factor'] = (100 - crime) / 100

    # Score property
    result = calculate_score(features, template)

    # Add metadata
    result['apn'] = apn
    result['property_type'] = prop_type
    result['data_source'] = source

    conn.close()
    return result

# Usage
result = score_property_from_database('4306026007', template='multifamily')
print(f"APN: {result['apn']}")
print(f"Score: {result['score']:.1f}/10")
print(f"Template: {result['template']}")
```

---

## Key Takeaways

1. **Minimum Fields:** Just `zoning` and `lot_size_sqft` required
2. **Enhanced Data:** `crime_factor` and financial data improve accuracy
3. **No Database Import:** Engine doesn't import database - caller provides data
4. **Field Conversion:** Database Decimals → float, crime_score → crime_factor
5. **7 Templates:** Choose appropriate template for property type
6. **Penalty System:** Risk factors automatically reduce scores
7. **Rich Output:** Scores, explanations, recommendations, grades

---

*Created: October 6, 2025*
*Source: scoring/engine.py analysis*
*Status: Ready for Test 02 implementation*
