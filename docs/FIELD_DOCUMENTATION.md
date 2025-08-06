# DealGenie Field Documentation 📋

## Complete Data Dictionary & Field Reference

This document provides comprehensive documentation for all fields in the DealGenie property scoring system.

## 🏷️ Data Sources

- **ZIMAS**: Los Angeles Zone Information and Map Access System
- **Assessor**: LA County Assessor property records
- **Planning**: LA City Planning Department data
- **Transit**: Metro LA transit-oriented development zones

## 📊 Core Property Fields

### Property Identifiers
| Field Name | Type | Description | Example | Completeness |
|------------|------|-------------|---------|--------------|
| `property_id` | Integer | Internal database ID | 1, 101, 670 | 100% |
| `assessor_parcel_id` | String | Standardized APN format | 4306-026-007 | 100% |
| `pin` | String | Parcel Identification Number | 129B165 778 | 100% |

**Notes**: 
- APNs standardized to XXXX-XXX-XXX format during cleaning
- PINs preserve original ZIMAS formatting

### Location Information
| Field Name | Type | Description | Example | Completeness |
|------------|------|-------------|---------|--------------|
| `site_address` | String | Cleaned property address | 9406 W OAKMORE RD | 91.3% |
| `zip_code` | String | ZIP code | 90035 | 91.3% |
| `council_district` | String | LA City Council District | CD 5 - Katy Young Yaroslavsky | 98.2% |
| `community_plan_area` | String | Community plan designation | West Los Angeles | 98.2% |
| `neighborhood_council` | String | Neighborhood council | South Robertson | 96.4% |

**Address Cleaning**:
- Standardized abbreviations (ROAD → RD, AVENUE → AVE)
- Consistent directional prefixes (W, N, S, E)
- Removed special characters and extra whitespace

### Zoning & Planning
| Field Name | Type | Description | Example | Completeness |
|------------|------|-------------|---------|--------------|
| `base_zoning` | String | Simplified zoning category | R1, R3, C2, M2 | 98.2% |
| `base_zoning_description` | String | Human-readable zoning type | Single Family Residential | 98.2% |
| `full_zoning_code` | String | Complete ZIMAS zoning code | R3-1VL-O | 98.2% |
| `general_plan_land_use` | String | General plan designation | Low Residential | 98.2% |
| `specific_plan_area` | String | Specific plan overlay | WEST LOS ANGELES TRANSPORTATION | 38.3% |
| `overlay_count` | Integer | Number of overlay zones | 0, 1, 2, 3, 4 | 100% |

**Zoning Categories**:
- **R1-R5**: Residential (Single Family to High Density)
- **C1-C4**: Commercial (Limited to Highway Commercial)  
- **M1-M3**: Industrial/Manufacturing
- **PF**: Public Facilities
- **OS**: Open Space

### Property Metrics
| Field Name | Type | Description | Example | Completeness |
|------------|------|-------------|---------|--------------|
| `lot_size_sqft` | Float | Lot size in square feet | 7172.3 | 99.9% |
| `building_size_sqft` | Float | Building square footage | 4648.0 | 92.4% |
| `year_built` | Integer | Construction year | 1996 | 92.4% |
| `number_of_units` | Integer | Dwelling units | 1, 66, 19 | 92.5% |
| `use_code` | String | Assessor use classification | 0100 - Residential - Single Family | 100% |

**Derived Metrics**:
- FAR (Floor Area Ratio) = building_size_sqft / lot_size_sqft
- Acres = lot_size_sqft / 43560
- Density = number_of_units / lot_size_sqft

### Financial Data
| Field Name | Type | Description | Example | Completeness |
|------------|------|-------------|---------|--------------|
| `assessed_land_value` | Float | Land assessment (numeric) | 537555.0 | 100% |
| `assessed_improvement_value` | Float | Building assessment | 769552.0 | 100% |
| `total_assessed_value` | Float | Combined assessed value | 1307107.0 | 100% |

**Financial Ratios**:
- **Land Value Ratio** = assessed_land_value / total_assessed_value
- **Price per Sq Ft** = total_assessed_value / lot_size_sqft
- **Improvement Ratio** = assessed_improvement_value / total_assessed_value

### Development Indicators
| Field Name | Type | Description | Example | Completeness |
|------------|------|-------------|---------|--------------|
| `toc_eligible` | Boolean | Transit-Oriented Communities eligible | True/False | 100% |
| `opportunity_zone` | String | Opportunity Corridor designation | Not Eligible, OC-2 | 100% |
| `high_quality_transit` | String | High quality transit access | Yes | 76.8% |
| `residential_market_area` | String | Residential market strength | High, Medium, Low | 100% |
| `commercial_market_area` | String | Commercial market strength | High, Medium, Low | 100% |

**Transit-Oriented Communities (TOC)**:
- Eligible properties receive density bonuses
- Located near high-quality transit
- Subject to affordability requirements

### Environmental & Risk Factors
| Field Name | Type | Description | Example | Completeness |
|------------|------|-------------|---------|--------------|
| `methane_zone` | String | Methane hazard designation | Methane Zone, Outside Zone | 100% |
| `flood_zone` | String | FEMA flood zone status | Outside Flood Zone | 100% |
| `fault_zone` | String | Alquist-Priolo fault zone | Yes/No/Blank | Variable |

**Risk Categories**:
- **Methane Zones**: Environmental remediation required
- **Flood Zones**: Insurance and building restrictions  
- **Fault Zones**: Seismic safety requirements

### Housing Regulations  
| Field Name | Type | Description | Example | Completeness |
|------------|------|-------------|---------|--------------|
| `rent_stabilization` | String | RSO coverage | No [APN: 4306026007] | 100% |
| `housing_replacement_required` | String | Housing Element replacement | Yes | 83.7% |

## 🎯 DealGenie Scoring Fields

### Development Score Components
| Field Name | Type | Description | Range | Weight |
|------------|------|-------------|-------|--------|
| `development_score` | Float | Overall development potential | 0-100 | Final Score |
| `investment_tier` | String | Investment grade | A, B, C, D | Derived |
| `suggested_use` | String | Recommended development type | Mixed-Use Development | Algorithm |

### Score Breakdown (JSON stored as string)
```json
{
  "zoning_score": 90.0,      // 35% weight
  "lot_size_score": 100.0,   // 25% weight  
  "transit_bonus": 35.0,     // 20% weight
  "financial_score": 8.0,    // 10% weight
  "risk_penalty": 0.0        // 10% weight (subtracted)
}
```

### Data Quality Metrics
| Field Name | Type | Description | Example |
|------------|------|-------------|---------|
| `data_completeness_score` | Float | Percentage of fields populated | 87.5 |

## 🏗️ Use Case Classifications

### Suggested Development Types
| Use Case | Typical Properties | Key Indicators |
|----------|-------------------|----------------|
| **Major TOC Development (100+ units)** | Large R3/R4 lots | >10k sqft + TOC eligible + density zoning |
| **Mixed-Use Development** | Large commercial lots | C2/C4 + large lot + transit access |
| **TOC Multi-Family (20-50 units)** | Medium density + TOC | R3/RD2 + 5k-10k sqft + TOC |
| **Retail/Restaurant Development** | Small-medium commercial | C zones + transit + moderate size |
| **Creative Office/Live-Work** | Industrial zones | M1/M2 + good location |
| **Teardown/Rebuild Opportunity** | High land value ratio | Land ratio >75% + underbuilt |
| **SB9 Lot Split Opportunity** | Large single family | R1 + >5k sqft + high land ratio |
| **Prime Redevelopment Site** | Underutilized valuable land | High $/sqft + low FAR |
| **ADU Development** | Small lots with TOC | R1 + TOC + moderate size |
| **Hold/Income Property** | Stable cash flow | Lower development scores |

## 🔢 Data Processing Notes

### Numeric Field Extraction
Original messy formats cleaned to pure numbers:
- `"7,172.3 (sq ft)"` → `7172.3`
- `"$1,307,107"` → `1307107.0`
- `"314,680.0 (sq ft)"` → `314680.0`

### Boolean Field Standardization
Text values converted to boolean:
- `"Yes"` → `True`
- `"No"` → `False`  
- `"Not Eligible"` → `False`

### Text Field Cleaning
- Removed `\r\n` characters
- Standardized abbreviations
- Trimmed whitespace
- Consistent capitalization

## 📊 Field Completeness Analysis

### High Completeness (>95%)
- Property identifiers (APN, PIN)
- Zoning information
- Lot size data
- Financial assessments
- Basic location data

### Medium Completeness (80-95%)
- Building characteristics
- Address information
- Development indicators

### Low Completeness (<80%)
- Specific plan areas (38.3%)
- Some overlay designations
- Historical data fields

## 🔧 Custom Field Creation

### Adding New Calculated Fields
Example of adding custom metrics:

```python
# Price per square foot
df['price_per_sqft'] = df['total_assessed_value'] / df['lot_size_sqft']

# Development capacity (estimated)
df['development_capacity'] = df['lot_size_sqft'] * 0.75 / 400  # Assume 75% lot coverage, 400 sqft/unit

# Years since construction
df['building_age'] = 2024 - df['year_built']
```

### Custom Scoring Components
Add new scoring factors by extending the scorer:

```python
def calculate_market_timing_score(self, row):
    # Custom scoring based on market conditions
    if row['community_plan_area'] in HIGH_GROWTH_AREAS:
        return 10
    return 0
```

## 🚨 Data Quality Warnings

### Known Issues
1. **Specific Plan Coverage**: Only 38.3% have specific plan data
2. **Address Normalization**: Some addresses may have formatting inconsistencies  
3. **Historical Data**: Building age data missing for ~8% of properties
4. **Overlay Complexity**: Some overlay zones may not be fully captured

### Validation Recommendations
1. **Cross-reference APNs** with LA County Assessor for accuracy
2. **Verify zoning codes** against current LA Planning Department data
3. **Confirm TOC eligibility** with Metro LA official maps
4. **Validate addresses** using postal service databases

## 🔄 Field Evolution

### Version History
- **v1.0**: Initial field extraction from ZIMAS
- **v1.1**: Added standardized APNs and cleaned addresses
- **v1.2**: Introduced scoring components and investment tiers
- **v1.3**: Enhanced use case classification algorithms

### Planned Enhancements
- [ ] **Permit Integration**: Active development permits
- [ ] **Market Data**: Recent sales comparables  
- [ ] **Neighborhood Metrics**: Walk scores, crime statistics
- [ ] **Environmental Data**: Air quality, noise levels
- [ ] **Transportation**: Distance to transit stations

---

*This field documentation supports accurate interpretation and analysis of DealGenie property data* 📊