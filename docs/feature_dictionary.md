# DealGenie Feature Dictionary

**Version:** 1.0  
**Last Updated:** September 4, 2025  
**Data Coverage:** 369,703 LA County Parcels

This document provides comprehensive documentation of all 44 standardized features extracted from LA County parcel data for development scoring analysis.

---

## 📊 Data Sources & Provenance

### Primary Data Source
- **Source**: Los Angeles County ZIMAS (Zone Information and Map Access System)
- **Dataset**: `la_parcels_complete_merged.csv` (581MB)
- **Records**: 369,703 parcels
- **Raw Columns**: 210 data fields per parcel
- **Snapshot Date**: September 2025 (current assessment year)
- **Geographic Coverage**: All incorporated and unincorporated areas of LA County

### Secondary Data Sources
- **US Census Bureau**: American Community Survey (ACS) 2022 5-year estimates
- **Data Vintage**: Most recent available demographic data (2018-2022 period)
- **Geographic Level**: Census tract level (mapped to parcels via ZIP code approximation)

---

## 🏷️ Feature Categories & Schema

DealGenie extracts **44 standardized features** organized into 6 primary categories:

### 1. 📍 **Location & Identification Features** (7 features)

| Feature Name | Unit | Type | Source Column | Description | Transform |
|--------------|------|------|---------------|-------------|-----------|
| `apn` | String | Categorical | `APN` | Assessor Parcel Number - unique property identifier | Direct mapping |
| `site_address` | String | Text | `SitusFullAddress` | Complete property street address | Direct mapping |
| `site_city` | String | Categorical | `SitusCity` | Property city name | Direct mapping |  
| `site_zip` | String | Categorical | `SitusZIP` | 5-digit ZIP code | Direct mapping |
| `latitude` | Decimal | Numeric | `CentroidLatitude` | Property centroid latitude (WGS84) | Direct mapping |
| `longitude` | Decimal | Numeric | `CentroidLongitude` | Property centroid longitude (WGS84) | Direct mapping |
| `legal_description` | String | Text | `LegalDescription` | Legal property description from assessor | Direct mapping |

**Data Quality Notes:**
- Address coverage: 99.8% of parcels have valid addresses
- Coordinate accuracy: ±10 meters (centroid-based)
- ZIP code mapping: Used for Census tract approximation

### 2. 🏘️ **Physical Property Characteristics** (8 features)

| Feature Name | Unit | Type | Source Column | Description | Transform |
|--------------|------|------|---------------|-------------|-----------|
| `lot_size` | Sq Ft | Numeric | `SqFtLot` | Lot size in square feet | Direct mapping, 0 → default 5000 |
| `zoning` | String | Categorical | `SpecificPlan`, `ZoneClass` | Current zoning designation | Standardized to 32 LA County zones |
| `assessed_land_value` | USD | Numeric | `LandBaseYear` | Land assessment value | Inflation-adjusted to current year |
| `assessed_total_value` | USD | Numeric | `TotalValue` | Total property assessment value | Direct mapping |
| `improvement_value` | USD | Numeric | Calculated | Improvement value (total - land) | `TotalValue - LandBaseYear` |
| `year_built` | Year | Numeric | `EffectiveYearBuilt` | Effective construction year | Direct mapping, null → 1970 |
| `building_sqft` | Sq Ft | Numeric | `SqFtMain` | Main structure square footage | Direct mapping, null → 0 |
| `units` | Count | Numeric | `Units` | Number of dwelling units | Direct mapping, null → 1 |

**Zoning Standardization:**
- Raw zoning codes mapped to 32 standardized LA County categories
- Overlay districts preserved (e.g., "R1-1-HCR", "C4-2-CUGU")
- Unknown zones default to "R1" (most restrictive)

### 3. 🚊 **Location & Accessibility Features** (6 features)

| Feature Name | Unit | Type | Source Column | Description | Transform |
|--------------|------|------|---------------|-------------|-----------|
| `metro_distance` | Miles | Numeric | Calculated | Distance to nearest Metro rail station | Haversine formula from coordinates |
| `transit_score` | 0-10 | Numeric | Derived | Public transit accessibility score | Distance-based scoring algorithm |
| `highway_access` | Boolean | Binary | Calculated | Within 2 miles of major highway | Proximity analysis |
| `airport_distance` | Miles | Numeric | Calculated | Distance to LAX airport | Straight-line distance |
| `downtown_distance` | Miles | Numeric | Calculated | Distance to Downtown LA | Straight-line distance |
| `beach_distance` | Miles | Numeric | Calculated | Distance to nearest Pacific beach | Proximity to coastline |

**Calculation Methods:**
- Transit scoring: 10 (< 0.5 miles), 8 (< 1 mile), 6 (< 2 miles), 4 (< 5 miles), 2 (≥ 5 miles)
- Highway access: I-405, I-5, I-10, I-110, I-210, US-101 corridors
- Reference points: Downtown LA (34.0522°N, 118.2437°W), LAX (33.9425°N, 118.4081°W)

### 4. 👥 **Demographics & Market Features** (9 features)

| Feature Name | Unit | Type | Source | Description | Transform |
|--------------|------|------|--------|-------------|-----------|
| `median_income` | USD | Numeric | Census ACS 2022 | Median household income (tract-level) | ZIP→tract mapping |
| `population_density` | People/Sq Mi | Numeric | Census ACS 2022 | Population density (tract-level) | Calculated from population/area |
| `college_educated_pct` | Percentage | Numeric | Census ACS 2022 | % adults with bachelor's degree+ | Calculated from education tables |
| `homeownership_rate` | Percentage | Numeric | Census ACS 2022 | % owner-occupied housing units | Calculated from housing tables |
| `median_home_value` | USD | Numeric | Census ACS 2022 | Median home value in tract | Direct from ACS estimates |
| `unemployment_rate` | Percentage | Numeric | Census ACS 2022 | Unemployment rate in tract | Calculated from labor force tables |
| `family_poverty_rate` | Percentage | Numeric | Census ACS 2022 | % families below poverty line | Calculated from poverty tables |
| `median_age` | Years | Numeric | Census ACS 2022 | Median age in tract | Direct from ACS estimates |
| `avg_household_size` | People | Numeric | Census ACS 2022 | Average persons per household | Calculated from household tables |

**Data Vintage & Quality:**
- Source: 2022 ACS 5-Year Estimates (2018-2022 survey period)
- Geographic Resolution: Census tract level (mapped via ZIP code approximation)
- Update Frequency: Annual (Census Bureau release schedule)
- Margin of Error: Varies by variable, typically ±5-15% for tract-level estimates

### 5. 🏛️ **Regulatory & Risk Features** (8 features)

| Feature Name | Unit | Type | Source Column | Description | Transform |
|--------------|------|------|---------------|-------------|-----------|
| `flood_zone` | Boolean | Binary | `FloodZone` | Located in FEMA flood zone | "A", "AE" zones → True |
| `seismic_zone` | Integer | Ordinal | `SeismicZone` | Seismic hazard zone (1-4) | Direct mapping, null → 2 |
| `fire_hazard` | Boolean | Binary | `FireHazardSeverityZone` | High fire hazard severity zone | "High", "Very High" → True |
| `historic_district` | Boolean | Binary | `HistoricDistrict` | Within historic preservation overlay | Any historic designation → True |
| `coastal_zone` | Boolean | Binary | `CaliforniaCoastalZone` | California Coastal Commission jurisdiction | Direct mapping |
| `toxic_sites` | Boolean | Binary | `ToxicSites` | Known contamination nearby | Within 1000ft of toxic site → True |
| `liquefaction_risk` | Boolean | Binary | `LiquefactionZone` | Earthquake liquefaction hazard zone | Direct mapping |
| `landslide_risk` | Boolean | Binary | `LandslideZone` | Landslide hazard zone | Direct mapping |

**Risk Assessment Methods:**
- Flood zones: FEMA Flood Insurance Rate Maps (FIRM)
- Seismic zones: California Geological Survey classifications
- Fire hazards: CAL FIRE severity zone mapping
- Toxic sites: EPA Superfund and state cleanup sites

### 6. 💰 **Financial & Market Features** (6 features)

| Feature Name | Unit | Type | Source | Description | Transform |
|--------------|------|------|--------|-------------|-----------|
| `price_per_sqft_lot` | $/Sq Ft | Numeric | Calculated | Assessed value per sq ft of land | `LandValue / SqFtLot` |
| `price_per_sqft_building` | $/Sq Ft | Numeric | Calculated | Assessed value per sq ft of building | `ImprovementValue / SqFtMain` |
| `lot_to_building_ratio` | Ratio | Numeric | Calculated | Lot size to building size ratio | `SqFtLot / SqFtMain`, null → 10.0 |
| `tax_assessment_ratio` | Ratio | Numeric | Calculated | Assessment to market value ratio | Estimated via neighborhood analysis |
| `development_potential` | 0-10 | Numeric | Calculated | Development capacity score | Zoning + lot size analysis |
| `market_tier` | 1-5 | Ordinal | Calculated | Market desirability tier | Based on demographics + location |

**Calculation Methods:**
- Development potential: Combines zoning density allowances with current lot utilization
- Market tier: Composite score from income, education, home values, and location factors
- Assessment ratios: Estimated using local sales data and neighborhood analysis

---

## 🔧 Data Processing & Quality Controls

### Missing Data Handling
- **Numeric Features**: Default values based on property type and location
- **Categorical Features**: "Unknown" category or most common value
- **Boolean Features**: Conservative defaults (assume risk present)

### Data Validation Rules
- Lot sizes: Range validation (100 - 1,000,000 sq ft)
- Assessment values: Outlier detection (3 standard deviations)
- Coordinates: Geographic bounds checking (LA County extent)
- Years: Reasonable ranges (1850 - current year)

### Feature Standardization
- **Scaling**: Z-score normalization for model input
- **Categorical Encoding**: One-hot encoding for zoning, ordinal for risk levels
- **Missing Indicators**: Binary flags for imputed values

---

## 📈 Feature Usage by Template

### Template-Specific Feature Weights

| Feature Category | Multifamily | Residential | Commercial | Industrial | Retail |
|------------------|-------------|-------------|------------|------------|--------|
| Location | High | Medium | High | Medium | High |
| Physical | High | High | Medium | High | Medium |
| Transit | High | Medium | High | Low | High |
| Demographics | High | High | High | Low | High |
| Regulatory | Medium | Medium | High | Medium | Medium |
| Financial | High | High | High | High | High |

### Critical Features per Template
- **Multifamily**: `lot_size`, `zoning`, `median_income`, `transit_score`
- **Residential**: `lot_size`, `zoning`, `homeownership_rate`, `median_home_value`
- **Commercial**: `zoning`, `demographics`, `highway_access`, `foot_traffic`
- **Industrial**: `lot_size`, `zoning`, `highway_access`, `regulatory_constraints`
- **Retail**: `demographics`, `transit_score`, `foot_traffic`, `visibility`

---

## 🎯 Feature Performance & Predictive Power

### Top 10 Most Predictive Features
1. `zoning` - Development rights and density allowances
2. `lot_size` - Development capacity and project scale
3. `median_income` - Market demand and pricing power
4. `assessed_total_value` - Current market positioning
5. `transit_score` - Accessibility and desirability
6. `development_potential` - Unused development capacity
7. `price_per_sqft_lot` - Land value efficiency
8. `college_educated_pct` - Market sophistication
9. `flood_zone` - Development constraints and risks
10. `market_tier` - Overall market positioning

### Feature Correlation Analysis
- Strong correlations: `median_income` ↔ `college_educated_pct` (0.78)
- Inverse correlations: `flood_zone` ↔ `assessed_total_value` (-0.34)
- Independence: `lot_size` shows low correlation with most demographic features

---

## 🔄 Update & Maintenance Schedule

### Quarterly Updates
- **Assessment Values**: Updated from LA County Assessor
- **Zoning Changes**: Planning department updates
- **Development Projects**: Major project completions

### Annual Updates  
- **Census Demographics**: New ACS 5-year estimates
- **Risk Mapping**: Updated FEMA and geological surveys
- **Transit Infrastructure**: New stations and route changes

### Real-time Integration
- **Market Data**: Property sales and listing updates
- **Regulatory Changes**: Zoning ordinance modifications
- **Infrastructure**: Transportation and utility improvements

---

## 🛡️ Data Privacy & Compliance

### Privacy Considerations
- **Public Records**: All parcel data sourced from public assessor records
- **Aggregated Demographics**: Census data aggregated at tract level (>1000 people)
- **No PII**: No personally identifiable information included

### Compliance Standards
- **GDPR**: Public record exemption applies
- **CCPA**: Consumer right to know satisfied through documentation
- **Fair Housing**: Analysis focuses on property characteristics, not protected classes

---

## 📋 Version History & Schema Changes

### v1.0 (September 2025)
- Initial feature dictionary with 44 standardized features
- Full LA County parcel coverage (369,703 properties)
- Census ACS 2022 5-year demographic integration
- Multi-template scoring system implementation

### Planned Enhancements (v1.1)
- **Additional Demographics**: Crime statistics, school district quality
- **Environmental Features**: Air quality, noise levels
- **Economic Indicators**: Employment centers, business density
- **Infrastructure Details**: Utility capacity, broadband availability

---

**For technical implementation details, see:**
- `/features/csv_feature_matrix.py` - Feature extraction engine
- `/scoring/engine.py` - Multi-template scoring algorithms  
- `/ingest/census_acs.py` - Demographic data integration
- `/tests/test_performance_regression.py` - Feature extraction performance tests