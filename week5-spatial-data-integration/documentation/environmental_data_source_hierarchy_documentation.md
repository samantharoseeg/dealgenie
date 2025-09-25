# Environmental Data Source Hierarchy Documentation

## Overview
This document establishes the authoritative ranking system for environmental data sources used in the DealGenie property intelligence platform. The hierarchy is based on comprehensive research of data accuracy, regulatory authority, update frequency, and reliability for the 457,768 LA County properties in our database.

## Authority Ranking System

### Flood Zone Data Sources (Ranked by Authority)

#### 1. **FEMA NFHL (National Flood Hazard Layer)** - Authority Rank: 1
- **Confidence Score**: 0.98
- **Source Type**: Government Authoritative
- **Regulatory Status**: Official federal flood hazard determination
- **API Endpoint**: https://hazards.fema.gov/nfhlv2/rest/services
- **Update Frequency**: Quarterly
- **Spatial Resolution**: 1:12,000 scale
- **Coverage**: National, comprehensive

**Justification for #1 Ranking:**
- Federal regulatory authority under National Flood Insurance Program (NFIP)
- Required source for flood insurance determinations
- Legally binding for FEMA flood insurance rate maps (FIRMs)
- Undergoes rigorous scientific review and public comment periods
- Updated through formal map revision process with community coordination
- Direct source used by all other flood mapping services

**Research Evidence:**
- NFHL provides authoritative flood hazard information for the National Flood Insurance Program
- Data is produced through detailed hydrologic and hydraulic studies
- Map updates require community adoption and FEMA approval
- Used as basis for flood insurance requirements and building codes

#### 2. **FEMA Map Service Center** - Authority Rank: 2
- **Confidence Score**: 0.98
- **Source Type**: Government Authoritative
- **Regulatory Status**: Official FEMA verification interface
- **API Endpoint**: https://msc.fema.gov/portal
- **Update Frequency**: Real-time
- **Spatial Resolution**: Parcel-level
- **Coverage**: National, property-specific

**Justification for #2 Ranking:**
- Same authoritative source as NFHL but manual verification method
- Real-time access to current effective flood maps
- Official FEMA interface for flood determinations
- Used by lenders, insurance agents, and surveyors for official determinations
- Provides most current flood map status including pending updates

**Research Evidence:**
- Official FEMA portal for flood map verification
- Provides legally defensible flood zone determinations
- Includes historical map data and pending revisions
- Integrates with FEMA's internal map management systems

#### 3. **ZIMAS Flood Zones** - Authority Rank: 3
- **Confidence Score**: 0.85
- **Source Type**: Municipal Derived
- **Regulatory Status**: LA City official data, derived from FEMA
- **API Endpoint**: http://zimas.lacity.org/
- **Update Frequency**: Monthly
- **Spatial Resolution**: Parcel-level
- **Coverage**: LA City only, 99.9% of properties

**Justification for #3 Ranking:**
- Excellent coverage (457,482 of 457,768 properties = 99.9%)
- Derived from FEMA sources but may have lag time
- Municipal processing can introduce discrepancies
- Known conflicts with authoritative FEMA data (APN 5036019013 example)
- Useful for bulk processing but requires verification for critical decisions

**Research Evidence:**
- Analysis of divTab7_flood_zone field shows comprehensive coverage
- Discrepancy identified: APN 5036019013 shows "500 Yr" vs FEMA "Zone X"
- LA City updates from FEMA sources but processing delays can occur
- Valuable for initial assessment but not suitable for regulatory compliance

**Known Discrepancies:**
```sql
-- Example of ZIMAS vs FEMA conflict
APN: 5036019013
Address: 3936 S DALTON AVE
ZIMAS: "500 Yr" flood zone
FEMA: "Zone X" (minimal hazard)
Coordinates: 34.013866, -118.303116
Resolution: FEMA determination takes precedence
```

#### 4. **USGS National Map** - Authority Rank: 4
- **Confidence Score**: 0.80
- **Source Type**: Government Scientific
- **Regulatory Status**: Scientific reference, not regulatory
- **API Endpoint**: https://apps.nationalmap.gov/services
- **Update Frequency**: Annual
- **Spatial Resolution**: 1:24,000 scale
- **Coverage**: National, hydrographic data

**Justification for #4 Ranking:**
- Provides foundational hydrographic and elevation data
- Used in flood modeling and validation
- High scientific accuracy but not regulatory authority
- Useful for understanding flood patterns and topography
- Supports FEMA flood studies but is not the final determination

#### 5. **Private Insurance Maps** - Authority Rank: 5
- **Confidence Score**: 0.70
- **Source Type**: Commercial
- **Regulatory Status**: No regulatory authority
- **API Endpoint**: Varies by provider
- **Update Frequency**: Varies
- **Spatial Resolution**: Varies
- **Coverage**: Varies

**Justification for #5 Ranking:**
- May include proprietary risk models and additional factors
- Not suitable for regulatory compliance or insurance requirements
- Can provide enhanced risk assessment beyond FEMA minimum standards
- Useful for private investment decisions but not authoritative
- Quality and methodology varies significantly by provider

## Implementation Guidelines

### Data Source Selection Logic
```sql
-- Recommended source selection logic for flood zone queries
SELECT
    property_id,
    COALESCE(
        fema_nfhl.flood_zone,           -- First preference: FEMA NFHL
        fema_msc.flood_zone,            -- Second preference: FEMA MSC
        zimas.zimas_flood_zone,         -- Third preference: ZIMAS
        'Unknown'                       -- Default if no data
    ) as authoritative_flood_zone,
    CASE
        WHEN fema_nfhl.flood_zone IS NOT NULL THEN 0.98
        WHEN fema_msc.flood_zone IS NOT NULL THEN 0.98
        WHEN zimas.zimas_flood_zone IS NOT NULL THEN 0.85
        ELSE 0.0
    END as data_confidence
FROM properties p
LEFT JOIN fema_nfhl ON p.apn = fema_nfhl.apn
LEFT JOIN fema_msc ON p.apn = fema_msc.apn
LEFT JOIN zimas_flood_zones zimas ON p.apn = zimas.apn;
```

### Conflict Resolution Protocol

#### When Sources Disagree:
1. **FEMA sources always take precedence** over municipal or commercial sources
2. **Manual FEMA verification** required for high-value transactions
3. **Flag conflicts** in data_quality_flags for review
4. **Document discrepancies** with coordinates and source details

#### Conflict Resolution Example:
```json
{
    "apn": "5036019013",
    "conflict_type": "source_disagreement",
    "primary_source": "FEMA",
    "primary_value": "Zone X",
    "secondary_source": "ZIMAS",
    "secondary_value": "500 Yr",
    "resolution": "FEMA authoritative - use Zone X",
    "verification_url": "https://msc.fema.gov/portal/search#34.013866,-118.303116"
}
```

### Data Quality Monitoring

#### Automated Quality Checks:
1. **Source Authority Validation**: Ensure highest authority source is used
2. **Conflict Detection**: Flag disagreements between sources
3. **Coverage Analysis**: Monitor data completeness across sources
4. **Freshness Monitoring**: Track last update dates by source
5. **Confidence Scoring**: Calculate composite confidence based on source mix

#### Quality Metrics for 457K Properties:
```sql
-- Coverage analysis query
SELECT
    COUNT(*) as total_properties,
    COUNT(fema_data.apn) as fema_coverage,
    COUNT(zimas_data.apn) as zimas_coverage,
    COUNT(CASE WHEN fema_data.apn IS NOT NULL AND zimas_data.apn IS NOT NULL
          AND fema_data.flood_zone != zimas_data.flood_zone THEN 1 END) as conflicts,
    ROUND(AVG(CASE
        WHEN fema_data.apn IS NOT NULL THEN 0.98
        WHEN zimas_data.apn IS NOT NULL THEN 0.85
        ELSE 0.0
    END), 3) as avg_confidence
FROM unified_property_data p
LEFT JOIN fema_flood_zones fema_data ON p.apn = fema_data.apn
LEFT JOIN zimas_flood_zones zimas_data ON p.apn = zimas_data.apn;
```

## Database Schema

### Data Source Hierarchy Table
```sql
CREATE TABLE data_source_hierarchy (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL UNIQUE,
    source_type VARCHAR(50) NOT NULL,
    authority_rank INTEGER NOT NULL,
    confidence_score DECIMAL(3,2) NOT NULL CHECK (confidence_score >= 0.0 AND confidence_score <= 1.0),
    data_category VARCHAR(50) NOT NULL,
    description TEXT,
    api_endpoint TEXT,
    update_frequency VARCHAR(50),
    spatial_resolution VARCHAR(50),
    temporal_coverage VARCHAR(100),
    access_method VARCHAR(50),
    cost_tier VARCHAR(20),
    last_verified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT
);

-- Indexes for performance
CREATE INDEX idx_source_hierarchy_rank ON data_source_hierarchy(authority_rank);
CREATE INDEX idx_source_hierarchy_category ON data_source_hierarchy(data_category);
CREATE INDEX idx_source_hierarchy_confidence ON data_source_hierarchy(confidence_score);
```

### Source Authority Tracking
```sql
-- Add to environmental data tables
ALTER TABLE zimas_flood_zones
ADD COLUMN source_authority_id INTEGER REFERENCES data_source_hierarchy(id),
ADD COLUMN data_confidence DECIMAL(3,2) DEFAULT 0.85,
ADD COLUMN last_source_verification TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE fema_flood_zones
ADD COLUMN source_authority_id INTEGER REFERENCES data_source_hierarchy(id),
ADD COLUMN data_confidence DECIMAL(3,2) DEFAULT 0.98,
ADD COLUMN last_source_verification TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
```

## Developer Guidelines

### Best Practices

#### 1. Always Use Source Hierarchy
- Query sources in authority rank order
- Use COALESCE() to fallback through source hierarchy
- Document which source was used for each determination

#### 2. Implement Conflict Detection
- Compare values across sources for the same property
- Flag discrepancies in data_quality_flags
- Require manual review for high-value decisions

#### 3. Maintain Data Lineage
- Track source_authority_id for all environmental data
- Record last_source_verification timestamps
- Log source changes and updates

#### 4. Performance Optimization
- Index source_authority_id columns
- Use materialized views for common source hierarchy queries
- Cache FEMA API responses to reduce external calls

### Code Examples

#### Source-Aware Flood Zone Query:
```python
def get_authoritative_flood_zone(apn: str) -> dict:
    """Get flood zone using source hierarchy"""
    query = """
    SELECT
        COALESCE(fema.flood_zone, zimas.zimas_flood_zone) as flood_zone,
        CASE
            WHEN fema.flood_zone IS NOT NULL THEN 'FEMA'
            WHEN zimas.zimas_flood_zone IS NOT NULL THEN 'ZIMAS'
            ELSE 'None'
        END as source_used,
        CASE
            WHEN fema.flood_zone IS NOT NULL THEN 0.98
            WHEN zimas.zimas_flood_zone IS NOT NULL THEN 0.85
            ELSE 0.0
        END as confidence_score
    FROM unified_property_data p
    LEFT JOIN fema_flood_zones fema ON p.apn = fema.apn
    LEFT JOIN zimas_flood_zones zimas ON p.apn = zimas.apn
    WHERE p.apn = %s;
    """
    # Execute query and return results
```

#### Conflict Detection:
```python
def detect_flood_zone_conflicts() -> list:
    """Detect conflicts between flood zone sources"""
    query = """
    SELECT p.apn, p.site_address,
           fema.flood_zone as fema_zone,
           zimas.zimas_flood_zone as zimas_zone
    FROM unified_property_data p
    JOIN fema_flood_zones fema ON p.apn = fema.apn
    JOIN zimas_flood_zones zimas ON p.apn = zimas.apn
    WHERE fema.flood_zone != zimas.zimas_flood_zone;
    """
    # Execute and return conflicts for review
```

## Compliance and Legal Considerations

### Regulatory Requirements
- **FEMA sources required** for flood insurance determinations
- **Municipal sources acceptable** for general planning purposes
- **Commercial sources prohibited** for regulatory compliance
- **Manual verification recommended** for high-value transactions

### Liability Considerations
- Document source hierarchy decisions in audit logs
- Maintain verification URLs for manual FEMA checks
- Flag uncertain determinations for professional review
- Update source authority rankings based on regulatory changes

## Future Enhancements

### Planned Improvements
1. **Automated FEMA API Integration**: Direct NFHL queries when APIs become available
2. **Real-time Conflict Monitoring**: Automated detection of source disagreements
3. **Historical Source Tracking**: Version control for source authority changes
4. **Enhanced Confidence Scoring**: Machine learning-based confidence algorithms
5. **Multi-hazard Extension**: Expand hierarchy to wildfire, earthquake, and other risks

### Source Authority Research Pipeline
- Quarterly review of source authority rankings
- Annual validation against regulatory requirements
- Continuous monitoring of API availability and reliability
- Integration with new authoritative data sources as they become available

---

**Document Version**: 1.0
**Last Updated**: 2025-01-23
**Author**: DealGenie Data Engineering Team
**Review Cycle**: Quarterly
**Next Review**: 2025-04-23