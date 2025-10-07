# WEEK 1-5 COMPONENT VALIDATION PLAN
## Systematic Testing on 2.4M Property Database

**Date:** October 6, 2025
**Status:** 🔄 VALIDATION IN PROGRESS
**Database:** dealgenie_production (PostgreSQL)
**Properties:** 2,429,023 total

---

## Executive Summary

This plan validates Week 1-5 scoring components against the complete 2.4M+ LA County property database to ensure:
- All scoring functions work with properties_unified view
- API endpoints properly integrate with PostgreSQL
- Performance is acceptable at scale
- Results match expected business logic

---

## Component Inventory

### 1. Core Scoring Engine

**Location:** `/scoring/engine.py`

**Main Function:**
```python
def score_property(features: Dict[str, Any], template: str) -> Dict[str, Any]:
    """
    Score a property for development potential

    Args:
        features: Property feature dictionary with:
            - zoning_code: str
            - lot_size_sqft: float
            - assessed_value: float
            - crime_factor: float (optional)
            - transit_distance: float (optional)
            - demographics data (optional)

        template: Development type ('multifamily', 'residential', 'commercial', 'industrial', 'retail')

    Returns:
        {
            'total_score': float (0-10),
            'component_scores': {
                'zoning': float,
                'lot_size': float,
                'transit': float,
                'demographics': float,
                'market': float
            },
            'penalties': dict,
            'explanation': str,
            'recommendations': list
        }
    """
```

**Dependencies:**
- Zoning code (required)
- Lot size (required)
- Assessed value (optional, defaults to median)
- Crime data (optional)
- Transit data (optional)
- Demographics (optional - Census API)

**Template Types:**
1. `multifamily` - High-density residential
2. `residential` - Single-family homes
3. `commercial` - Office/retail
4. `industrial` - Manufacturing/logistics
5. `retail` - Shopping centers

### 2. Component Scorers

**Zoning Score** (lines 160-320):
- Maps zoning codes to scores (0-10)
- Template-specific logic
- Supports 40+ LA County zone types

**Lot Size Score** (lines 320-350):
- Minimum thresholds by template
- Bonus for larger lots
- Penalties for undersized

**Transit Score** (lines 350-380):
- Distance to nearest station
- Walkability considerations
- Template-specific weights

**Demographics Score** (lines 380-410):
- Median income
- Population density
- Age distribution
- Census API integration

**Market Score** (lines 410-440):
- Assessed value analysis
- Comparable properties
- Market trends

### 3. Penalty System

**Location:** `scoring/engine.py` lines 34-130

**Penalty Types:**
```python
penalties = {
    'flood_zone': 1.2,           # Reduced from 2.0
    'high_crime': 1.0,           # Max reduced from 1.5
    'toxic_sites': 2.0,          # Max reduced from 3.0
    'airport_noise': 0.6,        # Max reduced from 1.0
    'homeless_concentration': 1.0, # Max reduced from 1.5
    'freeway_noise': 0.6,        # Reduced from 1.0
    'pollution': 1.2,            # Max reduced from 2.0
    'seismic_risk': 1.0,         # Max reduced from 1.5
    'infrastructure': 1.0        # Max reduced from 1.5
}
```

**Data Sources:**
- Flood zones: ZIMAS data or FEMA overlay
- Crime: LAPD crime density grid
- Toxic sites: EPA Superfund database
- Airport noise: FAA noise contours
- Seismic: California seismic hazard maps

### 4. API Services

**Service 1: User Preference System**
- **File:** `user_preference_system.py`
- **Port:** 8009
- **Endpoints:**
  - `GET /preferences` - Get user scoring weights
  - `POST /preferences` - Update weights
  - `GET /preferences/interface` - Interactive UI

**Service 2: Property Intelligence System**
- **File:** `expanded_property_intelligence_system.py`
- **Port:** 8010
- **Endpoints:**
  - `GET /intelligence` - Get property analysis
  - `POST /intelligence/score` - Score property
  - `GET /intelligence/interface` - Interactive UI

**Service 3: Data Import System**
- **File:** `user_data_import_system.py`
- **Port:** 8011
- **Endpoints:**
  - `POST /import/csv` - Upload property CSV
  - `GET /import/status` - Check import status
  - `GET /import/interface` - Upload UI

**Service 4: Auth/Security System**
- **File:** `auth_security_system.py`
- **Port:** 8012
- **Endpoints:**
  - `POST /auth/register` - Create API key
  - `POST /auth/validate` - Validate request
  - `GET /auth/usage` - Usage analytics

---

## Data Dependencies

### Required Fields (from properties_unified)

**Minimum for Scoring:**
```sql
SELECT
    apn,
    zoning_code,        -- REQUIRED
    lot_size_sqft,      -- REQUIRED
    land_value,         -- Optional (defaults used)
    improvement_value   -- Optional (defaults used)
FROM properties_unified
WHERE zoning_code IS NOT NULL
  AND lot_size_sqft > 0;
```

**Enhanced Scoring (if available):**
```sql
SELECT
    apn,
    zoning_code,
    lot_size_sqft,
    crime_score,                    -- From enhanced_scored_properties
    walkability_score,              -- From enhanced_scored_properties
    transit_accessibility_bonus,    -- From enhanced_scored_properties
    location_premium_bonus          -- From enhanced_scored_properties
FROM properties_unified
WHERE data_source = 'week1-5_enhanced';
```

### External Data Sources

**1. Census Demographics** (optional):
- **API:** census.gov/data/developers
- **Endpoint:** `/acs/acs5`
- **Fields:** median income, population, age distribution
- **Rate Limit:** 500/day (free tier)

**2. Crime Data** (optional):
- **Source:** LAPD crime density grid (if available in DB)
- **Fallback:** Crime score from enhanced_scored_properties
- **Field:** `crime_score` (0-100 scale)

**3. Transit Data** (optional):
- **Source:** LA Metro GTFS or database
- **Calculation:** Distance to nearest station
- **Fallback:** `transit_accessibility_bonus` from enhanced_scored_properties

---

## Validation Test Suite

### Test 1: Database Connectivity
**Objective:** Verify scoring engine can read from properties_unified

**Test Script:** `test_01_database_connection.py`
```python
import psycopg2
from scoring.engine import score_property

def test_database_connection():
    """Test we can connect and fetch properties"""
    conn = psycopg2.connect(
        host='localhost',
        database='dealgenie_production',
        user='samanthagrant',
        port=5432
    )
    cursor = conn.cursor()

    # Get 10 sample properties
    cursor.execute('''
        SELECT apn, zoning_code, lot_size_sqft, land_value, improvement_value
        FROM properties_unified
        WHERE zoning_code IS NOT NULL
          AND lot_size_sqft > 0
        LIMIT 10;
    ''')

    properties = cursor.fetchall()
    assert len(properties) > 0, "No properties retrieved"

    print(f"✅ Retrieved {len(properties)} properties from database")
    conn.close()
```

**Expected Result:** 10 properties retrieved successfully

**Pass Criteria:**
- No connection errors
- At least 1 property returned
- All required fields populated

---

### Test 2: Scoring Engine Integration
**Objective:** Verify scoring engine works with database properties

**Test Script:** `test_02_scoring_integration.py`
```python
import psycopg2
from scoring.engine import score_property

def test_scoring_integration():
    """Test scoring engine with database properties"""
    conn = psycopg2.connect(
        host='localhost',
        database='dealgenie_production',
        user='samanthagrant',
        port=5432
    )
    cursor = conn.cursor()

    # Get sample properties
    cursor.execute('''
        SELECT apn, zoning_code, lot_size_sqft, land_value, improvement_value
        FROM properties_unified
        WHERE zoning_code IS NOT NULL
          AND lot_size_sqft > 0
        ORDER BY RANDOM()
        LIMIT 5;
    ''')

    templates = ['multifamily', 'residential', 'commercial', 'industrial', 'retail']

    for row in cursor.fetchall():
        apn, zoning, lot_size, land_val, imp_val = row

        # Build features dict
        features = {
            'zoning_code': zoning,
            'lot_size_sqft': float(lot_size) if lot_size else 0,
            'land_value': float(land_val) if land_val else None,
            'improvement_value': float(imp_val) if imp_val else None
        }

        # Test each template
        for template in templates:
            result = score_property(features, template)

            assert 'total_score' in result
            assert 0 <= result['total_score'] <= 10
            assert 'component_scores' in result

            print(f"✅ {apn} ({template}): {result['total_score']:.1f}/10")

    conn.close()
```

**Expected Result:** All properties scored successfully for all templates

**Pass Criteria:**
- No errors during scoring
- Scores between 0-10
- All components populated
- Explanations generated

---

### Test 3: Zoning Code Coverage
**Objective:** Verify scoring engine handles all zoning codes in database

**Test Script:** `test_03_zoning_coverage.py`
```python
import psycopg2
from scoring.engine import score_property

def test_zoning_coverage():
    """Test scoring engine handles all unique zoning codes"""
    conn = psycopg2.connect(
        host='localhost',
        database='dealgenie_production',
        user='samanthagrant',
        port=5432
    )
    cursor = conn.cursor()

    # Get top 50 most common zoning codes
    cursor.execute('''
        SELECT zoning_code, COUNT(*) as count
        FROM properties_unified
        WHERE zoning_code IS NOT NULL
        GROUP BY zoning_code
        ORDER BY count DESC
        LIMIT 50;
    ''')

    zones = cursor.fetchall()
    print(f"\nTesting {len(zones)} most common zoning codes:\n")

    unhandled_zones = []

    for zone_code, count in zones:
        # Test if zone generates reasonable score
        features = {
            'zoning_code': zone_code,
            'lot_size_sqft': 5000.0
        }

        result = score_property(features, 'multifamily')

        # Check if score is reasonable (not default fallback)
        if result['component_scores']['zoning'] < 2.5:
            unhandled_zones.append((zone_code, count))

        print(f"  {zone_code}: {result['component_scores']['zoning']:.1f}/10 ({count:,} properties)")

    if unhandled_zones:
        print(f"\n⚠️  {len(unhandled_zones)} zones may need handler updates:")
        for zone, count in unhandled_zones:
            print(f"  {zone}: {count:,} properties")
    else:
        print(f"\n✅ All {len(zones)} zoning codes handled properly")

    conn.close()
```

**Expected Result:** All major zoning codes produce reasonable scores

**Pass Criteria:**
- Top 50 zones all scored
- Scores vary by zone type
- No crashes on unusual zones
- Documented list of unhandled zones

---

### Test 4: Performance at Scale
**Objective:** Verify acceptable performance on large batches

**Test Script:** `test_04_performance_scale.py`
```python
import psycopg2
import time
from scoring.engine import score_property

def test_performance_scale():
    """Test scoring performance on 1000 properties"""
    conn = psycopg2.connect(
        host='localhost',
        database='dealgenie_production',
        user='samanthagrant',
        port=5432
    )
    cursor = conn.cursor()

    # Get 1000 random properties
    cursor.execute('''
        SELECT apn, zoning_code, lot_size_sqft, land_value, improvement_value
        FROM properties_unified
        WHERE zoning_code IS NOT NULL
          AND lot_size_sqft > 0
        ORDER BY RANDOM()
        LIMIT 1000;
    ''')

    properties = cursor.fetchall()

    print(f"\nScoring {len(properties)} properties...\n")

    start_time = time.time()
    scores = []

    for row in properties:
        apn, zoning, lot_size, land_val, imp_val = row

        features = {
            'zoning_code': zoning,
            'lot_size_sqft': float(lot_size),
            'land_value': float(land_val) if land_val else None,
            'improvement_value': float(imp_val) if imp_val else None
        }

        result = score_property(features, 'multifamily')
        scores.append(result['total_score'])

    end_time = time.time()
    duration = end_time - start_time

    # Calculate metrics
    properties_per_second = len(properties) / duration
    avg_time_ms = (duration / len(properties)) * 1000

    print(f"✅ Performance Metrics:")
    print(f"  Total time: {duration:.2f} seconds")
    print(f"  Properties/second: {properties_per_second:.1f}")
    print(f"  Average time per property: {avg_time_ms:.2f}ms")
    print(f"  Min score: {min(scores):.1f}")
    print(f"  Max score: {max(scores):.1f}")
    print(f"  Average score: {sum(scores)/len(scores):.1f}")

    # Performance targets
    assert properties_per_second > 10, f"Too slow: {properties_per_second:.1f} props/sec"
    assert avg_time_ms < 100, f"Too slow: {avg_time_ms:.2f}ms per property"

    print(f"\n✅ Performance targets met")

    conn.close()
```

**Expected Result:** >10 properties/second, <100ms per property

**Pass Criteria:**
- Processes 1000 properties without errors
- Performance > 10 properties/second
- Average time < 100ms per property
- Memory usage stays reasonable

---

### Test 5: Enhanced Data Integration
**Objective:** Verify enhanced scoring fields are used when available

**Test Script:** `test_05_enhanced_data.py`
```python
import psycopg2
from scoring.engine import score_property

def test_enhanced_data():
    """Test enhanced scoring with Week 1-5 data"""
    conn = psycopg2.connect(
        host='localhost',
        database='dealgenie_production',
        user='samanthagrant',
        port=5432
    )
    cursor = conn.cursor()

    # Get enhanced properties
    cursor.execute('''
        SELECT
            apn,
            zoning_code,
            lot_size_sqft,
            crime_score,
            data_quality_score,
            property_type
        FROM properties_unified
        WHERE data_source = 'week1-5_enhanced';
    ''')

    enhanced = cursor.fetchall()

    print(f"\nTesting {len(enhanced)} enhanced properties:\n")

    for row in enhanced:
        apn, zoning, lot_size, crime, quality, prop_type = row

        # Test with enhanced data
        features = {
            'zoning_code': zoning,
            'lot_size_sqft': float(lot_size),
            'crime_factor': (100 - crime) / 100 if crime else 1.0,  # Convert to factor
            'data_quality': quality
        }

        result = score_property(features, 'multifamily')

        print(f"  {apn}:")
        print(f"    Type: {prop_type}")
        print(f"    Score: {result['total_score']:.1f}/10")
        print(f"    Crime penalty: {result['penalties'].get('high_crime', 0):.1f}")
        print(f"    Components: {result['component_scores']}")
        print()

    conn.close()
```

**Expected Result:** Enhanced properties use crime/quality data in scoring

**Pass Criteria:**
- Enhanced data fields properly integrated
- Crime scores affect penalties
- Quality scores considered
- Different scores than basic properties

---

### Test 6: API Endpoint Validation
**Objective:** Verify API services work with PostgreSQL backend

**Test Script:** `test_06_api_endpoints.py`
```python
import requests
import psycopg2

def test_api_endpoints():
    """Test API endpoints with database properties"""

    # Get a sample property from database
    conn = psycopg2.connect(
        host='localhost',
        database='dealgenie_production',
        user='samanthagrant',
        port=5432
    )
    cursor = conn.cursor()

    cursor.execute('''
        SELECT apn, zoning_code, lot_size_sqft
        FROM properties_unified
        WHERE zoning_code IS NOT NULL
        LIMIT 1;
    ''')

    apn, zoning, lot_size = cursor.fetchone()
    conn.close()

    # Test Property Intelligence API (if running)
    print("\nTesting Property Intelligence API:")
    try:
        response = requests.post(
            'http://localhost:8010/intelligence/score',
            json={
                'apn': apn,
                'zoning_code': zoning,
                'lot_size_sqft': float(lot_size),
                'template': 'multifamily'
            },
            timeout=5
        )

        if response.status_code == 200:
            result = response.json()
            print(f"  ✅ API returned score: {result.get('total_score', 'N/A')}")
        else:
            print(f"  ⚠️  API returned status {response.status_code}")
    except requests.exceptions.ConnectionError:
        print(f"  ⚠️  API not running (start with: python expanded_property_intelligence_system.py)")

    # Test User Preference API
    print("\nTesting User Preference API:")
    try:
        response = requests.get('http://localhost:8009/preferences', timeout=5)

        if response.status_code == 200:
            prefs = response.json()
            print(f"  ✅ API returned preferences")
        else:
            print(f"  ⚠️  API returned status {response.status_code}")
    except requests.exceptions.ConnectionError:
        print(f"  ⚠️  API not running (start with: python user_preference_system.py)")
```

**Expected Result:** APIs respond correctly with database data

**Pass Criteria:**
- APIs accept database property data
- Responses include proper scoring
- No errors on valid requests
- Documented which APIs need to be running

---

## Test Execution Plan

### Phase 1: Core Functionality (Day 1)
1. Run Test 1: Database Connectivity
2. Run Test 2: Scoring Integration
3. Fix any critical issues

**Success Criteria:**
- ✅ Database connection working
- ✅ Basic scoring functional
- ✅ No crashes on valid input

### Phase 2: Coverage & Scale (Day 2)
1. Run Test 3: Zoning Coverage
2. Run Test 4: Performance Scale
3. Document unhandled zones
4. Optimize if needed

**Success Criteria:**
- ✅ Top 50 zones handled
- ✅ >10 properties/second
- ✅ List of zones needing updates

### Phase 3: Enhanced Features (Day 3)
1. Run Test 5: Enhanced Data
2. Verify crime/quality integration
3. Test with all 3 enhanced properties

**Success Criteria:**
- ✅ Enhanced fields properly used
- ✅ Scores differ from basic properties
- ✅ Penalties applied correctly

### Phase 4: API Integration (Day 4)
1. Run Test 6: API Endpoints
2. Test each API service
3. Document startup requirements

**Success Criteria:**
- ✅ APIs work with PostgreSQL
- ✅ Documented startup process
- ✅ No errors on database queries

### Phase 5: Production Validation (Day 5)
1. Run full test suite
2. Generate validation report
3. Document known limitations
4. Create deployment checklist

**Success Criteria:**
- ✅ All tests passing
- ✅ Complete documentation
- ✅ Production readiness confirmed

---

## Known Limitations & Workarounds

### 1. Missing Enhanced Data
**Issue:** Only 3 properties have enhanced Week 1-5 scoring

**Impact:** Most properties won't have crime_score, walkability_score, etc.

**Workaround:**
- Use defaults for missing fields
- Clearly indicate when enhanced data is missing
- Plan to import full 1,000 property CSV

### 2. Census API Dependency
**Issue:** Scoring engine can use Census API for demographics

**Impact:** Rate limits (500/day free tier)

**Workaround:**
- Make demographics optional
- Cache results in database
- Use bulk API calls
- Consider paid tier for production

### 3. External Data Sources
**Issue:** Some penalty factors need external data (crime, toxic sites, flood zones)

**Impact:** May not have complete penalty calculation

**Workaround:**
- Document required data sources
- Provide fallback defaults
- Integrate ZIMAS flood zone data
- Use LAPD crime grid if available

### 4. Zoning Code Variations
**Issue:** Database has 99.79% zoning coverage but many variants

**Impact:** Some zones may get default scores

**Workaround:**
- Use envelope calculator normalization
- Map variants to base zones
- Document unhandled zones
- Update scoring engine mappings

---

## Success Metrics

### Functional Metrics
- ✅ All tests pass without errors
- ✅ >95% zoning code coverage
- ✅ Enhanced data properly integrated
- ✅ APIs functional with PostgreSQL

### Performance Metrics
- ✅ >10 properties/second scoring
- ✅ <100ms average per property
- ✅ <5 seconds for 100 property batch
- ✅ Memory usage <500MB for 1000 properties

### Data Quality Metrics
- ✅ Scores between 0-10 for all properties
- ✅ Component scores sum correctly
- ✅ Penalties applied appropriately
- ✅ Explanations generated for all

---

## Deployment Checklist

### Pre-Deployment
- [ ] All validation tests passing
- [ ] Performance benchmarks met
- [ ] Documentation complete
- [ ] Known limitations documented

### Database Setup
- [ ] properties_unified view created
- [ ] Permissions granted to app user
- [ ] Indexes on commonly queried fields
- [ ] Connection pooling configured

### Application Configuration
- [ ] Database connection strings updated
- [ ] Census API key configured (if using)
- [ ] Logging configured
- [ ] Error handling verified

### API Services
- [ ] All 4 API services tested
- [ ] Startup scripts created
- [ ] Health check endpoints working
- [ ] Rate limiting configured

### Monitoring
- [ ] Performance logging enabled
- [ ] Error tracking configured
- [ ] Usage analytics setup
- [ ] Alerting for failures

---

## Next Steps

1. **Execute Phase 1 Tests** - Validate core functionality
2. **Create Test Scripts** - Implement all 6 test scripts
3. **Run Validation Suite** - Execute complete test plan
4. **Document Results** - Generate validation report
5. **Fix Issues** - Address any failures
6. **Production Deploy** - Deploy validated system

---

*Created: October 6, 2025*
*Status: Ready for Execution*
*Target: Week 1-5 Component Validation*
