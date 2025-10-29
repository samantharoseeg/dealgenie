# Monte Carlo Readiness Audit

**Date:** 2025-10-09
**Database:** dealgenie_production
**Parcels:** 2,429,023 rows
**Audit Script:** scripts/db_health_audit.py

---

## Executive Summary

✅ **READY FOR MC ADD-ON** with specific gaps to address

The DealGenie database infrastructure is solid and contains envelope data needed for Monte Carlo analysis. However, the Python application layer is skeletal (empty modules), and key dependencies are missing. The envelope calculations exist in the `underutilization_scores` table but need to be exposed via API.

---

## 1. Current State (What Exists)

### ✅ Database Infrastructure (STRONG)

**Tables:**
- `parcels_complete`: 2,429,023 rows, 4360 MB (4015 MB table + 345 MB indexes)
- `parcels_with_boundaries`: 2,429,053 rows, 3433 MB (2722 MB table + 711 MB indexes)
- `underutilization_scores`: Contains envelope calculations (FAR, lot size, building size)
- `parcel_adjacency`: 2849 MB (for assemblage analysis)

**PostGIS:**
- Version: 3.6 (USE_GEOS=1 USE_PROJ=1 USE_STATS=1)
- Geometry validity: 2,428,949 valid / 2,429,053 total (99.996%)
- SRID: 4326 (WGS84)

**Indexes (EXCELLENT):**
- B-tree on `parcels_complete.apn` (16 MB)
- B-tree on `parcels_complete.zoning_code` (16 MB)
- B-tree on `parcels_with_boundaries.ain` (146 MB)
- GiST spatial index on `parcels_with_boundaries.geom` (98 MB)
- B-tree on centroid_lat (110 MB) and centroid_lon (114 MB)

**Performance Baseline (10 runs each):**
```
Random APN lookup:           Median 0.14 ms
Spatial point-in-polygon:    Median 0.15 ms
Large result set (1000 rows): Median 6.71 ms
```

### ✅ Envelope Data (EXISTS but separate table)

**Table:** `underutilization_scores` (19 columns)

Critical MC inputs available:
- `existing_far`: Current floor area ratio
- `allowed_far`: Zoning envelope maximum FAR
- `far_gap`: Unused development capacity (allowed_far - existing_far)
- `lot_size`: Parcel area in square feet
- `building_sqft`: Existing building size
- `year_built`: Building age
- `land_value`, `imp_value`: Assessor valuations
- `lon`, `lat`: Location coordinates
- `score`, `tier`: Underutilization scoring (already computed)

**Sample Row:**
```
AIN: 2249003017
Zone: C2-1VL
Score: 99.7 (High tier)
existing_far: 0.01
allowed_far: 1.5
far_gap: 0.995
lot_size: 22,628 sqft
building_sqft: 176 sqft
year_built: 1955
land_value: $1,697,614
```

### ⚠️ Python Application Layer (SKELETAL)

**Project Structure:**
```
~/workspace/dealgenie/
├── cli/          (empty - only __init__.py)
├── config/       (empty - only __init__.py)
├── db/           (empty - only __init__.py)
├── features/     (empty - only __init__.py)
├── ingest/       (empty - only __init__.py)
├── reporting/    (empty - only __init__.py)
├── scoring/      (empty - only __init__.py)
├── scripts/      (2 scripts: db_health_audit.py, check_envelope_data.py)
└── tests/        (empty - only __init__.py)
```

**No Implementation Code Found:**
- No envelope calculation code (data exists in DB but creation logic not in repo)
- No API endpoints
- No Monte Carlo code (expected - this is the add-on)
- No scoring algorithms (logic that created underutilization_scores is external)

### ⚠️ Python Environment

**Python Version:** 3.13.7 (pyproject.toml specifies ^3.11 ✓ compatible)

**Installed Packages:**
```
✅ psycopg2      2.9.10   (database connection)
✅ fastapi       0.117.1  (API framework)
✅ numpy         2.3.2    (numerical computing - CRITICAL for MC)
✅ pandas        2.3.2    (data manipulation)
✅ pydantic      2.11.9   (data validation)
✅ shapely       2.1.2    (geometry operations)
```

**Missing Packages (from pyproject.toml):**
```
❌ sqlalchemy               (ORM for database access)
❌ scipy                    (CRITICAL for MC: statistical distributions)
❌ geoalchemy2              (spatial database extensions for SQLAlchemy)
❌ redis                    (caching)
❌ uvicorn[standard]        (ASGI server)
❌ rich                     (CLI formatting)
❌ click                    (CLI framework)
❌ httpx                    (HTTP client)
```

**Package Manager:** Poetry not installed (pyproject.toml exists but poetry command not found)

---

## 2. Gaps (What's Missing for MC)

### 🔴 CRITICAL GAPS

1. **Python Dependencies Missing**
   - `scipy` required for statistical distributions (normal, lognormal, triangular, beta)
   - `sqlalchemy` + `geoalchemy2` required for database ORM
   - Install with: `pip install scipy sqlalchemy geoalchemy2` or setup Poetry

2. **No API Layer**
   - Envelope data exists in DB but not exposed via API
   - Need endpoints to query parcels and return envelope parameters
   - MC simulations will need to call APIs to get base case inputs

3. **APN Column Mismatch**
   - `parcels_complete` uses `AIN` column (2,429,022 populated)
   - `parcels_complete.apn` column is empty (0 populated)
   - `parcels_with_boundaries` uses `ain` column
   - Need to standardize on `AIN` for joins

### 🟡 MODERATE GAPS

4. **Missing Envelope Fields**
   - `underutilization_scores` has FAR data but missing:
     - `max_height`: Zoning height limit (feet)
     - `max_units`: Residential density limit
     - `setback_front`, `setback_rear`, `setback_side`: Setback requirements
     - `buildable_area`: Lot area after setbacks
   - These may need to be computed from zoning rules or added to scoring table

5. **No Priors API**
   - MC needs distribution priors (mean, sigma, dist_type) for:
     - Construction cost per sqft
     - Cap rate
     - Rent per sqft
     - Vacancy rate
     - Lease-up time
     - Exit cap rate
   - Will need to create endpoint: `GET /api/priors?zone={zone}&use={use}`

6. **No MC Database Schema**
   - Need to create tables:
     - `sim_inputs_snapshot`: Store simulation input assumptions
     - `sim_summary`: Store simulation results (percentiles, metrics)
   - Migration scripts required

### 🟢 MINOR GAPS

7. **No Tests**
   - `tests/` directory is empty
   - Will need unit tests for MC engine, integration tests for API

8. **Poetry Not Installed**
   - pyproject.toml exists but Poetry not available
   - Can work around with `pip install -r requirements.txt` or install Poetry

---

## 3. Recommended Actions (Prioritized)

### Phase 0: Environment Setup (DO FIRST)

```bash
# Option A: Install Poetry and use it
curl -sSL https://install.python-poetry.org | python3 -
poetry install

# Option B: Use pip directly
pip install scipy sqlalchemy geoalchemy2 redis uvicorn[standard] rich click httpx
```

**Test installation:**
```python
import scipy.stats
import sqlalchemy
import geoalchemy2
print("✅ Critical MC dependencies available")
```

### Phase 1: Database Schema (REQUIRED for MC)

Create MC tables:

```sql
-- Store simulation input snapshots
CREATE TABLE sim_inputs_snapshot (
    id SERIAL PRIMARY KEY,
    ain TEXT NOT NULL,
    sim_id UUID NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),

    -- Envelope inputs
    lot_size_sqft REAL,
    buildable_area_sqft REAL,
    existing_far REAL,
    allowed_far REAL,
    max_height_ft REAL,
    max_units INT,

    -- Cost/revenue priors (distributions)
    construction_cost_dist JSONB,  -- {dist: "lognormal", mean: 450, sigma: 50}
    rent_psf_dist JSONB,
    cap_rate_dist JSONB,

    -- Full snapshot
    inputs JSONB
);

CREATE INDEX idx_sim_inputs_ain ON sim_inputs_snapshot(ain);
CREATE INDEX idx_sim_inputs_sim_id ON sim_inputs_snapshot(sim_id);

-- Store simulation results
CREATE TABLE sim_summary (
    id SERIAL PRIMARY KEY,
    ain TEXT NOT NULL,
    sim_id UUID NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),

    -- Summary statistics
    num_scenarios INT,
    npv_p10 REAL,
    npv_p50 REAL,
    npv_p90 REAL,
    irr_p10 REAL,
    irr_p50 REAL,
    irr_p90 REAL,

    -- Risk metrics
    downside_risk REAL,  -- probability NPV < 0
    upside_potential REAL,  -- P90/P50 ratio

    -- Full results
    results JSONB  -- {scenarios: [...], percentiles: {...}, tornado: {...}}
);

CREATE INDEX idx_sim_summary_ain ON sim_summary(ain);
CREATE INDEX idx_sim_summary_created_at ON sim_summary(created_at DESC);
```

### Phase 2: API Endpoints (REQUIRED for MC)

Create minimal API to expose envelope data:

**File:** `features/envelope_api.py`

```python
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2

app = FastAPI()

class EnvelopeResponse(BaseModel):
    ain: str
    lot_size_sqft: float
    existing_far: float
    allowed_far: float
    far_gap: float
    building_sqft: float
    year_built: int
    land_value: float
    zoning_code: str

@app.get("/api/envelope/{ain}", response_model=EnvelopeResponse)
def get_envelope(ain: str):
    conn = psycopg2.connect(dbname='dealgenie_production', user='samanthagrant', host='localhost')
    cur = conn.cursor()

    cur.execute("""
        SELECT ain, lot_size, existing_far, allowed_far, far_gap,
               building_sqft, year_built, land_value, zone_code
        FROM underutilization_scores
        WHERE ain = %s
        LIMIT 1;
    """, (ain,))

    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail=f"AIN {ain} not found")

    return EnvelopeResponse(
        ain=row[0],
        lot_size_sqft=row[1],
        existing_far=row[2],
        allowed_far=row[3],
        far_gap=row[4],
        building_sqft=row[5],
        year_built=row[6],
        land_value=row[7],
        zoning_code=row[8]
    )
```

**Test:**
```bash
uvicorn features.envelope_api:app --reload
curl http://localhost:8000/api/envelope/2249003017
```

### Phase 3: MC Infrastructure (THE ADD-ON)

1. **MC Engine** (`features/monte_carlo.py`):
   - Cashflow model (10-year DCF)
   - Distribution sampling (scipy.stats)
   - Correlation handling (Cholesky decomposition)
   - Percentile calculation

2. **MC API** (`features/mc_api.py`):
   - `POST /api/simulate`: Run simulation
   - `GET /api/simulate/{sim_id}`: Get results
   - `GET /api/risk/{ain}`: Risk summary

3. **UI Components** (future):
   - Risk cards (P10/P50/P90 display)
   - Fan charts (NPV distribution over time)
   - Tornado diagrams (sensitivity analysis)

### Phase 4: Enhanced Envelope Data (NICE TO HAVE)

Add missing fields to `underutilization_scores` or create `envelope_complete` table:
- Max height (from zoning rules)
- Max units (from zoning rules)
- Setbacks (from zoning rules)
- Buildable area (computed: lot_size * (1 - setback_pct))

---

## 4. No-Go Items (Critical Blockers)

### 🚨 None currently blocking MC development

The following are NOT blockers:
- ✅ Empty Python modules (expected - this is new development)
- ✅ Poetry not installed (can use pip)
- ✅ Missing height/setback data (can use FAR-based model initially)
- ✅ APN column empty (AIN column is populated and works fine)

### ⚠️ Monitor These

1. **104 Invalid Geometries** in parcels_with_boundaries
   - 99.996% are valid, so not a blocker
   - But should fix eventually: `UPDATE parcels_with_boundaries SET geom = ST_MakeValid(geom) WHERE NOT ST_IsValid(geom);`

2. **Performance at Scale**
   - Current queries are fast (< 1ms)
   - MC will run 1000+ scenarios per parcel
   - May need caching layer (Redis) for priors
   - May need job queue for long-running simulations

---

## 5. Testing Strategy (Failure-First)

### MC Engine Tests (before trusting it)

1. **Deterministic Test** (seed=42):
   ```python
   np.random.seed(42)
   result = monte_carlo(scenarios=1000, cost_mean=400, cost_sigma=0)
   assert result.npv_p50 == expected_value  # No randomness, should match exactly
   ```

2. **Distribution Test**:
   ```python
   # Input: normal(100, 10), 10,000 scenarios
   # Output: P50 should be within 1% of 100
   result = monte_carlo(scenarios=10000, input_dist=normal(100, 10))
   assert 99 < result.output_p50 < 101
   ```

3. **Correlation Test**:
   ```python
   # Cost and rent should be correlated (rho=0.6)
   results = monte_carlo(scenarios=1000, cost_rent_corr=0.6)
   empirical_corr = np.corrcoef(results.costs, results.rents)[0,1]
   assert 0.55 < empirical_corr < 0.65
   ```

4. **Performance Test** (≥10 samples):
   ```bash
   for i in {1..10}; do
     time curl -X POST http://localhost:8000/api/simulate -d '{"ain":"2249003017"}'
   done
   # Report: min, max, median, p95
   ```

---

## 6. Evidence-Based Metrics

### Database Performance (10 runs, 2025-10-09)

| Query | Min (ms) | Median (ms) | Max (ms) | P95 (ms) |
|-------|----------|-------------|----------|----------|
| APN lookup | 0.13 | 0.14 | 1.64 | 1.64 |
| Spatial point-in-polygon | 0.14 | 0.15 | 12.46 | 12.46 |
| 1000 row fetch | 4.24 | 6.71 | 232.05 | 232.05 |
| Jurisdiction aggregate | 297.36 | 300.32 | 1797.35 | 1797.35 |

### Data Quality

| Metric | Value | % |
|--------|-------|---|
| Total parcels | 2,429,023 | 100% |
| AIN populated | 2,429,022 | 99.9996% |
| Valid geometries | 2,428,949 | 99.9957% |
| Zoning code populated | (not measured) | - |
| Underutilization scores | (not measured) | - |

---

## 7. Next Steps

**Immediate (Day 1):**
1. Install missing Python packages (scipy, sqlalchemy, geoalchemy2)
2. Create MC database tables (sim_inputs_snapshot, sim_summary)
3. Write minimal envelope API endpoint

**Short-term (Week 1):**
4. Build MC engine (cashflow model + distributions)
5. Write MC API endpoint (POST /api/simulate)
6. Unit tests for deterministic cases

**Medium-term (Week 2-3):**
7. Add correlation handling
8. Add priors API (get distribution params by zone)
9. Integration tests + performance tests (≥10 samples)

**Long-term (Month 1+):**
10. UI components (risk cards, fan charts, tornado diagrams)
11. Enhance envelope data (height, setbacks, units)
12. Caching layer for performance

---

## Appendix A: Connection Details

**Database:**
```python
DB_CONFIG = {
    'dbname': 'dealgenie_production',
    'user': 'samanthagrant',
    'host': 'localhost',
}
```

**Key Tables:**
- `parcels_complete`: Main assessor data (48 columns, AIN is key)
- `parcels_with_boundaries`: Spatial data (geom column, SRID 4326)
- `underutilization_scores`: Envelope + scoring data (19 columns)

**Join Key:** Use `AIN` (not `apn`) for all joins.

---

## Appendix B: Audit Commands

All commands run from: `~/workspace/dealgenie`

```bash
# Database health check
python3 scripts/db_health_audit.py

# Check envelope data
python3 scripts/check_envelope_data.py

# Check Python packages
python3 -c "import numpy, scipy, sqlalchemy"

# Performance test (10 runs)
for i in {1..10}; do
  python3 -c "import psycopg2, time; ..."
done
```

---

**Audit completed:** 2025-10-09
**Next audit:** After Phase 1 (database schema) completion
**Report location:** `docs/mc_readiness_audit.md`
