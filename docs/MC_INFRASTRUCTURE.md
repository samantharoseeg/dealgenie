# Monte Carlo Infrastructure (Week 7)

**Status:** Infrastructure complete, simulation engine pending (Week 8-9)

This document describes the Monte Carlo simulation infrastructure added to DealGenie. This is **infrastructure only** - the actual simulation engine will be implemented in Week 8-9.

---

## Overview

The Monte Carlo (MC) add-on provides risk analysis for real estate development opportunities. This release includes:

✅ **Database schema** for storing simulation inputs and results
✅ **Priors API** for distribution parameters (costs, revenues, timelines, financing)
✅ **Feature flags** for phased rollout (`MONTE_CARLO_ENABLED`, `CAPTURE_SIM_INPUTS`)
✅ **CLI command** for capturing simulation inputs
✅ **Report placeholder** for risk visualization
✅ **58 tests** with 100% coverage on infrastructure hooks

---

## Architecture

```
User runs CLI command
  ↓
Load envelope from database (underutilization_scores)
  ↓
Get priors from PriorsAPI (distribution parameters)
  ↓
Compute deterministic hash (SHA256 of inputs)
  ↓
Save snapshot to sim_inputs_snapshot table
  ↓
Check MONTE_CARLO_ENABLED flag
  ↓
If false: Show "MC disabled" message
If true: Run simulation (Week 8-9)
```

---

## Database Schema

### `sim_inputs_snapshot` Table

Stores simulation inputs for reproducibility.

| Column | Type | Description |
|--------|------|-------------|
| apn | TEXT | Assessor Parcel Number (primary key) |
| template | TEXT | Template name (e.g., 'multifamily') |
| inputs_hash | TEXT | SHA256 hash of inputs (primary key) |
| inputs_json | JSONB | Full envelope + priors JSON |
| sim_version | INTEGER | Simulation methodology version |
| created_at | TIMESTAMPTZ | Creation timestamp |

**Primary Key:** `(apn, template, inputs_hash)`
**Purpose:** Deterministic caching - same inputs → same hash → retrieve cached results

### `sim_summary` Table

Stores simulation results (not yet used - awaiting MC engine).

| Column | Type | Description |
|--------|------|-------------|
| apn | TEXT | Assessor Parcel Number |
| template | TEXT | Template name |
| sim_version | INTEGER | Simulation version |
| p5_irr | REAL | 5th percentile IRR (downside case) |
| p50_irr | REAL | Median IRR (base case) |
| p95_irr | REAL | 95th percentile IRR (upside case) |
| prob_hit_hurdle | REAL | Probability of hitting hurdle rate |
| var5_irr | REAL | Value at Risk at 5% |
| es5_irr | REAL | Expected Shortfall at 5% |
| inputs_hash | TEXT | Foreign key to sim_inputs_snapshot |
| runs | INTEGER | Number of MC scenarios |
| generated_at | TIMESTAMPTZ | Generation timestamp |

**Primary Key:** `(apn, template, sim_version)`
**Foreign Key:** `(apn, template, inputs_hash)` → `sim_inputs_snapshot`

---

## Priors API

Returns distribution parameters for uncertain variables.

### Categories

1. **Cost Priors** (`get_cost_priors`)
   - `hard_cost_psf`: Construction cost per SF (lognormal)
   - `soft_cost_pct`: Soft costs as % of hard costs (beta)
   - `contingency_pct`: Contingency as % of hard costs (beta)

2. **Revenue Priors** (`get_revenue_priors`)
   - `rent_psf_month`: Rental rate per SF/month (lognormal)
   - `vacancy_pct`: Stabilized vacancy rate (beta)
   - `absorption_months`: Lease-up duration (PERT)

3. **Timeline Priors** (`get_timeline_priors`)
   - `entitlement_months`: Time to get approvals (PERT)
   - `construction_months`: Time to build (PERT)

4. **Finance Priors** (`get_finance_priors`)
   - `interest_rate`: Construction loan rate (normal)
   - `cap_rate_exit`: Exit cap rate (normal)
   - `ltc`: Loan-to-cost ratio (beta)

### Distribution Format

Each prior returns:
```json
{
  "mean": 425,
  "sigma": 50,
  "dist": "lognormal",
  "source": "placeholder_la_mf_construction"
}
```

For PERT distributions:
```json
{
  "min": 18,
  "mode": 24,
  "max": 36,
  "dist": "pert",
  "source": "placeholder_la_entitlement_duration"
}
```

### Current Values (Placeholders)

**Note:** Current priors are placeholders based on LA market assumptions. Will be replaced with data-driven estimates in Week 9.

- **Hard cost:** $425 ± $50/sf (LA multifamily wood frame over podium)
- **Rent:** $3.75 ± $0.40/sf/month (LA rental market)
- **Entitlement:** 18-24-36 months (anecdotal LA experience)
- **Exit cap rate:** 4.5% ± 0.5% (LA multifamily market)

---

## Feature Flags

### `MONTE_CARLO_ENABLED` (default: `false`)

Controls whether actual MC simulation runs.

- **false:** Captures inputs, shows hash, displays "MC disabled" message
- **true:** Runs full simulation (Week 8-9 when engine is ready)

### `CAPTURE_SIM_INPUTS` (default: `true`)

Controls whether inputs are saved to database.

- **true:** Always saves snapshot for reproducibility
- **false:** Dry run (computes hash but doesn't save)

### Configuration

Set in `.env` file:
```bash
MONTE_CARLO_ENABLED=false
CAPTURE_SIM_INPUTS=true
MC_DEFAULT_RUNS=1000
MC_DEFAULT_SEED=42
MC_SIM_VERSION=1
```

---

## CLI Command

### Usage

```bash
python cli/simulate.py --apn <APN> --template <template>
```

### Example

```bash
python cli/simulate.py --apn 2249003017 --template multifamily
```

### Output

```
🎲 DealGenie Monte Carlo Simulator
============================================================

📊 Loading envelope data for APN: 2249003017
   ✅ Found parcel:
      Zone: C2-1VL
      Lot size: 22,628 sqft
      Existing FAR: 0.01
      Allowed FAR: 1.50

📈 Loading priors for template: multifamily
   ✅ Priors loaded:
      Cost params: 3
      Revenue params: 3
      Timeline params: 2
      Finance params: 3

🔒 Computing inputs hash...
   ✅ Hash: 0748dd88c1ca... (deterministic)

💾 Saving inputs snapshot...
   ✅ Snapshot saved to sim_inputs_snapshot
      Primary key: (apn=2249003017, template=multifamily, hash=0748dd88c1ca...)

⏸️  Monte Carlo simulation DISABLED
   Set MONTE_CARLO_ENABLED=true in .env to run simulations
   Inputs captured for reproducibility (hash: 0748dd88c1ca...)

📋 Configuration:
   Default runs: 1000
   Default seed: 42
   Sim version: 1
```

### Options

- `--apn TEXT`: Assessor Parcel Number (required)
- `--template TEXT`: Development template (required)
- `--runs INTEGER`: Number of MC scenarios (optional, default from settings)
- `--seed INTEGER`: Random seed (optional, default from settings)

---

## Risk Card Placeholder

Property reports now include a gray Monte Carlo risk card.

### With MC Data

When simulation inputs have been captured:

```
┌─────────────────────────────────────────┐
│ 🎲 Risk Analysis (Monte Carlo)          │
│                                         │
│ ⏸️ Monte Carlo simulation coming soon   │
│                                         │
│ ┌─────────────────────────────────────┐ │
│ │ Status: Inputs captured, pending    │ │
│ │ Simulation version: 1               │ │
│ │ Inputs hash: 0748dd88c1ca...        │ │
│ │ Captured: 2025-10-09 14:42          │ │
│ └─────────────────────────────────────┘ │
│                                         │
│ Coming soon: P5/P50/P95 IRR, VaR,      │
│ tornado diagrams                       │
└─────────────────────────────────────────┘
```

### Without MC Data

When no simulation inputs exist:

```
┌─────────────────────────────────────────┐
│ 🎲 Risk Analysis (Monte Carlo)          │
│                                         │
│ Risk analysis not yet captured         │
│                                         │
│ Run: python cli/simulate.py            │
│      --apn <APN> --template <TEMPLATE> │
└─────────────────────────────────────────┘
```

**Styling:** Gray background (#f5f5f5), gray border (#999) to indicate placeholder status.

---

## Test Coverage

**Total: 58/58 tests passing (100% coverage on hooks)**

### Test Breakdown

| Module | Tests | Coverage |
|--------|-------|----------|
| `test_sim_repository.py` | 12 | Hash determinism, snapshot storage, validation |
| `test_priors.py` | 21 | Distribution structure, validation, reasonable values |
| `test_risk_card.py` | 13 | Card rendering, metadata display, gray styling |
| Database migrations | ✓ | Tables created, indexes, foreign keys |
| CLI command | ✓ | Help text, required args, error handling |

### Key Tests

**Deterministic Hashing:**
```python
def test_inputs_hash_deterministic():
    # Same inputs → same hash
    hash1 = repo.compute_inputs_hash(envelope, priors, 'multifamily')
    hash2 = repo.compute_inputs_hash(envelope, priors, 'multifamily')
    assert hash1 == hash2  # ✓ Passes
```

**Key Order Independence:**
```python
def test_inputs_hash_ignores_key_order():
    envelope1 = {'far': 3.0, 'height': 75, 'units': 50}
    envelope2 = {'units': 50, 'height': 75, 'far': 3.0}  # Different order
    hash1 = repo.compute_inputs_hash(envelope1, priors, 'multifamily')
    hash2 = repo.compute_inputs_hash(envelope2, priors, 'multifamily')
    assert hash1 == hash2  # ✓ Canonical ordering works
```

**Duplicate Prevention:**
```python
def test_save_inputs_snapshot_duplicate():
    # Insert twice with same inputs
    hash1 = repo.save_inputs_snapshot('TEST', 'multifamily', env, priors)
    hash2 = repo.save_inputs_snapshot('TEST', 'multifamily', env, priors)
    # Verify only 1 row exists (ON CONFLICT DO NOTHING)
    count = query_db("SELECT COUNT(*) WHERE apn='TEST'")
    assert count == 1  # ✓ Duplicates prevented
```

---

## Installation

### Prerequisites

- Python 3.11+
- PostgreSQL 17.6 with PostGIS 3.6
- Database: `dealgenie_production` (~2.4M parcels)

### Dependencies

```bash
pip install scipy sqlalchemy geoalchemy2 pydantic-settings jinja2 click
```

Or use Poetry:
```bash
poetry install
```

### Database Setup

```bash
psql -U samanthagrant -d dealgenie_production -f db/migrations/010_add_mc_tables.sql
```

Verify tables:
```sql
\d+ sim_inputs_snapshot
\d+ sim_summary
```

### Environment Configuration

1. Copy `.env.example` to `.env`
2. Update `DATABASE_URL` with your credentials
3. Set feature flags as needed

---

## Usage Examples

### Capture Simulation Inputs

```bash
python cli/simulate.py --apn 2249003017 --template multifamily
```

### Verify Snapshot

```sql
SELECT apn, template, LEFT(inputs_hash, 12) as hash_preview, created_at
FROM sim_inputs_snapshot
WHERE apn = '2249003017'
ORDER BY created_at DESC
LIMIT 1;
```

### Generate Property Report

```python
from reporting.property_report import generate_property_report

html = generate_property_report('2249003017', 'multifamily', {})

with open('report.html', 'w') as f:
    f.write(html)
```

---

## File Structure

```
dealgenie/
├── db/
│   ├── migrations/
│   │   └── 010_add_mc_tables.sql       # Database schema
│   └── sim_repository.py               # Snapshot storage + hashing
├── scoring/
│   └── priors.py                       # Distribution parameters
├── config/
│   └── settings.py                     # Feature flags + config
├── cli/
│   └── simulate.py                     # CLI command
├── reporting/
│   ├── property_report.py              # Report generator
│   └── components/
│       └── risk_card_mc.html           # Risk card template
├── tests/
│   ├── test_sim_repository.py          # Repository tests (12)
│   ├── test_priors.py                  # Priors tests (21)
│   └── test_risk_card.py               # Risk card tests (13)
├── docs/
│   ├── mc_readiness_audit.md           # Pre-implementation audit
│   └── MC_INFRASTRUCTURE.md            # This file
├── .env.example                        # Environment template
└── CHANGELOG.md                        # Change log
```

---

## Next Steps (Week 8-9)

### Week 8: Monte Carlo Engine

1. **Cashflow Model**
   - 10-year DCF (acquisition → entitlement → construction → stabilization → sale)
   - Monthly cashflows with debt service
   - IRR and NPV calculations

2. **Distribution Sampling**
   - Sample from priors using scipy.stats
   - Implement correlation (Cholesky decomposition)
   - Run 1000+ scenarios in parallel

3. **Results Storage**
   - Compute percentiles (P5, P50, P95)
   - Calculate VaR and ES
   - Save to `sim_summary` table

### Week 9: Visualization + Grounding

4. **Risk Visualization**
   - Replace gray card with colorful dashboard
   - Fan charts (NPV distribution over time)
   - Tornado diagrams (sensitivity analysis)
   - Interactive charts (Chart.js or D3.js)

5. **Data-Driven Priors**
   - Replace placeholder values with real data
   - Ground in actual LA deals
   - Bayesian updating from comps

---

## Security & Performance

### Security

- ✅ SQL injection prevention: Parameterized queries in SimRepository
- ✅ Input validation: Pydantic models for settings
- ✅ No credentials in code: Environment variables only
- ✅ Hash-based caching: Prevents unauthorized result tampering

### Performance

- ✅ Indexes on foreign keys and lookup columns
- ✅ JSONB for flexible storage
- ✅ Deterministic hashing prevents redundant computation
- ✅ ON CONFLICT clauses for idempotent operations

**Query Performance (10 runs, median):**
- APN lookup: 0.14 ms
- Spatial query: 0.15 ms
- Hash computation: < 1 ms

---

## Contributing

### Before Submitting PR

1. Run all tests: `pytest tests/ -v`
2. Check code style: `black . && ruff check .`
3. Update CHANGELOG.md
4. Add test coverage for new features

### Code Review Focus Areas

1. Database schema design (indexes, constraints)
2. Deterministic hashing (reproducibility)
3. Priors API structure (distribution formats)
4. Feature flag architecture
5. Test coverage (edge cases, break-me tests)
6. Security (SQL injection, XSS)
7. Performance (query optimization)
8. Documentation (docstrings, assumptions)

---

## Assumptions & Limitations

### Assumptions

- Envelope data loaded from `underutilization_scores` table (existing_far, allowed_far, lot_size)
- AIN column used for parcel identification (not apn)
- Template parameter used for priors lookup ('multifamily', 'mixed_use', etc.)
- Simulation version starts at 1, increments when methodology changes
- Priors are placeholder values (not grounded in real data yet)

### Limitations

- **No simulation engine yet** - captures inputs only
- **Placeholder priors** - not validated against market data
- **Single template support** - 'multifamily' only tested extensively
- **No correlation modeling** - will be added in Week 8
- **No uncertainty quantification** - priors are point estimates with sigma

### Future Enhancements

- Multi-template support (mixed-use, office, retail)
- Bayesian updating of priors from comps
- Correlation matrices for related variables
- Parallel scenario execution (multiprocessing)
- Interactive risk dashboard
- Sensitivity analysis (tornado diagrams)
- Scenario comparison tool

---

## Support & Resources

- **Documentation:** `docs/mc_readiness_audit.md` - Pre-implementation audit
- **Tests:** `pytest tests/ -v` - Run all tests
- **Database Schema:** `\d+ sim_inputs_snapshot` - Inspect tables
- **CLI Help:** `python cli/simulate.py --help` - Command usage

---

**Version:** 1.0 (Week 7)
**Date:** 2025-10-09
**Status:** Infrastructure complete, simulation engine pending
**Test Coverage:** 58/58 tests passing (100%)
