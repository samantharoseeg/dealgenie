# Changelog

All notable changes, decisions, and assumptions for DealGenie will be documented in this file.

Format: `YYYY-MM-DD - [category] brief description`

Categories: `[init]` `[feature]` `[fix]` `[refactor]` `[perf]` `[test]` `[docs]` `[assumption]`

---

## 2025-10-09

- `[init]` Created monorepo structure at ~/workspace/dealgenie
- `[init]` Initialized git repository with main branch
- `[init]` Configured Poetry for Python 3.11 dependency management
- `[init]` Created directory structure: cli/, scoring/, features/, ingest/, db/, reporting/, config/, tests/, scripts/, docs/
- `[assumption]` Using PostgreSQL + PostGIS for spatial operations
- `[assumption]` Database dealgenie_production already exists with ~2.4M parcels
- `[assumption]` Python 3.11 minimum version requirement
- `[assumption]` FastAPI for API layer, Redis for caching layer

### Monte Carlo Add-On

- `[feature]` Added Monte Carlo database schema (sim_inputs_snapshot, sim_summary)
- `[feature]` Created SimRepository for MC input/output storage with deterministic hashing
- `[feature]` Created PriorsAPI for distribution parameter retrieval
- `[assumption]` MC priors use placeholder values (not grounded in real data yet)
- `[assumption]` Hard cost PSF: $425 ± $50/sf (lognormal) - based on typical LA multifamily 4-6 story wood frame over podium construction
- `[assumption]` Soft costs: 25% ± 5% of hard costs (beta) - typical range 20-30%
- `[assumption]` Contingency: 5% ± 2% of hard costs (beta)
- `[assumption]` Rent PSF: $3.75 ± $0.40/sf/month (lognormal) - based on LA rental market typical range $3.50-4.00
- `[assumption]` Vacancy rate: 5% ± 2% stabilized (beta)
- `[assumption]` Absorption: 6-12-24 months (PERT: min-mode-max) - lease-up timeline
- `[assumption]` Entitlement timeline: 18-24-36 months (PERT) - based on anecdotal LA entitlement experience
- `[assumption]` Construction timeline: 18-24-30 months (PERT)
- `[assumption]` Construction loan rate: 8% ± 1% (normal) - current market 2025
- `[assumption]` Exit cap rate: 4.5% ± 0.5% (normal) - LA multifamily market
- `[assumption]` Loan-to-cost: 65% ± 5% (beta) - typical construction financing
- `[test]` All MC priors validated: 21/21 tests passed
- `[docs]` Future: Replace placeholder priors with data-driven estimates (Week 9)
- `[feature]` Added Settings class with pydantic-settings for configuration management
- `[feature]` Created .env file for environment variables
- `[feature]` Added MONTE_CARLO_ENABLED feature flag (default: false)
- `[feature]` Added CAPTURE_SIM_INPUTS flag (default: true) - always captures inputs for reproducibility
- `[feature]` Created CLI command: python cli/simulate.py --apn X --template T
- `[assumption]` CLI uses click framework for argument parsing
- `[assumption]` Envelope loaded from underutilization_scores table using AIN column
- `[assumption]` Template parameter used for priors lookup (multifamily, mixed_use, etc.)
- `[assumption]` Default sim_version = 1 (will increment when methodology changes)
- `[test]` CLI tested with break-me scenarios: missing args, invalid APN, duplicate runs
- `[test]` Verified ON CONFLICT DO NOTHING prevents duplicate snapshots
- `[feature]` Added Monte Carlo risk card placeholder to property reports
- `[feature]` Created HTML template for risk card (reporting/components/risk_card_mc.html)
- `[feature]` Created property report generator with Jinja2 templating (reporting/property_report.py)
- `[assumption]` Risk card uses gray styling (#f5f5f5 background, #999 border) to indicate placeholder status
- `[assumption]` Shows first 12 characters of inputs_hash (full hash too long for display)
- `[assumption]` Timestamp formatted as YYYY-MM-DD HH:MM for readability
- `[assumption]` Card appears in main report flow (not hidden/collapsed)
- `[test]` Risk card tests: 13/13 passed - metadata display, placeholder, gray styling, hash shortening
- `[docs]` Card will be replaced with colorful risk visualization when MC engine complete (P5/P50/P95, VaR, tornado diagrams)
