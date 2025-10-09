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
