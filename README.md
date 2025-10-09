# DealGenie

Developer-first deal discovery engine for LA County real estate development opportunities.

## Product

Site discovery + buildability assessment + entitlement risk analysis for real estate developers.

## Tech Stack

- **Language**: Python 3.11
- **Framework**: FastAPI
- **Database**: PostgreSQL + PostGIS
- **Cache**: Redis
- **Package Manager**: Poetry
- **Testing**: pytest

## Database

- **Name**: dealgenie_production
- **Host**: localhost
- **User**: samanthagrant
- **Total parcels**: ~2.4M
- **Tables**:
  - `parcels_complete` (main attributes)
  - `parcels_with_boundaries` (spatial data)

## Project Structure

```
dealgenie/
├── cli/              # Command-line interface
├── scoring/          # Scoring algorithms and logic
├── features/         # Feature engineering
├── ingest/           # Data ingestion pipelines
├── db/               # Database models and migrations
├── reporting/        # Report generation
├── config/           # Configuration management
├── tests/            # Test suite
├── scripts/          # Utility scripts
└── docs/             # Documentation
```

## Setup

```bash
# Install dependencies
poetry install

# Run CLI
poetry run dealgenie --help

# Run tests
poetry run pytest

# Run development server
poetry run uvicorn api.main:app --reload
```

## Development Principles

1. **Evidence over claims**: Show diffs, commands, logs, timings, test outputs
2. **Single-issue scope**: One task, minimal files, no scope creep
3. **Failure-first**: Test edge cases and breakage scenarios before success paths
4. **Reproducibility**: Deterministic outputs, seedable randomness, documented assumptions
5. **Performance gates**: Measure before/after with ≥10 samples

## Status

**Active Development** - Initial repository structure created 2025-10-09
