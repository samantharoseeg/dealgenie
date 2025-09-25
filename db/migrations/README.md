# DealGenie Database Migrations

This directory contains database migration files for the DealGenie system.

## Applied Migrations

### ✅ 001_initial_schema.sql
- **Applied**: Week 1 Foundation
- **Description**: Initial database schema from Week 1
- **Tables**: parcels, parcel_scores, feature_cache, zoning_codes, system_config
- **Status**: Applied during initial bootstrap

### ✅ 002_week2_tables.sql  
- **Applied**: 2025-09-04 19:40:55
- **Description**: Week 2 advanced data infrastructure
- **Tables**: 
  - `etl_audit` - Data provenance tracking
  - `core_address` - Normalized address management
  - `link_address_parcel` - Address-to-parcel linkage
  - `raw_permits` - LA building permits
  - `feat_supply_bg` - Supply features (block group level)
  - `feat_supply_parcel` - Supply features (parcel level) 
  - `raw_crime` - LAPD crime data
  - `feat_crime_bg` - Crime features (block group level)
  - `feat_crime_parcel` - Crime features (parcel level)
- **Status**: ✅ Successfully applied

## How to Apply Migrations

### Manual Application (Current Method)
```bash
# Apply migration directly to SQLite
sqlite3 data/dealgenie.db ".read db/migrations/002_week2_tables.sql"

# Verify migration
python3 scripts/verify_week2_migration.py
```

### Future: Migration Runner (In Development)
```bash
# Check migration status
python3 db/run_migration.py status

# Apply pending migrations  
python3 db/run_migration.py run
```

## Migration Best Practices

1. **Naming Convention**: Use `XXX_descriptive_name.sql` format
2. **Idempotent**: All migrations use `IF NOT EXISTS` clauses
3. **Transactional**: Wrapped in BEGIN/COMMIT blocks
4. **Indexed**: Include relevant indexes for performance
5. **Documented**: Include comments explaining purpose

## Database Schema Evolution

- **Version 1.0**: Initial Week 1 schema (parcels, scoring)
- **Version 2.0**: Week 2 expansion (ETL audit, addresses, permits, crime)
- **Version 3.0**: Planned Week 3 features (TBD)

## Verification Commands

```bash
# Check schema version
sqlite3 data/dealgenie.db "SELECT value FROM system_config WHERE key='schema_version';"

# List all tables
sqlite3 data/dealgenie.db ".tables"

# Verify specific Week 2 tables
sqlite3 data/dealgenie.db ".schema etl_audit"
sqlite3 data/dealgenie.db ".schema core_address"
```

## Rollback Strategy

For SQLite databases:
- **Primary**: Restore from backup before migration
- **Secondary**: Manual reversal (DROP tables, restore schema_version)
- **Planned**: Automated rollback scripts in future migration runner