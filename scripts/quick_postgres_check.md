# Quick PostgreSQL Week 2 Migration Verification

## Your Commands (Copy-Paste Ready):

### 1. Basic Table Creation Check
```sql
psql $DATABASE_URL -c "
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'public' 
AND (table_name LIKE '%crime%' OR table_name LIKE '%permit%' OR table_name LIKE '%address%');"
```

### 2. Spatial Capabilities Check  
```sql
psql $DATABASE_URL -c "SELECT PostGIS_Version();"
```

### 3. ETL Audit Governance Columns
```sql
psql $DATABASE_URL -c "\d etl_audit"
```

## Additional Recommended Checks:

### 4. Verify All Week 2 Tables
```sql
psql $DATABASE_URL -c "
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'public' 
AND table_name IN (
    'etl_audit', 'core_address', 'link_address_parcel', 
    'raw_permits', 'feat_supply_bg', 'feat_supply_parcel',
    'raw_crime', 'feat_crime_bg', 'feat_crime_parcel'
);"
```

### 5. Check Spatial Indexes (PostGIS)
```sql
psql $DATABASE_URL -c "
SELECT indexname, tablename FROM pg_indexes 
WHERE indexname LIKE '%geometry%' OR indexname LIKE '%gist%';"
```

### 6. Verify JSONB Support
```sql
psql $DATABASE_URL -c "
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'etl_audit' AND data_type = 'jsonb';"
```

### 7. Test Functionality
```sql
psql $DATABASE_URL -c "
INSERT INTO etl_audit (process_name, run_id, status) 
VALUES ('test', 'test_001', 'completed') 
RETURNING id, process_name, started_at;"
```

### 8. Schema Version Check
```sql
psql $DATABASE_URL -c "
SELECT value FROM system_config WHERE key = 'schema_version';"
```

## Expected Results:

### ✅ Table Creation (Should see):
- `core_address`
- `etl_audit` 
- `feat_crime_bg`
- `feat_crime_parcel`
- `feat_supply_bg`
- `feat_supply_parcel`
- `link_address_parcel`
- `raw_crime`
- `raw_permits`

### ✅ PostGIS Version (Should show):
```
PostGIS 3.x.x USE_GEOS=1 USE_PROJ=1 USE_STATS=1
```

### ✅ ETL Audit Structure (Should include):
- `id` (SERIAL)
- `process_name` (VARCHAR)
- `run_id` (VARCHAR) 
- `started_at` (TIMESTAMPTZ)
- `process_config` (JSONB)
- `error_details` (JSONB)
- And many more governance columns...

### ❌ Common Issues:
- **PostGIS Error**: Extension not installed (`CREATE EXTENSION postgis;`)
- **Missing Tables**: Migration didn't run (`psql $DATABASE_URL -f db/migrations/002_week2_tables_postgres.sql`)
- **Permission Errors**: User lacks CREATE privileges
- **JSONB Errors**: PostgreSQL version < 9.4