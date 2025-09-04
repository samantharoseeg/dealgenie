#!/bin/bash
# DealGenie Week 2 PostgreSQL Verification Commands
# Run these commands to verify your migration worked correctly

echo "🔍 DealGenie Week 2 PostgreSQL Migration Verification"
printf '=%.0s' {1..60}; echo

# Check if DATABASE_URL is set
if [ -z "$DATABASE_URL" ]; then
    echo "❌ DATABASE_URL not set. Please export it first:"
    echo "export DATABASE_URL='postgresql://user:pass@localhost/dealgenie'"
    exit 1
fi

echo "Database URL: $DATABASE_URL"
echo ""

# Helper variable for cleaner psql commands
PSQL="psql \"$DATABASE_URL\""

# 1. Basic table creation check
echo "📋 1. Checking Week 2 Table Creation:"
$PSQL -c "
SELECT table_name, 
       CASE 
           WHEN table_name LIKE '%crime%' THEN '🔴 Crime'
           WHEN table_name LIKE '%permit%' THEN '🏗️ Permit' 
           WHEN table_name LIKE '%address%' THEN '📍 Address'
           WHEN table_name LIKE '%supply%' THEN '📊 Supply'
           WHEN table_name LIKE '%etl%' THEN '⚙️ ETL'
           ELSE '📁 Other'
       END as category
FROM information_schema.tables 
WHERE table_schema = 'public' 
    AND (table_name LIKE '%crime%' 
         OR table_name LIKE '%permit%' 
         OR table_name LIKE '%address%' 
         OR table_name LIKE '%supply%'
         OR table_name LIKE '%etl%')
ORDER BY category, table_name;
"

echo ""
echo "📍 2. Verifying PostGIS Spatial Capabilities:"
$PSQL -c "SELECT PostGIS_Version();" 2>/dev/null || echo "⚠️  PostGIS not installed or enabled"

echo ""
echo "⚙️ 3. ETL Audit Table Structure (Governance Columns):"
$PSQL -c "\d etl_audit"

echo ""
echo "📍 4. Core Address Table Structure (Spatial Columns):"
$PSQL -c "\d core_address"

echo ""
echo "🏗️ 5. Raw Permits Table Structure:"
$PSQL -c "\d raw_permits"

echo ""
echo "🔴 6. Raw Crime Table Structure:"
$PSQL -c "\d raw_crime"

echo ""
echo "🔍 7. Checking Spatial Indexes:"
$PSQL -c "
SELECT schemaname, tablename, indexname, indexdef
FROM pg_indexes 
WHERE indexdef LIKE '%GIST%' 
    AND tablename IN ('core_address', 'raw_permits', 'raw_crime')
ORDER BY tablename, indexname;
"

echo ""
echo "📊 8. Checking JSONB Indexes:"
$PSQL -c "
SELECT schemaname, tablename, indexname, indexdef
FROM pg_indexes 
WHERE indexdef LIKE '%GIN%' 
    AND tablename IN ('etl_audit', 'raw_permits', 'raw_crime', 'feat_crime_bg')
ORDER BY tablename, indexname;
"

echo ""
echo "🔗 9. Foreign Key Constraints:"
$PSQL -c "
SELECT 
    conname as constraint_name,
    conrelid::regclass as table_name,
    confrelid::regclass as references_table,
    pg_get_constraintdef(oid) as definition
FROM pg_constraint 
WHERE contype = 'f' 
    AND conrelid IN (
        'public.link_address_parcel'::regclass,
        'public.raw_permits'::regclass,
        'public.feat_supply_parcel'::regclass,
        'public.feat_crime_parcel'::regclass,
        'public.raw_crime'::regclass,
        'public.feat_supply_bg'::regclass,
        'public.feat_crime_bg'::regclass
    )
ORDER BY conrelid::regclass;
"

echo ""
echo "📈 10. Schema Version and Configuration:"
$PSQL -c "
SELECT key, value, description, updated_at 
FROM system_config 
WHERE key LIKE '%schema%' OR key LIKE '%week2%' OR key LIKE '%migration%'
ORDER BY key;
"

echo ""
echo "🧪 11. Test Basic Functionality:"
$PSQL -c "
-- Test ETL audit insert
INSERT INTO etl_audit (
    process_name, 
    run_id, 
    status, 
    records_processed,
    source_system,
    process_config
) VALUES (
    'verification_test',
    'test_' || extract(epoch from now())::text,
    'completed',
    0,
    'verification_system',
    '{\"test\": true}'::jsonb
) RETURNING id, process_name, started_at;
"

echo ""
echo "🧹 12. Cleanup Test Data:"
$PSQL -c "DELETE FROM etl_audit WHERE process_name = 'verification_test';"

echo ""
echo "✅ Verification Complete!"
echo "If no errors appeared above, your Week 2 PostgreSQL migration was successful."