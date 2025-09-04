-- DealGenie Week 2 Migration Verification - PostgreSQL
-- Run with: psql your_database -f scripts/verify_week2_postgres.sql

\echo '🔍 Verifying Week 2 Migration in PostgreSQL...'
\echo ''

-- Check if all new tables exist
\echo '📋 Checking Week 2 Tables:'
SELECT 
    CASE 
        WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'etl_audit') 
        THEN '✅ etl_audit' 
        ELSE '❌ etl_audit MISSING' 
    END as status
UNION ALL
SELECT 
    CASE 
        WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'core_address') 
        THEN '✅ core_address' 
        ELSE '❌ core_address MISSING' 
    END
UNION ALL
SELECT 
    CASE 
        WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'link_address_parcel') 
        THEN '✅ link_address_parcel' 
        ELSE '❌ link_address_parcel MISSING' 
    END
UNION ALL
SELECT 
    CASE 
        WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'raw_permits') 
        THEN '✅ raw_permits' 
        ELSE '❌ raw_permits MISSING' 
    END
UNION ALL
SELECT 
    CASE 
        WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'feat_supply_bg') 
        THEN '✅ feat_supply_bg' 
        ELSE '❌ feat_supply_bg MISSING' 
    END
UNION ALL
SELECT 
    CASE 
        WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'feat_supply_parcel') 
        THEN '✅ feat_supply_parcel' 
        ELSE '❌ feat_supply_parcel MISSING' 
    END
UNION ALL
SELECT 
    CASE 
        WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'raw_crime') 
        THEN '✅ raw_crime' 
        ELSE '❌ raw_crime MISSING' 
    END
UNION ALL
SELECT 
    CASE 
        WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'feat_crime_bg') 
        THEN '✅ feat_crime_bg' 
        ELSE '❌ feat_crime_bg MISSING' 
    END
UNION ALL
SELECT 
    CASE 
        WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'feat_crime_parcel') 
        THEN '✅ feat_crime_parcel' 
        ELSE '❌ feat_crime_parcel MISSING' 
    END;

\echo ''
\echo '📊 Table Record Counts:'
SELECT 
    schemaname,
    tablename,
    n_tup_ins as "Records"
FROM pg_stat_user_tables 
WHERE tablename IN (
    'etl_audit', 'core_address', 'link_address_parcel', 
    'raw_permits', 'feat_supply_bg', 'feat_supply_parcel',
    'raw_crime', 'feat_crime_bg', 'feat_crime_parcel'
)
ORDER BY tablename;

\echo ''
\echo '🔑 Checking Key Indexes:'
SELECT 
    indexname,
    tablename,
    CASE 
        WHEN indexname IS NOT NULL THEN '✅ ' || indexname
        ELSE '❌ Missing index'
    END as status
FROM pg_indexes 
WHERE tablename IN ('etl_audit', 'core_address', 'raw_permits', 'raw_crime', 'feat_supply_bg')
    AND indexname IN (
        'idx_etl_audit_process_name',
        'idx_core_address_standardized',
        'idx_raw_permits_apn', 
        'idx_raw_crime_date_occurred',
        'idx_feat_supply_bg_cbg'
    )
ORDER BY tablename, indexname;

\echo ''
\echo '🔗 Foreign Key Constraints:'
SELECT 
    conname as "Constraint Name",
    conrelid::regclass as "Table",
    confrelid::regclass as "References"
FROM pg_constraint 
WHERE contype = 'f' 
    AND conrelid::regclass::text IN (
        'link_address_parcel', 'raw_permits', 'feat_supply_parcel', 
        'feat_crime_parcel', 'raw_crime'
    )
ORDER BY conrelid::regclass;

\echo ''
\echo '🏗️ Table Structure Samples:'

\echo ''
\echo 'ETL Audit Table:'
\d etl_audit

\echo ''
\echo 'Core Address Table:'
\d core_address  

\echo ''
\echo 'Raw Permits Table:'
\d raw_permits

\echo ''
\echo 'Crime Features Block Group Table:'
\d feat_crime_bg

\echo ''
\echo '🧪 Functionality Test - ETL Audit Insert:'
INSERT INTO etl_audit (
    process_name, 
    run_id, 
    status, 
    records_processed,
    source_system
) VALUES (
    'migration_verification_test',
    'test_' || extract(epoch from now())::text,
    'completed',
    0,
    'verification_system'
) RETURNING id, process_name, run_id, started_at;

\echo ''
\echo '🧹 Cleanup Test Record:'
DELETE FROM etl_audit WHERE process_name = 'migration_verification_test';

\echo ''
\echo '✨ Migration Verification Complete!'
\echo 'If no errors appeared above, Week 2 migration was successful.'