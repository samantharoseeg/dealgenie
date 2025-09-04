-- DealGenie Week 2 Enhancement: Add Missing Governance Columns
-- Migration: 003_add_governance_columns.sql
-- Description: Adds critical API provenance and governance fields to Week 2 tables
-- Date: 2024-09-04

BEGIN TRANSACTION;

-- ==============================================================================
-- ADD GOVERNANCE COLUMNS TO RAW_PERMITS
-- ==============================================================================
ALTER TABLE raw_permits ADD COLUMN source_endpoint TEXT;
ALTER TABLE raw_permits ADD COLUMN query_params TEXT;  -- JSON string of API parameters
ALTER TABLE raw_permits ADD COLUMN as_of_date DATE;    -- For backtest snapshots
ALTER TABLE raw_permits ADD COLUMN ingest_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE raw_permits ADD COLUMN response_headers TEXT;  -- API response metadata
ALTER TABLE raw_permits ADD COLUMN api_version VARCHAR(20);
ALTER TABLE raw_permits ADD COLUMN rate_limit_remaining INTEGER;
ALTER TABLE raw_permits ADD COLUMN response_time_ms INTEGER;

-- ==============================================================================
-- ADD GOVERNANCE COLUMNS TO RAW_CRIME
-- ==============================================================================
ALTER TABLE raw_crime ADD COLUMN source_endpoint TEXT;
ALTER TABLE raw_crime ADD COLUMN query_params TEXT;
ALTER TABLE raw_crime ADD COLUMN as_of_date DATE;
ALTER TABLE raw_crime ADD COLUMN ingest_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE raw_crime ADD COLUMN response_headers TEXT;
ALTER TABLE raw_crime ADD COLUMN api_version VARCHAR(20);
ALTER TABLE raw_crime ADD COLUMN rate_limit_remaining INTEGER;
ALTER TABLE raw_crime ADD COLUMN response_time_ms INTEGER;

-- ==============================================================================
-- ADD GOVERNANCE COLUMNS TO CORE_ADDRESS
-- ==============================================================================
ALTER TABLE core_address ADD COLUMN geocoding_endpoint TEXT;
ALTER TABLE core_address ADD COLUMN geocoding_params TEXT;
ALTER TABLE core_address ADD COLUMN geocoded_at TIMESTAMP;
ALTER TABLE core_address ADD COLUMN geocoding_confidence REAL;
ALTER TABLE core_address ADD COLUMN geocoding_api_version VARCHAR(20);

-- ==============================================================================
-- ENHANCE ETL_AUDIT FOR API-SPECIFIC TRACKING
-- ==============================================================================
ALTER TABLE etl_audit ADD COLUMN api_endpoint TEXT;
ALTER TABLE etl_audit ADD COLUMN api_params TEXT;  -- JSON of all API parameters
ALTER TABLE etl_audit ADD COLUMN api_version VARCHAR(20);
ALTER TABLE etl_audit ADD COLUMN total_api_calls INTEGER DEFAULT 0;
ALTER TABLE etl_audit ADD COLUMN successful_api_calls INTEGER DEFAULT 0;
ALTER TABLE etl_audit ADD COLUMN failed_api_calls INTEGER DEFAULT 0;
ALTER TABLE etl_audit ADD COLUMN api_error_log TEXT;  -- JSON array of errors
ALTER TABLE etl_audit ADD COLUMN rate_limit_hits INTEGER DEFAULT 0;
ALTER TABLE etl_audit ADD COLUMN total_response_time_ms INTEGER;
ALTER TABLE etl_audit ADD COLUMN backtest_mode BOOLEAN DEFAULT FALSE;
ALTER TABLE etl_audit ADD COLUMN backtest_date DATE;

-- ==============================================================================
-- ADD GOVERNANCE TO FEATURE TABLES
-- ==============================================================================
-- Block Group Supply Features
ALTER TABLE feat_supply_bg ADD COLUMN calculation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE feat_supply_bg ADD COLUMN source_tables TEXT;  -- JSON array of source tables
ALTER TABLE feat_supply_bg ADD COLUMN feature_version VARCHAR(20);
ALTER TABLE feat_supply_bg ADD COLUMN validation_status VARCHAR(20);
ALTER TABLE feat_supply_bg ADD COLUMN validation_errors TEXT;

-- Parcel Supply Features  
ALTER TABLE feat_supply_parcel ADD COLUMN calculation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE feat_supply_parcel ADD COLUMN source_tables TEXT;
ALTER TABLE feat_supply_parcel ADD COLUMN feature_version VARCHAR(20);
ALTER TABLE feat_supply_parcel ADD COLUMN validation_status VARCHAR(20);
ALTER TABLE feat_supply_parcel ADD COLUMN validation_errors TEXT;

-- Block Group Crime Features
ALTER TABLE feat_crime_bg ADD COLUMN calculation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE feat_crime_bg ADD COLUMN source_tables TEXT;
ALTER TABLE feat_crime_bg ADD COLUMN feature_version VARCHAR(20);
ALTER TABLE feat_crime_bg ADD COLUMN validation_status VARCHAR(20);
ALTER TABLE feat_crime_bg ADD COLUMN validation_errors TEXT;

-- Parcel Crime Features
ALTER TABLE feat_crime_parcel ADD COLUMN calculation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE feat_crime_parcel ADD COLUMN source_tables TEXT;
ALTER TABLE feat_crime_parcel ADD COLUMN feature_version VARCHAR(20);
ALTER TABLE feat_crime_parcel ADD COLUMN validation_status VARCHAR(20);
ALTER TABLE feat_crime_parcel ADD COLUMN validation_errors TEXT;

-- ==============================================================================
-- CREATE API GOVERNANCE TRACKING TABLE
-- ==============================================================================
CREATE TABLE IF NOT EXISTS api_governance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- API Identification
    api_name VARCHAR(100) NOT NULL,           -- 'LADBS_Permits', 'LAPD_Crime', 'Census_ACS'
    endpoint_url TEXT NOT NULL,
    api_version VARCHAR(20),
    
    -- Rate Limiting
    rate_limit_max INTEGER,                   -- Max calls per period
    rate_limit_period_seconds INTEGER,        -- Period length in seconds
    current_usage INTEGER DEFAULT 0,
    usage_reset_at TIMESTAMP,
    
    -- Authentication
    auth_method VARCHAR(50),                  -- 'api_key', 'oauth', 'none'
    auth_valid_until TIMESTAMP,
    last_auth_refresh TIMESTAMP,
    
    -- Performance Tracking
    total_calls INTEGER DEFAULT 0,
    successful_calls INTEGER DEFAULT 0,
    failed_calls INTEGER DEFAULT 0,
    total_response_time_ms INTEGER DEFAULT 0,
    avg_response_time_ms REAL,
    
    -- Error Tracking
    last_error_at TIMESTAMP,
    last_error_message TEXT,
    consecutive_errors INTEGER DEFAULT 0,
    
    -- Data Quality
    last_successful_call TIMESTAMP,
    last_data_freshness_check TIMESTAMP,
    data_lag_hours REAL,                     -- How stale is the API data
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    notes TEXT
);

-- ==============================================================================
-- CREATE DATA LINEAGE TABLE
-- ==============================================================================
CREATE TABLE IF NOT EXISTS data_lineage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Source Information
    source_type VARCHAR(50) NOT NULL,        -- 'api', 'file', 'database', 'computed'
    source_name VARCHAR(100) NOT NULL,       -- API name or file path
    source_endpoint TEXT,
    source_query_params TEXT,
    source_timestamp TIMESTAMP,
    
    -- Target Information
    target_table VARCHAR(100) NOT NULL,
    target_record_id VARCHAR(100),           -- Primary key of target record
    target_column VARCHAR(100),              -- Specific column if applicable
    
    -- Transformation
    transformation_type VARCHAR(50),         -- 'direct', 'aggregation', 'calculation'
    transformation_function TEXT,            -- Function or query used
    transformation_version VARCHAR(20),
    
    -- Quality and Validation
    confidence_score REAL DEFAULT 1.0,
    validation_status VARCHAR(20),
    validation_message TEXT,
    
    -- Temporal Tracking
    as_of_date DATE,                        -- For backtesting
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_run_id VARCHAR(50),
    
    FOREIGN KEY (etl_run_id) REFERENCES etl_audit(run_id)
);

-- ==============================================================================
-- CREATE INDEXES FOR GOVERNANCE QUERIES
-- ==============================================================================
CREATE INDEX IF NOT EXISTS idx_raw_permits_as_of_date ON raw_permits(as_of_date);
CREATE INDEX IF NOT EXISTS idx_raw_permits_ingest_timestamp ON raw_permits(ingest_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_raw_permits_source_endpoint ON raw_permits(source_endpoint);

CREATE INDEX IF NOT EXISTS idx_raw_crime_as_of_date ON raw_crime(as_of_date);
CREATE INDEX IF NOT EXISTS idx_raw_crime_ingest_timestamp ON raw_crime(ingest_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_raw_crime_source_endpoint ON raw_crime(source_endpoint);

CREATE INDEX IF NOT EXISTS idx_api_governance_api_name ON api_governance(api_name);
CREATE INDEX IF NOT EXISTS idx_api_governance_last_successful ON api_governance(last_successful_call DESC);

CREATE INDEX IF NOT EXISTS idx_data_lineage_source ON data_lineage(source_name, source_type);
CREATE INDEX IF NOT EXISTS idx_data_lineage_target ON data_lineage(target_table, target_record_id);
CREATE INDEX IF NOT EXISTS idx_data_lineage_as_of ON data_lineage(as_of_date);
CREATE INDEX IF NOT EXISTS idx_data_lineage_etl ON data_lineage(etl_run_id);

-- ==============================================================================
-- CREATE GOVERNANCE VIEWS
-- ==============================================================================

-- View: API Health Dashboard
CREATE VIEW IF NOT EXISTS view_api_health AS
SELECT 
    api_name,
    endpoint_url,
    CASE 
        WHEN consecutive_errors >= 5 THEN '❌ DOWN'
        WHEN consecutive_errors > 0 THEN '⚠️ DEGRADED'
        WHEN last_successful_call > datetime('now', '-1 hour') THEN '✅ HEALTHY'
        ELSE '❓ UNKNOWN'
    END as status,
    total_calls,
    successful_calls,
    failed_calls,
    ROUND(100.0 * successful_calls / NULLIF(total_calls, 0), 2) as success_rate,
    avg_response_time_ms,
    last_successful_call,
    last_error_message,
    data_lag_hours
FROM api_governance
WHERE is_active = TRUE
ORDER BY api_name;

-- View: Recent Data Ingestion
CREATE VIEW IF NOT EXISTS view_recent_ingestion AS
SELECT 
    'permits' as data_type,
    COUNT(*) as records_ingested,
    MIN(ingest_timestamp) as earliest,
    MAX(ingest_timestamp) as latest,
    COUNT(DISTINCT source_endpoint) as unique_endpoints
FROM raw_permits
WHERE ingest_timestamp > datetime('now', '-24 hours')
UNION ALL
SELECT 
    'crime' as data_type,
    COUNT(*) as records_ingested,
    MIN(ingest_timestamp) as earliest,
    MAX(ingest_timestamp) as latest,
    COUNT(DISTINCT source_endpoint) as unique_endpoints
FROM raw_crime
WHERE ingest_timestamp > datetime('now', '-24 hours');

-- View: Backtest Data Availability
CREATE VIEW IF NOT EXISTS view_backtest_availability AS
SELECT 
    'permits' as data_type,
    as_of_date,
    COUNT(*) as record_count,
    COUNT(DISTINCT apn) as unique_parcels
FROM raw_permits
WHERE as_of_date IS NOT NULL
GROUP BY as_of_date
UNION ALL
SELECT 
    'crime' as data_type,
    as_of_date,
    COUNT(*) as record_count,
    COUNT(DISTINCT nearest_apn) as affected_parcels
FROM raw_crime
WHERE as_of_date IS NOT NULL
GROUP BY as_of_date
ORDER BY data_type, as_of_date DESC;

-- ==============================================================================
-- UPDATE SYSTEM CONFIGURATION
-- ==============================================================================
INSERT OR REPLACE INTO system_config (key, value, description) VALUES
('schema_version', '2.1', 'Database schema version with governance columns'),
('week2_governance_added', datetime('now'), 'Date when governance columns were added'),
('api_governance_enabled', '1', 'Enable API governance tracking'),
('data_lineage_enabled', '1', 'Enable data lineage tracking'),
('backtest_mode_available', '1', 'System supports backtesting with as_of_date');

COMMIT;

-- Success message
SELECT 'Governance columns migration completed successfully! Added:
- API provenance fields (source_endpoint, query_params, as_of_date, ingest_timestamp)
- Response tracking (headers, version, rate limits, response time)
- Feature validation fields (validation_status, validation_errors)
- New api_governance table for API health monitoring
- New data_lineage table for full data provenance
- 3 governance views for monitoring
Schema version updated to 2.1' as migration_status;