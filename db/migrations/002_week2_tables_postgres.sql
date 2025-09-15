-- DealGenie Week 2 Migration: Advanced Data Infrastructure - PostgreSQL Version
-- Migration: 002_week2_tables_postgres.sql
-- Description: Adds ETL audit, normalized addresses, permits, and crime data tables
-- Date: 2024-09-04
-- Database: PostgreSQL with PostGIS

BEGIN;

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ==============================================================================
-- 1. ETL AUDIT TABLE - Data Provenance Tracking
-- ==============================================================================
CREATE TABLE IF NOT EXISTS etl_audit (
    id SERIAL PRIMARY KEY,
    
    -- Process identification
    process_name VARCHAR(100) NOT NULL,
    process_version VARCHAR(20),
    run_id VARCHAR(50) NOT NULL,
    
    -- Execution tracking
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    status VARCHAR(20) CHECK (status IN ('running', 'completed', 'failed', 'cancelled')),
    
    -- Data metrics
    records_processed INTEGER DEFAULT 0,
    records_success INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    records_skipped INTEGER DEFAULT 0,
    
    -- Source and target information
    source_system VARCHAR(100),
    source_file VARCHAR(255),
    target_table VARCHAR(100),
    
    -- Quality and error tracking
    data_quality_score NUMERIC(5,2),
    error_count INTEGER DEFAULT 0,
    error_details JSONB,
    
    -- Metadata and configuration
    process_config JSONB,
    data_vintage VARCHAR(10),
    geographic_scope VARCHAR(100),
    
    -- Performance metrics
    execution_time_seconds NUMERIC(10,3),
    memory_peak_mb NUMERIC(8,2),
    
    -- Lineage and dependencies
    parent_run_id VARCHAR(50),
    downstream_processes JSONB,
    
    created_by VARCHAR(100) DEFAULT 'dealgenie_system',
    notes TEXT
);

-- Indexes for ETL audit
CREATE INDEX IF NOT EXISTS idx_etl_audit_process_name ON etl_audit(process_name);
CREATE INDEX IF NOT EXISTS idx_etl_audit_run_id ON etl_audit(run_id);
CREATE INDEX IF NOT EXISTS idx_etl_audit_status ON etl_audit(status);
CREATE INDEX IF NOT EXISTS idx_etl_audit_started_at ON etl_audit(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_etl_audit_source_system ON etl_audit(source_system);
CREATE INDEX IF NOT EXISTS idx_etl_audit_target_table ON etl_audit(target_table);

-- GIN indexes for JSONB fields
CREATE INDEX IF NOT EXISTS idx_etl_audit_error_details ON etl_audit USING GIN(error_details);
CREATE INDEX IF NOT EXISTS idx_etl_audit_process_config ON etl_audit USING GIN(process_config);

-- ==============================================================================
-- 2. CORE ADDRESS TABLE - Normalized Address Management
-- ==============================================================================
CREATE TABLE IF NOT EXISTS core_address (
    address_id SERIAL PRIMARY KEY,
    
    -- Raw address components
    raw_address TEXT NOT NULL,
    
    -- Standardized components
    street_number VARCHAR(20),
    street_direction VARCHAR(5),
    street_name VARCHAR(100),
    street_type VARCHAR(20),
    street_suffix VARCHAR(5),
    
    unit_designator VARCHAR(10),
    unit_number VARCHAR(20),
    
    city VARCHAR(100),
    state VARCHAR(2) DEFAULT 'CA',
    zip_code VARCHAR(10),
    zip_plus4 VARCHAR(4),
    
    -- Standardized full address
    standardized_address VARCHAR(255),
    
    -- Geographic coordinates with PostGIS
    geometry GEOMETRY(POINT, 4326),  -- WGS84
    coordinate_source VARCHAR(50),
    coordinate_accuracy VARCHAR(20),
    
    -- Address validation and quality
    usps_validated BOOLEAN DEFAULT FALSE,
    validation_score NUMERIC(5,2),
    address_type VARCHAR(20),
    
    -- Metadata
    first_seen TIMESTAMPTZ DEFAULT NOW(),
    last_updated TIMESTAMPTZ DEFAULT NOW(),
    source_count INTEGER DEFAULT 1,
    
    -- Administrative boundaries
    census_tract VARCHAR(20),
    census_block_group VARCHAR(20),
    council_district VARCHAR(50),
    zip_code_tabulation_area VARCHAR(10),
    
    -- Quality flags
    is_active BOOLEAN DEFAULT TRUE,
    is_duplicate BOOLEAN DEFAULT FALSE,
    canonical_address_id INTEGER,
    
    -- JSON metadata for extensibility
    metadata JSONB,
    
    FOREIGN KEY (canonical_address_id) REFERENCES core_address(address_id)
);

-- Indexes for core addresses
CREATE INDEX IF NOT EXISTS idx_core_address_raw ON core_address(raw_address);
CREATE INDEX IF NOT EXISTS idx_core_address_standardized ON core_address(standardized_address);
CREATE INDEX IF NOT EXISTS idx_core_address_street_name ON core_address(street_name);
CREATE INDEX IF NOT EXISTS idx_core_address_city ON core_address(city);
CREATE INDEX IF NOT EXISTS idx_core_address_zip_code ON core_address(zip_code);
CREATE INDEX IF NOT EXISTS idx_core_address_geometry ON core_address USING GIST(geometry);
CREATE INDEX IF NOT EXISTS idx_core_address_census_tract ON core_address(census_tract);
CREATE INDEX IF NOT EXISTS idx_core_address_is_active ON core_address(is_active);
CREATE INDEX IF NOT EXISTS idx_core_address_last_updated ON core_address(last_updated DESC);

-- ==============================================================================
-- 3. LINK ADDRESS PARCEL TABLE - Address-to-Parcel Linkage
-- ==============================================================================
CREATE TABLE IF NOT EXISTS link_address_parcel (
    id SERIAL PRIMARY KEY,
    
    -- Core relationships
    address_id INTEGER NOT NULL,
    apn VARCHAR(20) NOT NULL,
    
    -- Relationship type and confidence
    relationship_type VARCHAR(30) NOT NULL,
    confidence_score NUMERIC(3,2) DEFAULT 1.0,
    
    -- Source and validation
    link_source VARCHAR(50) NOT NULL,
    verified BOOLEAN DEFAULT FALSE,
    verification_date TIMESTAMPTZ,
    verified_by VARCHAR(100),
    
    -- Temporal tracking
    effective_from TIMESTAMPTZ DEFAULT NOW(),
    effective_to TIMESTAMPTZ,
    is_current BOOLEAN DEFAULT TRUE,
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    created_by VARCHAR(100) DEFAULT 'system',
    notes TEXT,
    
    FOREIGN KEY (address_id) REFERENCES core_address(address_id),
    FOREIGN KEY (apn) REFERENCES parcels(apn)
);

-- Indexes for address-parcel links
CREATE INDEX IF NOT EXISTS idx_link_address_parcel_address ON link_address_parcel(address_id);
CREATE INDEX IF NOT EXISTS idx_link_address_parcel_apn ON link_address_parcel(apn);
CREATE INDEX IF NOT EXISTS idx_link_address_parcel_relationship ON link_address_parcel(relationship_type);
CREATE INDEX IF NOT EXISTS idx_link_address_parcel_current ON link_address_parcel(is_current, effective_from DESC);
CREATE INDEX IF NOT EXISTS idx_link_address_parcel_source ON link_address_parcel(link_source);

-- ==============================================================================
-- 4. RAW PERMITS TABLE - LA Building Permits
-- ==============================================================================
CREATE TABLE IF NOT EXISTS raw_permits (
    permit_id VARCHAR(50) PRIMARY KEY,
    
    -- Application details
    application_date DATE,
    issue_date DATE,
    final_date DATE,
    status VARCHAR(50),
    
    -- Permit classification
    permit_type VARCHAR(100),
    permit_subtype VARCHAR(100),
    work_description TEXT,
    permit_category VARCHAR(50),
    
    -- Location information
    address TEXT,
    apn VARCHAR(20),
    lot_area NUMERIC(12,2),
    council_district INTEGER,
    
    -- Project details
    units_existing INTEGER DEFAULT 0,
    units_proposed INTEGER DEFAULT 0,
    units_net_change INTEGER,
    
    stories_existing INTEGER,
    stories_proposed INTEGER,
    
    square_footage_existing NUMERIC(12,2),
    square_footage_proposed NUMERIC(12,2),
    square_footage_net_change NUMERIC(12,2),
    
    -- Valuation
    estimated_cost NUMERIC(12,2),
    permit_fees NUMERIC(10,2),
    
    -- Construction details
    construction_type VARCHAR(50),
    occupancy_classification VARCHAR(20),
    use_description TEXT,
    
    -- Geographic coordinates
    geometry GEOMETRY(POINT, 4326),
    
    -- Processing metadata
    scraped_at TIMESTAMPTZ DEFAULT NOW(),
    data_source VARCHAR(100) DEFAULT 'LADBS_Permit_Portal',
    data_quality_flags JSONB,
    
    -- Raw data preservation
    raw_record JSONB,
    
    -- ETL tracking
    etl_run_id VARCHAR(50),
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Link to address normalization
    normalized_address_id INTEGER,
    
    FOREIGN KEY (normalized_address_id) REFERENCES core_address(address_id),
    FOREIGN KEY (etl_run_id) REFERENCES etl_audit(run_id)
);

-- Indexes for raw permits
CREATE INDEX IF NOT EXISTS idx_raw_permits_apn ON raw_permits(apn);
CREATE INDEX IF NOT EXISTS idx_raw_permits_address ON raw_permits(address);
CREATE INDEX IF NOT EXISTS idx_raw_permits_status ON raw_permits(status);
CREATE INDEX IF NOT EXISTS idx_raw_permits_permit_type ON raw_permits(permit_type);
CREATE INDEX IF NOT EXISTS idx_raw_permits_application_date ON raw_permits(application_date DESC);
CREATE INDEX IF NOT EXISTS idx_raw_permits_issue_date ON raw_permits(issue_date DESC);
CREATE INDEX IF NOT EXISTS idx_raw_permits_council_district ON raw_permits(council_district);
CREATE INDEX IF NOT EXISTS idx_raw_permits_scraped_at ON raw_permits(scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_raw_permits_etl_run ON raw_permits(etl_run_id);
CREATE INDEX IF NOT EXISTS idx_raw_permits_geometry ON raw_permits USING GIST(geometry);

-- ==============================================================================
-- 5. SUPPLY FEATURES - Block Group Level
-- ==============================================================================
CREATE TABLE IF NOT EXISTS feat_supply_bg (
    id SERIAL PRIMARY KEY,
    
    -- Geographic identifier
    census_block_group VARCHAR(20) NOT NULL,
    
    -- Time dimension
    data_vintage VARCHAR(10) NOT NULL,
    measurement_date DATE,
    
    -- Residential supply metrics
    total_housing_units INTEGER,
    occupied_housing_units INTEGER,
    vacant_housing_units INTEGER,
    vacancy_rate NUMERIC(5,4),
    
    single_family_units INTEGER,
    multi_family_units INTEGER,
    condo_units INTEGER,
    rental_units INTEGER,
    owner_occupied_units INTEGER,
    
    -- Construction activity
    residential_permits_1yr INTEGER,
    residential_permits_3yr INTEGER,
    residential_units_permitted_1yr INTEGER,
    residential_units_permitted_3yr INTEGER,
    
    -- Commercial supply metrics
    total_commercial_sqft NUMERIC(15,2),
    retail_sqft NUMERIC(15,2),
    office_sqft NUMERIC(15,2),
    industrial_sqft NUMERIC(15,2),
    vacant_commercial_sqft NUMERIC(15,2),
    commercial_vacancy_rate NUMERIC(5,4),
    
    -- Land use composition
    residential_parcels INTEGER,
    commercial_parcels INTEGER,
    industrial_parcels INTEGER,
    mixed_use_parcels INTEGER,
    vacant_land_parcels INTEGER,
    
    -- Development potential indicators
    total_lot_area_sqft NUMERIC(15,2),
    developed_lot_area_sqft NUMERIC(15,2),
    undeveloped_lot_area_sqft NUMERIC(15,2),
    average_lot_size_sqft NUMERIC(12,2),
    
    -- Market indicators
    median_assessed_value NUMERIC(12,2),
    median_price_per_sqft NUMERIC(8,2),
    assessment_growth_1yr NUMERIC(6,4),
    
    -- Data quality and metadata
    data_completeness_score NUMERIC(5,2),
    source_record_count INTEGER,
    calculation_methodology JSONB,
    
    -- ETL tracking
    etl_run_id VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    FOREIGN KEY (etl_run_id) REFERENCES etl_audit(run_id),
    UNIQUE(census_block_group, data_vintage)
);

-- Indexes for supply features - block group
CREATE INDEX IF NOT EXISTS idx_feat_supply_bg_cbg ON feat_supply_bg(census_block_group);
CREATE INDEX IF NOT EXISTS idx_feat_supply_bg_vintage ON feat_supply_bg(data_vintage);
CREATE INDEX IF NOT EXISTS idx_feat_supply_bg_date ON feat_supply_bg(measurement_date DESC);
CREATE INDEX IF NOT EXISTS idx_feat_supply_bg_etl ON feat_supply_bg(etl_run_id);

-- ==============================================================================
-- 6. SUPPLY FEATURES - Parcel Level
-- ==============================================================================
CREATE TABLE IF NOT EXISTS feat_supply_parcel (
    id SERIAL PRIMARY KEY,
    
    -- Parcel identifier
    apn VARCHAR(20) NOT NULL,
    
    -- Time dimension
    data_vintage VARCHAR(10) NOT NULL,
    measurement_date DATE,
    
    -- Development capacity analysis
    current_units INTEGER DEFAULT 0,
    max_allowable_units INTEGER,
    development_capacity INTEGER,
    capacity_utilization_rate NUMERIC(5,4),
    
    -- Building characteristics
    total_building_sqft NUMERIC(12,2),
    residential_sqft NUMERIC(12,2),
    commercial_sqft NUMERIC(12,2),
    
    -- Development ratios
    lot_coverage_ratio NUMERIC(5,4),
    floor_area_ratio NUMERIC(6,4),
    max_allowable_far NUMERIC(6,4),
    far_utilization_rate NUMERIC(5,4),
    
    -- Recent activity
    permits_5yr_count INTEGER,
    major_renovation_date DATE,
    last_permit_value NUMERIC(12,2),
    
    -- Market positioning
    assessed_value_per_unit NUMERIC(12,2),
    assessed_value_per_sqft NUMERIC(8,2),
    land_value_ratio NUMERIC(5,4),
    
    -- Feasibility indicators
    teardown_candidate BOOLEAN DEFAULT FALSE,
    expansion_candidate BOOLEAN DEFAULT FALSE,
    subdivision_candidate BOOLEAN DEFAULT FALSE,
    
    -- Special designations
    rent_stabilized BOOLEAN DEFAULT FALSE,
    historic_district BOOLEAN DEFAULT FALSE,
    inclusionary_housing_req BOOLEAN DEFAULT FALSE,
    
    -- Data quality
    data_source_mask INTEGER,
    confidence_score NUMERIC(3,2) DEFAULT 1.0,
    
    -- ETL tracking
    etl_run_id VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    FOREIGN KEY (apn) REFERENCES parcels(apn),
    FOREIGN KEY (etl_run_id) REFERENCES etl_audit(run_id),
    UNIQUE(apn, data_vintage)
);

-- Indexes for supply features - parcel
CREATE INDEX IF NOT EXISTS idx_feat_supply_parcel_apn ON feat_supply_parcel(apn);
CREATE INDEX IF NOT EXISTS idx_feat_supply_parcel_vintage ON feat_supply_parcel(data_vintage);
CREATE INDEX IF NOT EXISTS idx_feat_supply_parcel_capacity ON feat_supply_parcel(development_capacity DESC);
CREATE INDEX IF NOT EXISTS idx_feat_supply_parcel_teardown ON feat_supply_parcel(teardown_candidate);

-- ==============================================================================
-- 7. RAW CRIME TABLE - LAPD Crime Data
-- ==============================================================================
CREATE TABLE IF NOT EXISTS raw_crime (
    incident_id VARCHAR(20) PRIMARY KEY,
    
    -- Temporal information
    date_reported DATE,
    date_occurred DATE,
    time_occurred TIME,
    
    -- Location information
    area_id INTEGER,
    area_name VARCHAR(50),
    reporting_district VARCHAR(10),
    
    address VARCHAR(255),
    cross_street VARCHAR(255),
    
    geometry GEOMETRY(POINT, 4326),
    location_description VARCHAR(100),
    
    -- Crime classification
    crime_code VARCHAR(10),
    crime_description VARCHAR(200),
    crime_category VARCHAR(50),
    
    -- UCR classification
    part_1_2 INTEGER,
    
    -- Case details
    modus_operandi TEXT,
    victim_age INTEGER,
    victim_sex VARCHAR(1),
    victim_descent VARCHAR(1),
    
    weapon_used_code VARCHAR(10),
    weapon_description VARCHAR(100),
    
    -- Status
    status VARCHAR(20),
    status_description VARCHAR(100),
    
    premise_code VARCHAR(10),
    premise_description VARCHAR(100),
    
    -- Geographic processing
    census_tract VARCHAR(20),
    census_block_group VARCHAR(20),
    council_district INTEGER,
    
    -- Nearest parcel analysis
    nearest_apn VARCHAR(20),
    distance_to_nearest_parcel_feet NUMERIC(8,2),
    
    -- Processing metadata
    data_source VARCHAR(50) DEFAULT 'LAPD_OpenData',
    scraped_at TIMESTAMPTZ DEFAULT NOW(),
    data_quality_score NUMERIC(5,2),
    
    -- ETL tracking
    etl_run_id VARCHAR(50),
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    
    FOREIGN KEY (etl_run_id) REFERENCES etl_audit(run_id)
);

-- Indexes for raw crime data
CREATE INDEX IF NOT EXISTS idx_raw_crime_date_occurred ON raw_crime(date_occurred DESC);
CREATE INDEX IF NOT EXISTS idx_raw_crime_area ON raw_crime(area_id);
CREATE INDEX IF NOT EXISTS idx_raw_crime_geometry ON raw_crime USING GIST(geometry);
CREATE INDEX IF NOT EXISTS idx_raw_crime_category ON raw_crime(crime_category);
CREATE INDEX IF NOT EXISTS idx_raw_crime_part ON raw_crime(part_1_2);
CREATE INDEX IF NOT EXISTS idx_raw_crime_census_tract ON raw_crime(census_tract);
CREATE INDEX IF NOT EXISTS idx_raw_crime_census_bg ON raw_crime(census_block_group);
CREATE INDEX IF NOT EXISTS idx_raw_crime_nearest_apn ON raw_crime(nearest_apn);

-- ==============================================================================
-- 8. CRIME FEATURES - Block Group Level
-- ==============================================================================
CREATE TABLE IF NOT EXISTS feat_crime_bg (
    id SERIAL PRIMARY KEY,
    
    -- Geographic identifier
    census_block_group VARCHAR(20) NOT NULL,
    
    -- Time dimension
    data_vintage VARCHAR(10) NOT NULL,
    analysis_start_date DATE,
    analysis_end_date DATE,
    
    -- Crime metrics
    total_crimes INTEGER DEFAULT 0,
    crime_rate_per_1000_pop NUMERIC(8,2),
    crime_density_per_sq_mile NUMERIC(8,2),
    
    part1_crimes INTEGER DEFAULT 0,
    part1_rate_per_1000_pop NUMERIC(8,2),
    
    -- Crime categories
    violent_crimes INTEGER DEFAULT 0,
    property_crimes INTEGER DEFAULT 0,
    drug_crimes INTEGER DEFAULT 0,
    quality_of_life_crimes INTEGER DEFAULT 0,
    
    -- Specific crimes
    burglary_residential INTEGER DEFAULT 0,
    burglary_commercial INTEGER DEFAULT 0,
    vehicle_theft INTEGER DEFAULT 0,
    vandalism INTEGER DEFAULT 0,
    theft_from_vehicle INTEGER DEFAULT 0,
    
    -- Safety metrics
    safety_index NUMERIC(5,2),
    relative_safety_rank INTEGER,
    safety_percentile NUMERIC(5,2),
    
    -- Hot spot analysis
    is_crime_hotspot BOOLEAN DEFAULT FALSE,
    hotspot_confidence NUMERIC(5,4),
    hotspot_crime_types JSONB,
    
    -- Data quality
    data_completeness_score NUMERIC(5,2),
    source_incident_count INTEGER,
    
    -- ETL tracking
    etl_run_id VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    FOREIGN KEY (etl_run_id) REFERENCES etl_audit(run_id),
    UNIQUE(census_block_group, data_vintage)
);

-- Indexes for crime features - block group
CREATE INDEX IF NOT EXISTS idx_feat_crime_bg_cbg ON feat_crime_bg(census_block_group);
CREATE INDEX IF NOT EXISTS idx_feat_crime_bg_vintage ON feat_crime_bg(data_vintage);
CREATE INDEX IF NOT EXISTS idx_feat_crime_bg_safety_index ON feat_crime_bg(safety_index DESC);
CREATE INDEX IF NOT EXISTS idx_feat_crime_bg_hotspot ON feat_crime_bg(is_crime_hotspot);

-- ==============================================================================
-- 9. CRIME FEATURES - Parcel Level
-- ==============================================================================
CREATE TABLE IF NOT EXISTS feat_crime_parcel (
    id SERIAL PRIMARY KEY,
    
    -- Parcel identifier
    apn VARCHAR(20) NOT NULL,
    
    -- Time dimension
    data_vintage VARCHAR(10) NOT NULL,
    analysis_start_date DATE,
    analysis_end_date DATE,
    
    -- Proximity-based metrics
    crimes_within_500ft INTEGER DEFAULT 0,
    crimes_within_1000ft INTEGER DEFAULT 0,
    crimes_within_quarter_mile INTEGER DEFAULT 0,
    
    -- Risk scores
    residential_burglary_risk_score NUMERIC(5,2),
    vehicle_crime_risk_score NUMERIC(5,2),
    vandalism_risk_score NUMERIC(5,2),
    overall_property_crime_risk NUMERIC(5,2),
    
    -- Investment factors
    high_crime_area BOOLEAN DEFAULT FALSE,
    crime_impact_on_value NUMERIC(6,4),
    insurance_risk_factor NUMERIC(4,3),
    
    -- Incidents
    incidents_on_parcel INTEGER DEFAULT 0,
    incidents_on_block INTEGER DEFAULT 0,
    last_incident_on_parcel DATE,
    
    -- Data quality
    analysis_radius_feet NUMERIC(6,1) DEFAULT 1320,
    incident_data_completeness NUMERIC(5,2),
    confidence_score NUMERIC(3,2) DEFAULT 1.0,
    
    -- ETL tracking
    etl_run_id VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    FOREIGN KEY (apn) REFERENCES parcels(apn),
    FOREIGN KEY (etl_run_id) REFERENCES etl_audit(run_id),
    UNIQUE(apn, data_vintage)
);

-- Indexes for crime features - parcel
CREATE INDEX IF NOT EXISTS idx_feat_crime_parcel_apn ON feat_crime_parcel(apn);
CREATE INDEX IF NOT EXISTS idx_feat_crime_parcel_vintage ON feat_crime_parcel(data_vintage);
CREATE INDEX IF NOT EXISTS idx_feat_crime_parcel_risk ON feat_crime_parcel(overall_property_crime_risk DESC);
CREATE INDEX IF NOT EXISTS idx_feat_crime_parcel_high_crime ON feat_crime_parcel(high_crime_area);

-- ==============================================================================
-- CREATE VIEWS FOR COMMON QUERIES
-- ==============================================================================

-- View: Recent ETL processes
CREATE OR REPLACE VIEW view_etl_recent AS
SELECT 
    process_name,
    run_id,
    status,
    started_at,
    completed_at,
    records_processed,
    records_success,
    execution_time_seconds,
    data_quality_score
FROM etl_audit
WHERE started_at > NOW() - INTERVAL '7 days'
ORDER BY started_at DESC;

-- View: Active address-parcel relationships
CREATE OR REPLACE VIEW view_address_parcel_current AS
SELECT 
    ca.address_id,
    ca.standardized_address,
    ca.city,
    ca.zip_code,
    lap.apn,
    lap.relationship_type,
    lap.confidence_score,
    ST_X(ca.geometry) as longitude,
    ST_Y(ca.geometry) as latitude
FROM core_address ca
INNER JOIN link_address_parcel lap ON ca.address_id = lap.address_id
WHERE lap.is_current = TRUE
    AND ca.is_active = TRUE;

-- ==============================================================================
-- UPDATE SYSTEM CONFIGURATION
-- ==============================================================================

-- Create system_config table if it doesn't exist (for new installations)
CREATE TABLE IF NOT EXISTS system_config (
    key VARCHAR(50) PRIMARY KEY,
    value TEXT,
    description TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Update system configuration
INSERT INTO system_config (key, value, description) VALUES
('schema_version', '2.0', 'Database schema version after Week 2 migration'),
('week2_migration_date', NOW()::text, 'Date when Week 2 tables were added'),
('etl_audit_enabled', '1', 'Enable ETL audit logging'),
('address_normalization_enabled', '1', 'Enable address normalization pipeline'),
('crime_analysis_enabled', '1', 'Enable crime data analysis features'),
('permit_tracking_enabled', '1', 'Enable building permit tracking')
ON CONFLICT (key) DO UPDATE SET 
    value = EXCLUDED.value,
    updated_at = NOW();

COMMIT;

-- Success message
SELECT 'Week 2 PostgreSQL migration completed successfully! Added:
- ETL audit infrastructure with JSONB support
- PostGIS-enabled address management
- Building permits tracking with spatial data
- Supply analysis features (block group & parcel)
- Crime data analysis with spatial indexing
- 9 new tables with proper indexes and constraints' as migration_status;