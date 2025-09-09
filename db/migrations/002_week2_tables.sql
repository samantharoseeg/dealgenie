-- DealGenie Week 2 Migration: Advanced Data Infrastructure
-- Migration: 002_week2_tables.sql
-- Description: Adds ETL audit, normalized addresses, permits, and crime data tables
-- Date: 2024-09-04

BEGIN TRANSACTION;

PRAGMA foreign_keys=ON;

-- ==============================================================================
-- 1. ETL AUDIT TABLE - Data Provenance Tracking
-- ==============================================================================
-- Tracks all data ingestion, transformation, and quality processes
CREATE TABLE IF NOT EXISTS etl_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Process identification
    process_name VARCHAR(100) NOT NULL,           -- 'census_acs_ingest', 'permit_scraper', etc.
    process_version VARCHAR(20),                  -- Version of the ETL process
    run_id VARCHAR(50) NOT NULL UNIQUE,           -- Unique identifier for this run
    
    -- Execution tracking
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    status VARCHAR(20) CHECK (status IN ('running', 'completed', 'failed', 'cancelled')),
    
    -- Data metrics
    records_processed INTEGER DEFAULT 0,
    records_success INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    records_skipped INTEGER DEFAULT 0,
    
    -- Source and target information
    source_system VARCHAR(100),                   -- 'LA_County_ZIMAS', 'Census_ACS_2022', etc.
    source_file VARCHAR(255),                     -- File path or API endpoint
    target_table VARCHAR(100),                    -- Destination table
    
    -- Quality and error tracking
    data_quality_score REAL,                     -- 0-100 quality score
    error_count INTEGER DEFAULT 0,
    error_details TEXT,                           -- JSON array of error messages
    
    -- Metadata and configuration
    process_config TEXT,                          -- JSON configuration used
    data_vintage VARCHAR(10),                     -- '2022', '2023Q1', etc.
    geographic_scope VARCHAR(100),                -- 'LA_County', 'City_of_LA', etc.
    
    -- Performance metrics
    execution_time_seconds REAL,
    memory_peak_mb REAL,
    
    -- Lineage and dependencies
    parent_run_id VARCHAR(50),                    -- For dependent processes
    downstream_processes TEXT,                    -- JSON array of triggered processes
    
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

-- ==============================================================================
-- 2. CORE ADDRESS TABLE - Normalized Address Management
-- ==============================================================================
-- Central repository for all normalized addresses across the system
CREATE TABLE IF NOT EXISTS core_address (
    address_id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Raw address components
    raw_address TEXT NOT NULL,                    -- Original address as received
    
    -- Standardized components
    street_number VARCHAR(20),                    -- 123, 123-125, 123A
    street_direction VARCHAR(5),                  -- N, S, E, W, NE, SW, etc.
    street_name VARCHAR(100),                     -- MAIN, BROADWAY, WILSHIRE
    street_type VARCHAR(20),                      -- ST, AVE, BLVD, RD, WAY
    street_suffix VARCHAR(5),                     -- Secondary direction
    
    unit_designator VARCHAR(10),                  -- APT, UNIT, STE, #
    unit_number VARCHAR(20),                      -- 101, A, 1-5
    
    city VARCHAR(100),
    state VARCHAR(2) DEFAULT 'CA',
    zip_code VARCHAR(10),
    zip_plus4 VARCHAR(4),
    
    -- Standardized full address
    standardized_address VARCHAR(255),            -- Complete standardized format
    
    -- Geographic coordinates
    latitude REAL,
    longitude REAL,
    coordinate_source VARCHAR(50),                -- 'google_geocoding', 'usps', 'manual'
    coordinate_accuracy VARCHAR(20),              -- 'rooftop', 'street', 'block', 'zip'
    
    -- Address validation and quality
    usps_validated BOOLEAN DEFAULT FALSE,
    validation_score REAL,                        -- 0-100 confidence score
    address_type VARCHAR(20),                     -- 'residential', 'commercial', 'mixed', 'vacant'
    
    -- Metadata
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_count INTEGER DEFAULT 1,              -- How many times we've seen this address
    
    -- Administrative boundaries
    census_tract VARCHAR(20),
    census_block_group VARCHAR(20),
    council_district VARCHAR(50),
    zip_code_tabulation_area VARCHAR(10),
    
    -- Quality flags
    is_active BOOLEAN DEFAULT TRUE,
    is_duplicate BOOLEAN DEFAULT FALSE,
    canonical_address_id INTEGER,                 -- Points to master record for duplicates
    
    -- JSON metadata for extensibility
    metadata TEXT,                                -- JSON for additional attributes
    
    FOREIGN KEY (canonical_address_id) REFERENCES core_address(address_id)
);

-- Indexes for core addresses
CREATE INDEX IF NOT EXISTS idx_core_address_raw ON core_address(raw_address);
CREATE INDEX IF NOT EXISTS idx_core_address_standardized ON core_address(standardized_address);
CREATE INDEX IF NOT EXISTS idx_core_address_street_name ON core_address(street_name);
CREATE INDEX IF NOT EXISTS idx_core_address_city ON core_address(city);
CREATE INDEX IF NOT EXISTS idx_core_address_zip_code ON core_address(zip_code);
CREATE INDEX IF NOT EXISTS idx_core_address_coordinates ON core_address(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_core_address_census_tract ON core_address(census_tract);
CREATE INDEX IF NOT EXISTS idx_core_address_is_active ON core_address(is_active);
CREATE INDEX IF NOT EXISTS idx_core_address_last_updated ON core_address(last_updated DESC);

-- ==============================================================================
-- 3. LINK ADDRESS PARCEL TABLE - Address-to-Parcel Linkage
-- ==============================================================================
-- Many-to-many relationship between addresses and parcels
CREATE TABLE IF NOT EXISTS link_address_parcel (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Core relationships
    address_id INTEGER NOT NULL,
    apn VARCHAR(20) NOT NULL,
    
    -- Relationship type and confidence
    relationship_type VARCHAR(30) NOT NULL,       -- 'primary', 'secondary', 'mailbox', 'entrance'
    confidence_score REAL DEFAULT 1.0,           -- 0-1 confidence in this linkage
    
    -- Source and validation
    link_source VARCHAR(50) NOT NULL,            -- 'assessor_data', 'geocoding', 'manual'
    verified BOOLEAN DEFAULT FALSE,
    verification_date TIMESTAMP,
    verified_by VARCHAR(100),
    
    -- Temporal tracking
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP,                      -- NULL = still active
    is_current BOOLEAN DEFAULT TRUE,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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

-- Unique constraint for current primary relationships
CREATE UNIQUE INDEX IF NOT EXISTS idx_link_address_parcel_primary 
ON link_address_parcel(apn, relationship_type) 
WHERE is_current = TRUE AND relationship_type = 'primary';

-- ==============================================================================
-- 4. RAW PERMITS TABLE - LA Building Permits
-- ==============================================================================
-- Raw building permit data from LA Department of Building and Safety
CREATE TABLE IF NOT EXISTS raw_permits (
    permit_id VARCHAR(50) PRIMARY KEY,           -- LA DBS permit number
    
    -- Application details
    application_date DATE,
    issue_date DATE,
    final_date DATE,
    status VARCHAR(50),                          -- 'Issued', 'Finaled', 'Cancelled', 'Expired'
    
    -- Permit classification
    permit_type VARCHAR(100),                    -- 'Addition', 'New Construction', 'Alteration'
    permit_subtype VARCHAR(100),                 -- More specific classification
    work_description TEXT,                       -- Full description of work
    permit_category VARCHAR(50),                 -- 'Residential', 'Commercial', 'Industrial'
    
    -- Location information
    address TEXT,
    apn VARCHAR(20),                            -- Assessor parcel number
    lot_area REAL,
    council_district INTEGER,
    
    -- Project details
    units_existing INTEGER DEFAULT 0,
    units_proposed INTEGER DEFAULT 0,
    units_net_change INTEGER,                   -- Calculated: proposed - existing
    
    stories_existing INTEGER,
    stories_proposed INTEGER,
    
    square_footage_existing REAL,
    square_footage_proposed REAL,
    square_footage_net_change REAL,
    
    -- Valuation
    estimated_cost REAL,
    permit_fees REAL,
    
    -- Construction details
    construction_type VARCHAR(50),              -- Type I, II, III, IV, V
    occupancy_classification VARCHAR(20),       -- R-1, R-2, B, M, etc.
    use_description TEXT,
    
    -- Geographic coordinates (if available)
    latitude REAL,
    longitude REAL,
    
    -- Processing metadata
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_source VARCHAR(100) DEFAULT 'LADBS_Permit_Portal',
    data_quality_flags TEXT,                    -- JSON array of quality issues
    
    -- Raw data preservation
    raw_record TEXT,                            -- Original JSON/XML from source
    
    -- ETL tracking
    etl_run_id VARCHAR(50),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
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

-- ==============================================================================
-- 5. SUPPLY FEATURES - Block Group Level
-- ==============================================================================
-- Housing and commercial supply metrics aggregated to block group level
CREATE TABLE IF NOT EXISTS feat_supply_bg (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Geographic identifier
    census_block_group VARCHAR(20) NOT NULL,     -- State(2) + County(3) + Tract(6) + BG(1)
    
    -- Time dimension
    data_vintage VARCHAR(10) NOT NULL,           -- '2022', '2023Q1', etc.
    measurement_date DATE,
    
    -- Residential supply metrics
    total_housing_units INTEGER,
    occupied_housing_units INTEGER,
    vacant_housing_units INTEGER,
    vacancy_rate REAL,                           -- Calculated: vacant / total
    
    single_family_units INTEGER,
    multi_family_units INTEGER,
    condo_units INTEGER,
    rental_units INTEGER,
    owner_occupied_units INTEGER,
    
    -- Residential construction activity (from permits)
    residential_permits_1yr INTEGER,            -- Past 12 months
    residential_permits_3yr INTEGER,            -- Past 36 months
    residential_units_permitted_1yr INTEGER,
    residential_units_permitted_3yr INTEGER,
    
    -- Commercial supply metrics
    total_commercial_sqft REAL,
    retail_sqft REAL,
    office_sqft REAL,
    industrial_sqft REAL,
    vacant_commercial_sqft REAL,
    commercial_vacancy_rate REAL,
    
    -- Commercial construction activity
    commercial_permits_1yr INTEGER,
    commercial_permits_3yr INTEGER,
    commercial_sqft_permitted_1yr REAL,
    commercial_sqft_permitted_3yr REAL,
    
    -- Land use composition
    residential_parcels INTEGER,
    commercial_parcels INTEGER,
    industrial_parcels INTEGER,
    mixed_use_parcels INTEGER,
    vacant_land_parcels INTEGER,
    
    -- Development potential indicators
    total_lot_area_sqft REAL,
    developed_lot_area_sqft REAL,
    undeveloped_lot_area_sqft REAL,
    average_lot_size_sqft REAL,
    
    -- Zoning analysis
    high_density_zoned_parcels INTEGER,         -- R4, R5, RAS, etc.
    medium_density_zoned_parcels INTEGER,       -- R3, RD zones
    low_density_zoned_parcels INTEGER,          -- R1, R2 zones
    commercial_zoned_parcels INTEGER,           -- C zones
    
    -- Market indicators
    median_assessed_value REAL,
    median_price_per_sqft REAL,
    assessment_growth_1yr REAL,                 -- % change in assessed values
    
    -- Data quality and metadata
    data_completeness_score REAL,              -- 0-100
    source_record_count INTEGER,               -- Number of records used
    calculation_methodology TEXT,              -- JSON describing calculation methods
    
    -- ETL tracking
    etl_run_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (etl_run_id) REFERENCES etl_audit(run_id)
);

-- Indexes for supply features - block group
CREATE INDEX IF NOT EXISTS idx_feat_supply_bg_cbg ON feat_supply_bg(census_block_group);
CREATE INDEX IF NOT EXISTS idx_feat_supply_bg_vintage ON feat_supply_bg(data_vintage);
CREATE INDEX IF NOT EXISTS idx_feat_supply_bg_date ON feat_supply_bg(measurement_date DESC);
CREATE INDEX IF NOT EXISTS idx_feat_supply_bg_etl ON feat_supply_bg(etl_run_id);
CREATE INDEX IF NOT EXISTS idx_feat_supply_bg_updated ON feat_supply_bg(updated_at DESC);

-- Unique constraint for block group + vintage
CREATE UNIQUE INDEX IF NOT EXISTS idx_feat_supply_bg_unique 
ON feat_supply_bg(census_block_group, data_vintage);

-- ==============================================================================
-- 6. SUPPLY FEATURES - Parcel Level
-- ==============================================================================
-- Supply-related features specific to individual parcels
CREATE TABLE IF NOT EXISTS feat_supply_parcel (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Parcel identifier
    apn VARCHAR(20) NOT NULL,
    
    -- Time dimension
    data_vintage VARCHAR(10) NOT NULL,
    measurement_date DATE,
    
    -- Parcel supply characteristics
    current_units INTEGER DEFAULT 0,
    max_allowable_units INTEGER,                -- Based on zoning
    development_capacity INTEGER,               -- max_allowable - current
    capacity_utilization_rate REAL,            -- current / max_allowable
    
    -- Building characteristics
    total_building_sqft REAL,
    residential_sqft REAL,
    commercial_sqft REAL,
    
    -- Development constraints and opportunities
    lot_coverage_ratio REAL,                   -- building footprint / lot size
    floor_area_ratio REAL,                     -- building sqft / lot sqft
    max_allowable_far REAL,                    -- From zoning code
    far_utilization_rate REAL,                 -- current FAR / max FAR
    
    -- Recent development activity
    permits_5yr_count INTEGER,                 -- Permits in last 5 years
    major_renovation_date DATE,                -- Most recent major permit
    last_permit_value REAL,                   -- Value of most recent permit
    
    -- Market positioning
    assessed_value_per_unit REAL,             -- For residential parcels
    assessed_value_per_sqft REAL,
    land_value_ratio REAL,                    -- land value / total assessed value
    
    -- Neighborhood supply context (from block group)
    neighborhood_vacancy_rate REAL,           -- From feat_supply_bg
    neighborhood_permits_1yr INTEGER,         -- From feat_supply_bg
    relative_density_index REAL,              -- Parcel density vs neighborhood avg
    
    -- Development feasibility indicators
    teardown_candidate BOOLEAN DEFAULT FALSE, -- High land ratio, old building
    expansion_candidate BOOLEAN DEFAULT FALSE,-- Low FAR utilization
    subdivision_candidate BOOLEAN DEFAULT FALSE,-- Large lot, appropriate zoning
    
    -- Special designations
    rent_stabilized BOOLEAN DEFAULT FALSE,
    historic_district BOOLEAN DEFAULT FALSE,
    inclusionary_housing_req BOOLEAN DEFAULT FALSE,
    
    -- Data quality and sources
    data_source_mask INTEGER,                 -- Bitmask of data sources used
    confidence_score REAL DEFAULT 1.0,       -- Overall confidence 0-1
    
    -- ETL tracking
    etl_run_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (apn) REFERENCES parcels(apn),
    FOREIGN KEY (etl_run_id) REFERENCES etl_audit(run_id)
);

-- Indexes for supply features - parcel
CREATE INDEX IF NOT EXISTS idx_feat_supply_parcel_apn ON feat_supply_parcel(apn);
CREATE INDEX IF NOT EXISTS idx_feat_supply_parcel_vintage ON feat_supply_parcel(data_vintage);
CREATE INDEX IF NOT EXISTS idx_feat_supply_parcel_date ON feat_supply_parcel(measurement_date DESC);
CREATE INDEX IF NOT EXISTS idx_feat_supply_parcel_capacity ON feat_supply_parcel(development_capacity DESC);
CREATE INDEX IF NOT EXISTS idx_feat_supply_parcel_far ON feat_supply_parcel(far_utilization_rate);
CREATE INDEX IF NOT EXISTS idx_feat_supply_parcel_teardown ON feat_supply_parcel(teardown_candidate);
CREATE INDEX IF NOT EXISTS idx_feat_supply_parcel_expansion ON feat_supply_parcel(expansion_candidate);
CREATE INDEX IF NOT EXISTS idx_feat_supply_parcel_etl ON feat_supply_parcel(etl_run_id);

-- Unique constraint for parcel + vintage
CREATE UNIQUE INDEX IF NOT EXISTS idx_feat_supply_parcel_unique 
ON feat_supply_parcel(apn, data_vintage);

-- ==============================================================================
-- 7. RAW CRIME TABLE - LAPD Crime Data
-- ==============================================================================
-- Raw crime incident data from LAPD
CREATE TABLE IF NOT EXISTS raw_crime (
    incident_id VARCHAR(20) PRIMARY KEY,        -- LAPD report number
    
    -- Temporal information
    date_reported DATE,
    date_occurred DATE,
    time_occurred TIME,
    
    -- Location information
    area_id INTEGER,                           -- LAPD area/division ID
    area_name VARCHAR(50),                     -- 'Hollywood', 'Downtown', etc.
    reporting_district VARCHAR(10),
    
    address VARCHAR(255),
    cross_street VARCHAR(255),
    
    latitude REAL,
    longitude REAL,
    location_description VARCHAR(100),         -- 'STREET', 'RESIDENCE', 'PARKING LOT'
    
    -- Crime classification
    crime_code VARCHAR(10),                    -- LAPD crime code
    crime_description VARCHAR(200),            -- Full description
    crime_category VARCHAR(50),                -- Grouped category
    
    -- UCR (Uniform Crime Reporting) classification
    part_1_2 INTEGER,                         -- 1 = Part I crime, 2 = Part II crime
    
    -- Case details
    modus_operandi TEXT,                      -- MO codes and descriptions
    victim_age INTEGER,
    victim_sex VARCHAR(1),                    -- M, F, X
    victim_descent VARCHAR(1),                -- Descent code
    
    weapon_used_code VARCHAR(10),
    weapon_description VARCHAR(100),
    
    -- Status
    status VARCHAR(20),                       -- 'IC' (Invest Cont), 'AA' (Adult Arrest), etc.
    status_description VARCHAR(100),
    
    -- Administrative
    premise_code VARCHAR(10),
    premise_description VARCHAR(100),
    
    -- Data quality and processing
    data_source VARCHAR(50) DEFAULT 'LAPD_OpenData',
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_quality_score REAL,
    
    -- Geographic processing
    census_tract VARCHAR(20),
    census_block_group VARCHAR(20),
    council_district INTEGER,
    
    -- Nearest parcel analysis (for real estate impact)
    nearest_apn VARCHAR(20),
    distance_to_nearest_parcel_feet REAL,
    
    -- ETL tracking
    etl_run_id VARCHAR(50),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (etl_run_id) REFERENCES etl_audit(run_id)
);

-- Indexes for raw crime data
CREATE INDEX IF NOT EXISTS idx_raw_crime_date_occurred ON raw_crime(date_occurred DESC);
CREATE INDEX IF NOT EXISTS idx_raw_crime_area ON raw_crime(area_id);
CREATE INDEX IF NOT EXISTS idx_raw_crime_coordinates ON raw_crime(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_raw_crime_category ON raw_crime(crime_category);
CREATE INDEX IF NOT EXISTS idx_raw_crime_part ON raw_crime(part_1_2);
CREATE INDEX IF NOT EXISTS idx_raw_crime_census_tract ON raw_crime(census_tract);
CREATE INDEX IF NOT EXISTS idx_raw_crime_census_bg ON raw_crime(census_block_group);
CREATE INDEX IF NOT EXISTS idx_raw_crime_nearest_apn ON raw_crime(nearest_apn);
CREATE INDEX IF NOT EXISTS idx_raw_crime_scraped_at ON raw_crime(scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_raw_crime_etl ON raw_crime(etl_run_id);

-- ==============================================================================
-- 8. CRIME FEATURES - Block Group Level
-- ==============================================================================
-- Crime statistics aggregated to census block group level
CREATE TABLE IF NOT EXISTS feat_crime_bg (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Geographic identifier
    census_block_group VARCHAR(20) NOT NULL,
    
    -- Time dimension
    data_vintage VARCHAR(10) NOT NULL,          -- '2022', '2023Q1', etc.
    analysis_start_date DATE,                   -- Start of analysis period
    analysis_end_date DATE,                     -- End of analysis period
    
    -- Overall crime metrics
    total_crimes INTEGER DEFAULT 0,
    crime_rate_per_1000_pop REAL,              -- Crimes per 1000 residents
    crime_density_per_sq_mile REAL,            -- Crimes per square mile
    
    -- Part I crimes (serious crimes)
    part1_crimes INTEGER DEFAULT 0,
    part1_rate_per_1000_pop REAL,
    
    -- Crime by category
    violent_crimes INTEGER DEFAULT 0,           -- Homicide, rape, robbery, assault
    property_crimes INTEGER DEFAULT 0,          -- Burglary, theft, auto theft, arson
    drug_crimes INTEGER DEFAULT 0,
    quality_of_life_crimes INTEGER DEFAULT 0,   -- Public intoxication, vandalism, etc.
    
    -- Specific high-impact crimes for real estate
    burglary_residential INTEGER DEFAULT 0,
    burglary_commercial INTEGER DEFAULT 0,
    vehicle_theft INTEGER DEFAULT 0,
    vandalism INTEGER DEFAULT 0,
    theft_from_vehicle INTEGER DEFAULT 0,
    
    -- Temporal patterns
    crimes_daytime INTEGER DEFAULT 0,           -- 6 AM - 6 PM
    crimes_nighttime INTEGER DEFAULT 0,         -- 6 PM - 6 AM
    crimes_weekend INTEGER DEFAULT 0,           -- Fri 6PM - Mon 6AM
    
    -- Location types
    crimes_street INTEGER DEFAULT 0,
    crimes_residence INTEGER DEFAULT 0,
    crimes_business INTEGER DEFAULT 0,
    crimes_parking INTEGER DEFAULT 0,
    
    -- Trend analysis
    crime_trend_6mo REAL,                      -- % change vs 6 months ago
    crime_trend_1yr REAL,                      -- % change vs 1 year ago
    seasonal_index REAL,                       -- Current vs historical seasonal average
    
    -- Relative safety metrics
    safety_index REAL,                         -- 0-100, higher = safer
    relative_safety_rank INTEGER,              -- Rank within city/county
    safety_percentile REAL,                    -- 0-100 percentile
    
    -- Hot spot analysis
    is_crime_hotspot BOOLEAN DEFAULT FALSE,
    hotspot_confidence REAL,                   -- Statistical significance
    hotspot_crime_types TEXT,                  -- JSON array of prevalent crimes
    
    -- Data quality
    data_completeness_score REAL,
    source_incident_count INTEGER,             -- Number of raw incidents used
    
    -- ETL tracking
    etl_run_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (etl_run_id) REFERENCES etl_audit(run_id)
);

-- Indexes for crime features - block group
CREATE INDEX IF NOT EXISTS idx_feat_crime_bg_cbg ON feat_crime_bg(census_block_group);
CREATE INDEX IF NOT EXISTS idx_feat_crime_bg_vintage ON feat_crime_bg(data_vintage);
CREATE INDEX IF NOT EXISTS idx_feat_crime_bg_analysis_date ON feat_crime_bg(analysis_end_date DESC);
CREATE INDEX IF NOT EXISTS idx_feat_crime_bg_safety_index ON feat_crime_bg(safety_index DESC);
CREATE INDEX IF NOT EXISTS idx_feat_crime_bg_hotspot ON feat_crime_bg(is_crime_hotspot);
CREATE INDEX IF NOT EXISTS idx_feat_crime_bg_etl ON feat_crime_bg(etl_run_id);

-- Unique constraint for block group + vintage
CREATE UNIQUE INDEX IF NOT EXISTS idx_feat_crime_bg_unique 
ON feat_crime_bg(census_block_group, data_vintage);

-- ==============================================================================
-- 9. CRIME FEATURES - Parcel Level
-- ==============================================================================
-- Crime impact features specific to individual parcels
CREATE TABLE IF NOT EXISTS feat_crime_parcel (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Parcel identifier
    apn VARCHAR(20) NOT NULL,
    
    -- Time dimension
    data_vintage VARCHAR(10) NOT NULL,
    analysis_start_date DATE,
    analysis_end_date DATE,
    
    -- Proximity-based crime metrics
    crimes_within_500ft INTEGER DEFAULT 0,     -- Crimes within 500 feet
    crimes_within_1000ft INTEGER DEFAULT 0,    -- Crimes within 1000 feet
    crimes_within_quarter_mile INTEGER DEFAULT 0,
    
    -- Distance to crime incidents
    distance_to_nearest_crime_ft REAL,
    distance_to_nearest_violent_crime_ft REAL,
    distance_to_nearest_property_crime_ft REAL,
    
    -- Property-relevant crime impacts
    residential_burglary_risk_score REAL,      -- 0-100 risk score
    vehicle_crime_risk_score REAL,             -- Auto theft, theft from vehicle
    vandalism_risk_score REAL,
    overall_property_crime_risk REAL,          -- Composite score
    
    -- Neighborhood crime context
    block_group_crime_rate REAL,               -- From feat_crime_bg
    relative_safety_vs_bg REAL,                -- Parcel safety vs block group
    relative_safety_vs_city REAL,              -- Parcel safety vs citywide
    
    -- Crime trend impacts
    local_crime_trend_6mo REAL,                -- Trend in immediate area
    improving_safety_indicator BOOLEAN DEFAULT FALSE,
    deteriorating_safety_indicator BOOLEAN DEFAULT FALSE,
    
    -- Investment risk factors
    high_crime_area BOOLEAN DEFAULT FALSE,     -- Above threshold for high crime
    crime_impact_on_value REAL,                -- Estimated % impact on property value
    insurance_risk_factor REAL,                -- 0-1 factor for insurance costs
    
    -- Specific incident impacts
    incidents_on_parcel INTEGER DEFAULT 0,     -- Crimes directly on this parcel
    incidents_on_block INTEGER DEFAULT 0,      -- Crimes on same block
    last_incident_on_parcel DATE,              -- Most recent incident on parcel
    
    -- Safety infrastructure
    street_lighting_score REAL,                -- 0-100 lighting adequacy
    visibility_score REAL,                     -- 0-100 visibility from street
    foot_traffic_score REAL,                   -- 0-100 pedestrian activity
    natural_surveillance_score REAL,           -- CPTED principles
    
    -- Data quality and confidence
    analysis_radius_feet REAL DEFAULT 1320,    -- Quarter mile
    incident_data_completeness REAL,           -- % of expected data available
    confidence_score REAL DEFAULT 1.0,
    
    -- ETL tracking
    etl_run_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (apn) REFERENCES parcels(apn),
    FOREIGN KEY (etl_run_id) REFERENCES etl_audit(run_id)
);

-- Indexes for crime features - parcel
CREATE INDEX IF NOT EXISTS idx_feat_crime_parcel_apn ON feat_crime_parcel(apn);
CREATE INDEX IF NOT EXISTS idx_feat_crime_parcel_vintage ON feat_crime_parcel(data_vintage);
CREATE INDEX IF NOT EXISTS idx_feat_crime_parcel_analysis_date ON feat_crime_parcel(analysis_end_date DESC);
CREATE INDEX IF NOT EXISTS idx_feat_crime_parcel_risk ON feat_crime_parcel(overall_property_crime_risk DESC);
CREATE INDEX IF NOT EXISTS idx_feat_crime_parcel_high_crime ON feat_crime_parcel(high_crime_area);
CREATE INDEX IF NOT EXISTS idx_feat_crime_parcel_incidents_on_parcel ON feat_crime_parcel(incidents_on_parcel);
CREATE INDEX IF NOT EXISTS idx_feat_crime_parcel_etl ON feat_crime_parcel(etl_run_id);

-- Unique constraint for parcel + vintage
CREATE UNIQUE INDEX IF NOT EXISTS idx_feat_crime_parcel_unique 
ON feat_crime_parcel(apn, data_vintage);

-- ==============================================================================
-- CREATE VIEWS FOR COMMON QUERIES
-- ==============================================================================

-- View: Recent ETL processes with status
CREATE VIEW IF NOT EXISTS view_etl_recent AS
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
WHERE started_at > datetime('now', '-7 days')
ORDER BY started_at DESC;

-- View: Active address-parcel relationships
CREATE VIEW IF NOT EXISTS view_address_parcel_current AS
SELECT 
    ca.address_id,
    ca.standardized_address,
    ca.city,
    ca.zip_code,
    lap.apn,
    lap.relationship_type,
    lap.confidence_score,
    ca.latitude,
    ca.longitude
FROM core_address ca
INNER JOIN link_address_parcel lap ON ca.address_id = lap.address_id
WHERE lap.is_current = TRUE
    AND ca.is_active = TRUE;

-- View: Recent permits with address linkage
CREATE VIEW IF NOT EXISTS view_permits_recent AS
SELECT 
    rp.permit_id,
    rp.application_date,
    rp.permit_type,
    rp.status,
    rp.units_net_change,
    rp.square_footage_net_change,
    rp.estimated_cost,
    ca.standardized_address,
    rp.apn
FROM raw_permits rp
LEFT JOIN core_address ca ON rp.normalized_address_id = ca.address_id
WHERE rp.application_date > date('now', '-2 years')
ORDER BY rp.application_date DESC;

-- ==============================================================================
-- MIGRATION COMPLETION
-- ==============================================================================

-- Update system configuration
INSERT OR REPLACE INTO system_config (key, value, description) VALUES
('schema_version', '2.0', 'Database schema version after Week 2 migration'),
('week2_migration_date', datetime('now'), 'Date when Week 2 tables were added'),
('etl_audit_enabled', '1', 'Enable ETL audit logging'),
('address_normalization_enabled', '1', 'Enable address normalization pipeline'),
('crime_analysis_enabled', '1', 'Enable crime data analysis features'),
('permit_tracking_enabled', '1', 'Enable building permit tracking');

COMMIT;

-- Success message
SELECT 'Week 2 migration completed successfully! Added:
- ETL audit infrastructure
- Normalized address management
- Building permits tracking  
- Supply analysis features (block group & parcel)
- Crime data analysis (block group & parcel)
- 9 new tables with proper indexes and constraints' as migration_status;