-- DealGenie SQLite Database Schema
-- Week 1 Foundation: SQLite-compatible schema for real estate development scoring
-- Converted from PostGIS design to work without PostgreSQL dependencies

-- Drop existing tables if they exist (for clean setup)
DROP TABLE IF EXISTS parcel_scores;
DROP TABLE IF EXISTS feature_cache;
DROP TABLE IF EXISTS parcels;
DROP TABLE IF EXISTS zoning_codes;

-- ==============================================================================
-- ZONING CODES REFERENCE TABLE
-- ==============================================================================
-- Master reference for LA County zoning classifications
CREATE TABLE zoning_codes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code VARCHAR(20) UNIQUE NOT NULL,
    description TEXT,
    category VARCHAR(50), -- residential, commercial, industrial, mixed, etc.
    density_type VARCHAR(30), -- single-family, multi-family, high-density, etc.
    development_potential REAL CHECK (development_potential >= 0 AND development_potential <= 10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for fast zoning lookups
CREATE INDEX idx_zoning_codes_code ON zoning_codes(code);
CREATE INDEX idx_zoning_codes_category ON zoning_codes(category);

-- ==============================================================================
-- MAIN PARCELS TABLE
-- ==============================================================================
-- Core parcel data with basic geometry support (no PostGIS required)
CREATE TABLE parcels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    apn VARCHAR(20) UNIQUE NOT NULL,
    
    -- Basic spatial data (text-based for compatibility)
    geometry_wkt TEXT,    -- WKT (Well-Known Text) format geometry
    centroid_lat REAL,    -- Latitude of centroid
    centroid_lon REAL,    -- Longitude of centroid
    
    -- Basic property info
    address TEXT,
    city VARCHAR(100),
    zip_code VARCHAR(10),
    
    -- Zoning and land use
    zoning VARCHAR(20),
    land_use_code VARCHAR(10),
    land_use_description TEXT,
    
    -- Physical characteristics
    lot_size_sqft REAL,
    lot_size_acres REAL,
    frontage_feet REAL,
    
    -- Assessed values (from county assessor)
    assessed_value REAL,
    land_value REAL,
    improvement_value REAL,
    tax_year INTEGER,
    
    -- Development constraints
    slope_percentage REAL,
    flood_zone VARCHAR(10),
    earthquake_zone VARCHAR(10),
    fire_hazard_zone VARCHAR(10),
    
    -- Infrastructure access
    sewer_available INTEGER DEFAULT 0, -- SQLite uses INTEGER for boolean
    water_available INTEGER DEFAULT 0,
    gas_available INTEGER DEFAULT 0,
    electricity_available INTEGER DEFAULT 0,
    
    -- Data provenance
    data_source VARCHAR(50) DEFAULT 'LA_County_ZIMAS',
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key to zoning reference (SQLite syntax)
    FOREIGN KEY (zoning) REFERENCES zoning_codes(code)
);

-- Indexes for efficient queries (no GIST spatial indexes in SQLite)
CREATE INDEX idx_parcels_apn ON parcels(apn);
CREATE INDEX idx_parcels_zoning ON parcels(zoning);
CREATE INDEX idx_parcels_city ON parcels(city);
CREATE INDEX idx_parcels_zip_code ON parcels(zip_code);
CREATE INDEX idx_parcels_lot_size ON parcels(lot_size_sqft);
CREATE INDEX idx_parcels_assessed_value ON parcels(assessed_value);
CREATE INDEX idx_parcels_centroid_lat ON parcels(centroid_lat);
CREATE INDEX idx_parcels_centroid_lon ON parcels(centroid_lon);

-- ==============================================================================
-- FEATURE CACHE TABLE
-- ==============================================================================
-- Cached computed features for performance optimization
CREATE TABLE feature_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    apn VARCHAR(20) NOT NULL,
    template VARCHAR(50) NOT NULL, -- multifamily, residential, commercial, etc.
    
    -- Core scoring features (matching feature_matrix.py structure)
    location_score REAL,
    infrastructure_score REAL,
    zoning_score REAL,
    market_score REAL,
    development_score REAL,
    financial_score REAL,
    
    -- Demographic features (from Census API)
    population_density REAL,
    median_income REAL,
    age_distribution TEXT, -- JSON as text in SQLite
    education_levels TEXT, -- JSON as text
    employment_stats TEXT, -- JSON as text
    
    -- Market analysis features
    comparable_sales TEXT, -- JSON as text
    market_trends TEXT,    -- JSON as text
    
    -- Transportation access
    transit_score REAL,
    walkability_score REAL,
    highway_access_score REAL,
    
    -- Environmental factors
    air_quality_index REAL,
    noise_level REAL,
    green_space_proximity REAL, -- meters to nearest park
    
    -- Development potential indicators
    upzoning_probability REAL,
    development_pipeline TEXT, -- JSON as text
    permit_history TEXT,       -- JSON as text
    
    -- Raw feature vector (for ML model input)
    feature_vector TEXT, -- JSON as text
    
    -- Cache metadata
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP,
    data_version VARCHAR(20),
    
    -- Unique constraint to prevent duplicate cache entries
    UNIQUE(apn, template)
);

-- Indexes for feature cache
CREATE INDEX idx_feature_cache_apn ON feature_cache(apn);
CREATE INDEX idx_feature_cache_template ON feature_cache(template);
CREATE INDEX idx_feature_cache_computed_at ON feature_cache(computed_at);
CREATE INDEX idx_feature_cache_expires_at ON feature_cache(expires_at);

-- ==============================================================================
-- PARCEL SCORES TABLE
-- ==============================================================================
-- Historical scoring results for tracking and analysis
CREATE TABLE parcel_scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    apn VARCHAR(20) NOT NULL,
    template VARCHAR(50) NOT NULL,
    
    -- Final scores
    overall_score REAL CHECK (overall_score >= 0 AND overall_score <= 10),
    grade CHAR(1) CHECK (grade IN ('A', 'B', 'C', 'D', 'F')),
    
    -- Component scores (detailed breakdown)
    location_score REAL,
    infrastructure_score REAL,
    zoning_score REAL,
    market_score REAL,
    development_score REAL,
    financial_score REAL,
    
    -- Scoring metadata
    scoring_algorithm VARCHAR(50),
    algorithm_version VARCHAR(20),
    feature_data_version VARCHAR(20),
    
    -- Analysis results
    explanation TEXT,
    recommendations TEXT, -- JSON as text
    risk_factors TEXT,    -- JSON as text
    opportunities TEXT,   -- JSON as text
    
    -- Performance tracking
    computation_time_ms INTEGER,
    feature_cache_hit INTEGER DEFAULT 0, -- SQLite boolean as integer
    
    -- Temporal data
    scored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_until TIMESTAMP, -- When this score expires
    
    -- Foreign key relationship
    FOREIGN KEY (apn) REFERENCES parcels(apn)
);

-- Indexes for scoring queries
CREATE INDEX idx_parcel_scores_apn ON parcel_scores(apn);
CREATE INDEX idx_parcel_scores_template ON parcel_scores(template);
CREATE INDEX idx_parcel_scores_overall_score ON parcel_scores(overall_score DESC);
CREATE INDEX idx_parcel_scores_grade ON parcel_scores(grade);
CREATE INDEX idx_parcel_scores_scored_at ON parcel_scores(scored_at DESC);

-- Composite indexes for common query patterns
CREATE INDEX idx_parcel_scores_apn_template ON parcel_scores(apn, template);
CREATE INDEX idx_parcel_scores_template_score ON parcel_scores(template, overall_score DESC);

-- ==============================================================================
-- DATA VIEWS FOR COMMON QUERIES
-- ==============================================================================

-- View combining parcel data with latest scores
CREATE VIEW parcel_scoring_summary AS
SELECT 
    p.apn,
    p.address,
    p.city,
    p.zip_code,
    p.zoning,
    p.lot_size_sqft,
    p.assessed_value,
    ps.template,
    ps.overall_score,
    ps.grade,
    ps.scored_at,
    p.centroid_lon AS longitude,
    p.centroid_lat AS latitude
FROM parcels p
LEFT JOIN parcel_scores ps ON p.apn = ps.apn
WHERE ps.scored_at = (
    SELECT MAX(scored_at) 
    FROM parcel_scores ps2 
    WHERE ps2.apn = p.apn 
    AND ps2.template = ps.template
)
OR ps.scored_at IS NULL;

-- View for high-scoring development opportunities
CREATE VIEW high_value_opportunities AS
SELECT 
    p.apn,
    p.address,
    p.city,
    p.zoning,
    p.lot_size_sqft,
    p.assessed_value,
    ps.overall_score,
    ps.template,
    ps.explanation,
    ps.scored_at,
    p.centroid_lat,
    p.centroid_lon
FROM parcels p
INNER JOIN parcel_scores ps ON p.apn = ps.apn
WHERE ps.overall_score >= 7.0
    AND ps.scored_at = (
        SELECT MAX(scored_at) 
        FROM parcel_scores ps2 
        WHERE ps2.apn = ps.apn 
        AND ps2.template = ps.template
    );

-- ==============================================================================
-- TRIGGERS FOR AUTOMATIC UPDATES
-- ==============================================================================

-- Trigger to update updated_at timestamp for zoning_codes
CREATE TRIGGER update_zoning_codes_updated_at 
    BEFORE UPDATE ON zoning_codes 
    FOR EACH ROW 
BEGIN
    UPDATE zoning_codes SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Trigger to compute lot size in acres when sqft is updated
CREATE TRIGGER compute_lot_size_acres
    BEFORE INSERT ON parcels
    FOR EACH ROW
BEGIN
    UPDATE parcels SET lot_size_acres = NEW.lot_size_sqft / 43560.0 
    WHERE id = NEW.id AND NEW.lot_size_sqft IS NOT NULL;
END;

-- ==============================================================================
-- UTILITY FUNCTIONS (Stored as helper procedures)
-- ==============================================================================

-- SQLite doesn't have stored procedures, but we can create helper tables
-- for functions we might need

-- Cache cleanup helper - create a scheduled task table
CREATE TABLE scheduled_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_name VARCHAR(50) UNIQUE,
    last_run TIMESTAMP,
    frequency_minutes INTEGER
);

INSERT INTO scheduled_tasks (task_name, frequency_minutes) 
VALUES ('clean_expired_cache', 60); -- Run every hour

-- ==============================================================================
-- SAMPLE DATA AND INITIAL SETUP
-- ==============================================================================

-- Insert common LA County zoning codes
INSERT OR IGNORE INTO zoning_codes (code, description, category, density_type, development_potential) VALUES
-- Residential zones
('R1', 'Single Family Residential', 'residential', 'single-family', 4.0),
('R2', 'Two Family Residential', 'residential', 'two-family', 5.5),
('R3', 'Multiple Residential', 'residential', 'multi-family', 7.0),
('R4', 'Multiple Residential', 'residential', 'high-density', 8.0),
('R5', 'Multiple Residential', 'residential', 'very-high-density', 8.5),
('R1V2', 'Single Family Variable', 'residential', 'single-family', 4.5),
('R1-1', 'Single Family One Family', 'residential', 'single-family', 4.0),
('R1-1-O', 'Single Family One Family with Overlay', 'residential', 'single-family', 4.2),
('R1-1-HCR', 'Single Family Hillside', 'residential', 'single-family', 3.5),
('R2-1', 'Two Family Low Density', 'residential', 'two-family', 5.5),
('R2-1-O', 'Two Family with Overlay', 'residential', 'two-family', 5.7),
('R3-1', 'Multiple Residential Medium Density', 'residential', 'multi-family', 7.0),
('R4-1VL', 'Multiple Residential Very Low Density', 'residential', 'high-density', 7.8),
('RE11-1', 'Residential Estate 11,000 sqft lots', 'residential', 'estate', 3.0),
('RE15-1-HCR', 'Residential Estate 15,000 sqft Hillside', 'residential', 'estate', 2.5),
('RA-1', 'Residential Agriculture', 'residential', 'agricultural-residential', 3.2),
('RD2-1-HPOZ', 'Duplex Historical Overlay', 'residential', 'duplex', 5.8),
('RS-1', 'Suburban Residential', 'residential', 'suburban', 4.5),

-- Commercial zones  
('C1', 'Limited Commercial', 'commercial', 'neighborhood', 6.0),
('C2', 'Commercial', 'commercial', 'community', 7.5),
('C4', 'Commercial', 'commercial', 'regional', 8.5),
('C2-1VL', 'Commercial Very Limited', 'commercial', 'neighborhood', 6.2),
('C2-1VL-O-CPIO', 'Commercial with Community Plan Overlay', 'commercial', 'community', 7.2),

-- Mixed use zones
('MR1', 'Restricted Mixed Residential', 'mixed', 'low-density-mixed', 7.0),
('MR2', 'Mixed Residential', 'mixed', 'high-density-mixed', 8.0),

-- Industrial zones
('M1', 'Limited Industrial', 'industrial', 'light-industrial', 5.5),
('M2', 'Heavy Industrial', 'industrial', 'heavy-industrial', 4.5),
('M3', 'Heavy Industrial', 'industrial', 'very-heavy-industrial', 3.5),

-- Special zones
('PF', 'Public Facilities', 'public', 'institutional', 2.0),
('OS', 'Open Space', 'open-space', 'conservation', 1.0),
('A1', 'Heavy Agricultural', 'agricultural', 'farming', 2.5),
('A2', 'Light Agricultural', 'agricultural', 'light-farming', 3.0);

-- ==============================================================================
-- MAINTENANCE COMMANDS
-- ==============================================================================

-- Analyze tables for optimal query planning (SQLite equivalent)
ANALYZE;

-- Create indexes for performance optimization
-- (Already created above, but this is where additional indexes would go)

-- ==============================================================================
-- CSV IMPORT HELPER PROCEDURES
-- ==============================================================================

-- Since SQLite doesn't have stored procedures, we'll create helper tables
-- for tracking CSV imports

CREATE TABLE csv_import_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    import_type VARCHAR(50),    -- 'parcels', 'features', etc.
    file_path VARCHAR(255),
    records_imported INTEGER,
    import_started TIMESTAMP,
    import_completed TIMESTAMP,
    status VARCHAR(20),         -- 'in_progress', 'completed', 'failed'
    error_message TEXT
);

-- ==============================================================================
-- PERFORMANCE MONITORING
-- ==============================================================================

CREATE TABLE query_performance_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query_type VARCHAR(50),
    apn VARCHAR(20),
    execution_time_ms INTEGER,
    cache_hit INTEGER DEFAULT 0,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for performance monitoring
CREATE INDEX idx_query_performance_type ON query_performance_log(query_type);
CREATE INDEX idx_query_performance_time ON query_performance_log(execution_time_ms);

-- ==============================================================================
-- INITIAL CONFIGURATION
-- ==============================================================================

-- Create configuration table for system settings
CREATE TABLE system_config (
    key VARCHAR(50) PRIMARY KEY,
    value TEXT,
    description TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO system_config (key, value, description) VALUES
('schema_version', '1.0', 'Database schema version'),
('csv_data_source', 'scraper/la_parcels_complete_merged.csv', 'Primary CSV data file'),
('total_parcels', '369703', 'Total parcels in dataset'),
('last_full_import', NULL, 'Timestamp of last complete data import'),
('cache_ttl_hours', '24', 'Feature cache TTL in hours'),
('enable_performance_logging', '1', 'Enable query performance logging');

-- ==============================================================================
-- SUCCESS MESSAGE
-- ==============================================================================

-- SQLite pragma to verify setup
PRAGMA table_info(parcels);
PRAGMA table_info(parcel_scores);
PRAGMA table_info(feature_cache);
PRAGMA table_info(zoning_codes);

-- Show table count
SELECT COUNT(*) as table_count FROM sqlite_master WHERE type='table';

SELECT 'SQLite schema setup complete!' as status;