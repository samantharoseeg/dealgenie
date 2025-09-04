-- DealGenie PostGIS Database Schema
-- Week 1 Foundation: Spatial tables for real estate development scoring
-- PostGIS spatial database setup for LA County parcel analysis

-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- Drop existing tables if they exist (for clean setup)
DROP TABLE IF EXISTS parcel_scores CASCADE;
DROP TABLE IF EXISTS feature_cache CASCADE;
DROP TABLE IF EXISTS parcels CASCADE;
DROP TABLE IF EXISTS zoning_codes CASCADE;

-- ==============================================================================
-- ZONING CODES REFERENCE TABLE
-- ==============================================================================
-- Master reference for LA County zoning classifications
CREATE TABLE zoning_codes (
    id SERIAL PRIMARY KEY,
    code VARCHAR(20) UNIQUE NOT NULL,
    description TEXT,
    category VARCHAR(50), -- residential, commercial, industrial, mixed, etc.
    density_type VARCHAR(30), -- single-family, multi-family, high-density, etc.
    development_potential NUMERIC(3,1) CHECK (development_potential >= 0 AND development_potential <= 10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for fast zoning lookups
CREATE INDEX idx_zoning_codes_code ON zoning_codes(code);
CREATE INDEX idx_zoning_codes_category ON zoning_codes(category);

-- ==============================================================================
-- MAIN PARCELS TABLE
-- ==============================================================================
-- Core parcel data with spatial geometry from LA County
CREATE TABLE parcels (
    id SERIAL PRIMARY KEY,
    apn VARCHAR(20) UNIQUE NOT NULL,
    -- Spatial data
    geometry GEOMETRY(MULTIPOLYGON, 4326), -- WGS84 for compatibility
    centroid GEOMETRY(POINT, 4326),
    
    -- Basic property info
    address TEXT,
    city VARCHAR(100),
    zip_code VARCHAR(10),
    
    -- Zoning and land use
    zoning VARCHAR(20),
    land_use_code VARCHAR(10),
    land_use_description TEXT,
    
    -- Physical characteristics
    lot_size_sqft NUMERIC(12,2),
    lot_size_acres NUMERIC(10,4),
    frontage_feet NUMERIC(8,2),
    
    -- Assessed values (from county assessor)
    assessed_value NUMERIC(12,2),
    land_value NUMERIC(12,2),
    improvement_value NUMERIC(12,2),
    tax_year INTEGER,
    
    -- Development constraints
    slope_percentage NUMERIC(5,2),
    flood_zone VARCHAR(10),
    earthquake_zone VARCHAR(10),
    fire_hazard_zone VARCHAR(10),
    
    -- Infrastructure access
    sewer_available BOOLEAN DEFAULT FALSE,
    water_available BOOLEAN DEFAULT FALSE,
    gas_available BOOLEAN DEFAULT FALSE,
    electricity_available BOOLEAN DEFAULT FALSE,
    
    -- Data provenance
    data_source VARCHAR(50) DEFAULT 'LA_County_ZIMAS',
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key to zoning reference
    FOREIGN KEY (zoning) REFERENCES zoning_codes(code)
);

-- Spatial indexes for efficient geographic queries
CREATE INDEX idx_parcels_geometry ON parcels USING GIST(geometry);
CREATE INDEX idx_parcels_centroid ON parcels USING GIST(centroid);

-- Standard indexes for common queries
CREATE INDEX idx_parcels_apn ON parcels(apn);
CREATE INDEX idx_parcels_zoning ON parcels(zoning);
CREATE INDEX idx_parcels_city ON parcels(city);
CREATE INDEX idx_parcels_zip_code ON parcels(zip_code);
CREATE INDEX idx_parcels_lot_size ON parcels(lot_size_sqft);
CREATE INDEX idx_parcels_assessed_value ON parcels(assessed_value);

-- ==============================================================================
-- FEATURE CACHE TABLE
-- ==============================================================================
-- Cached computed features for performance optimization
CREATE TABLE feature_cache (
    id SERIAL PRIMARY KEY,
    apn VARCHAR(20) NOT NULL,
    template VARCHAR(50) NOT NULL, -- multifamily, residential, commercial, etc.
    
    -- Core scoring features (matching feature_matrix.py structure)
    location_score NUMERIC(4,2),
    infrastructure_score NUMERIC(4,2),
    zoning_score NUMERIC(4,2),
    market_score NUMERIC(4,2),
    development_score NUMERIC(4,2),
    financial_score NUMERIC(4,2),
    
    -- Demographic features (from Census API)
    population_density NUMERIC(8,2),
    median_income NUMERIC(10,2),
    age_distribution JSONB, -- Store as JSON for flexibility
    education_levels JSONB,
    employment_stats JSONB,
    
    -- Market analysis features
    comparable_sales JSONB, -- Recent comparable sales data
    market_trends JSONB,    -- Price trends, inventory levels
    
    -- Transportation access
    transit_score NUMERIC(4,2),
    walkability_score NUMERIC(4,2),
    highway_access_score NUMERIC(4,2),
    
    -- Environmental factors
    air_quality_index NUMERIC(6,2),
    noise_level NUMERIC(4,1),
    green_space_proximity NUMERIC(6,2), -- meters to nearest park
    
    -- Development potential indicators
    upzoning_probability NUMERIC(4,2),
    development_pipeline JSONB, -- Nearby planned developments
    permit_history JSONB,       -- Historical permit data
    
    -- Raw feature vector (for ML model input)
    feature_vector JSONB,
    
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

-- GIN index for JSON columns (for efficient JSON queries)
CREATE INDEX idx_feature_cache_age_distribution ON feature_cache USING GIN(age_distribution);
CREATE INDEX idx_feature_cache_comparable_sales ON feature_cache USING GIN(comparable_sales);
CREATE INDEX idx_feature_cache_feature_vector ON feature_cache USING GIN(feature_vector);

-- ==============================================================================
-- PARCEL SCORES TABLE
-- ==============================================================================
-- Historical scoring results for tracking and analysis
CREATE TABLE parcel_scores (
    id SERIAL PRIMARY KEY,
    apn VARCHAR(20) NOT NULL,
    template VARCHAR(50) NOT NULL,
    
    -- Final scores
    overall_score NUMERIC(4,2) CHECK (overall_score >= 0 AND overall_score <= 10),
    grade CHAR(1) CHECK (grade IN ('A', 'B', 'C', 'D', 'F')),
    
    -- Component scores (detailed breakdown)
    location_score NUMERIC(4,2),
    infrastructure_score NUMERIC(4,2),
    zoning_score NUMERIC(4,2),
    market_score NUMERIC(4,2),
    development_score NUMERIC(4,2),
    financial_score NUMERIC(4,2),
    
    -- Scoring metadata
    scoring_algorithm VARCHAR(50),
    algorithm_version VARCHAR(20),
    feature_data_version VARCHAR(20),
    
    -- Analysis results
    explanation TEXT,
    recommendations JSONB, -- Array of recommendation strings
    risk_factors JSONB,    -- Identified risk factors
    opportunities JSONB,   -- Development opportunities
    
    -- Performance tracking
    computation_time_ms INTEGER,
    feature_cache_hit BOOLEAN DEFAULT FALSE,
    
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
    ST_X(p.centroid) AS longitude,
    ST_Y(p.centroid) AS latitude
FROM parcels p
LEFT JOIN LATERAL (
    SELECT * FROM parcel_scores ps2 
    WHERE ps2.apn = p.apn 
    ORDER BY ps2.scored_at DESC 
    LIMIT 1
) ps ON true;

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
    p.geometry
FROM parcels p
INNER JOIN parcel_scores ps ON p.apn = ps.apn
WHERE ps.overall_score >= 7.0
    AND ps.scored_at = (
        SELECT MAX(scored_at) 
        FROM parcel_scores ps2 
        WHERE ps2.apn = ps.apn AND ps2.template = ps.template
    );

-- ==============================================================================
-- FUNCTIONS AND TRIGGERS
-- ==============================================================================

-- Function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger for zoning_codes table
CREATE TRIGGER update_zoning_codes_updated_at 
    BEFORE UPDATE ON zoning_codes 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Function to automatically compute parcel centroid from geometry
CREATE OR REPLACE FUNCTION compute_parcel_centroid()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.geometry IS NOT NULL THEN
        NEW.centroid = ST_Centroid(NEW.geometry);
        
        -- Compute lot size in acres if not provided
        IF NEW.lot_size_acres IS NULL AND NEW.lot_size_sqft IS NOT NULL THEN
            NEW.lot_size_acres = NEW.lot_size_sqft / 43560.0;
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger for automatic centroid computation
CREATE TRIGGER compute_parcel_centroid_trigger
    BEFORE INSERT OR UPDATE ON parcels
    FOR EACH ROW
    EXECUTE FUNCTION compute_parcel_centroid();

-- Function to clean expired cache entries
CREATE OR REPLACE FUNCTION clean_expired_cache()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM feature_cache 
    WHERE expires_at IS NOT NULL 
        AND expires_at < CURRENT_TIMESTAMP;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ language 'plpgsql';

-- ==============================================================================
-- SAMPLE DATA AND INITIAL SETUP
-- ==============================================================================

-- Insert common LA County zoning codes
INSERT INTO zoning_codes (code, description, category, density_type, development_potential) VALUES
-- Residential zones
('R1', 'Single Family Residential', 'residential', 'single-family', 4.0),
('R2', 'Two Family Residential', 'residential', 'two-family', 5.5),
('R3', 'Multiple Residential', 'residential', 'multi-family', 7.0),
('R4', 'Multiple Residential', 'residential', 'high-density', 8.0),
('R5', 'Multiple Residential', 'residential', 'very-high-density', 8.5),

-- Commercial zones  
('C1', 'Limited Commercial', 'commercial', 'neighborhood', 6.0),
('C2', 'Commercial', 'commercial', 'community', 7.5),
('C4', 'Commercial', 'commercial', 'regional', 8.5),

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
('A2', 'Light Agricultural', 'agricultural', 'light-farming', 3.0)

ON CONFLICT (code) DO NOTHING;

-- ==============================================================================
-- PERFORMANCE OPTIMIZATION QUERIES
-- ==============================================================================

-- Spatial query examples (commented for reference)
/*
-- Find parcels within 1 mile of a specific point
SELECT apn, address, ST_Distance_Sphere(centroid, ST_MakePoint(-118.2437, 34.0522)) as distance_meters
FROM parcels 
WHERE ST_DWithin(centroid, ST_MakePoint(-118.2437, 34.0522), 1609.34)
ORDER BY distance_meters;

-- Find parcels by zoning within a polygon boundary
SELECT p.apn, p.address, p.zoning
FROM parcels p
WHERE p.zoning IN ('R3', 'R4', 'R5') 
    AND ST_Within(p.centroid, ST_GeomFromText('POLYGON(...)', 4326));

-- Aggregate statistics by zoning type
SELECT 
    zoning,
    COUNT(*) as parcel_count,
    AVG(lot_size_sqft) as avg_lot_size,
    AVG(assessed_value) as avg_assessed_value,
    AVG(ps.overall_score) as avg_score
FROM parcels p
LEFT JOIN parcel_scores ps ON p.apn = ps.apn
GROUP BY zoning
ORDER BY avg_score DESC NULLS LAST;
*/

-- ==============================================================================
-- MAINTENANCE COMMANDS
-- ==============================================================================

-- Analyze tables for optimal query planning
ANALYZE parcels;
ANALYZE feature_cache;  
ANALYZE parcel_scores;
ANALYZE zoning_codes;

-- Display table sizes
SELECT 
    schemaname,
    tablename,
    attname,
    n_distinct,
    correlation
FROM pg_stats 
WHERE schemaname = 'public' 
    AND tablename IN ('parcels', 'parcel_scores', 'feature_cache')
ORDER BY tablename, attname;

COMMIT;