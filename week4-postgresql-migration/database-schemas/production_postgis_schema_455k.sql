-- DealGenie Production PostGIS Schema for 455K Property Migration
-- PostgreSQL 17 + PostGIS 3.5.3 Ready
-- Optimized for bulk CSV import and spatial operations

-- Drop existing tables if needed
DROP TABLE IF EXISTS enhanced_scored_properties CASCADE;

-- Create the main properties table with full PostGIS support and complete column structure
CREATE TABLE enhanced_scored_properties (
    -- Primary identifiers
    property_id INTEGER PRIMARY KEY,
    assessor_parcel_id TEXT,
    pin TEXT,

    -- Location and address information
    site_address TEXT,
    zip_code TEXT,
    council_district TEXT,
    community_plan_area TEXT,
    neighborhood_council TEXT,

    -- Zoning and land use
    base_zoning TEXT,
    base_zoning_description TEXT,
    full_zoning_code TEXT,
    general_plan_land_use TEXT,
    specific_plan_area TEXT,
    overlay_count INTEGER,

    -- Property characteristics
    lot_size_sqft REAL,
    building_size_sqft REAL,
    year_built REAL,
    number_of_units REAL,
    use_code TEXT,

    -- Assessment values
    assessed_land_value REAL,
    assessed_improvement_value REAL,
    total_assessed_value REAL,

    -- Development opportunities
    toc_eligible BOOLEAN,
    opportunity_zone TEXT,
    high_quality_transit BOOLEAN,

    -- Market areas
    residential_market_area TEXT,
    commercial_market_area TEXT,

    -- Environmental factors
    methane_zone TEXT,
    flood_zone TEXT,
    fault_zone TEXT,
    rent_stabilization TEXT,
    housing_replacement_required TEXT,

    -- Scoring and analysis
    data_completeness_score REAL,
    development_score REAL,
    score_breakdown JSONB,
    suggested_use TEXT,
    investment_tier TEXT,

    -- Spatial coordinates - PostGIS geometry
    latitude REAL,
    longitude REAL,
    geom GEOMETRY(POINT, 4326),  -- PostGIS spatial column

    -- Distance calculations (in miles)
    dist_downtown_miles REAL,
    dist_santa_monica_miles REAL,
    dist_hollywood_miles REAL,
    dist_lax_miles REAL,
    dist_ucla_miles REAL,
    dist_usc_miles REAL,

    -- Transit access
    nearest_metro_station TEXT,
    nearest_metro_distance REAL,
    nearest_metro_lines TEXT,
    freeway_distance_miles REAL,

    -- Location scoring
    walkability_score REAL,
    location_premium_bonus REAL,
    transit_accessibility_bonus REAL,
    highway_access_bonus REAL,
    total_geographic_bonus REAL,

    -- Enhanced scoring
    enhanced_development_score REAL,
    enhanced_investment_tier TEXT,
    enhanced_suggested_use TEXT,

    -- Metadata
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create comprehensive indexes for optimal query performance

-- Primary spatial index (GIST for PostGIS geometry)
CREATE INDEX idx_properties_geom_gist ON enhanced_scored_properties USING GIST(geom);

-- Primary key and identifier indexes
CREATE INDEX idx_properties_property_id ON enhanced_scored_properties(property_id);
CREATE INDEX idx_properties_apn ON enhanced_scored_properties(assessor_parcel_id);
CREATE INDEX idx_properties_pin ON enhanced_scored_properties(pin);

-- Location and address indexes
CREATE INDEX idx_properties_latitude ON enhanced_scored_properties(latitude);
CREATE INDEX idx_properties_longitude ON enhanced_scored_properties(longitude);
CREATE INDEX idx_properties_zip_code ON enhanced_scored_properties(zip_code);
CREATE INDEX idx_properties_council_district ON enhanced_scored_properties(council_district);

-- Zoning and land use indexes
CREATE INDEX idx_properties_base_zoning ON enhanced_scored_properties(base_zoning);
CREATE INDEX idx_properties_general_plan ON enhanced_scored_properties(general_plan_land_use);
CREATE INDEX idx_properties_use_code ON enhanced_scored_properties(use_code);

-- Property characteristics indexes
CREATE INDEX idx_properties_lot_size ON enhanced_scored_properties(lot_size_sqft);
CREATE INDEX idx_properties_building_size ON enhanced_scored_properties(building_size_sqft);
CREATE INDEX idx_properties_year_built ON enhanced_scored_properties(year_built);
CREATE INDEX idx_properties_units ON enhanced_scored_properties(number_of_units);

-- Assessment value indexes
CREATE INDEX idx_properties_total_value ON enhanced_scored_properties(total_assessed_value);
CREATE INDEX idx_properties_land_value ON enhanced_scored_properties(assessed_land_value);

-- Development opportunity indexes
CREATE INDEX idx_properties_toc_eligible ON enhanced_scored_properties(toc_eligible);
CREATE INDEX idx_properties_opportunity_zone ON enhanced_scored_properties(opportunity_zone);
CREATE INDEX idx_properties_transit ON enhanced_scored_properties(high_quality_transit);

-- Scoring indexes
CREATE INDEX idx_properties_development_score ON enhanced_scored_properties(development_score);
CREATE INDEX idx_properties_investment_tier ON enhanced_scored_properties(investment_tier);
CREATE INDEX idx_properties_enhanced_score ON enhanced_scored_properties(enhanced_development_score);
CREATE INDEX idx_properties_enhanced_tier ON enhanced_scored_properties(enhanced_investment_tier);

-- Distance and accessibility indexes
CREATE INDEX idx_properties_downtown_dist ON enhanced_scored_properties(dist_downtown_miles);
CREATE INDEX idx_properties_metro_distance ON enhanced_scored_properties(nearest_metro_distance);
CREATE INDEX idx_properties_walkability ON enhanced_scored_properties(walkability_score);

-- Composite indexes for common queries
CREATE INDEX idx_properties_tier_score ON enhanced_scored_properties(enhanced_investment_tier, enhanced_development_score);
CREATE INDEX idx_properties_zoning_size ON enhanced_scored_properties(base_zoning, lot_size_sqft);
CREATE INDEX idx_properties_location_score ON enhanced_scored_properties(latitude, longitude, enhanced_development_score);

-- JSONB index for score breakdown queries
CREATE INDEX idx_properties_score_breakdown_gin ON enhanced_scored_properties USING GIN(score_breakdown);

-- Create trigger to automatically populate geometry from lat/lon coordinates
CREATE OR REPLACE FUNCTION update_geometry_from_coordinates()
RETURNS TRIGGER AS $$
BEGIN
    -- Only update geometry if lat/lon are not null and valid
    IF NEW.latitude IS NOT NULL AND NEW.longitude IS NOT NULL
       AND NEW.latitude BETWEEN -90 AND 90
       AND NEW.longitude BETWEEN -180 AND 180 THEN
        NEW.geom := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
    END IF;

    -- Update the updated_at timestamp
    NEW.updated_at := NOW();

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_geometry_and_timestamp
    BEFORE INSERT OR UPDATE ON enhanced_scored_properties
    FOR EACH ROW
    EXECUTE FUNCTION update_geometry_from_coordinates();

-- Insert test data to verify schema
INSERT INTO enhanced_scored_properties (
    property_id, assessor_parcel_id, pin, site_address, zip_code,
    council_district, base_zoning, base_zoning_description,
    lot_size_sqft, building_size_sqft, year_built, number_of_units,
    assessed_land_value, assessed_improvement_value, total_assessed_value,
    toc_eligible, high_quality_transit, investment_tier,
    latitude, longitude,
    dist_downtown_miles, enhanced_development_score, enhanced_investment_tier,
    score_breakdown
) VALUES (
    999999, 'TEST-001', 'TEST001', '123 TEST ST, Los Angeles, CA', '90210',
    'CD 1 - Test District', 'R1', 'Single Family Residential',
    7500.0, 2500.0, 1980.0, 1.0,
    500000.0, 300000.0, 800000.0,
    false, true, 'A',
    34.0522, -118.2437,
    0.5, 85.5, 'A+',
    '{"zoning_score": 40.0, "lot_size_score": 70.0, "transit_bonus": 15}'::jsonb
);

-- Verification queries
SELECT 'Production PostGIS Schema Created Successfully!' as status;

-- Test spatial functionality
SELECT 'Testing PostGIS Functionality:' as test_section;

-- Test 1: Verify geometry creation from coordinates
SELECT property_id, assessor_parcel_id,
       ST_AsText(geom) as geometry_wkt,
       ST_X(geom) as longitude_from_geom,
       ST_Y(geom) as latitude_from_geom
FROM enhanced_scored_properties
WHERE property_id = 999999;

-- Test 2: Spatial distance calculation
SELECT 'Distance Calculation Test:' as test_name,
       ST_Distance(
           geom::geography,
           ST_SetSRID(ST_MakePoint(-118.25, 34.05), 4326)::geography
       ) as distance_meters
FROM enhanced_scored_properties
WHERE property_id = 999999;

-- Test 3: Spatial index performance
EXPLAIN ANALYZE
SELECT property_id, assessor_parcel_id
FROM enhanced_scored_properties
WHERE ST_DWithin(
    geom::geography,
    ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326)::geography,
    1000  -- 1km radius
);

-- Test 4: JSONB score breakdown query
SELECT property_id,
       score_breakdown->>'zoning_score' as zoning_score,
       score_breakdown->>'lot_size_score' as lot_size_score,
       score_breakdown->>'transit_bonus' as transit_bonus
FROM enhanced_scored_properties
WHERE property_id = 999999;

-- Table statistics
SELECT 'Table Statistics:' as info;
SELECT
    schemaname,
    tablename,
    attname as column_name,
    n_distinct,
    correlation
FROM pg_stats
WHERE tablename = 'enhanced_scored_properties'
AND schemaname = 'public'
ORDER BY attname;

-- Index information
SELECT 'Index Information:' as info;
SELECT
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename = 'enhanced_scored_properties'
AND schemaname = 'public'
ORDER BY indexname;

-- Ready for bulk import
SELECT 'Schema ready for 455K property CSV import!' as ready_status;
SELECT 'Use COPY command or \\copy for bulk data loading' as import_note;