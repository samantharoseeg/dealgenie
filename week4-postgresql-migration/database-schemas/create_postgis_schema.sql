-- DealGenie Full PostGIS Schema for PostgreSQL 17 + PostGIS 3.5.3
-- This implementation uses full PostGIS spatial functionality

-- Drop existing tables if needed
DROP TABLE IF EXISTS enhanced_scored_properties CASCADE;

-- Create the main properties table with full PostGIS support
CREATE TABLE enhanced_scored_properties (
    apn TEXT PRIMARY KEY,
    site_address TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    geom GEOMETRY(POINT, 4326),  -- PostGIS GEOMETRY with SRID 4326 (WGS84)
    property_type TEXT,
    zoning_code TEXT,
    lot_sqft DOUBLE PRECISION,
    existing_sqft DOUBLE PRECISION,
    year_built INTEGER,
    crime_score DOUBLE PRECISION,
    data_quality_score DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create spatial index using PostGIS GIST indexing
CREATE INDEX idx_properties_geom_gist ON enhanced_scored_properties USING GIST(geom);

-- Create additional indexes for performance
CREATE INDEX idx_properties_latitude ON enhanced_scored_properties(latitude);
CREATE INDEX idx_properties_longitude ON enhanced_scored_properties(longitude);
CREATE INDEX idx_properties_apn ON enhanced_scored_properties(apn);
CREATE INDEX idx_properties_zoning ON enhanced_scored_properties(zoning_code);

-- Create trigger to automatically populate geometry from lat/lon
CREATE OR REPLACE FUNCTION update_geometry_from_coordinates()
RETURNS TRIGGER AS $$
BEGIN
    -- Only update geometry if lat/lon are not null
    IF NEW.latitude IS NOT NULL AND NEW.longitude IS NOT NULL THEN
        NEW.geom := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_geometry
    BEFORE INSERT OR UPDATE ON enhanced_scored_properties
    FOR EACH ROW
    EXECUTE FUNCTION update_geometry_from_coordinates();

-- Insert sample data with PostGIS functions
INSERT INTO enhanced_scored_properties (
    apn, site_address, latitude, longitude,
    property_type, zoning_code, lot_sqft, existing_sqft,
    year_built, crime_score, data_quality_score
) VALUES
    ('4306026007', '9406 OAKMORE RD, Los Angeles, CA',
     34.04754039, -118.3935813,
     'Single Family', 'R1', 7500, 2200,
     1955, 85.5, 92.0),
    ('4330019015', '1193 RODEO DR, Los Angeles, CA',
     34.05603323, -118.39970631,
     'Single Family', 'R1', 8200, 2800,
     1962, 78.3, 88.5),
    ('5555012001', '1600 VINE ST, Los Angeles, CA',
     34.0998, -118.3268,
     'Commercial', 'C2', 12000, 8500,
     1975, 72.0, 85.0),
    ('6789123456', '123 SUNSET BLVD, Los Angeles, CA',
     34.0928, -118.3287,
     'Multi-Family', 'R3', 5000, 4200,
     1980, 68.9, 84.2),
    ('9876543210', '456 HOLLYWOOD BLVD, Los Angeles, CA',
     34.1022, -118.3412,
     'Commercial', 'C1', 15000, 12000,
     1965, 75.6, 90.1)
ON CONFLICT (apn) DO NOTHING;

-- Verify PostGIS functionality
SELECT 'PostGIS Schema Created Successfully!' as status;

-- Test PostGIS spatial functions
SELECT 'Testing PostGIS Functions:' as test_section;

-- Test 1: Basic PostGIS geometry creation
SELECT apn, site_address,
       ST_AsText(geom) as geometry_text,
       ST_X(geom) as longitude_from_geom,
       ST_Y(geom) as latitude_from_geom
FROM enhanced_scored_properties
LIMIT 3;

-- Test 2: Distance calculations using PostGIS ST_Distance (spherical)
SELECT 'Distance Tests:' as test_section;
SELECT p1.apn as property1, p2.apn as property2,
       ST_Distance(p1.geom::geography, p2.geom::geography) as distance_meters
FROM enhanced_scored_properties p1
CROSS JOIN enhanced_scored_properties p2
WHERE p1.apn = '4306026007' AND p2.apn != p1.apn
ORDER BY distance_meters
LIMIT 3;

-- Test 3: Find properties within radius using ST_DWithin
SELECT 'Radius Search Tests:' as test_section;
SELECT apn, site_address,
       ST_Distance(
           ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326)::geography,
           geom::geography
       ) as distance_meters
FROM enhanced_scored_properties
WHERE ST_DWithin(
    ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326)::geography,
    geom::geography,
    10000  -- 10km radius
)
ORDER BY distance_meters;

-- Test 4: Nearest neighbor search using ST_Distance ordering
SELECT 'Nearest Neighbor Tests:' as test_section;
SELECT apn, site_address, property_type,
       ST_Distance(
           ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326)::geography,
           geom::geography
       ) as distance_meters
FROM enhanced_scored_properties
ORDER BY geom <-> ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326)
LIMIT 5;

-- Test 5: Geometry validation
SELECT 'Geometry Validation Tests:' as test_section;
SELECT apn, ST_IsValid(geom) as is_valid_geometry,
       ST_GeometryType(geom) as geometry_type,
       ST_SRID(geom) as spatial_reference_id
FROM enhanced_scored_properties
LIMIT 3;

-- Test 6: Spatial aggregation - get bounding box of all properties
SELECT 'Spatial Aggregation Tests:' as test_section;
SELECT ST_AsText(ST_Envelope(ST_Collect(geom))) as bounding_box,
       ST_AsText(ST_Centroid(ST_Collect(geom))) as center_point,
       COUNT(*) as total_properties
FROM enhanced_scored_properties;

-- Performance test query
EXPLAIN ANALYZE
SELECT apn, site_address
FROM enhanced_scored_properties
WHERE ST_DWithin(
    geom::geography,
    ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326)::geography,
    5000
);