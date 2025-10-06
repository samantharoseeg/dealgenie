-- UNIFIED VIEW: Week 1-5 Enhanced Data + Week 6 Complete Coverage
-- Joins enhanced_scored_properties (3 records) with parcels_complete (2.4M records)

CREATE OR REPLACE VIEW properties_unified AS
SELECT
    -- Core identifiers (from parcels_complete / Week 6)
    pc."AIN" as apn,
    pc."Descriptio" as zoning_code,
    pc."Jurisdicti" as jurisdiction,
    pc."Code" as zone_class,

    -- Property characteristics (from parcels_complete)
    CASE
        WHEN pc."Shape_Area" ~ '^[0-9.]+$' THEN pc."Shape_Area"::numeric
        ELSE NULL
    END as lot_size_sqft,

    CASE
        WHEN pc."SQFTmain1" ~ '^[0-9.]+$' THEN pc."SQFTmain1"::numeric
        ELSE NULL
    END as total_building_sqft,

    CASE
        WHEN pc."YearBuilt1" ~ '^[0-9]+$' THEN pc."YearBuilt1"::integer
        ELSE NULL
    END as year_built,

    -- Financial data (from parcels_complete)
    CASE
        WHEN pc."Roll_LandV" ~ '^[0-9.]+$' THEN pc."Roll_LandV"::numeric
        ELSE NULL
    END as land_value,

    CASE
        WHEN pc."Roll_ImpVa" ~ '^[0-9.]+$' THEN pc."Roll_ImpVa"::numeric
        ELSE NULL
    END as improvement_value,

    -- Geographic data (from parcels_complete)
    CASE
        WHEN pc."CENTER_LAT" ~ '^-?[0-9.]+$' THEN pc."CENTER_LAT"::numeric
        ELSE NULL
    END as latitude,

    CASE
        WHEN pc."CENTER_LON" ~ '^-?[0-9.]+$' THEN pc."CENTER_LON"::numeric
        ELSE NULL
    END as longitude,

    pc.geometry as geom,

    -- Week 1-5 enhanced scoring (if available from enhanced_scored_properties)
    esp.crime_score,
    esp.data_quality_score,
    esp.property_type,

    -- Enhanced fields from Week 1-5 (currently minimal, but structure ready for future expansion)
    NULL::numeric as development_score,
    NULL::numeric as enhanced_development_score,
    NULL::text as investment_tier,
    NULL::text as enhanced_investment_tier,
    NULL::numeric as walkability_score,
    NULL::numeric as transit_accessibility_bonus,
    NULL::numeric as location_premium_bonus,

    -- Data source indicator
    CASE
        WHEN esp.apn IS NOT NULL THEN 'week1-5_enhanced'
        ELSE 'week6_coverage_only'
    END as data_source,

    -- Timestamps
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at

FROM parcels_complete pc
LEFT JOIN enhanced_scored_properties esp ON pc."AIN" = esp.apn;

-- Grant access to application user
GRANT SELECT ON properties_unified TO dealgenie_app;

-- Create index on apn for fast lookups
-- Note: Cannot create index on view directly, but underlying tables should have indexes
