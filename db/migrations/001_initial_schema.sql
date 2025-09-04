-- DealGenie Week 1 Migration: Initial Schema
-- Migration: 001_initial_schema.sql  
-- Description: Initial database schema from Week 1 foundation
-- Date: 2024-09-04

-- This file represents the Week 1 schema that was already applied
-- Documented here for migration tracking purposes

BEGIN TRANSACTION;

-- Week 1 tables (already exist)
-- - parcels
-- - parcel_scores  
-- - feature_cache
-- - zoning_codes
-- - system_config

-- Ensure system_config table exists (idempotent on fresh DBs)
CREATE TABLE IF NOT EXISTS system_config (
    key TEXT PRIMARY KEY,
    value TEXT,
    description TEXT
);

-- Update system configuration to track this as first migration
INSERT OR REPLACE INTO system_config (key, value, description) VALUES
('schema_version', '1.0', 'Initial database schema version'),
('migration_001_applied', datetime('now'), 'Week 1 initial schema migration'),
('total_parcels', '369703', 'Total parcels in dataset'),
('csv_data_source', 'scraper/la_parcels_complete_merged.csv', 'Primary CSV data file');

COMMIT;

SELECT 'Migration 001 (Initial Schema) marked as applied' as migration_status;