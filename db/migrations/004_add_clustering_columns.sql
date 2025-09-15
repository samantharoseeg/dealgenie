-- DealGenie Clustering Integration Migration
-- Migration: 004_add_clustering_columns.sql
-- Description: Adds clustering columns and metadata tables for Task 2.2
-- Date: 2024-09-09

BEGIN TRANSACTION;

-- ==============================================================================
-- ADD CLUSTERING COLUMNS TO RAW_PERMITS
-- ==============================================================================
ALTER TABLE raw_permits ADD COLUMN cluster_id INTEGER;
ALTER TABLE raw_permits ADD COLUMN cluster_run_id TEXT;
ALTER TABLE raw_permits ADD COLUMN cluster_confidence_score REAL;
ALTER TABLE raw_permits ADD COLUMN cluster_assigned_at TIMESTAMP;
ALTER TABLE raw_permits ADD COLUMN clustering_algorithm VARCHAR(50);
ALTER TABLE raw_permits ADD COLUMN clustering_version VARCHAR(20);

-- Add business metadata columns
ALTER TABLE raw_permits ADD COLUMN permit_category_code VARCHAR(10);  -- For business rules
ALTER TABLE raw_permits ADD COLUMN is_administrative BOOLEAN DEFAULT FALSE;
ALTER TABLE raw_permits ADD COLUMN exclusion_reason TEXT;

-- ==============================================================================
-- CREATE CLUSTERING METADATA TABLE (Enhanced)
-- ==============================================================================
DROP TABLE IF EXISTS clustering_metadata;

CREATE TABLE clustering_metadata (
    run_id TEXT PRIMARY KEY,
    
    -- Execution metadata
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    execution_time_seconds REAL,
    clustering_version VARCHAR(20) DEFAULT '1.0',
    
    -- Algorithm configuration
    algorithm VARCHAR(50) NOT NULL,        -- 'simple_dbscan', 'production_clustering'
    parameters TEXT NOT NULL,              -- JSON of all parameters
    
    -- Input data
    permits_processed INTEGER NOT NULL,
    permits_excluded INTEGER DEFAULT 0,
    exclusion_criteria TEXT,               -- JSON of exclusion rules applied
    
    -- Results
    clusters_found INTEGER NOT NULL,
    noise_points INTEGER NOT NULL,
    clustered_permits INTEGER NOT NULL,
    
    -- Quality metrics
    avg_confidence_score REAL,
    high_confidence_clusters INTEGER,
    medium_confidence_clusters INTEGER,
    low_confidence_clusters INTEGER,
    invalid_clusters INTEGER,
    
    -- Business metrics
    project_aggregations INTEGER DEFAULT 0,
    assembly_opportunities INTEGER DEFAULT 0,
    megaprojects_identified INTEGER DEFAULT 0,  -- >$10M projects
    
    -- Safety and validation
    max_cost_ratio REAL,
    extreme_cost_violations INTEGER DEFAULT 0,  -- >100x violations
    business_rule_violations INTEGER DEFAULT 0,
    
    -- Governance
    created_by VARCHAR(100) DEFAULT 'production_clustering_engine',
    git_commit_hash VARCHAR(40),
    config_hash VARCHAR(64),               -- Hash of parameters for reproducibility
    validation_status VARCHAR(20) DEFAULT 'pending',
    validation_errors TEXT,
    
    -- Lineage
    source_data_version TEXT,
    depends_on_runs TEXT                   -- JSON array of dependent run_ids
);

-- ==============================================================================
-- CREATE CLUSTER CONFIDENCE SCORES TABLE
-- ==============================================================================
CREATE TABLE cluster_confidence_scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Cluster identification
    cluster_id INTEGER NOT NULL,
    cluster_run_id TEXT NOT NULL,
    
    -- Confidence metrics
    overall_confidence_score REAL NOT NULL,    -- 0-100
    confidence_level VARCHAR(20),              -- 'high', 'medium', 'low', 'invalid'
    
    -- Component scores
    cost_coherence_score REAL,
    temporal_coherence_score REAL,
    spatial_coherence_score REAL,
    permit_type_coherence_score REAL,
    business_logic_score REAL,
    
    -- Cluster characteristics
    permits_count INTEGER,
    unique_apns INTEGER,
    unique_addresses INTEGER,
    total_estimated_cost REAL,
    cost_ratio REAL,                          -- max/min cost ratio
    
    -- Temporal span
    date_span_days INTEGER,
    earliest_permit_date DATE,
    latest_permit_date DATE,
    
    -- Spatial characteristics
    spatial_extent_meters REAL,
    geographic_coherence VARCHAR(20),
    
    -- Assessment
    is_assembly_opportunity BOOLEAN DEFAULT FALSE,
    is_megaproject BOOLEAN DEFAULT FALSE,
    requires_manual_review BOOLEAN DEFAULT FALSE,
    reviewer_notes TEXT,
    
    -- Metadata
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    calculation_version VARCHAR(20),
    
    FOREIGN KEY (cluster_run_id) REFERENCES clustering_metadata(run_id),
    UNIQUE(cluster_id, cluster_run_id)
);

-- ==============================================================================
-- CREATE PROJECT AGGREGATIONS TABLE
-- ==============================================================================
CREATE TABLE project_aggregations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Project identification
    project_cluster_id INTEGER NOT NULL,
    cluster_run_id TEXT NOT NULL,
    
    -- Aggregated metrics
    permits_count INTEGER NOT NULL,
    total_estimated_cost REAL,
    net_units_change INTEGER DEFAULT 0,
    net_square_footage_change REAL DEFAULT 0,
    
    -- Project characteristics
    primary_permit_type VARCHAR(100),
    dominant_permit_category VARCHAR(50),
    project_complexity_score REAL,          -- Based on permit diversity
    
    -- Geographic summary
    unique_apns INTEGER,
    unique_addresses INTEGER,
    council_districts TEXT,                  -- JSON array of districts
    
    -- Temporal summary
    project_start_date DATE,
    project_end_date DATE,
    estimated_completion_date DATE,
    project_duration_days INTEGER,
    
    -- Investment metrics
    estimated_roi REAL,
    investment_tier VARCHAR(10),             -- 'A+', 'A', 'B', 'C+', 'C', 'D'
    risk_assessment VARCHAR(20),
    
    -- Assembly analysis
    is_potential_assembly BOOLEAN DEFAULT FALSE,
    assembly_potential_score REAL,
    adjacent_parcels_analysis TEXT,          -- JSON
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (cluster_run_id) REFERENCES clustering_metadata(run_id),
    UNIQUE(project_cluster_id, cluster_run_id)
);

-- ==============================================================================
-- CREATE ASSEMBLY OPPORTUNITIES TABLE
-- ==============================================================================
CREATE TABLE assembly_opportunities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Assembly identification
    cluster_id INTEGER NOT NULL,
    cluster_run_id TEXT NOT NULL,
    
    -- Assembly metrics
    apn_count INTEGER NOT NULL,
    total_value REAL NOT NULL,
    permits_count INTEGER,
    
    -- Geographic analysis
    spatial_coherence_score REAL,
    apn_prefix_similarity REAL,             -- How similar are APN prefixes
    address_number_range INTEGER,            -- Range of street numbers
    
    -- Development potential
    zoning_compatibility_score REAL,
    development_potential_score REAL,
    infrastructure_readiness_score REAL,
    
    -- Investment analysis
    estimated_acquisition_cost REAL,
    estimated_development_value REAL,
    estimated_roi REAL,
    payback_period_months INTEGER,
    
    -- Risk assessment
    regulatory_risk_score REAL,
    market_risk_score REAL,
    execution_risk_score REAL,
    overall_risk_level VARCHAR(20),
    
    -- Confidence and validation
    confidence_score REAL,                  -- 0-100
    validation_status VARCHAR(20),
    manual_review_required BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    identified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_analyzed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (cluster_run_id) REFERENCES clustering_metadata(run_id),
    UNIQUE(cluster_id, cluster_run_id)
);

-- ==============================================================================
-- CREATE INDEXES FOR PERFORMANCE
-- ==============================================================================
CREATE INDEX IF NOT EXISTS idx_raw_permits_cluster_id ON raw_permits(cluster_id);
CREATE INDEX IF NOT EXISTS idx_raw_permits_cluster_run ON raw_permits(cluster_run_id);
CREATE INDEX IF NOT EXISTS idx_raw_permits_cluster_assigned ON raw_permits(cluster_assigned_at DESC);
CREATE INDEX IF NOT EXISTS idx_raw_permits_administrative ON raw_permits(is_administrative);

CREATE INDEX IF NOT EXISTS idx_clustering_metadata_timestamp ON clustering_metadata(run_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_clustering_metadata_algorithm ON clustering_metadata(algorithm);
CREATE INDEX IF NOT EXISTS idx_clustering_metadata_validation ON clustering_metadata(validation_status);

CREATE INDEX IF NOT EXISTS idx_cluster_confidence_cluster ON cluster_confidence_scores(cluster_id);
CREATE INDEX IF NOT EXISTS idx_cluster_confidence_run ON cluster_confidence_scores(cluster_run_id);
CREATE INDEX IF NOT EXISTS idx_cluster_confidence_level ON cluster_confidence_scores(confidence_level);
CREATE INDEX IF NOT EXISTS idx_cluster_confidence_score ON cluster_confidence_scores(overall_confidence_score DESC);

CREATE INDEX IF NOT EXISTS idx_project_aggregations_cluster ON project_aggregations(project_cluster_id);
CREATE INDEX IF NOT EXISTS idx_project_aggregations_cost ON project_aggregations(total_estimated_cost DESC);
CREATE INDEX IF NOT EXISTS idx_project_aggregations_tier ON project_aggregations(investment_tier);

CREATE INDEX IF NOT EXISTS idx_assembly_opportunities_cluster ON assembly_opportunities(cluster_id);
CREATE INDEX IF NOT EXISTS idx_assembly_opportunities_value ON assembly_opportunities(total_value DESC);
CREATE INDEX IF NOT EXISTS idx_assembly_opportunities_confidence ON assembly_opportunities(confidence_score DESC);

-- ==============================================================================
-- CREATE VIEWS FOR PRODUCTION QUERIES
-- ==============================================================================

-- View: High-Confidence Clusters for Production Use
CREATE VIEW IF NOT EXISTS view_production_clusters AS
SELECT 
    p.cluster_id,
    p.cluster_run_id,
    COUNT(*) as permits_count,
    c.overall_confidence_score,
    c.confidence_level,
    c.total_estimated_cost,
    c.cost_ratio,
    c.spatial_extent_meters,
    c.date_span_days,
    c.is_megaproject,
    c.is_assembly_opportunity,
    pa.investment_tier,
    pa.estimated_roi
FROM raw_permits p
JOIN cluster_confidence_scores c ON p.cluster_id = c.cluster_id AND p.cluster_run_id = c.cluster_run_id
LEFT JOIN project_aggregations pa ON p.cluster_id = pa.project_cluster_id AND p.cluster_run_id = pa.cluster_run_id
WHERE p.cluster_id IS NOT NULL
  AND c.confidence_level IN ('high', 'medium')
  AND c.overall_confidence_score >= 70
GROUP BY p.cluster_id, p.cluster_run_id, c.overall_confidence_score, c.confidence_level, 
         c.total_estimated_cost, c.cost_ratio, c.spatial_extent_meters, c.date_span_days,
         c.is_megaproject, c.is_assembly_opportunity, pa.investment_tier, pa.estimated_roi
ORDER BY c.overall_confidence_score DESC, c.total_estimated_cost DESC;

-- View: Assembly Opportunities Ready for Analysis
CREATE VIEW IF NOT EXISTS view_assembly_ready AS
SELECT 
    ao.cluster_id,
    ao.apn_count,
    ao.total_value,
    ao.confidence_score,
    ao.development_potential_score,
    ao.estimated_roi,
    ao.overall_risk_level,
    cm.run_timestamp,
    COUNT(DISTINCT p.apn) as confirmed_parcels
FROM assembly_opportunities ao
JOIN clustering_metadata cm ON ao.cluster_run_id = cm.run_id
JOIN raw_permits p ON ao.cluster_id = p.cluster_id AND ao.cluster_run_id = p.cluster_run_id
WHERE ao.confidence_score >= 70
  AND ao.manual_review_required = FALSE
  AND cm.validation_status = 'passed'
GROUP BY ao.cluster_id, ao.apn_count, ao.total_value, ao.confidence_score, 
         ao.development_potential_score, ao.estimated_roi, ao.overall_risk_level, cm.run_timestamp
ORDER BY ao.total_value DESC, ao.confidence_score DESC;

-- View: Clustering Quality Dashboard
CREATE VIEW IF NOT EXISTS view_clustering_quality AS
SELECT 
    cm.run_id,
    cm.run_timestamp,
    cm.algorithm,
    cm.permits_processed,
    cm.permits_excluded,
    cm.clusters_found,
    cm.clustered_permits,
    ROUND(100.0 * cm.clustered_permits / cm.permits_processed, 1) as clustering_rate,
    cm.avg_confidence_score,
    cm.high_confidence_clusters,
    cm.extreme_cost_violations,
    cm.business_rule_violations,
    cm.max_cost_ratio,
    cm.validation_status,
    CASE 
        WHEN cm.avg_confidence_score >= 90 AND cm.extreme_cost_violations = 0 THEN '✅ EXCELLENT'
        WHEN cm.avg_confidence_score >= 80 AND cm.extreme_cost_violations <= 1 THEN '✅ GOOD'  
        WHEN cm.avg_confidence_score >= 70 THEN '⚠️ ACCEPTABLE'
        ELSE '❌ NEEDS_IMPROVEMENT'
    END as quality_assessment
FROM clustering_metadata cm
ORDER BY cm.run_timestamp DESC;

-- ==============================================================================
-- UPDATE SYSTEM CONFIGURATION
-- ==============================================================================
INSERT OR REPLACE INTO system_config (key, value, description) VALUES
('clustering_schema_version', '1.0', 'Clustering database schema version'),
('clustering_enabled', '1', 'Enable clustering functionality'),
('clustering_tables_created', datetime('now'), 'Date when clustering tables were created'),
('max_cluster_cost_ratio', '100', 'Maximum allowed cost ratio within clusters'),
('clustering_confidence_threshold', '70', 'Minimum confidence score for production use');

-- ==============================================================================
-- MIGRATION VALIDATION
-- ==============================================================================
-- Test that all tables were created successfully
SELECT 
    'clustering_metadata' as table_name,
    COUNT(*) as exists_check
FROM sqlite_master 
WHERE type='table' AND name='clustering_metadata'
UNION ALL
SELECT 
    'cluster_confidence_scores' as table_name,
    COUNT(*) as exists_check  
FROM sqlite_master
WHERE type='table' AND name='cluster_confidence_scores'
UNION ALL
SELECT
    'project_aggregations' as table_name,
    COUNT(*) as exists_check
FROM sqlite_master
WHERE type='table' AND name='project_aggregations'
UNION ALL
SELECT
    'assembly_opportunities' as table_name,
    COUNT(*) as exists_check
FROM sqlite_master
WHERE type='table' AND name='assembly_opportunities';

COMMIT;

-- Success message
SELECT 'Clustering migration completed successfully! Added:
- cluster_id, cluster_run_id, cluster_confidence_score columns to raw_permits
- clustering_metadata table with full governance
- cluster_confidence_scores table for quality tracking
- project_aggregations table for investment analysis  
- assembly_opportunities table for land assembly analysis
- 3 production views for high-confidence queries
- Performance indexes for clustering queries
Schema ready for Task 2.2 clustering integration!' as migration_status;