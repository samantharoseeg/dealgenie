-- Migration 010: Add Monte Carlo simulation tables
-- Purpose: Store simulation inputs and results for reproducibility
-- Date: 2025-10-09

\c dealgenie_production

-- Table to store simulation inputs (for reproducibility)
-- Ensures deterministic results: same inputs → same hash → same results
CREATE TABLE IF NOT EXISTS sim_inputs_snapshot (
    apn TEXT NOT NULL,
    template TEXT NOT NULL,
    inputs_hash TEXT NOT NULL,
    inputs_json JSONB NOT NULL,
    sim_version INTEGER DEFAULT 1,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (apn, template, inputs_hash)
);

-- Table to store simulation results
-- Links back to inputs via foreign key for traceability
CREATE TABLE IF NOT EXISTS sim_summary (
    apn TEXT NOT NULL,
    template TEXT NOT NULL,
    sim_version INTEGER NOT NULL,
    p5_irr REAL,
    p50_irr REAL,
    p95_irr REAL,
    prob_hit_hurdle REAL,
    var5_irr REAL,
    es5_irr REAL,
    inputs_hash TEXT NOT NULL,
    runs INTEGER NOT NULL,
    generated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (apn, template, sim_version),
    FOREIGN KEY (apn, template, inputs_hash)
        REFERENCES sim_inputs_snapshot(apn, template, inputs_hash)
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_sim_summary_prob_hurdle
    ON sim_summary(prob_hit_hurdle DESC);

CREATE INDEX IF NOT EXISTS idx_sim_summary_p50_irr
    ON sim_summary(p50_irr DESC);

CREATE INDEX IF NOT EXISTS idx_sim_inputs_created
    ON sim_inputs_snapshot(created_at DESC);

-- Documentation
COMMENT ON TABLE sim_inputs_snapshot IS 'Stores simulation inputs for reproducibility. Primary key ensures no duplicate inputs.';
COMMENT ON TABLE sim_summary IS 'Stores Monte Carlo simulation results. Foreign key ensures inputs exist before results.';
COMMENT ON COLUMN sim_inputs_snapshot.inputs_hash IS 'SHA256 hash of envelope + priors + template + version for deterministic caching';
COMMENT ON COLUMN sim_summary.p5_irr IS '5th percentile IRR (downside case)';
COMMENT ON COLUMN sim_summary.p50_irr IS 'Median IRR (base case)';
COMMENT ON COLUMN sim_summary.p95_irr IS '95th percentile IRR (upside case)';
COMMENT ON COLUMN sim_summary.prob_hit_hurdle IS 'Probability of achieving hurdle rate (e.g., IRR > 15%)';
COMMENT ON COLUMN sim_summary.var5_irr IS 'Value at Risk at 5% (expected shortfall threshold)';
COMMENT ON COLUMN sim_summary.es5_irr IS 'Expected Shortfall at 5% (conditional tail expectation)';
