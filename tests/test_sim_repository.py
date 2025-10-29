"""
Tests for SimRepository

Covers:
- Hash determinism (same inputs → same hash)
- Hash sensitivity (different inputs → different hash)
- Input snapshot storage
- Results storage
- Duplicate handling (ON CONFLICT)
"""

import pytest
from db.sim_repository import SimRepository


# Test database connection
CONN_STRING = "postgresql://samanthagrant@localhost/dealgenie_production"


def test_inputs_hash_deterministic():
    """Same inputs should produce same hash (deterministic)."""
    repo = SimRepository(CONN_STRING)

    envelope = {'far': 3.0, 'height': 75}
    priors = {'cost': {'mean': 425}}

    hash1 = repo.compute_inputs_hash(envelope, priors, 'multifamily')
    hash2 = repo.compute_inputs_hash(envelope, priors, 'multifamily')

    assert hash1 == hash2
    assert len(hash1) == 64  # SHA256 produces 64 hex characters


def test_inputs_hash_changes_with_input():
    """Different inputs should produce different hash (sensitivity)."""
    repo = SimRepository(CONN_STRING)

    envelope1 = {'far': 3.0}
    envelope2 = {'far': 3.1}
    priors = {'cost': {'mean': 425}}

    hash1 = repo.compute_inputs_hash(envelope1, priors, 'multifamily')
    hash2 = repo.compute_inputs_hash(envelope2, priors, 'multifamily')

    assert hash1 != hash2


def test_inputs_hash_ignores_key_order():
    """Different JSON key order should produce same hash (canonical)."""
    repo = SimRepository(CONN_STRING)

    # Same data, different key order
    envelope1 = {'far': 3.0, 'height': 75, 'units': 50}
    envelope2 = {'units': 50, 'height': 75, 'far': 3.0}
    priors = {'cost': {'mean': 425}}

    hash1 = repo.compute_inputs_hash(envelope1, priors, 'multifamily')
    hash2 = repo.compute_inputs_hash(envelope2, priors, 'multifamily')

    assert hash1 == hash2


def test_inputs_hash_changes_with_template():
    """Different template should produce different hash."""
    repo = SimRepository(CONN_STRING)

    envelope = {'far': 3.0}
    priors = {'cost': {'mean': 425}}

    hash1 = repo.compute_inputs_hash(envelope, priors, 'multifamily')
    hash2 = repo.compute_inputs_hash(envelope, priors, 'mixed_use')

    assert hash1 != hash2


def test_inputs_hash_changes_with_version():
    """Different sim_version should produce different hash."""
    repo = SimRepository(CONN_STRING)

    envelope = {'far': 3.0}
    priors = {'cost': {'mean': 425}}

    hash1 = repo.compute_inputs_hash(envelope, priors, 'multifamily', sim_version=1)
    hash2 = repo.compute_inputs_hash(envelope, priors, 'multifamily', sim_version=2)

    assert hash1 != hash2


def test_save_inputs_snapshot():
    """Should save inputs and return hash."""
    repo = SimRepository(CONN_STRING)

    envelope = {'far': 3.0, 'height': 75, 'units': 50}
    priors = {'cost': {'mean': 425, 'sigma': 76}}

    hash_val = repo.save_inputs_snapshot(
        'TEST_PYTEST_001',
        'multifamily',
        envelope,
        priors,
        sim_version=1
    )

    assert hash_val is not None
    assert len(hash_val) == 64

    # Verify we can retrieve it
    inputs = repo.get_inputs_snapshot('TEST_PYTEST_001', 'multifamily', hash_val)
    assert inputs is not None
    assert inputs['inputs_json']['envelope']['far'] == 3.0
    assert inputs['inputs_json']['priors']['cost']['mean'] == 425


def test_save_inputs_snapshot_duplicate():
    """Duplicate inputs should not create new row (ON CONFLICT DO NOTHING)."""
    repo = SimRepository(CONN_STRING)

    envelope = {'far': 3.0}
    priors = {'cost': {'mean': 425}}

    # Save twice with same inputs
    hash1 = repo.save_inputs_snapshot('TEST_PYTEST_002', 'multifamily', envelope, priors)
    hash2 = repo.save_inputs_snapshot('TEST_PYTEST_002', 'multifamily', envelope, priors)

    assert hash1 == hash2

    # Should still be retrievable
    inputs = repo.get_inputs_snapshot('TEST_PYTEST_002', 'multifamily', hash1)
    assert inputs is not None


def test_save_sim_summary():
    """Should save simulation results."""
    repo = SimRepository(CONN_STRING)

    # First, save inputs
    envelope = {'far': 3.0, 'height': 75}
    priors = {'cost': {'mean': 425}}
    inputs_hash = repo.save_inputs_snapshot('TEST_PYTEST_003', 'multifamily', envelope, priors)

    # Save results
    results = {
        'p5_irr': 0.08,
        'p50_irr': 0.15,
        'p95_irr': 0.22,
        'prob_hit_hurdle': 0.67,
        'var5_irr': 0.06,
        'es5_irr': 0.03
    }

    repo.save_sim_summary(
        'TEST_PYTEST_003',
        'multifamily',
        sim_version=1,
        results=results,
        inputs_hash=inputs_hash,
        runs=1000
    )

    # Retrieve and verify
    summary = repo.get_sim_summary('TEST_PYTEST_003', 'multifamily', 1)
    assert summary is not None
    assert summary['p50_irr'] == pytest.approx(0.15)
    assert summary['prob_hit_hurdle'] == pytest.approx(0.67)
    assert summary['runs'] == 1000


def test_save_sim_summary_update():
    """Should update existing results (ON CONFLICT DO UPDATE)."""
    repo = SimRepository(CONN_STRING)

    # First, save inputs
    envelope = {'far': 3.0}
    priors = {'cost': {'mean': 425}}
    inputs_hash = repo.save_inputs_snapshot('TEST_PYTEST_004', 'multifamily', envelope, priors)

    # Save initial results
    results1 = {
        'p5_irr': 0.08,
        'p50_irr': 0.15,
        'p95_irr': 0.22,
        'prob_hit_hurdle': 0.67,
        'var5_irr': 0.06,
        'es5_irr': 0.03
    }

    repo.save_sim_summary('TEST_PYTEST_004', 'multifamily', 1, results1, inputs_hash, 1000)

    # Save updated results (same apn, template, version)
    results2 = {
        'p5_irr': 0.09,
        'p50_irr': 0.16,
        'p95_irr': 0.23,
        'prob_hit_hurdle': 0.70,
        'var5_irr': 0.07,
        'es5_irr': 0.04
    }

    repo.save_sim_summary('TEST_PYTEST_004', 'multifamily', 1, results2, inputs_hash, 2000)

    # Should have updated results
    summary = repo.get_sim_summary('TEST_PYTEST_004', 'multifamily', 1)
    assert summary is not None
    assert summary['p50_irr'] == pytest.approx(0.16)  # Updated value
    assert summary['runs'] == 2000  # Updated value


def test_get_sim_summary_not_found():
    """Should return None for non-existent simulation."""
    repo = SimRepository(CONN_STRING)

    summary = repo.get_sim_summary('NONEXISTENT', 'multifamily', 1)
    assert summary is None


def test_get_inputs_snapshot_not_found():
    """Should return None for non-existent inputs."""
    repo = SimRepository(CONN_STRING)

    inputs = repo.get_inputs_snapshot('NONEXISTENT', 'multifamily', 'fakehash')
    assert inputs is None


# Cleanup tests
def test_cleanup():
    """Clean up test data (run last)."""
    import psycopg2

    conn = psycopg2.connect(
        dbname='dealgenie_production',
        user='samanthagrant',
        host='localhost'
    )
    cur = conn.cursor()

    # Delete test data
    cur.execute("DELETE FROM sim_summary WHERE apn LIKE 'TEST_PYTEST_%'")
    cur.execute("DELETE FROM sim_inputs_snapshot WHERE apn LIKE 'TEST_PYTEST_%'")

    conn.commit()
    cur.close()
    conn.close()

    print("✅ Test data cleaned up")
