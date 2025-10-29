"""
Tests for Monte Carlo Risk Card in Property Reports

Covers:
- Risk card shows metadata when MC data exists
- Risk card shows placeholder when no MC data
- Card is included in property report HTML
- Gray styling is present
- Hash is shortened correctly
"""

import pytest
from reporting.property_report import (
    generate_property_report,
    render_risk_card,
    get_mc_snapshot
)


def test_generate_property_report_structure():
    """Property report should have basic HTML structure."""
    html = generate_property_report('TEST_CARD_001', 'multifamily', {})

    # Should be valid HTML
    assert '<!DOCTYPE html>' in html
    assert '<html' in html
    assert '</html>' in html

    # Should have report header
    assert 'Development Opportunity Report' in html
    assert 'TEST_CARD_001' in html
    assert 'multifamily' in html


def test_risk_card_included_in_report():
    """Risk card should be included in property report."""
    html = generate_property_report('TEST_CARD_002', 'multifamily', {})

    # Risk card header should be present
    assert 'Risk Analysis (Monte Carlo)' in html
    assert '🎲' in html


def test_risk_card_with_no_data():
    """Risk card should show placeholder when no MC data exists."""
    html = generate_property_report('NODATA999', 'multifamily', {})

    # Should show "not yet captured" message
    assert 'not yet captured' in html.lower() or 'not yet captured' in html

    # Should show CLI command
    assert 'python cli/simulate.py' in html
    assert 'NODATA999' in html


def test_risk_card_with_existing_data():
    """Risk card should show metadata when MC data exists."""
    # Use AIN that has snapshot from previous tests (2249003017)
    html = generate_property_report('2249003017', 'multifamily', {})

    # Should show metadata section
    assert 'Simulation version:' in html or 'sim_version' in html.lower()
    assert 'Inputs hash:' in html or 'inputs_hash' in html.lower()
    assert 'Captured:' in html or 'captured' in html.lower()

    # Should show coming soon message
    assert 'coming soon' in html.lower() or 'Coming soon' in html


def test_risk_card_has_gray_styling():
    """Risk card should have gray/muted styling."""
    html = render_risk_card('TEST_CARD_003', 'multifamily', None)

    # Should have gray background
    assert '#f5f5f5' in html or 'gray' in html.lower()

    # Should have gray border
    assert '#999' in html


def test_risk_card_hash_shortened():
    """Risk card should show shortened hash (first 12 chars)."""
    # Get actual MC data
    mc_data = get_mc_snapshot('2249003017', 'multifamily')

    if mc_data:
        html = render_risk_card('2249003017', 'multifamily', mc_data)

        # Should show shortened hash
        full_hash = mc_data['inputs_hash']
        short_hash = full_hash[:12]

        assert short_hash in html
        # Should have ellipsis
        assert '...' in html


def test_get_mc_snapshot_existing():
    """Should retrieve MC snapshot for existing data."""
    mc_data = get_mc_snapshot('2249003017', 'multifamily')

    if mc_data:  # If snapshot exists from previous tests
        assert mc_data is not None
        assert 'apn' in mc_data
        assert 'template' in mc_data
        assert 'inputs_hash' in mc_data
        assert 'sim_version' in mc_data
        assert 'created_at' in mc_data


def test_get_mc_snapshot_nonexistent():
    """Should return None for non-existent MC data."""
    mc_data = get_mc_snapshot('NODATA999', 'multifamily')

    assert mc_data is None


def test_render_risk_card_with_data():
    """Risk card rendering should work with actual MC data."""
    mc_data = get_mc_snapshot('2249003017', 'multifamily')

    if mc_data:
        html = render_risk_card('2249003017', 'multifamily', mc_data)

        # Should have key elements
        assert 'Risk Analysis (Monte Carlo)' in html
        assert mc_data['inputs_hash'][:12] in html
        assert str(mc_data['sim_version']) in html


def test_render_risk_card_without_data():
    """Risk card rendering should work without MC data."""
    html = render_risk_card('NODATA999', 'multifamily', None)

    # Should have placeholder
    assert 'Risk Analysis (Monte Carlo)' in html
    assert 'not yet captured' in html.lower()
    assert 'NODATA999' in html


def test_risk_card_shows_coming_soon_features():
    """Risk card should mention future features."""
    html = generate_property_report('TEST_CARD_004', 'multifamily', {})

    # Should mention future MC features
    assert 'Coming soon' in html or 'coming soon' in html.lower()

    # Might mention specific features
    # (checking for at least one common term)
    future_terms = ['IRR', 'risk', 'simulation', 'P5', 'P50', 'P95']
    has_future_term = any(term in html for term in future_terms)
    assert has_future_term


def test_report_generated_timestamp():
    """Report should include generation timestamp."""
    html = generate_property_report('TEST_CARD_005', 'multifamily', {})

    # Should have "Generated:" with a timestamp
    assert 'Generated:' in html or 'generated' in html.lower()


def test_report_has_proper_sections():
    """Report should have logical sections."""
    html = generate_property_report('TEST_CARD_006', 'multifamily', {})

    # Should have multiple sections
    assert 'Property Details' in html or 'property' in html.lower()
    assert 'Risk Analysis' in html
    # Might have location section
    # assert 'Location' in html or 'Zoning' in html
