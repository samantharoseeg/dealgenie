"""
Tests for PriorsAPI

Covers:
- All priors return required fields (mean/sigma or min/mode/max)
- Distribution types are valid
- get_all_priors returns all categories
- Priors are stable/deterministic for testing
- Validation logic works
"""

import pytest
from scoring.priors import PriorsAPI


def test_priors_api_initialization():
    """API should initialize without errors."""
    api = PriorsAPI()
    assert api is not None


def test_get_cost_priors_structure():
    """Cost priors should have expected structure."""
    api = PriorsAPI()
    priors = api.get_cost_priors('TEST001', 'multifamily')

    # Should have three cost parameters
    assert 'hard_cost_psf' in priors
    assert 'soft_cost_pct' in priors
    assert 'contingency_pct' in priors

    # Each should have required fields
    for param_name, param in priors.items():
        assert 'mean' in param or 'min' in param  # Either mean or min/mode/max
        assert 'dist' in param
        assert 'source' in param


def test_get_revenue_priors_structure():
    """Revenue priors should have expected structure."""
    api = PriorsAPI()
    priors = api.get_revenue_priors('TEST001', 'multifamily')

    # Should have three revenue parameters
    assert 'rent_psf_month' in priors
    assert 'vacancy_pct' in priors
    assert 'absorption_months' in priors

    # Each should have required fields
    for param_name, param in priors.items():
        assert 'dist' in param
        assert 'source' in param


def test_get_timeline_priors_structure():
    """Timeline priors should have expected structure."""
    api = PriorsAPI()
    priors = api.get_timeline_priors('TEST001', 'multifamily')

    # Should have timeline parameters
    assert 'entitlement_months' in priors
    assert 'construction_months' in priors

    # PERT distributions should have min/mode/max
    for param_name, param in priors.items():
        assert param['dist'] == 'pert'
        assert 'min' in param
        assert 'mode' in param
        assert 'max' in param
        assert 'source' in param


def test_get_finance_priors_structure():
    """Finance priors should have expected structure."""
    api = PriorsAPI()
    priors = api.get_finance_priors('TEST001', 'multifamily')

    # Should have finance parameters
    assert 'interest_rate' in priors
    assert 'cap_rate_exit' in priors
    assert 'ltc' in priors

    # Each should have required fields
    for param_name, param in priors.items():
        assert 'dist' in param
        assert 'source' in param


def test_get_all_priors_categories():
    """get_all_priors should return all four categories."""
    api = PriorsAPI()
    priors = api.get_all_priors('TEST001', 'multifamily')

    # Should have all four categories
    assert 'cost' in priors
    assert 'revenue' in priors
    assert 'timeline' in priors
    assert 'finance' in priors

    # Each category should be a dict
    assert isinstance(priors['cost'], dict)
    assert isinstance(priors['revenue'], dict)
    assert isinstance(priors['timeline'], dict)
    assert isinstance(priors['finance'], dict)


def test_priors_have_valid_distribution_types():
    """All priors should have valid distribution types."""
    api = PriorsAPI()
    priors = api.get_all_priors('TEST001', 'multifamily')

    valid_dists = ['lognormal', 'beta', 'pert', 'normal']

    for category_name, category in priors.items():
        for param_name, param in category.items():
            assert param['dist'] in valid_dists, \
                f"{category_name}.{param_name} has invalid dist: {param['dist']}"


def test_priors_are_deterministic():
    """Same inputs should produce same priors (for testing)."""
    api = PriorsAPI()

    priors1 = api.get_all_priors('TEST001', 'multifamily')
    priors2 = api.get_all_priors('TEST001', 'multifamily')

    # Should be identical
    assert priors1 == priors2


def test_cost_priors_values_reasonable():
    """Cost priors should have reasonable values."""
    api = PriorsAPI()
    priors = api.get_cost_priors('TEST001', 'multifamily')

    # Hard cost should be positive
    assert priors['hard_cost_psf']['mean'] > 0
    assert priors['hard_cost_psf']['sigma'] > 0

    # Soft cost pct should be between 0 and 1
    assert 0 < priors['soft_cost_pct']['mean'] < 1
    assert priors['soft_cost_pct']['dist'] == 'beta'

    # Contingency should be between 0 and 1
    assert 0 < priors['contingency_pct']['mean'] < 1


def test_revenue_priors_values_reasonable():
    """Revenue priors should have reasonable values."""
    api = PriorsAPI()
    priors = api.get_revenue_priors('TEST001', 'multifamily')

    # Rent should be positive
    assert priors['rent_psf_month']['mean'] > 0

    # Vacancy should be between 0 and 1
    assert 0 < priors['vacancy_pct']['mean'] < 1

    # Absorption months should be positive
    assert priors['absorption_months']['min'] > 0
    assert priors['absorption_months']['mode'] > priors['absorption_months']['min']
    assert priors['absorption_months']['max'] > priors['absorption_months']['mode']


def test_timeline_priors_values_reasonable():
    """Timeline priors should have reasonable values."""
    api = PriorsAPI()
    priors = api.get_timeline_priors('TEST001', 'multifamily')

    # Entitlement timeline should be ordered: min < mode < max
    assert priors['entitlement_months']['min'] > 0
    assert priors['entitlement_months']['mode'] > priors['entitlement_months']['min']
    assert priors['entitlement_months']['max'] > priors['entitlement_months']['mode']

    # Construction timeline should be ordered
    assert priors['construction_months']['min'] > 0
    assert priors['construction_months']['mode'] > priors['construction_months']['min']
    assert priors['construction_months']['max'] > priors['construction_months']['mode']


def test_finance_priors_values_reasonable():
    """Finance priors should have reasonable values."""
    api = PriorsAPI()
    priors = api.get_finance_priors('TEST001', 'multifamily')

    # Interest rate should be positive but reasonable
    assert 0 < priors['interest_rate']['mean'] < 0.20  # Less than 20%

    # Cap rate should be positive but reasonable
    assert 0 < priors['cap_rate_exit']['mean'] < 0.15  # Less than 15%

    # LTC should be between 0 and 1
    assert 0 < priors['ltc']['mean'] < 1


def test_validate_prior_normal():
    """Validation should pass for valid normal distribution."""
    api = PriorsAPI()
    prior = {
        'mean': 100,
        'sigma': 10,
        'dist': 'normal',
        'source': 'test'
    }
    assert api.validate_prior(prior) is True


def test_validate_prior_pert():
    """Validation should pass for valid PERT distribution."""
    api = PriorsAPI()
    prior = {
        'min': 10,
        'mode': 20,
        'max': 30,
        'dist': 'pert',
        'source': 'test'
    }
    assert api.validate_prior(prior) is True


def test_validate_prior_missing_fields():
    """Validation should fail for missing required fields."""
    api = PriorsAPI()

    # Missing sigma
    prior = {
        'mean': 100,
        'dist': 'normal',
        'source': 'test'
    }
    assert api.validate_prior(prior) is False

    # Missing dist
    prior = {
        'mean': 100,
        'sigma': 10,
        'source': 'test'
    }
    assert api.validate_prior(prior) is False


def test_validate_prior_pert_missing_fields():
    """Validation should fail for PERT missing min/mode/max."""
    api = PriorsAPI()

    # Missing max
    prior = {
        'min': 10,
        'mode': 20,
        'dist': 'pert',
        'source': 'test'
    }
    assert api.validate_prior(prior) is False


def test_all_priors_pass_validation():
    """All returned priors should pass validation."""
    api = PriorsAPI()
    priors = api.get_all_priors('TEST001', 'multifamily')

    for category_name, category in priors.items():
        for param_name, param in category.items():
            assert api.validate_prior(param), \
                f"{category_name}.{param_name} failed validation"


def test_hard_cost_distribution_type():
    """Hard cost should use lognormal (always positive, right-skewed)."""
    api = PriorsAPI()
    priors = api.get_cost_priors('TEST001', 'multifamily')
    assert priors['hard_cost_psf']['dist'] == 'lognormal'


def test_percentage_priors_use_beta():
    """Percentage-based priors should use beta distribution."""
    api = PriorsAPI()

    cost_priors = api.get_cost_priors('TEST001', 'multifamily')
    assert cost_priors['soft_cost_pct']['dist'] == 'beta'
    assert cost_priors['contingency_pct']['dist'] == 'beta'

    revenue_priors = api.get_revenue_priors('TEST001', 'multifamily')
    assert revenue_priors['vacancy_pct']['dist'] == 'beta'

    finance_priors = api.get_finance_priors('TEST001', 'multifamily')
    assert finance_priors['ltc']['dist'] == 'beta'


def test_timeline_priors_use_pert():
    """Timeline priors should use PERT distribution (min/mode/max)."""
    api = PriorsAPI()
    timeline_priors = api.get_timeline_priors('TEST001', 'multifamily')

    assert timeline_priors['entitlement_months']['dist'] == 'pert'
    assert timeline_priors['construction_months']['dist'] == 'pert'

    revenue_priors = api.get_revenue_priors('TEST001', 'multifamily')
    assert revenue_priors['absorption_months']['dist'] == 'pert'


def test_priors_have_source_attribution():
    """All priors should have source attribution."""
    api = PriorsAPI()
    priors = api.get_all_priors('TEST001', 'multifamily')

    for category_name, category in priors.items():
        for param_name, param in category.items():
            assert 'source' in param
            assert isinstance(param['source'], str)
            assert len(param['source']) > 0
