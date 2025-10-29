"""
PriorsAPI: Returns distribution parameters for Monte Carlo simulation inputs

Provides structured priors (probability distributions) for uncertain variables:
- Cost: hard_cost_psf, soft_cost_pct, contingency_pct
- Revenue: rent_psf_month, vacancy_pct, absorption_months
- Timeline: entitlement_months, construction_months
- Finance: interest_rate, cap_rate_exit, ltc

Each prior returns: {mean, sigma, dist, source}
where dist in ['lognormal', 'beta', 'pert', 'normal']

NOTE: Current values are placeholders based on LA market assumptions.
Will be grounded in real data in future iteration.
"""

from typing import Dict, Any


class PriorsAPI:
    """
    API for retrieving probability distribution priors for Monte Carlo simulations.

    Current implementation uses placeholder values based on:
    - LA County multifamily construction costs (~$400-450/sf)
    - LA rental market rates (~$3.50-4.00/sf/month)
    - Typical entitlement timelines (18-36 months)
    - Current market cap rates and financing terms

    Future: Will be replaced with data-driven priors from actual deals.
    """

    def __init__(self):
        """Initialize PriorsAPI with placeholder distributions."""
        pass

    def get_cost_priors(self, apn: str, template: str) -> Dict[str, Dict[str, Any]]:
        """
        Get cost-related distribution priors.

        Args:
            apn: Assessor Parcel Number
            template: Template name (e.g., 'multifamily', 'mixed_use')

        Returns:
            Dictionary with cost priors:
            - hard_cost_psf: Construction cost per square foot
            - soft_cost_pct: Soft costs as % of hard costs
            - contingency_pct: Contingency as % of hard costs

        Example:
            >>> api = PriorsAPI()
            >>> priors = api.get_cost_priors('TEST001', 'multifamily')
            >>> print(priors['hard_cost_psf']['mean'])
            425
        """
        # Placeholder values based on LA multifamily construction
        return {
            'hard_cost_psf': {
                'mean': 425,  # $/sf (typical for 4-6 story wood frame over podium)
                'sigma': 50,   # ~12% CV (coefficient of variation)
                'dist': 'lognormal',
                'source': 'placeholder_la_mf_construction'
            },
            'soft_cost_pct': {
                'mean': 0.25,  # 25% of hard costs (typical range: 20-30%)
                'sigma': 0.05,  # ~20% CV
                'dist': 'beta',
                'source': 'placeholder_typical_soft_costs'
            },
            'contingency_pct': {
                'mean': 0.05,  # 5% of hard costs
                'sigma': 0.02,  # Can vary 3-7%
                'dist': 'beta',
                'source': 'placeholder_contingency'
            }
        }

    def get_revenue_priors(self, apn: str, template: str) -> Dict[str, Dict[str, Any]]:
        """
        Get revenue-related distribution priors.

        Args:
            apn: Assessor Parcel Number
            template: Template name

        Returns:
            Dictionary with revenue priors:
            - rent_psf_month: Rental rate per sf per month
            - vacancy_pct: Stabilized vacancy rate
            - absorption_months: Lease-up duration

        Example:
            >>> api = PriorsAPI()
            >>> priors = api.get_revenue_priors('TEST001', 'multifamily')
            >>> print(priors['rent_psf_month']['mean'])
            3.75
        """
        # Placeholder values based on LA rental market
        return {
            'rent_psf_month': {
                'mean': 3.75,  # $/sf/month (typical LA multifamily: $3.50-4.00)
                'sigma': 0.40,  # ~11% CV
                'dist': 'lognormal',
                'source': 'placeholder_la_rental_market'
            },
            'vacancy_pct': {
                'mean': 0.05,  # 5% stabilized vacancy
                'sigma': 0.02,  # Range: 3-7%
                'dist': 'beta',
                'source': 'placeholder_typical_vacancy'
            },
            'absorption_months': {
                'min': 6,   # Optimistic: 6 months
                'mode': 12,  # Most likely: 12 months
                'max': 24,   # Pessimistic: 24 months
                'dist': 'pert',
                'source': 'placeholder_lease_up_timeline'
            }
        }

    def get_timeline_priors(self, apn: str, template: str) -> Dict[str, Dict[str, Any]]:
        """
        Get timeline-related distribution priors.

        Args:
            apn: Assessor Parcel Number
            template: Template name

        Returns:
            Dictionary with timeline priors:
            - entitlement_months: Time to get approvals
            - construction_months: Time to build

        Example:
            >>> api = PriorsAPI()
            >>> priors = api.get_timeline_priors('TEST001', 'multifamily')
            >>> print(priors['entitlement_months']['mode'])
            24
        """
        # Placeholder values based on LA entitlement experience
        return {
            'entitlement_months': {
                'min': 18,   # Optimistic: 1.5 years
                'mode': 24,  # Most likely: 2 years
                'max': 36,   # Pessimistic: 3 years
                'dist': 'pert',
                'source': 'placeholder_la_entitlement_duration'
            },
            'construction_months': {
                'min': 18,   # Fast-track: 1.5 years
                'mode': 24,  # Typical: 2 years
                'max': 30,   # Delayed: 2.5 years
                'dist': 'pert',
                'source': 'placeholder_construction_duration'
            }
        }

    def get_finance_priors(self, apn: str, template: str) -> Dict[str, Dict[str, Any]]:
        """
        Get finance-related distribution priors.

        Args:
            apn: Assessor Parcel Number
            template: Template name

        Returns:
            Dictionary with finance priors:
            - interest_rate: Construction loan rate (annual)
            - cap_rate_exit: Exit cap rate for sale
            - ltc: Loan-to-cost ratio

        Example:
            >>> api = PriorsAPI()
            >>> priors = api.get_finance_priors('TEST001', 'multifamily')
            >>> print(priors['cap_rate_exit']['mean'])
            0.045
        """
        # Placeholder values based on current market (2025)
        return {
            'interest_rate': {
                'mean': 0.08,  # 8% construction loan rate
                'sigma': 0.01,  # 100 bp variation
                'dist': 'normal',
                'source': 'placeholder_construction_financing'
            },
            'cap_rate_exit': {
                'mean': 0.045,  # 4.5% exit cap rate (LA multifamily)
                'sigma': 0.005,  # 50 bp variation
                'dist': 'normal',
                'source': 'placeholder_exit_cap_rate'
            },
            'ltc': {
                'mean': 0.65,  # 65% loan-to-cost
                'sigma': 0.05,  # Range: 60-70%
                'dist': 'beta',
                'source': 'placeholder_construction_loan_terms'
            }
        }

    def get_all_priors(self, apn: str, template: str) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """
        Get all priors for a development scenario.

        Args:
            apn: Assessor Parcel Number
            template: Template name

        Returns:
            Dictionary with all four prior categories:
            - cost: Cost-related priors
            - revenue: Revenue-related priors
            - timeline: Timeline-related priors
            - finance: Finance-related priors

        Example:
            >>> api = PriorsAPI()
            >>> priors = api.get_all_priors('TEST001', 'multifamily')
            >>> print(list(priors.keys()))
            ['cost', 'revenue', 'timeline', 'finance']
        """
        return {
            'cost': self.get_cost_priors(apn, template),
            'revenue': self.get_revenue_priors(apn, template),
            'timeline': self.get_timeline_priors(apn, template),
            'finance': self.get_finance_priors(apn, template)
        }

    def validate_prior(self, prior: Dict[str, Any]) -> bool:
        """
        Validate that a prior has required fields.

        Args:
            prior: Prior dictionary to validate

        Returns:
            True if valid, False otherwise

        Example:
            >>> api = PriorsAPI()
            >>> prior = {'mean': 100, 'sigma': 10, 'dist': 'normal', 'source': 'test'}
            >>> api.validate_prior(prior)
            True
        """
        # Check for required fields based on distribution type
        if 'dist' not in prior:
            return False

        dist_type = prior['dist']

        if dist_type == 'pert':
            # PERT requires min, mode, max
            return all(k in prior for k in ['min', 'mode', 'max', 'source'])
        else:
            # Other distributions require mean and sigma
            return all(k in prior for k in ['mean', 'sigma', 'source'])
