#!/usr/bin/env python3
"""
CLI command for Monte Carlo simulation

Usage:
    python cli/simulate.py --apn <APN> --template <template>

Example:
    python cli/simulate.py --apn 2249003017 --template multifamily

When MONTE_CARLO_ENABLED=false:
- Captures inputs
- Computes hash
- Saves snapshot
- Shows "MC disabled" message

When MONTE_CARLO_ENABLED=true (future):
- Runs actual simulation
- Saves results
"""

import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import click
import psycopg2
from typing import Dict, Any, Optional
from config.settings import get_settings
from db.sim_repository import SimRepository
from scoring.priors import PriorsAPI


def load_envelope_from_db(apn: str) -> Optional[Dict[str, Any]]:
    """
    Load envelope data from underutilization_scores table.

    Args:
        apn: Assessor Parcel Number (AIN in database)

    Returns:
        Dictionary with envelope data or None if not found
    """
    settings = get_settings()

    try:
        conn = psycopg2.connect(settings.DATABASE_URL)
        cur = conn.cursor()

        cur.execute("""
            SELECT ain, zone_code, lot_size, building_sqft,
                   existing_far, allowed_far, far_gap,
                   year_built, land_value, imp_value,
                   lon, lat
            FROM underutilization_scores
            WHERE ain = %s
            LIMIT 1;
        """, (apn,))

        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            return None

        return {
            'apn': row[0],
            'zone_code': row[1],
            'lot_size_sqft': row[2],
            'building_sqft': row[3],
            'existing_far': row[4],
            'allowed_far': row[5],
            'far_gap': row[6],
            'year_built': row[7],
            'land_value': row[8],
            'imp_value': row[9],
            'lon': row[10],
            'lat': row[11]
        }
    except psycopg2.Error as e:
        click.echo(f"❌ Database error: {e}", err=True)
        return None
    except Exception as e:
        click.echo(f"❌ Error loading envelope: {e}", err=True)
        return None


@click.command()
@click.option('--apn', required=True, help='Assessor Parcel Number (AIN)')
@click.option('--template', required=True, help='Development template (e.g., multifamily, mixed_use)')
@click.option('--runs', default=None, type=int, help='Number of MC scenarios (default: from settings)')
@click.option('--seed', default=None, type=int, help='Random seed for reproducibility (default: from settings)')
def simulate(apn: str, template: str, runs: Optional[int], seed: Optional[int]):
    """
    Run Monte Carlo simulation for a development opportunity.

    When MONTE_CARLO_ENABLED=false (default):
    - Loads envelope and priors
    - Computes deterministic hash
    - Saves input snapshot
    - Shows "MC disabled" message

    When MONTE_CARLO_ENABLED=true (future):
    - Runs actual simulation
    - Saves results to database
    - Shows percentiles and risk metrics

    Example:
        python cli/simulate.py --apn 2249003017 --template multifamily
    """
    settings = get_settings()

    click.echo(f"🎲 {settings.APP_NAME} Monte Carlo Simulator")
    click.echo(f"{'='*60}")
    click.echo()

    # Load envelope from database
    click.echo(f"📊 Loading envelope data for APN: {apn}")
    envelope = load_envelope_from_db(apn)

    if envelope is None:
        click.echo(f"❌ APN {apn} not found in underutilization_scores table", err=True)
        click.echo(f"   Tip: Check if APN exists in database", err=True)
        return

    click.echo(f"   ✅ Found parcel:")
    click.echo(f"      Zone: {envelope['zone_code']}")
    click.echo(f"      Lot size: {envelope['lot_size_sqft']:,.0f} sqft")
    click.echo(f"      Existing FAR: {envelope['existing_far']:.2f}")
    click.echo(f"      Allowed FAR: {envelope['allowed_far']:.2f}")
    click.echo()

    # Get priors from PriorsAPI
    click.echo(f"📈 Loading priors for template: {template}")
    priors_api = PriorsAPI()
    priors = priors_api.get_all_priors(apn, template)

    click.echo(f"   ✅ Priors loaded:")
    click.echo(f"      Cost params: {len(priors['cost'])}")
    click.echo(f"      Revenue params: {len(priors['revenue'])}")
    click.echo(f"      Timeline params: {len(priors['timeline'])}")
    click.echo(f"      Finance params: {len(priors['finance'])}")
    click.echo()

    # Compute inputs hash
    click.echo(f"🔒 Computing inputs hash...")
    repo = SimRepository(settings.DATABASE_URL)
    inputs_hash = repo.compute_inputs_hash(envelope, priors, template, settings.MC_SIM_VERSION)

    click.echo(f"   ✅ Hash: {inputs_hash[:12]}... (deterministic)")
    click.echo()

    # Save snapshot if enabled
    if settings.CAPTURE_SIM_INPUTS:
        click.echo(f"💾 Saving inputs snapshot...")
        try:
            repo.save_inputs_snapshot(apn, template, envelope, priors, settings.MC_SIM_VERSION)
            click.echo(f"   ✅ Snapshot saved to sim_inputs_snapshot")
            click.echo(f"      Primary key: (apn={apn}, template={template}, hash={inputs_hash[:12]}...)")
        except Exception as e:
            click.echo(f"   ⚠️  Snapshot save failed: {e}", err=True)
        click.echo()

    # Check feature flag
    if not settings.MONTE_CARLO_ENABLED:
        click.echo(f"⏸️  Monte Carlo simulation DISABLED")
        click.echo(f"   Set MONTE_CARLO_ENABLED=true in .env to run simulations")
        click.echo(f"   Inputs captured for reproducibility (hash: {inputs_hash[:12]}...)")
        click.echo()
        click.echo(f"📋 Configuration:")
        click.echo(f"   Default runs: {settings.MC_DEFAULT_RUNS}")
        click.echo(f"   Default seed: {settings.MC_DEFAULT_SEED}")
        click.echo(f"   Sim version: {settings.MC_SIM_VERSION}")
        return

    # Future: Run actual simulation when flag enabled
    click.echo(f"🎲 Running Monte Carlo simulation...")
    mc_runs = runs if runs is not None else settings.MC_DEFAULT_RUNS
    mc_seed = seed if seed is not None else settings.MC_DEFAULT_SEED

    click.echo(f"   Scenarios: {mc_runs}")
    click.echo(f"   Seed: {mc_seed}")
    click.echo(f"   Version: {settings.MC_SIM_VERSION}")
    click.echo()

    # TODO: Implement actual simulation
    click.echo(f"   ⚠️  Simulation engine not yet implemented")
    click.echo(f"   Future: Will compute NPV/IRR distributions")
    click.echo(f"   Future: Will save results to sim_summary")


if __name__ == '__main__':
    simulate()
