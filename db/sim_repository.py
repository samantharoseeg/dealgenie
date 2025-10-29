"""
SimRepository: Database access layer for Monte Carlo simulations

Provides:
- Deterministic input hashing (same inputs → same hash)
- Input snapshot storage (for reproducibility)
- Results storage (percentiles, probabilities, risk metrics)
- Foreign key enforcement (inputs must exist before results)
"""

from sqlalchemy import create_engine, text
from datetime import datetime
import hashlib
import json
from typing import Dict, Any, Optional


class SimRepository:
    """Repository for storing and retrieving Monte Carlo simulation data."""

    def __init__(self, connection_string: str):
        """
        Initialize repository with database connection.

        Args:
            connection_string: SQLAlchemy connection string
                e.g., 'postgresql://user@localhost/dbname'
        """
        self.engine = create_engine(connection_string)

    def compute_inputs_hash(
        self,
        envelope: Dict[str, Any],
        priors: Dict[str, Any],
        template: str,
        sim_version: int = 1
    ) -> str:
        """
        Compute deterministic SHA256 hash of simulation inputs.

        Key properties:
        - Same inputs → same hash (deterministic)
        - Different JSON key order → same hash (canonical ordering)
        - Any input change → different hash (sensitivity)

        Args:
            envelope: Development envelope (FAR, height, units, etc.)
            priors: Prior distributions (cost, rent, cap rate, etc.)
            template: Template name (e.g., 'multifamily', 'mixed_use')
            sim_version: Simulation version number

        Returns:
            64-character hex SHA256 hash

        Example:
            >>> repo = SimRepository(conn_str)
            >>> hash1 = repo.compute_inputs_hash({'far': 3.0}, {'cost': {'mean': 425}}, 'mf')
            >>> hash2 = repo.compute_inputs_hash({'far': 3.0}, {'cost': {'mean': 425}}, 'mf')
            >>> assert hash1 == hash2  # Deterministic
        """
        # Create canonical representation (sorted keys)
        canonical = json.dumps({
            'template': template,
            'sim_version': sim_version,
            'envelope': envelope,
            'priors': priors
        }, sort_keys=True)

        return hashlib.sha256(canonical.encode()).hexdigest()

    def save_inputs_snapshot(
        self,
        apn: str,
        template: str,
        envelope: Dict[str, Any],
        priors: Dict[str, Any],
        sim_version: int = 1
    ) -> str:
        """
        Save simulation inputs to database.

        Uses ON CONFLICT DO NOTHING to prevent duplicate storage
        of identical inputs.

        Args:
            apn: Assessor Parcel Number
            template: Template name
            envelope: Development envelope parameters
            priors: Prior distributions
            sim_version: Simulation version

        Returns:
            inputs_hash: SHA256 hash of inputs

        Example:
            >>> hash_val = repo.save_inputs_snapshot(
            ...     'TEST001',
            ...     'multifamily',
            ...     {'far': 3.0, 'height': 75, 'units': 50},
            ...     {'cost': {'mean': 425, 'sigma': 76}}
            ... )
            >>> print(f"Saved with hash: {hash_val}")
        """
        inputs_hash = self.compute_inputs_hash(envelope, priors, template, sim_version)
        inputs_json = {
            'envelope': envelope,
            'priors': priors,
            'template': template
        }

        with self.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO sim_inputs_snapshot (apn, template, inputs_hash, inputs_json, sim_version)
                VALUES (:apn, :template, :hash, CAST(:json AS jsonb), :version)
                ON CONFLICT (apn, template, inputs_hash) DO NOTHING
            """), {
                'apn': apn,
                'template': template,
                'hash': inputs_hash,
                'json': json.dumps(inputs_json),
                'version': sim_version
            })
            conn.commit()

        return inputs_hash

    def save_sim_summary(
        self,
        apn: str,
        template: str,
        sim_version: int,
        results: Dict[str, float],
        inputs_hash: str,
        runs: int
    ) -> None:
        """
        Save simulation results to database.

        Uses ON CONFLICT DO UPDATE to replace existing results
        for the same (apn, template, sim_version).

        Args:
            apn: Assessor Parcel Number
            template: Template name
            sim_version: Simulation version
            results: Dictionary with keys:
                - p5_irr: 5th percentile IRR
                - p50_irr: Median IRR
                - p95_irr: 95th percentile IRR
                - prob_hit_hurdle: Probability of hitting hurdle rate
                - var5_irr: Value at Risk at 5%
                - es5_irr: Expected Shortfall at 5%
            inputs_hash: Hash from save_inputs_snapshot()
            runs: Number of Monte Carlo scenarios run

        Example:
            >>> results = {
            ...     'p5_irr': 0.08,
            ...     'p50_irr': 0.15,
            ...     'p95_irr': 0.22,
            ...     'prob_hit_hurdle': 0.67,
            ...     'var5_irr': 0.06,
            ...     'es5_irr': 0.03
            ... }
            >>> repo.save_sim_summary('TEST001', 'multifamily', 1, results, hash_val, 1000)
        """
        with self.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO sim_summary (
                    apn, template, sim_version,
                    p5_irr, p50_irr, p95_irr,
                    prob_hit_hurdle, var5_irr, es5_irr,
                    inputs_hash, runs
                ) VALUES (
                    :apn, :template, :version,
                    :p5, :p50, :p95,
                    :prob, :var, :es,
                    :hash, :runs
                )
                ON CONFLICT (apn, template, sim_version)
                DO UPDATE SET
                    p5_irr = EXCLUDED.p5_irr,
                    p50_irr = EXCLUDED.p50_irr,
                    p95_irr = EXCLUDED.p95_irr,
                    prob_hit_hurdle = EXCLUDED.prob_hit_hurdle,
                    var5_irr = EXCLUDED.var5_irr,
                    es5_irr = EXCLUDED.es5_irr,
                    inputs_hash = EXCLUDED.inputs_hash,
                    runs = EXCLUDED.runs,
                    generated_at = NOW()
            """), {
                'apn': apn,
                'template': template,
                'version': sim_version,
                'p5': results['p5_irr'],
                'p50': results['p50_irr'],
                'p95': results['p95_irr'],
                'prob': results['prob_hit_hurdle'],
                'var': results['var5_irr'],
                'es': results['es5_irr'],
                'hash': inputs_hash,
                'runs': runs
            })
            conn.commit()

    def get_sim_summary(
        self,
        apn: str,
        template: str,
        sim_version: int
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve simulation results for a parcel.

        Args:
            apn: Assessor Parcel Number
            template: Template name
            sim_version: Simulation version

        Returns:
            Dictionary with results or None if not found

        Example:
            >>> results = repo.get_sim_summary('TEST001', 'multifamily', 1)
            >>> print(f"Median IRR: {results['p50_irr']:.2%}")
        """
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT apn, template, sim_version,
                       p5_irr, p50_irr, p95_irr,
                       prob_hit_hurdle, var5_irr, es5_irr,
                       inputs_hash, runs, generated_at
                FROM sim_summary
                WHERE apn = :apn
                  AND template = :template
                  AND sim_version = :version
                LIMIT 1
            """), {
                'apn': apn,
                'template': template,
                'version': sim_version
            })

            row = result.fetchone()
            if not row:
                return None

            return {
                'apn': row[0],
                'template': row[1],
                'sim_version': row[2],
                'p5_irr': row[3],
                'p50_irr': row[4],
                'p95_irr': row[5],
                'prob_hit_hurdle': row[6],
                'var5_irr': row[7],
                'es5_irr': row[8],
                'inputs_hash': row[9],
                'runs': row[10],
                'generated_at': row[11]
            }

    def get_inputs_snapshot(
        self,
        apn: str,
        template: str,
        inputs_hash: str
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve simulation inputs by hash.

        Args:
            apn: Assessor Parcel Number
            template: Template name
            inputs_hash: SHA256 hash of inputs

        Returns:
            Dictionary with inputs or None if not found

        Example:
            >>> inputs = repo.get_inputs_snapshot('TEST001', 'multifamily', hash_val)
            >>> print(inputs['inputs_json']['envelope']['far'])
        """
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT apn, template, inputs_hash, inputs_json, sim_version, created_at
                FROM sim_inputs_snapshot
                WHERE apn = :apn
                  AND template = :template
                  AND inputs_hash = :hash
                LIMIT 1
            """), {
                'apn': apn,
                'template': template,
                'hash': inputs_hash
            })

            row = result.fetchone()
            if not row:
                return None

            return {
                'apn': row[0],
                'template': row[1],
                'inputs_hash': row[2],
                'inputs_json': row[3],
                'sim_version': row[4],
                'created_at': row[5]
            }
