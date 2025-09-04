#!/usr/bin/env python3
"""
Database Manager for DealGenie - SQLite Integration
CodeRabbit: Please review this core database operations system
Manages the SQLite database for parcel data, scoring results, and feature caching.
"""

import sqlite3
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
import sys

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

class DealGenieDatabase:
    """
    Database manager for DealGenie SQLite database operations.
    
    Provides methods for:
    - Parcel data storage and retrieval
    - Scoring result persistence
    - Feature caching
    - Performance tracking
    """
    
    def __init__(self, db_path: str = "data/dealgenie.db"):
        """Initialize database manager."""
        self.db_path = db_path
        self.ensure_database_exists()
    
    def ensure_database_exists(self):
        """Ensure database exists and is properly initialized."""
        if not Path(self.db_path).exists():
            print(f"⚠️  Database not found at {self.db_path}")
            print("Run: sqlite3 data/dealgenie.db < db/sqlite_schema.sql")
            return False
        return True
    
    def get_connection(self) -> sqlite3.Connection:
        """Get database connection with proper configuration."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Enable dict-like access to rows
        
        # Enable foreign key constraints
        conn.execute("PRAGMA foreign_keys = ON")
        
        # Set journal mode for better performance
        conn.execute("PRAGMA journal_mode = WAL")
        
        return conn
    
    # ==============================================================================
    # PARCEL DATA OPERATIONS
    # ==============================================================================
    
    def store_parcel(self, apn: str, features: Dict[str, Any]) -> bool:
        """
        Store or update parcel data from feature matrix.
        
        Args:
            apn: Assessor Parcel Number
            features: Feature dictionary from get_feature_matrix()
            
        Returns:
            True if successful, False otherwise
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Extract relevant fields from features
            parcel_data = {
                'apn': apn,
                'address': features.get('site_address', ''),
                'city': features.get('site_city', 'Los Angeles'),
                'zip_code': features.get('site_zip', ''),
                'zoning': features.get('zoning', ''),
                'lot_size_sqft': features.get('lot_size_sqft', 0),
                'assessed_value': features.get('assessed_land_value', 0),
                # Add latitude/longitude if available from features
                'centroid_lat': features.get('latitude'),
                'centroid_lon': features.get('longitude'),
            }
            
            # Insert or replace parcel data
            cursor.execute('''
                INSERT OR REPLACE INTO parcels 
                (apn, address, city, zip_code, zoning, lot_size_sqft, assessed_value, 
                 centroid_lat, centroid_lon, data_source, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'CSV_Import', CURRENT_TIMESTAMP)
            ''', (
                parcel_data['apn'],
                parcel_data['address'],
                parcel_data['city'],
                parcel_data['zip_code'],
                parcel_data['zoning'],
                parcel_data['lot_size_sqft'],
                parcel_data['assessed_value'],
                parcel_data['centroid_lat'],
                parcel_data['centroid_lon']
            ))
            
            conn.commit()
            return True
            
        except sqlite3.Error as e:
            print(f"❌ Database error storing parcel {apn}: {e}")
            return False
        finally:
            conn.close()
    
    def get_parcel(self, apn: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve parcel data by APN.
        
        Args:
            apn: Assessor Parcel Number
            
        Returns:
            Dictionary of parcel data or None if not found
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM parcels WHERE apn = ?
            ''', (apn,))
            
            row = cursor.fetchone()
            return dict(row) if row else None
            
        except sqlite3.Error as e:
            print(f"❌ Database error retrieving parcel {apn}: {e}")
            return None
        finally:
            conn.close()
    
    # ==============================================================================
    # SCORING OPERATIONS
    # ==============================================================================
    
    def store_score(self, apn: str, template: str, score_result: Dict[str, Any], 
                   computation_time_ms: int = None, cache_hit: bool = False) -> bool:
        """
        Store scoring result in database.
        
        Args:
            apn: Assessor Parcel Number
            template: Development template
            score_result: Result from calculate_score()
            computation_time_ms: Time taken to compute score
            cache_hit: Whether result came from cache
            
        Returns:
            True if successful, False otherwise
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Determine grade from score
            score = score_result.get('score', 0)
            if score >= 8.0:
                grade = 'A'
            elif score >= 6.5:
                grade = 'B'
            elif score >= 5.0:
                grade = 'C'
            else:
                grade = 'D'
            
            # Extract component scores
            component_scores = score_result.get('component_scores', {})
            
            cursor.execute('''
                INSERT INTO parcel_scores 
                (apn, template, overall_score, grade, location_score, infrastructure_score,
                 zoning_score, market_score, development_score, financial_score,
                 scoring_algorithm, explanation, recommendations, computation_time_ms,
                 feature_cache_hit, scored_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                apn, template, score,  grade,
                component_scores.get('location', component_scores.get('demographics', 0)),
                component_scores.get('infrastructure', component_scores.get('transit', 0)),
                component_scores.get('zoning', 0),
                component_scores.get('market', 0),
                component_scores.get('development', component_scores.get('lot_size', 0)),
                component_scores.get('financial', 0),
                'DealGenie_v1.0',
                score_result.get('explanation', ''),
                json.dumps(score_result.get('recommendations', [])),
                computation_time_ms,
                1 if cache_hit else 0
            ))
            
            conn.commit()
            return True
            
        except sqlite3.Error as e:
            print(f"❌ Database error storing score for {apn}: {e}")
            return False
        finally:
            conn.close()
    
    def get_latest_score(self, apn: str, template: str = None) -> Optional[Dict[str, Any]]:
        """
        Get latest scoring result for an APN.
        
        Args:
            apn: Assessor Parcel Number
            template: Development template (optional)
            
        Returns:
            Dictionary of score data or None if not found
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if template:
                cursor.execute('''
                    SELECT * FROM parcel_scores 
                    WHERE apn = ? AND template = ?
                    ORDER BY scored_at DESC LIMIT 1
                ''', (apn, template))
            else:
                cursor.execute('''
                    SELECT * FROM parcel_scores 
                    WHERE apn = ?
                    ORDER BY scored_at DESC LIMIT 1
                ''', (apn,))
            
            row = cursor.fetchone()
            return dict(row) if row else None
            
        except sqlite3.Error as e:
            print(f"❌ Database error retrieving score for {apn}: {e}")
            return None
        finally:
            conn.close()
    
    # ==============================================================================
    # FEATURE CACHING OPERATIONS
    # ==============================================================================
    
    def cache_features(self, apn: str, template: str, features: Dict[str, Any],
                      demographics: Dict[str, Any] = None, expires_hours: int = 24) -> bool:
        """
        Cache computed features for an APN and template.
        
        Args:
            apn: Assessor Parcel Number
            template: Development template
            features: Feature dictionary
            demographics: Demographic data from Census API
            expires_hours: Cache expiration in hours
            
        Returns:
            True if successful, False otherwise
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            expires_at = datetime.now() + timedelta(hours=expires_hours)
            
            # Extract demographic data if provided
            median_income = None
            if demographics:
                median_income = demographics.get('median_household_income')
            
            cursor.execute('''
                INSERT OR REPLACE INTO feature_cache
                (apn, template, median_income, feature_vector, computed_at, expires_at, data_version)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, '1.0')
            ''', (
                apn, template, median_income,
                json.dumps(features),
                expires_at
            ))
            
            conn.commit()
            return True
            
        except sqlite3.Error as e:
            print(f"❌ Database error caching features for {apn}: {e}")
            return False
        finally:
            conn.close()
    
    def get_cached_features(self, apn: str, template: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached features if not expired.
        
        Args:
            apn: Assessor Parcel Number
            template: Development template
            
        Returns:
            Dictionary of cached features or None if not found/expired
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT feature_vector, computed_at, expires_at FROM feature_cache
                WHERE apn = ? AND template = ? 
                AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
                ORDER BY computed_at DESC LIMIT 1
            ''', (apn, template))
            
            row = cursor.fetchone()
            if row:
                try:
                    return json.loads(row['feature_vector'])
                except json.JSONDecodeError:
                    print(f"⚠️  Invalid JSON in cached features for {apn}")
            
            return None
            
        except sqlite3.Error as e:
            print(f"❌ Database error retrieving cached features for {apn}: {e}")
            return None
        finally:
            conn.close()
    
    # ==============================================================================
    # ANALYTICS AND REPORTING
    # ==============================================================================
    
    def get_scoring_statistics(self) -> Dict[str, Any]:
        """Get overall scoring statistics."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Total scores by template
            cursor.execute('''
                SELECT template, COUNT(*) as count, AVG(overall_score) as avg_score,
                       MIN(overall_score) as min_score, MAX(overall_score) as max_score
                FROM parcel_scores
                GROUP BY template
            ''')
            
            template_stats = {}
            for row in cursor.fetchall():
                template_stats[row['template']] = {
                    'count': row['count'],
                    'average_score': round(row['avg_score'], 2),
                    'min_score': row['min_score'],
                    'max_score': row['max_score']
                }
            
            # Overall statistics
            cursor.execute('''
                SELECT COUNT(*) as total_scores,
                       COUNT(DISTINCT apn) as unique_apns,
                       AVG(overall_score) as overall_avg,
                       AVG(computation_time_ms) as avg_computation_time
                FROM parcel_scores
            ''')
            
            overall_stats = dict(cursor.fetchone())
            
            return {
                'overall': overall_stats,
                'by_template': template_stats,
                'last_updated': datetime.now().isoformat()
            }
            
        except sqlite3.Error as e:
            print(f"❌ Database error getting statistics: {e}")
            return {}
        finally:
            conn.close()
    
    def get_high_value_opportunities(self, min_score: float = 7.0, 
                                   limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get highest-scoring development opportunities.
        
        Args:
            min_score: Minimum score threshold
            limit: Maximum number of results
            
        Returns:
            List of high-value opportunity records
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT p.apn, p.address, p.city, p.zoning, p.lot_size_sqft,
                       ps.overall_score, ps.template, ps.explanation, ps.scored_at
                FROM parcels p
                INNER JOIN parcel_scores ps ON p.apn = ps.apn
                WHERE ps.overall_score >= ?
                ORDER BY ps.overall_score DESC
                LIMIT ?
            ''', (min_score, limit))
            
            return [dict(row) for row in cursor.fetchall()]
            
        except sqlite3.Error as e:
            print(f"❌ Database error getting opportunities: {e}")
            return []
        finally:
            conn.close()
    
    # ==============================================================================
    # MAINTENANCE OPERATIONS
    # ==============================================================================
    
    def clean_expired_cache(self) -> int:
        """Remove expired cache entries."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                DELETE FROM feature_cache 
                WHERE expires_at IS NOT NULL AND expires_at < CURRENT_TIMESTAMP
            ''')
            
            deleted_count = cursor.rowcount
            conn.commit()
            
            print(f"✓ Cleaned {deleted_count} expired cache entries")
            return deleted_count
            
        except sqlite3.Error as e:
            print(f"❌ Database error cleaning cache: {e}")
            return 0
        finally:
            conn.close()
    
    def vacuum_database(self) -> bool:
        """Optimize database storage."""
        try:
            conn = self.get_connection()
            conn.execute("VACUUM")
            print("✓ Database vacuumed successfully")
            return True
            
        except sqlite3.Error as e:
            print(f"❌ Database error during vacuum: {e}")
            return False
        finally:
            conn.close()
    
    def get_database_info(self) -> Dict[str, Any]:
        """Get database information and statistics."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Table counts
            tables = ['parcels', 'parcel_scores', 'feature_cache', 'zoning_codes']
            table_counts = {}
            
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                table_counts[table] = cursor.fetchone()['count']
            
            # Database size
            cursor.execute("PRAGMA page_count")
            page_count = cursor.fetchone()[0]
            cursor.execute("PRAGMA page_size")
            page_size = cursor.fetchone()[0]
            db_size_bytes = page_count * page_size
            
            return {
                'database_path': self.db_path,
                'size_mb': round(db_size_bytes / 1024 / 1024, 2),
                'table_counts': table_counts,
                'schema_version': '1.0'
            }
            
        except sqlite3.Error as e:
            print(f"❌ Database error getting info: {e}")
            return {}
        finally:
            conn.close()


# Command-line interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="DealGenie Database Manager")
    parser.add_argument("command", choices=['info', 'stats', 'clean', 'vacuum', 'opportunities'],
                       help="Command to execute")
    parser.add_argument("--apn", help="APN for specific operations")
    parser.add_argument("--template", help="Development template")
    parser.add_argument("--min-score", type=float, default=7.0, help="Minimum score for opportunities")
    
    args = parser.parse_args()
    
    # Initialize database manager
    db = DealGenieDatabase()
    
    if args.command == "info":
        info = db.get_database_info()
        print("📊 DATABASE INFORMATION")
        print("=" * 50)
        print(f"Path: {info.get('database_path')}")
        print(f"Size: {info.get('size_mb', 0)} MB")
        print(f"Schema Version: {info.get('schema_version')}")
        print("\n📋 TABLE COUNTS:")
        for table, count in info.get('table_counts', {}).items():
            print(f"  {table}: {count:,} records")
    
    elif args.command == "stats":
        stats = db.get_scoring_statistics()
        print("📈 SCORING STATISTICS")
        print("=" * 50)
        overall = stats.get('overall', {})
        print(f"Total Scores: {overall.get('total_scores', 0):,}")
        print(f"Unique APNs: {overall.get('unique_apns', 0):,}")
        print(f"Average Score: {overall.get('overall_avg', 0):.2f}")
        print(f"Avg Computation Time: {overall.get('avg_computation_time', 0):.0f}ms")
        
        print("\n📊 BY TEMPLATE:")
        for template, data in stats.get('by_template', {}).items():
            print(f"  {template.title()}: {data['count']} scores, "
                  f"avg {data['average_score']:.1f} "
                  f"(range: {data['min_score']:.1f}-{data['max_score']:.1f})")
    
    elif args.command == "opportunities":
        opportunities = db.get_high_value_opportunities(min_score=args.min_score)
        print(f"🎯 HIGH-VALUE OPPORTUNITIES (Score >= {args.min_score})")
        print("=" * 80)
        
        for opp in opportunities:
            print(f"APN {opp['apn']}: {opp['overall_score']:.1f}/10 ({opp['template']})")
            print(f"  Address: {opp['address']}, {opp['city']}")
            print(f"  Zoning: {opp['zoning']}, Lot: {opp['lot_size_sqft']:,.0f} sqft")
            print()
    
    elif args.command == "clean":
        cleaned = db.clean_expired_cache()
        print(f"🧹 Cache cleanup completed: {cleaned} entries removed")
    
    elif args.command == "vacuum":
        success = db.vacuum_database()
        if success:
            print("✓ Database optimization completed")
        else:
            print("❌ Database optimization failed")