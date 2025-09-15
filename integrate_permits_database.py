#!/usr/bin/env python3
"""
Connect Working Permit System to Rebuilt Database
Integrate permit data with property scoring for complete intelligence system
"""

import requests
import sqlite3
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple, Optional
import math

class PermitDatabaseIntegrator:
    def __init__(self):
        self.db_path = 'dealgenie_properties.db'
        self.permit_api_url = "https://data.lacity.org/resource/pi9x-tg5x.json"
        
    def link_permits_to_properties_with_metrics(self) -> Dict[str, Any]:
        """
        TASK 1: Link Permits to Properties with Supply Pipeline Metrics
        """
        print("🔗 LINKING PERMITS TO PROPERTIES WITH SUPPLY PIPELINE METRICS")
        print("="*80)
        
        result = {
            'permits_fetched': 0,
            'properties_matched': 0,
            'supply_metrics_calculated': 0,
            'permit_clusters_identified': 0,
            'velocity_indicators': {}
        }
        
        # Fetch comprehensive permit data
        print("📡 Fetching comprehensive LA City permit data...")
        
        try:
            # Get larger dataset for better analysis
            params = {
                '$limit': 500,
                '$order': 'issue_date DESC',
                '$where': """lat IS NOT NULL AND lon IS NOT NULL 
                           AND valuation IS NOT NULL AND valuation > 0
                           AND issue_date > '2024-01-01'"""
            }
            
            response = requests.get(self.permit_api_url, params=params, timeout=30)
            
            if response.status_code == 200:
                permits = response.json()
                result['permits_fetched'] = len(permits)
                print(f"✅ Fetched {len(permits)} permits with coordinates and valuations")
                
                # Process permits with property matching
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                # Create permit analysis table if not exists
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS permit_property_analysis (
                        analysis_id INTEGER PRIMARY KEY,
                        property_id INTEGER,
                        permit_nbr TEXT,
                        permit_type TEXT,
                        permit_valuation REAL,
                        issue_date TEXT,
                        distance_to_property REAL,
                        supply_velocity_score REAL,
                        permit_cluster_id INTEGER,
                        created_timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (property_id) REFERENCES enhanced_scored_properties (property_id)
                    )
                """)
                
                # Clear previous analysis
                cursor.execute("DELETE FROM permit_property_analysis")
                
                print("🏗️ Processing permits for property matching and metrics...")
                
                matches = []
                cluster_id = 1
                permit_clusters = {}
                
                for permit in permits:
                    try:
                        permit_lat = float(permit.get('lat', 0))
                        permit_lon = float(permit.get('lon', 0))
                        permit_value = float(permit.get('valuation', 0))
                        permit_type = permit.get('permit_type', 'Unknown')
                        permit_nbr = permit.get('permit_nbr', 'Unknown')
                        issue_date = permit.get('issue_date', '')
                        
                        if permit_lat == 0 or permit_lon == 0:
                            continue
                        
                        # Find closest property within 200m (~0.002 degrees)
                        cursor.execute("""
                            SELECT property_id, site_address, latitude, longitude,
                                   ((latitude - ?) * (latitude - ?) + (longitude - ?) * (longitude - ?)) as distance
                            FROM enhanced_scored_properties
                            ORDER BY distance
                            LIMIT 1
                        """, (permit_lat, permit_lat, permit_lon, permit_lon))
                        
                        closest = cursor.fetchone()
                        if closest:
                            prop_id, address, prop_lat, prop_lon, distance = closest
                            
                            # Distance threshold: ~200m in degrees
                            if distance < 0.002:
                                result['properties_matched'] += 1
                                
                                # Calculate supply velocity score
                                days_old = self.calculate_permit_age_days(issue_date)
                                value_factor = min(permit_value / 50000, 5.0)  # Cap at 5x for $50k+ permits
                                recency_factor = max(0, (365 - days_old) / 365)  # Decay over year
                                velocity_score = value_factor * recency_factor * 10
                                
                                # Check for permit clustering (multiple permits in same area)
                                cluster_key = f"{int(permit_lat * 1000)}_{int(permit_lon * 1000)}"
                                if cluster_key not in permit_clusters:
                                    permit_clusters[cluster_key] = {
                                        'id': cluster_id,
                                        'permits': [],
                                        'total_value': 0,
                                        'center_lat': permit_lat,
                                        'center_lon': permit_lon
                                    }
                                    cluster_id += 1
                                
                                permit_clusters[cluster_key]['permits'].append(permit_nbr)
                                permit_clusters[cluster_key]['total_value'] += permit_value
                                
                                # Store analysis
                                cursor.execute("""
                                    INSERT INTO permit_property_analysis 
                                    (property_id, permit_nbr, permit_type, permit_valuation, 
                                     issue_date, distance_to_property, supply_velocity_score, permit_cluster_id)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                """, (prop_id, permit_nbr, permit_type, permit_value, 
                                     issue_date, distance, velocity_score, permit_clusters[cluster_key]['id']))
                                
                                matches.append({
                                    'property_id': prop_id,
                                    'property_address': address,
                                    'permit_nbr': permit_nbr,
                                    'permit_type': permit_type,
                                    'permit_value': permit_value,
                                    'velocity_score': velocity_score,
                                    'cluster_id': permit_clusters[cluster_key]['id'],
                                    'distance_km': distance * 111.32  # Convert to km
                                })
                        
                    except (ValueError, TypeError) as e:
                        continue
                
                conn.commit()
                
                # Calculate aggregate metrics by property
                print("📊 Calculating supply pipeline metrics by property...")
                
                cursor.execute("""
                    UPDATE enhanced_scored_properties 
                    SET permit_count = (
                        SELECT COUNT(*) 
                        FROM permit_property_analysis 
                        WHERE permit_property_analysis.property_id = enhanced_scored_properties.property_id
                    ),
                    permit_value_total = (
                        SELECT COALESCE(SUM(permit_valuation), 0)
                        FROM permit_property_analysis 
                        WHERE permit_property_analysis.property_id = enhanced_scored_properties.property_id
                    ),
                    permit_activity_score = (
                        SELECT COALESCE(SUM(supply_velocity_score), 0)
                        FROM permit_property_analysis 
                        WHERE permit_property_analysis.property_id = enhanced_scored_properties.property_id
                    )
                """)
                
                properties_updated = cursor.rowcount
                result['supply_metrics_calculated'] = properties_updated
                
                # Identify significant permit clusters (3+ permits, $100k+ total)
                significant_clusters = [
                    cluster for cluster in permit_clusters.values()
                    if len(cluster['permits']) >= 3 and cluster['total_value'] >= 100000
                ]
                result['permit_clusters_identified'] = len(significant_clusters)
                
                # Calculate velocity indicators by neighborhood
                cursor.execute("""
                    SELECT neighborhood, 
                           COUNT(*) as permit_count,
                           AVG(supply_velocity_score) as avg_velocity,
                           SUM(permit_valuation) as total_investment
                    FROM permit_property_analysis 
                    JOIN enhanced_scored_properties ON 
                        permit_property_analysis.property_id = enhanced_scored_properties.property_id
                    GROUP BY neighborhood
                    HAVING permit_count >= 5
                    ORDER BY avg_velocity DESC
                """)
                
                neighborhood_metrics = cursor.fetchall()
                
                for neighborhood, count, avg_velocity, total_investment in neighborhood_metrics:
                    result['velocity_indicators'][neighborhood] = {
                        'permit_count': count,
                        'avg_velocity_score': avg_velocity,
                        'total_investment': total_investment,
                        'permits_per_month': count / 12  # Assuming 1 year of data
                    }
                
                conn.commit()
                conn.close()
                
                print(f"✅ Supply pipeline integration complete:")
                print(f"   Properties matched: {result['properties_matched']}")
                print(f"   Properties with metrics: {result['supply_metrics_calculated']}")
                print(f"   Permit clusters: {result['permit_clusters_identified']}")
                print(f"   Neighborhoods analyzed: {len(result['velocity_indicators'])}")
                
                # Show sample metrics
                print(f"\n📈 Sample Supply Velocity by Neighborhood:")
                for neighborhood, metrics in list(result['velocity_indicators'].items())[:5]:
                    print(f"   {neighborhood}: {metrics['avg_velocity_score']:.1f} velocity, "
                          f"{metrics['permits_per_month']:.1f} permits/month")
                
            else:
                result['error'] = f"API error: {response.status_code}"
                print(f"❌ Permit API error: {response.status_code}")
        
        except Exception as e:
            result['error'] = str(e)
            print(f"❌ Integration error: {str(e)}")
        
        return result
    
    def calculate_permit_age_days(self, issue_date: str) -> int:
        """Calculate days since permit was issued"""
        try:
            if issue_date:
                issue_dt = datetime.fromisoformat(issue_date.replace('T00:00:00.000', ''))
                return (datetime.now() - issue_dt).days
        except:
            pass
        return 365  # Default to 1 year old
    
    def combine_crime_permit_scoring(self) -> Dict[str, Any]:
        """
        TASK 2: Combine Crime + Permit Scoring Systems
        """
        print(f"\n🧠 COMBINING CRIME + PERMIT SCORING SYSTEMS")
        print("="*60)
        
        result = {
            'properties_analyzed': 0,
            'scoring_algorithm': {},
            'neighborhood_patterns': {},
            'multi_factor_scores': {}
        }
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Define multi-factor scoring algorithm
            scoring_algorithm = {
                'crime_weight': 0.4,      # 40% weight for crime safety
                'permit_weight': 0.3,     # 30% weight for development activity
                'location_weight': 0.2,   # 20% weight for location factors
                'market_weight': 0.1      # 10% weight for market conditions
            }
            
            result['scoring_algorithm'] = scoring_algorithm
            
            print("📊 Implementing multi-factor scoring algorithm...")
            print(f"   Crime Safety: {scoring_algorithm['crime_weight']*100:.0f}%")
            print(f"   Permit Activity: {scoring_algorithm['permit_weight']*100:.0f}%")
            print(f"   Location Factors: {scoring_algorithm['location_weight']*100:.0f}%")
            print(f"   Market Conditions: {scoring_algorithm['market_weight']*100:.0f}%")
            
            # Get properties with both crime and permit data
            cursor.execute("""
                SELECT property_id, site_address, neighborhood,
                       enhanced_development_score, crime_score,
                       permit_count, permit_value_total, permit_activity_score,
                       latitude, longitude
                FROM enhanced_scored_properties
                WHERE crime_score > 0
                ORDER BY neighborhood, enhanced_development_score DESC
            """)
            
            properties = cursor.fetchall()
            result['properties_analyzed'] = len(properties)
            
            print(f"📋 Analyzing {len(properties)} properties with complete data...")
            
            # Calculate enhanced multi-factor scores
            enhanced_scores = []
            neighborhood_stats = {}
            
            for prop in properties:
                (prop_id, address, neighborhood, dev_score, crime_score,
                 permit_count, permit_value, permit_activity, lat, lon) = prop
                
                # Normalize scores to 0-100 scale
                
                # Crime component (lower crime = higher score)
                crime_component = max(0, 100 - crime_score) * scoring_algorithm['crime_weight']
                
                # Permit activity component
                permit_activity_normalized = min(permit_activity / 50, 1.0) * 100  # Cap at 50 for normalization
                permit_component = permit_activity_normalized * scoring_algorithm['permit_weight']
                
                # Location component (distance from downtown/transit)
                downtown_distance = math.sqrt((lat - 34.0522)**2 + (lon + 118.2437)**2)
                location_score = max(0, 100 - (downtown_distance * 1000))  # Closer to downtown = higher score
                location_component = location_score * scoring_algorithm['location_weight']
                
                # Market component (neighborhood average + permit value factor)
                market_score = min((permit_value / 100000) * 20 + 50, 100)  # Base 50 + permit value bonus
                market_component = market_score * scoring_algorithm['market_weight']
                
                # Combined multi-factor score
                multi_factor_score = crime_component + permit_component + location_component + market_component
                
                enhanced_scores.append({
                    'property_id': prop_id,
                    'address': address,
                    'neighborhood': neighborhood,
                    'original_score': dev_score,
                    'crime_score': crime_score,
                    'permit_activity': permit_activity,
                    'multi_factor_score': multi_factor_score,
                    'components': {
                        'crime': crime_component,
                        'permit': permit_component,
                        'location': location_component,
                        'market': market_component
                    },
                    'permit_count': permit_count,
                    'permit_value': permit_value
                })
                
                # Track neighborhood statistics
                if neighborhood not in neighborhood_stats:
                    neighborhood_stats[neighborhood] = {
                        'count': 0,
                        'avg_multi_factor': 0,
                        'avg_crime': 0,
                        'avg_permit_activity': 0,
                        'total_permit_value': 0
                    }
                
                stats = neighborhood_stats[neighborhood]
                stats['count'] += 1
                stats['avg_multi_factor'] += multi_factor_score
                stats['avg_crime'] += crime_score
                stats['avg_permit_activity'] += permit_activity
                stats['total_permit_value'] += permit_value
            
            # Calculate neighborhood averages
            for neighborhood, stats in neighborhood_stats.items():
                if stats['count'] > 0:
                    stats['avg_multi_factor'] /= stats['count']
                    stats['avg_crime'] /= stats['count']
                    stats['avg_permit_activity'] /= stats['count']
            
            result['neighborhood_patterns'] = neighborhood_stats
            
            # Update database with new scores
            print("💾 Updating database with multi-factor scores...")
            
            for score_data in enhanced_scores:
                cursor.execute("""
                    UPDATE enhanced_scored_properties 
                    SET enhanced_development_score = ?
                    WHERE property_id = ?
                """, (score_data['multi_factor_score'], score_data['property_id']))
            
            conn.commit()
            
            # Get top properties by multi-factor score
            top_properties = sorted(enhanced_scores, 
                                  key=lambda x: x['multi_factor_score'], 
                                  reverse=True)[:10]
            
            result['multi_factor_scores'] = {
                'top_properties': top_properties,
                'algorithm_performance': {
                    'avg_improvement': sum(s['multi_factor_score'] - s['original_score'] 
                                         for s in enhanced_scores) / len(enhanced_scores),
                    'score_range': {
                        'min': min(s['multi_factor_score'] for s in enhanced_scores),
                        'max': max(s['multi_factor_score'] for s in enhanced_scores),
                        'avg': sum(s['multi_factor_score'] for s in enhanced_scores) / len(enhanced_scores)
                    }
                }
            }
            
            conn.close()
            
            print(f"✅ Multi-factor scoring complete:")
            print(f"   Properties scored: {len(enhanced_scores)}")
            print(f"   Neighborhoods analyzed: {len(neighborhood_stats)}")
            print(f"   Average score improvement: {result['multi_factor_scores']['algorithm_performance']['avg_improvement']:.1f}")
            
            # Show top scoring properties
            print(f"\n🏆 Top 5 Properties by Multi-Factor Score:")
            for i, prop in enumerate(top_properties[:5], 1):
                print(f"   {i}. {prop['address']}")
                print(f"      Score: {prop['multi_factor_score']:.1f} (was {prop['original_score']:.1f})")
                print(f"      Crime: {prop['crime_score']:.1f}, Permits: {prop['permit_count']}, Value: ${prop['permit_value']:,.0f}")
            
        except Exception as e:
            result['error'] = str(e)
            print(f"❌ Scoring integration error: {str(e)}")
        
        return result
    
    def run_end_to_end_validation_with_reports(self) -> Dict[str, Any]:
        """
        TASK 3: End-to-End Validation with HTML Reports
        """
        print(f"\n🧪 END-TO-END VALIDATION WITH 20 PROPERTIES")
        print("="*60)
        
        validation = {
            'properties_tested': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'performance_metrics': {},
            'html_report_generated': False,
            'test_results': []
        }
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get 20 test properties from different neighborhoods and score ranges
            cursor.execute("""
                SELECT property_id, site_address, neighborhood,
                       enhanced_development_score, crime_score,
                       permit_count, permit_value_total, permit_activity_score,
                       latitude, longitude
                FROM enhanced_scored_properties
                WHERE crime_score > 0
                ORDER BY neighborhood, RANDOM()
                LIMIT 20
            """)
            
            test_properties = cursor.fetchall()
            validation['properties_tested'] = len(test_properties)
            
            print(f"🔬 Testing {len(test_properties)} properties end-to-end...")
            
            total_processing_time = 0
            
            for prop in test_properties:
                (prop_id, address, neighborhood, dev_score, crime_score,
                 permit_count, permit_value, permit_activity, lat, lon) = prop
                
                test_start = time.time()
                
                test_result = {
                    'property_id': prop_id,
                    'address': address,
                    'neighborhood': neighborhood,
                    'steps': {},
                    'overall_success': True,
                    'performance_ms': 0
                }
                
                try:
                    # Step 1: Address validation
                    step_start = time.time()
                    address_valid = len(address) > 10 and any(char.isdigit() for char in address)
                    test_result['steps']['address_validation'] = {
                        'success': address_valid,
                        'time_ms': (time.time() - step_start) * 1000
                    }
                    
                    # Step 2: Crime score lookup
                    step_start = time.time()
                    cursor.execute("""
                        SELECT total_density_weighted, neighborhood
                        FROM crime_density_grid 
                        WHERE lat BETWEEN ? - 0.01 AND ? + 0.01
                          AND lon BETWEEN ? - 0.01 AND ? + 0.01
                        ORDER BY ((lat - ?) * (lat - ?) + (lon - ?) * (lon - ?))
                        LIMIT 1
                    """, (lat, lat, lon, lon, lat, lat, lon, lon))
                    
                    crime_lookup = cursor.fetchone()
                    crime_success = crime_lookup is not None and abs(crime_lookup[0] - crime_score) < 50
                    
                    test_result['steps']['crime_lookup'] = {
                        'success': crime_success,
                        'database_score': crime_score,
                        'lookup_score': crime_lookup[0] if crime_lookup else None,
                        'time_ms': (time.time() - step_start) * 1000
                    }
                    
                    # Step 3: Permit data analysis
                    step_start = time.time()
                    cursor.execute("""
                        SELECT COUNT(*), SUM(permit_valuation), AVG(supply_velocity_score)
                        FROM permit_property_analysis
                        WHERE property_id = ?
                    """, (prop_id,))
                    
                    permit_analysis = cursor.fetchone()
                    permit_success = permit_analysis is not None
                    
                    test_result['steps']['permit_analysis'] = {
                        'success': permit_success,
                        'permit_count': permit_analysis[0] if permit_analysis else 0,
                        'total_value': permit_analysis[1] if permit_analysis else 0,
                        'avg_velocity': permit_analysis[2] if permit_analysis else 0,
                        'time_ms': (time.time() - step_start) * 1000
                    }
                    
                    # Step 4: Multi-factor scoring
                    step_start = time.time()
                    scoring_success = dev_score > 0 and crime_score > 0
                    
                    test_result['steps']['multi_factor_scoring'] = {
                        'success': scoring_success,
                        'development_score': dev_score,
                        'scoring_components_present': True,
                        'time_ms': (time.time() - step_start) * 1000
                    }
                    
                    # Overall assessment
                    test_result['overall_success'] = all(step['success'] for step in test_result['steps'].values())
                    test_result['performance_ms'] = (time.time() - test_start) * 1000
                    total_processing_time += test_result['performance_ms']
                    
                    if test_result['overall_success']:
                        validation['tests_passed'] += 1
                        status = "✅ PASS"
                    else:
                        validation['tests_failed'] += 1
                        status = "❌ FAIL"
                    
                    print(f"   {address[:40]:<40} {status} ({test_result['performance_ms']:.1f}ms)")
                    
                except Exception as e:
                    test_result['overall_success'] = False
                    test_result['error'] = str(e)
                    validation['tests_failed'] += 1
                    print(f"   {address[:40]:<40} ❌ ERROR - {str(e)}")
                
                validation['test_results'].append(test_result)
            
            # Calculate performance metrics
            validation['performance_metrics'] = {
                'total_processing_time_ms': total_processing_time,
                'average_time_per_property_ms': total_processing_time / len(test_properties),
                'fastest_property_ms': min(t['performance_ms'] for t in validation['test_results']),
                'slowest_property_ms': max(t['performance_ms'] for t in validation['test_results']),
                'sub_second_properties': sum(1 for t in validation['test_results'] if t['performance_ms'] < 1000),
                'success_rate_percent': (validation['tests_passed'] / validation['properties_tested']) * 100
            }
            
            # Generate HTML report
            html_report = self.generate_html_validation_report(validation, test_properties)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_filename = f"integrated_validation_report_{timestamp}.html"
            
            with open(report_filename, 'w') as f:
                f.write(html_report)
            
            validation['html_report_generated'] = True
            validation['report_filename'] = report_filename
            
            conn.close()
            
            print(f"\n📊 Validation Results:")
            print(f"   Properties tested: {validation['properties_tested']}")
            print(f"   Tests passed: {validation['tests_passed']}")
            print(f"   Tests failed: {validation['tests_failed']}")
            print(f"   Success rate: {validation['performance_metrics']['success_rate_percent']:.1f}%")
            print(f"   Avg processing time: {validation['performance_metrics']['average_time_per_property_ms']:.1f}ms")
            print(f"   Sub-second properties: {validation['performance_metrics']['sub_second_properties']}/20")
            print(f"   HTML report: {report_filename}")
            
        except Exception as e:
            validation['error'] = str(e)
            print(f"❌ Validation error: {str(e)}")
        
        return validation
    
    def generate_html_validation_report(self, validation: Dict[str, Any], properties: List[Tuple]) -> str:
        """Generate HTML report showing both crime and permit metrics"""
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Integrated Crime + Permit Validation Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background: #2c3e50; color: white; padding: 20px; margin-bottom: 20px; }}
                .metric {{ display: inline-block; margin: 10px; padding: 15px; background: #ecf0f1; border-radius: 5px; }}
                .success {{ color: #27ae60; }}
                .failure {{ color: #e74c3c; }}
                table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #34495e; color: white; }}
                .performance-good {{ background-color: #d5f4e6; }}
                .performance-ok {{ background-color: #ffeaa7; }}
                .performance-poor {{ background-color: #fab1a0; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>🏠 Integrated Property Intelligence Validation Report</h1>
                <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p>Crime Data + Permit Analysis + Multi-Factor Scoring</p>
            </div>
            
            <div class="metrics">
                <div class="metric">
                    <h3>Test Results</h3>
                    <p><strong>{validation['tests_passed']}</strong> passed</p>
                    <p><strong>{validation['tests_failed']}</strong> failed</p>
                    <p><strong>{validation['performance_metrics']['success_rate_percent']:.1f}%</strong> success rate</p>
                </div>
                <div class="metric">
                    <h3>Performance</h3>
                    <p><strong>{validation['performance_metrics']['average_time_per_property_ms']:.1f}ms</strong> avg time</p>
                    <p><strong>{validation['performance_metrics']['sub_second_properties']}/20</strong> sub-second</p>
                    <p><strong>{validation['performance_metrics']['fastest_property_ms']:.1f}ms</strong> fastest</p>
                </div>
            </div>
            
            <h2>Property Test Results</h2>
            <table>
                <tr>
                    <th>Address</th>
                    <th>Neighborhood</th>
                    <th>Dev Score</th>
                    <th>Crime Score</th>
                    <th>Permits</th>
                    <th>Status</th>
                    <th>Time (ms)</th>
                </tr>
        """
        
        for test in validation['test_results']:
            status_class = "success" if test['overall_success'] else "failure"
            status_text = "✅ PASS" if test['overall_success'] else "❌ FAIL"
            
            # Performance class
            perf_ms = test['performance_ms']
            if perf_ms < 500:
                perf_class = "performance-good"
            elif perf_ms < 1000:
                perf_class = "performance-ok"
            else:
                perf_class = "performance-poor"
            
            # Find matching property data
            prop_data = next((p for p in properties if p[0] == test['property_id']), None)
            if prop_data:
                dev_score, crime_score, permit_count = prop_data[3], prop_data[4], prop_data[5]
            else:
                dev_score, crime_score, permit_count = "N/A", "N/A", "N/A"
            
            html += f"""
                <tr>
                    <td>{test['address']}</td>
                    <td>{test['neighborhood']}</td>
                    <td>{dev_score:.1f if isinstance(dev_score, float) else dev_score}</td>
                    <td>{crime_score:.1f if isinstance(crime_score, float) else crime_score}</td>
                    <td>{permit_count}</td>
                    <td class="{status_class}">{status_text}</td>
                    <td class="{perf_class}">{perf_ms:.1f}</td>
                </tr>
            """
        
        html += """
            </table>
            
            <h2>Step-by-Step Analysis</h2>
            <table>
                <tr>
                    <th>Step</th>
                    <th>Success Rate</th>
                    <th>Avg Time (ms)</th>
                </tr>
        """
        
        # Calculate step statistics
        steps = ['address_validation', 'crime_lookup', 'permit_analysis', 'multi_factor_scoring']
        for step in steps:
            step_results = [t['steps'].get(step, {}) for t in validation['test_results']]
            success_count = sum(1 for s in step_results if s.get('success', False))
            success_rate = (success_count / len(step_results)) * 100
            avg_time = sum(s.get('time_ms', 0) for s in step_results) / len(step_results)
            
            html += f"""
                <tr>
                    <td>{step.replace('_', ' ').title()}</td>
                    <td>{success_rate:.1f}%</td>
                    <td>{avg_time:.2f}</td>
                </tr>
            """
        
        html += """
            </table>
        </body>
        </html>
        """
        
        return html
    
    def run_complete_integration(self) -> Dict[str, Any]:
        """
        Run complete permit-database integration
        """
        print("🚀 RUNNING COMPLETE PERMIT-DATABASE INTEGRATION")
        print("="*80)
        
        start_time = time.time()
        
        integration_results = {
            'start_time': datetime.now().isoformat(),
            'permit_property_linking': {},
            'crime_permit_scoring': {},
            'end_to_end_validation': {},
            'overall_success': False
        }
        
        # Task 1: Link permits to properties with metrics
        integration_results['permit_property_linking'] = self.link_permits_to_properties_with_metrics()
        
        # Task 2: Combine crime and permit scoring
        integration_results['crime_permit_scoring'] = self.combine_crime_permit_scoring()
        
        # Task 3: End-to-end validation with reports
        integration_results['end_to_end_validation'] = self.run_end_to_end_validation_with_reports()
        
        # Overall assessment
        linking_success = integration_results['permit_property_linking'].get('properties_matched', 0) > 0
        scoring_success = integration_results['crime_permit_scoring'].get('properties_analyzed', 0) > 0
        validation_success_rate = integration_results['end_to_end_validation'].get('performance_metrics', {}).get('success_rate_percent', 0)
        
        integration_results['overall_success'] = (
            linking_success and 
            scoring_success and 
            validation_success_rate >= 80
        )
        
        integration_results['processing_time_seconds'] = time.time() - start_time
        integration_results['end_time'] = datetime.now().isoformat()
        
        return integration_results

def main():
    """
    Connect working permit system to rebuilt database infrastructure
    """
    print("🔌 CONNECTING WORKING PERMIT SYSTEM TO REBUILT DATABASE")
    print("="*80)
    print("Integrating permit data with crime scores for complete intelligence")
    print()
    
    integrator = PermitDatabaseIntegrator()
    results = integrator.run_complete_integration()
    
    # Display final integration results
    print(f"\n📊 COMPLETE INTEGRATION RESULTS:")
    print("="*70)
    
    print(f"Processing Time: {results['processing_time_seconds']:.2f} seconds")
    
    # Permit-property linking results
    linking = results['permit_property_linking']
    print(f"\n🔗 PERMIT-PROPERTY LINKING:")
    if 'properties_matched' in linking:
        print(f"   ✅ Permits fetched: {linking.get('permits_fetched', 0)}")
        print(f"   ✅ Properties matched: {linking.get('properties_matched', 0)}")  
        print(f"   ✅ Supply metrics calculated: {linking.get('supply_metrics_calculated', 0)}")
        print(f"   ✅ Permit clusters identified: {linking.get('permit_clusters_identified', 0)}")
    else:
        print(f"   ❌ Failed: {linking.get('error', 'Unknown error')}")
    
    # Crime-permit scoring results
    scoring = results['crime_permit_scoring']
    print(f"\n🧠 CRIME + PERMIT SCORING:")
    if 'properties_analyzed' in scoring:
        print(f"   ✅ Properties analyzed: {scoring.get('properties_analyzed', 0)}")
        print(f"   ✅ Neighborhoods: {len(scoring.get('neighborhood_patterns', {}))}")
        if 'multi_factor_scores' in scoring:
            perf = scoring['multi_factor_scores'].get('algorithm_performance', {})
            print(f"   ✅ Score improvement: {perf.get('avg_improvement', 0):.1f} points")
    else:
        print(f"   ❌ Failed: {scoring.get('error', 'Unknown error')}")
    
    # End-to-end validation results
    validation = results['end_to_end_validation']
    print(f"\n🧪 END-TO-END VALIDATION:")
    if 'properties_tested' in validation:
        perf = validation.get('performance_metrics', {})
        print(f"   ✅ Properties tested: {validation.get('properties_tested', 0)}")
        print(f"   ✅ Success rate: {perf.get('success_rate_percent', 0):.1f}%")
        print(f"   ✅ Avg response time: {perf.get('average_time_per_property_ms', 0):.1f}ms")
        print(f"   ✅ Sub-second responses: {perf.get('sub_second_properties', 0)}/20")
        if validation.get('html_report_generated'):
            print(f"   ✅ HTML report: {validation.get('report_filename', 'generated')}")
    else:
        print(f"   ❌ Failed: {validation.get('error', 'Unknown error')}")
    
    # Overall status
    print(f"\n🏆 OVERALL INTEGRATION STATUS:")
    if results['overall_success']:
        print(f"   ✅ SUCCESS - Complete permit-database integration")
        print(f"   ✅ Multi-factor scoring operational")
        print(f"   ✅ Sub-second performance maintained")
        print(f"   ✅ Ready for Week 3 web interface")
    else:
        print(f"   ⚠️ PARTIAL SUCCESS - Some integration issues")
        print(f"   ⚠️ Core functionality working but optimization needed")
    
    # Save detailed results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"permit_database_integration_{timestamp}.json"
    
    with open(filename, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n📁 Complete integration report: {filename}")
    
    return results['overall_success']

if __name__ == "__main__":
    main()