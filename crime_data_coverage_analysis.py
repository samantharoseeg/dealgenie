#!/usr/bin/env python3
"""
CRIME DATA COVERAGE ANALYSIS
Analyze spatial coverage of crime_density_grid and identify gaps
"""

import sys
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple, Optional

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

def analyze_crime_database_coverage():
    """Comprehensive analysis of crime database coverage."""
    print("=" * 80)
    print("🗺️ CRIME DATA COVERAGE ANALYSIS")
    print("=" * 80)
    print("Analyzing spatial extent and coverage of crime_density_grid...")
    print()
    
    # Test coordinates from validation
    test_coordinates = [
        {'name': 'Downtown LA', 'lat': 34.052235, 'lon': -118.243685, 'status': 'missing'},
        {'name': 'Beverly Hills', 'lat': 34.073620, 'lon': -118.400356, 'status': 'working'},
        {'name': 'Koreatown', 'lat': 34.058000, 'lon': -118.291000, 'status': 'missing'},
        {'name': 'Venice', 'lat': 33.994200, 'lon': -118.475100, 'status': 'working'}
    ]
    
    # Database paths to check
    db_paths = [
        'data/dealgenie.db',
        'data/crime_kde_optimized.db',
        'data/crime_january_test_weighted.db'
    ]
    
    for db_path in db_paths:
        print(f"📊 Analyzing database: {db_path}")
        print("-" * 60)
        
        if not Path(db_path).exists():
            print(f"❌ Database not found: {db_path}")
            print()
            continue
            
        try:
            conn = sqlite3.connect(db_path)
            
            # 1. Check if crime_density_grid table exists
            cursor = conn.cursor()
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='crime_density_grid'
            """)
            
            if not cursor.fetchone():
                print(f"❌ No crime_density_grid table found in {db_path}")
                
                # Show what tables do exist
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = cursor.fetchall()
                print(f"   Available tables: {[t[0] for t in tables]}")
                print()
                continue
            
            # 2. Get table structure
            cursor.execute("PRAGMA table_info(crime_density_grid)")
            columns = cursor.fetchall()
            print(f"✅ Found crime_density_grid table")
            print(f"   Columns: {[col[1] for col in columns]}")
            
            # 3. Count total cells
            cursor.execute("SELECT COUNT(*) FROM crime_density_grid")
            total_cells = cursor.fetchone()[0]
            print(f"   Total cells: {total_cells:,}")
            
            # 4. Get geographic bounds (using lat/lon columns directly)
            cursor.execute("""
                SELECT 
                    MIN(lat) as min_lat,
                    MAX(lat) as max_lat,
                    MIN(lon) as min_lon,
                    MAX(lon) as max_lon
                FROM crime_density_grid
            """)
            bounds = cursor.fetchone()
            min_lat, max_lat, min_lon, max_lon = bounds
            
            print(f"   Geographic Bounds:")
            print(f"     Latitude:  {min_lat:.6f} to {max_lat:.6f} ({max_lat-min_lat:.6f}° span)")
            print(f"     Longitude: {min_lon:.6f} to {max_lon:.6f} ({max_lon-min_lon:.6f}° span)")
            
            # 5. Calculate grid resolution (estimate from point spacing)
            cursor.execute("""
                SELECT lat, lon FROM crime_density_grid 
                ORDER BY lat, lon 
                LIMIT 1000
            """)
            sample_points = cursor.fetchall()
            
            # Calculate average spacing between adjacent points
            lat_diffs = []
            lon_diffs = []
            for i in range(1, min(100, len(sample_points))):
                lat_diff = abs(sample_points[i][0] - sample_points[i-1][0])
                lon_diff = abs(sample_points[i][1] - sample_points[i-1][1])
                if lat_diff > 0:
                    lat_diffs.append(lat_diff)
                if lon_diff > 0:
                    lon_diffs.append(lon_diff)
            
            if lat_diffs and lon_diffs:
                avg_lat_size = min(lat_diffs)  # Smallest non-zero difference
                avg_lon_size = min(lon_diffs)
                
                # Convert to meters (approximate)
                lat_meters = avg_lat_size * 111000  # ~111km per degree latitude
                lon_meters = avg_lon_size * 111000 * np.cos(np.radians(np.mean([min_lat, max_lat])))
                
                print(f"   Grid Resolution (estimated):")
                print(f"     Latitude:  {avg_lat_size:.6f}° (~{lat_meters:.0f}m)")
                print(f"     Longitude: {avg_lon_size:.6f}° (~{lon_meters:.0f}m)")
            else:
                avg_lat_size = avg_lon_size = 0.001  # Default estimate
                print(f"   Grid Resolution: Unable to determine from sample")
            
            # 6. Check test coordinates coverage
            print(f"\n🎯 Test Coordinates Coverage:")
            print("   Location".ljust(15) + "Coordinates".ljust(25) + "Status".ljust(10) + "Coverage")
            print("   " + "-" * 65)
            
            for coord in test_coordinates:
                # Find closest grid points (within tolerance)
                tolerance = 0.01  # ~1km tolerance
                cursor.execute("""
                    SELECT lat, lon, total_density, 
                           ABS(? - lat) + ABS(? - lon) as distance
                    FROM crime_density_grid
                    WHERE ABS(? - lat) <= ? AND ABS(? - lon) <= ?
                    ORDER BY distance
                    LIMIT 1
                """, (coord['lat'], coord['lon'], coord['lat'], tolerance, coord['lon'], tolerance))
                
                result = cursor.fetchone()
                
                if result:
                    closest_lat, closest_lon, density, distance = result
                    distance_km = distance * 111  # Approximate km per degree
                    if density > 0:
                        coverage_status = f"✅ COVERED (density: {density:.1f})"
                    else:
                        coverage_status = f"🟡 ZERO DENSITY"
                    print(f"   {coord['name'][:14].ljust(15)}{coord['lat']:.3f},{coord['lon']:.3f}".ljust(25) + f"{coord['status'][:9].ljust(10)}{coverage_status}")
                else:
                    # Find absolutely closest point
                    cursor.execute("""
                        SELECT lat, lon, total_density,
                               ABS(? - lat) + ABS(? - lon) as distance
                        FROM crime_density_grid
                        ORDER BY distance
                        LIMIT 1
                    """, (coord['lat'], coord['lon']))
                    
                    closest = cursor.fetchone()
                    if closest:
                        closest_lat, closest_lon, density, distance = closest
                        distance_km = distance * 111  # Approximate km per degree
                        coverage_status = f"❌ MISSING (closest: {distance_km:.1f}km, density: {density:.1f})"
                        print(f"   {coord['name'][:14].ljust(15)}{coord['lat']:.3f},{coord['lon']:.3f}".ljust(25) + f"{coord['status'][:9].ljust(10)}{coverage_status}")
                    else:
                        coverage_status = "❌ NO DATA"
                        print(f"   {coord['name'][:14].ljust(15)}{coord['lat']:.3f},{coord['lon']:.3f}".ljust(25) + f"{coord['status'][:9].ljust(10)}{coverage_status}")
            
            # 7. Analyze density distribution
            cursor.execute("""
                SELECT 
                    MIN(total_density) as min_density,
                    MAX(total_density) as max_density,
                    AVG(total_density) as avg_density,
                    COUNT(CASE WHEN total_density > 0 THEN 1 END) as non_zero_cells
                FROM crime_density_grid
            """)
            density_stats = cursor.fetchone()
            min_density, max_density, avg_density, non_zero_cells = density_stats
            
            print(f"\n📈 Crime Density Statistics:")
            print(f"   Density Range: {min_density:.2f} to {max_density:.2f}")
            print(f"   Average Density: {avg_density:.2f}")
            print(f"   Non-zero Cells: {non_zero_cells:,}/{total_cells:,} ({non_zero_cells/total_cells*100:.1f}%)")
            
            # 8. Check specific problem areas
            print(f"\n🔍 Investigating Missing Data Areas:")
            
            # Downtown LA specific analysis
            downtown_lat, downtown_lon = 34.052235, -118.243685
            cursor.execute("""
                SELECT 
                    lat, lon, total_density, violent_density, property_density,
                    ABS(? - lat) + ABS(? - lon) as distance
                FROM crime_density_grid
                ORDER BY distance
                LIMIT 5
            """, (downtown_lat, downtown_lon))
            
            closest_cells = cursor.fetchall()
            print(f"   Downtown LA ({downtown_lat:.6f}, {downtown_lon:.6f}):")
            print(f"   Closest 5 grid cells:")
            for i, cell in enumerate(closest_cells, 1):
                lat, lon, total_density, violent_density, property_density, distance = cell
                distance_km = distance * 111
                print(f"     {i}. Point: ({lat:.6f}, {lon:.6f}) - {distance_km:.2f}km away")
                print(f"        Crime: Total={total_density:.2f}, Violent={violent_density:.2f}, Property={property_density:.2f}")
            
            # 9. Coverage analysis for LA area  
            cursor.execute("""
                SELECT COUNT(*) 
                FROM crime_density_grid
                WHERE lat >= 33.7 AND lat <= 34.3
                  AND lon >= -118.7 AND lon <= -118.1
            """)
            la_area_cells = cursor.fetchone()[0]
            
            # Expected cells for LA area (rough calculation)
            la_lat_span = 34.3 - 33.7  # 0.6 degrees
            la_lon_span = 118.7 - 118.1  # 0.6 degrees
            if avg_lat_size > 0 and avg_lon_size > 0:
                expected_cells = (la_lat_span / avg_lat_size) * (la_lon_span / avg_lon_size)
                coverage_pct = la_area_cells/expected_cells*100 if expected_cells > 0 else 0
            else:
                expected_cells = 0
                coverage_pct = 0
            
            print(f"\n📍 LA Area Coverage:")
            print(f"   LA Bounding Box: 33.7-34.3°N, 118.1-118.7°W")
            print(f"   Cells in LA area: {la_area_cells:,}")
            print(f"   Expected cells: ~{expected_cells:,.0f}")
            print(f"   Coverage: {coverage_pct:.1f}%")
            
            conn.close()
            
        except Exception as e:
            print(f"❌ Error analyzing {db_path}: {e}")
            if 'conn' in locals():
                conn.close()
        
        print()

def analyze_coordinate_transformation():
    """Analyze how coordinates are transformed to grid cells."""
    print("=" * 80)
    print("🔄 COORDINATE TRANSFORMATION ANALYSIS")
    print("=" * 80)
    
    # Import the crime density service to check transformation logic
    try:
        from src.crime.crime_density_service import CrimeDensityService
        
        # Test with different databases
        db_paths = ['data/dealgenie.db', 'data/crime_kde_optimized.db']
        
        for db_path in db_paths:
            if not Path(db_path).exists():
                continue
                
            print(f"🔄 Testing coordinate transformation with {db_path}")
            
            service = CrimeDensityService(db_path)
            
            # Test problem coordinates
            test_coords = [
                {'name': 'Downtown LA', 'lat': 34.052235, 'lon': -118.243685},
                {'name': 'Beverly Hills', 'lat': 34.073620, 'lon': -118.400356}
            ]
            
            for coord in test_coords:
                print(f"\n📍 {coord['name']}: ({coord['lat']:.6f}, {coord['lon']:.6f})")
                
                try:
                    # Check if this matches any nearby grid points
                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()
                    
                    # Find closest grid points within tolerance
                    tolerance = 0.01  # ~1km tolerance
                    cursor.execute("""
                        SELECT lat, lon, total_density,
                               ABS(? - lat) + ABS(? - lon) as distance
                        FROM crime_density_grid
                        WHERE ABS(? - lat) <= ? AND ABS(? - lon) <= ?
                        ORDER BY distance
                        LIMIT 5
                    """, (coord['lat'], coord['lon'], coord['lat'], tolerance, coord['lon'], tolerance))
                    
                    matches = cursor.fetchall()
                    if matches:
                        print(f"   ✅ Found {len(matches)} nearby grid point(s) within {tolerance:.3f}°:")
                        for i, match in enumerate(matches, 1):
                            lat, lon, density, distance = match
                            distance_km = distance * 111
                            print(f"      {i}. ({lat:.6f}, {lon:.6f}) = {density:.2f} density ({distance_km:.2f}km away)")
                    else:
                        print(f"   ❌ No grid points found within {tolerance:.3f}°")
                        
                        # Find absolutely closest points
                        cursor.execute("""
                            SELECT lat, lon, total_density,
                                   ABS(? - lat) + ABS(? - lon) as distance
                            FROM crime_density_grid
                            ORDER BY distance
                            LIMIT 3
                        """, (coord['lat'], coord['lon']))
                        
                        closest = cursor.fetchall()
                        print(f"   Closest 3 points:")
                        for i, cell in enumerate(closest, 1):
                            lat, lon, density, distance = cell
                            distance_km = distance * 111
                            print(f"      {i}. ({lat:.6f}, {lon:.6f}) = {density:.2f} density ({distance_km:.2f}km away)")
                    
                    conn.close()
                    
                except Exception as e:
                    print(f"   ❌ Error testing {coord['name']}: {e}")
                    
            break  # Only test first available database
            
    except ImportError as e:
        print(f"❌ Could not import CrimeDensityService: {e}")

def main():
    """Main execution function."""
    print("🚨 CRIME DATA COVERAGE ANALYSIS")
    print("Investigating spatial coverage and coordinate transformation issues...\n")
    
    analyze_crime_database_coverage()
    analyze_coordinate_transformation()

if __name__ == "__main__":
    main()