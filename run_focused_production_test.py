#!/usr/bin/env python3
"""
Focused Production Readiness Test Runner

This script runs a streamlined version of the production readiness test
with the LAPD API token, focusing on real data validation rather than
error simulation scenarios.

Usage:
    LAPD_SOCRATA_APP_TOKEN='your_token' python run_focused_production_test.py
"""

import os
import sys
import sqlite3
from pathlib import Path
from datetime import datetime
import pandas as pd
import json

# Add the src directory to Python path
sys.path.append(str(Path(__file__).parent / "src" / "etl"))

from crime_extractor import CrimeETLExtractor, CrimeExtractionConfig
import asyncio
import time

def main():
    """Run focused production readiness test with API token."""
    print("🚔 DealGenie Crime Data - Focused Production Test")
    print("=" * 60)
    
    # Check API token
    api_token = os.getenv('LAPD_SOCRATA_APP_TOKEN')
    if not api_token:
        print("❌ Error: LAPD_SOCRATA_APP_TOKEN environment variable not set")
        return 1
    
    print(f"✅ API Token: {api_token[:10]}...")
    print()
    
    # Test configuration
    test_db_path = "./data/crime_focused_test.db"
    staging_path = "./data/crime_focused_staging"
    
    # Clean up previous test
    if os.path.exists(test_db_path):
        os.remove(test_db_path)
    if os.path.exists(staging_path):
        import shutil
        shutil.rmtree(staging_path)
    
    Path(staging_path).mkdir(parents=True, exist_ok=True)
    
    print("🧪 Test 1: Full Monthly Extract (October 2024)")
    print("-" * 50)
    
    try:
        # Configure for October 2024 extraction
        config = CrimeExtractionConfig(
            months_back=1,
            enable_pii_protection=True,
            enable_spatial_obfuscation=True,
            staging_path=staging_path,
            enable_trends=True
        )
        
        extractor = CrimeETLExtractor(config, db_path=test_db_path)
        
        # Extract October 2024 data
        start_time = time.time()
        records = asyncio.run(extractor.extract_crime_data(
            months_back=1,
            as_of_date=datetime(2024, 11, 1)  # End of October 2024
        ))
        extraction_time = time.time() - start_time
        
        print(f"✅ Records Extracted: {len(records):,}")
        print(f"⏱️  Processing Time: {extraction_time:.1f} seconds ({extraction_time/60:.2f} minutes)")
        print(f"🚀 Processing Speed: {len(records)/extraction_time:.1f} records/second")
        print(f"📊 API Requests: {extractor.metrics.api_requests_made}")
        print(f"📈 Coordinate Success: {extractor.metrics.coordinate_success_rate:.1f}%")
        print(f"🔄 Deduplication Rate: {extractor.metrics.deduplication_rate:.1f}%")
        
        if len(records) == 0:
            print("❌ No records extracted - cannot proceed with validation")
            return 1
        
        # Load to database
        print("\n💾 Loading to database...")
        extractor.load_to_database(records)
        print(f"✅ Database loaded: {len(records):,} records")
        
        # Save to Parquet
        parquet_file = extractor.save_to_parquet(records)
        print(f"✅ Parquet saved: {parquet_file}")
        
        print("\n🧪 Test 2: Data Quality Analysis")
        print("-" * 50)
        
        # Analyze data quality
        df = pd.DataFrame([{
            'incident_id': r.incident_id,
            'event_date': r.event_date,
            'area_id': r.area_id,
            'area_name': r.area_name,
            'crime_category': r.crime_category,
            'latitude': r.latitude,
            'longitude': r.longitude,
            'spatial_uncertainty_flag': r.spatial_uncertainty_flag
        } for r in records])
        
        df['event_date'] = pd.to_datetime(df['event_date'])
        df['hour'] = df['event_date'].dt.hour
        df['day_of_week'] = df['event_date'].dt.day_of_week
        df['is_weekend'] = df['day_of_week'].isin([5, 6])
        
        # Geographic Distribution
        print("📍 Geographic Distribution:")
        area_counts = df['area_name'].value_counts()
        print(f"   Total Areas Covered: {len(area_counts)}")
        print("   Top 10 Areas by Incidents:")
        for area, count in area_counts.head(10).items():
            print(f"     {area}: {count:,} incidents")
        
        # Crime Categories
        print("\n🔍 Crime Category Distribution:")
        category_counts = df['crime_category'].value_counts()
        category_pct = (category_counts / len(df) * 100).round(1)
        for category in ['VIOLENT', 'PROPERTY', 'OTHER']:
            count = category_counts.get(category, 0)
            pct = category_pct.get(category, 0)
            print(f"   {category}: {count:,} incidents ({pct}%)")
        
        # Temporal Patterns
        print("\n⏰ Temporal Patterns:")
        weekend_pct = (df['is_weekend'].sum() / len(df) * 100)
        print(f"   Weekend Crimes: {weekend_pct:.1f}%")
        
        hourly_counts = df['hour'].value_counts().sort_index()
        peak_hour = hourly_counts.idxmax()
        print(f"   Peak Crime Hour: {peak_hour}:00 ({hourly_counts[peak_hour]:,} incidents)")
        
        # Coordinate Quality
        print("\n🗺️  Coordinate Quality:")
        valid_coords = (~df['latitude'].isnull()) & (~df['longitude'].isnull())
        coord_pct = (valid_coords.sum() / len(df) * 100)
        print(f"   Records with Coordinates: {valid_coords.sum():,} ({coord_pct:.1f}%)")
        
        spatial_uncertainty = df['spatial_uncertainty_flag'].sum()
        uncertainty_pct = (spatial_uncertainty / len(df) * 100)
        print(f"   Spatial Uncertainty Applied: {spatial_uncertainty:,} ({uncertainty_pct:.1f}%)")
        
        print("\n🧪 Test 3: Database Performance")
        print("-" * 50)
        
        # Test database queries
        conn = sqlite3.connect(test_db_path)
        
        queries = [
            ("Total Records", "SELECT COUNT(*) FROM crime_incidents"),
            ("Recent Crimes", "SELECT COUNT(*) FROM crime_incidents WHERE event_date >= '2024-10-25'"),
            ("Violent Crimes", "SELECT COUNT(*) FROM crime_incidents WHERE crime_category = 'VIOLENT'"),
            ("Property Crimes", "SELECT COUNT(*) FROM crime_incidents WHERE crime_category = 'PROPERTY'"),
            ("Crimes with Coordinates", "SELECT COUNT(*) FROM crime_incidents WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
        ]
        
        for query_name, query_sql in queries:
            start_time = time.time()
            cursor = conn.cursor()
            cursor.execute(query_sql)
            result = cursor.fetchone()[0]
            query_time = time.time() - start_time
            print(f"   {query_name}: {result:,} ({query_time*1000:.1f}ms)")
        
        conn.close()
        
        print("\n🧪 Test 4: Trends Calculation")
        print("-" * 50)
        
        # Calculate trends
        trends = extractor.calculate_rolling_trends(records)
        if trends:
            print(f"   3-Month Incidents: {trends.get('total_incidents_3m', 0):,}")
            print(f"   12-Month Incidents: {trends.get('total_incidents_12m', 0):,}")
            print(f"   Monthly Average (3m): {trends.get('incidents_per_month_3m', 0):.1f}")
            print(f"   Monthly Average (12m): {trends.get('incidents_per_month_12m', 0):.1f}")
            
            for period in ['3m', '12m']:
                violent = trends.get(f'violent_incidents_{period}', 0)
                property_crimes = trends.get(f'property_incidents_{period}', 0)
                other = trends.get(f'other_incidents_{period}', 0)
                print(f"   {period.upper()} Breakdown - Violent: {violent:,}, Property: {property_crimes:,}, Other: {other:,}")
        
        print("\n🎯 PRODUCTION READINESS ASSESSMENT")
        print("=" * 60)
        
        # Performance Assessment
        performance_pass = extraction_time < 300  # 5 minutes
        data_quality_pass = len(area_counts) >= 10 and coord_pct > 95
        volume_pass = len(records) > 15000  # Reasonable monthly volume for LA
        
        print(f"✅ Performance Test: {'PASS' if performance_pass else 'FAIL'}")
        print(f"   Processing Time: {extraction_time/60:.2f} minutes ({'✅' if performance_pass else '❌'} < 5 min target)")
        
        print(f"✅ Data Volume Test: {'PASS' if volume_pass else 'FAIL'}")
        print(f"   Records Extracted: {len(records):,} ({'✅' if volume_pass else '❌'} > 15K expected)")
        
        print(f"✅ Data Quality Test: {'PASS' if data_quality_pass else 'FAIL'}")
        print(f"   Geographic Coverage: {len(area_counts)} areas ({'✅' if len(area_counts) >= 10 else '❌'} >= 10 expected)")
        print(f"   Coordinate Success: {coord_pct:.1f}% ({'✅' if coord_pct > 95 else '❌'} > 95% expected)")
        
        print(f"✅ Crime Pattern Validation:")
        print(f"   Violent: {category_pct.get('VIOLENT', 0):.1f}% ({'✅' if 15 <= category_pct.get('VIOLENT', 0) <= 25 else '⚠️'} 15-25% expected)")
        print(f"   Property: {category_pct.get('PROPERTY', 0):.1f}% ({'✅' if 35 <= category_pct.get('PROPERTY', 0) <= 55 else '⚠️'} 35-55% expected)")
        print(f"   Weekend: {weekend_pct:.1f}% ({'✅' if 25 <= weekend_pct <= 35 else '⚠️'} 25-35% expected)")
        
        # Overall assessment
        all_tests_pass = performance_pass and data_quality_pass and volume_pass
        
        print(f"\n🎉 OVERALL RESULT: {'✅ PRODUCTION READY' if all_tests_pass else '⚠️ NEEDS REVIEW'}")
        
        if all_tests_pass:
            print("   System meets all production readiness criteria!")
            print("   ✅ Ready to proceed to Task 3.2 (Crime Density Analysis)")
        else:
            print("   Some criteria need attention, but core functionality is working.")
        
        # Save detailed results
        results = {
            'test_timestamp': datetime.now().isoformat(),
            'overall_result': 'PASS' if all_tests_pass else 'NEEDS_REVIEW',
            'performance': {
                'records_extracted': len(records),
                'processing_time_seconds': extraction_time,
                'processing_speed_records_per_second': len(records)/extraction_time,
                'api_requests': extractor.metrics.api_requests_made,
                'coordinate_success_rate': extractor.metrics.coordinate_success_rate,
                'deduplication_rate': extractor.metrics.deduplication_rate
            },
            'data_quality': {
                'total_areas': len(area_counts),
                'coordinate_coverage_pct': coord_pct,
                'crime_categories': category_counts.to_dict(),
                'crime_category_percentages': category_pct.to_dict(),
                'weekend_crime_pct': weekend_pct,
                'peak_hour': int(peak_hour),
                'spatial_uncertainty_pct': uncertainty_pct
            },
            'trends': trends,
            'test_files': {
                'database': test_db_path,
                'parquet': parquet_file,
                'staging': staging_path
            }
        }
        
        results_file = Path(staging_path) / f"focused_production_test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"\n📄 Detailed results saved to: {results_file}")
        
        return 0 if all_tests_pass else 1
        
    except Exception as e:
        print(f"\n❌ Test Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)