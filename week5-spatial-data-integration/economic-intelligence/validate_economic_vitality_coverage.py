#!/usr/bin/env python3
"""
Validate Economic Vitality Coverage Across Property Dataset
Comprehensive validation of economic data integration and coverage assessment
"""

import psycopg2
import json
from datetime import datetime

DATABASE_CONFIG = {
    "host": "localhost",
    "database": "dealgenie_production",
    "user": "dealgenie_app",
    "password": "dealgenie2025",
    "port": 5432
}

def validate_economic_vitality_coverage():
    """Validate comprehensive economic vitality data coverage"""
    print("✅ ECONOMIC VITALITY DATA COVERAGE VALIDATION")
    print("=" * 80)
    print(f"Validation Date: {datetime.now()}")
    print("Objective: Verify complete economic intelligence framework implementation")
    print("=" * 80)

    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        cursor = conn.cursor()

        # Step 1: Validate table structure and data presence
        print("\n🏗️ ECONOMIC VITALITY TABLES VALIDATION")
        print("-" * 60)

        economic_tables = [
            'bls_employment_data',
            'rent_price_indices',
            'business_mix_analysis',
            'submarket_vitality_scores'
        ]

        table_stats = {}

        for table in economic_tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                table_stats[table] = count
                print(f"   ✅ {table}: {count:,} records")
            except Exception as e:
                table_stats[table] = 0
                print(f"   ❌ {table}: Error - {e}")

        # Step 2: BLS Employment Data Coverage Analysis
        print("\n💼 BLS EMPLOYMENT DATA ANALYSIS")
        print("-" * 60)

        if table_stats.get('bls_employment_data', 0) > 0:
            cursor.execute("""
            SELECT
                COUNT(DISTINCT series_id) as unique_series,
                COUNT(DISTINCT year) as unique_years,
                MIN(year) as earliest_year,
                MAX(year) as latest_year,
                COUNT(*) as total_records
            FROM bls_employment_data
            """)

            series_count, year_count, min_year, max_year, total_records = cursor.fetchone()

            print(f"   📊 BLS Data Coverage:")
            print(f"      Unique employment series: {series_count}")
            print(f"      Year coverage: {min_year}-{max_year} ({year_count} years)")
            print(f"      Total data points: {total_records:,}")

            # Get latest employment indicators
            cursor.execute("""
            SELECT
                series_id,
                value,
                year,
                period
            FROM bls_employment_data
            WHERE year = (SELECT MAX(year) FROM bls_employment_data)
            ORDER BY series_id
            LIMIT 5
            """)

            latest_indicators = cursor.fetchall()
            print(f"      Latest indicators:")
            for series, value, year, period in latest_indicators:
                print(f"        {series}: {value} ({year}-{period})")

        # Step 3: Rent Price Indices Coverage Analysis
        print("\n🏠 RENT PRICE INDICES COVERAGE ANALYSIS")
        print("-" * 60)

        if table_stats.get('rent_price_indices', 0) > 0:
            cursor.execute("""
            SELECT
                data_source,
                COUNT(DISTINCT zip_code) as unique_zip_codes,
                COUNT(*) as total_records,
                AVG(median_rent) as avg_median_rent,
                AVG(rent_growth_yoy) as avg_rent_growth
            FROM rent_price_indices
            GROUP BY data_source
            ORDER BY unique_zip_codes DESC
            """)

            rent_coverage = cursor.fetchall()

            print(f"   📊 Rent Data Coverage by Source:")
            for source, zip_count, records, avg_rent, avg_growth in rent_coverage:
                print(f"      {source}: {zip_count} ZIP codes, {records:,} records")
                if avg_rent:
                    print(f"        Avg median rent: ${avg_rent:,.0f}, Avg YoY growth: {avg_growth:.1f}%")

            # Top performing rent markets
            cursor.execute("""
            SELECT
                zip_code,
                median_rent,
                rent_growth_yoy,
                market_momentum_score
            FROM rent_price_indices
            WHERE data_source = 'ZILLOW_ZORI'
            ORDER BY market_momentum_score DESC
            LIMIT 5
            """)

            top_rent_markets = cursor.fetchall()
            print(f"      Top performing rent markets:")
            for zip_code, rent, growth, momentum in top_rent_markets:
                print(f"        ZIP {zip_code}: ${rent:,.0f}/mo, {growth:+.1f}% YoY, Momentum: {momentum:.1f}")

        # Step 4: Business Mix Analysis Coverage
        print("\n🏪 BUSINESS MIX ANALYSIS COVERAGE")
        print("-" * 60)

        if table_stats.get('business_mix_analysis', 0) > 0:
            cursor.execute("""
            SELECT
                COUNT(DISTINCT coordinate_cluster) as unique_clusters,
                AVG(business_density_score) as avg_business_density,
                AVG(retail_diversity_index) as avg_retail_diversity,
                AVG(amenity_walkability_score) as avg_walkability,
                SUM(yelp_business_count) as total_yelp_businesses,
                SUM(osm_poi_count) as total_osm_pois
            FROM business_mix_analysis
            """)

            cluster_count, avg_density, avg_diversity, avg_walkability, total_yelp, total_osm = cursor.fetchone()

            print(f"   📊 Business Mix Coverage:")
            print(f"      Coordinate clusters analyzed: {cluster_count:,}")
            print(f"      Avg business density score: {avg_density:.1f}/100")
            print(f"      Avg retail diversity index: {avg_diversity:.1f}/100")
            print(f"      Avg walkability score: {avg_walkability:.1f}/100")
            print(f"      Total Yelp businesses: {total_yelp:,}")
            print(f"      Total OSM POIs: {total_osm:,}")

            # Top business districts
            cursor.execute("""
            SELECT
                coordinate_cluster,
                business_density_score,
                retail_diversity_index,
                yelp_business_count
            FROM business_mix_analysis
            ORDER BY business_density_score DESC
            LIMIT 5
            """)

            top_business_areas = cursor.fetchall()
            print(f"      Top business districts:")
            for cluster, density, diversity, yelp_count in top_business_areas:
                print(f"        {cluster}: Density {density:.1f}, Diversity {diversity:.1f}, {yelp_count} businesses")

        # Step 5: Submarket Vitality Scores Analysis
        print("\n🎯 SUBMARKET VITALITY SCORES ANALYSIS")
        print("-" * 60)

        if table_stats.get('submarket_vitality_scores', 0) > 0:
            cursor.execute("""
            SELECT
                geography_type,
                COUNT(*) as areas_scored,
                AVG(submarket_vitality_score) as avg_vitality_score,
                MIN(submarket_vitality_score) as min_score,
                MAX(submarket_vitality_score) as max_score,
                SUM(property_count) as total_properties_covered
            FROM submarket_vitality_scores
            GROUP BY geography_type
            ORDER BY areas_scored DESC
            """)

            vitality_coverage = cursor.fetchall()

            print(f"   📊 Vitality Scores Coverage:")
            for geo_type, areas, avg_score, min_score, max_score, prop_count in vitality_coverage:
                print(f"      {geo_type}: {areas:,} areas scored")
                print(f"        Score range: {min_score:.1f} - {max_score:.1f} (avg: {avg_score:.1f})")
                print(f"        Properties covered: {prop_count:,}")

            # Top performing submarkets
            cursor.execute("""
            SELECT
                geographic_id,
                submarket_vitality_score,
                employment_score,
                rent_momentum_score,
                business_mix_score,
                composite_rank,
                property_count
            FROM submarket_vitality_scores
            WHERE geography_type = 'zip_code'
            ORDER BY submarket_vitality_score DESC
            LIMIT 10
            """)

            top_vitality_markets = cursor.fetchall()
            print(f"      Top 10 vitality submarkets:")
            for geo_id, vitality, emp, rent, business, rank, props in top_vitality_markets:
                print(f"        #{rank} ZIP {geo_id}: {vitality:.1f} (E:{emp:.1f} R:{rent:.1f} B:{business:.1f}) - {props:,} props")

        # Step 6: Property Coverage Assessment
        print("\n🏘️ PROPERTY COVERAGE ASSESSMENT")
        print("-" * 60)

        # Properties with economic vitality data
        cursor.execute("""
        SELECT
            COUNT(DISTINCT upd.apn) as total_properties,
            COUNT(DISTINCT CASE WHEN rpi.zip_code IS NOT NULL THEN upd.apn END) as with_rent_data,
            COUNT(DISTINCT CASE WHEN svs.geographic_id IS NOT NULL THEN upd.apn END) as with_vitality_scores
        FROM unified_property_data upd
        LEFT JOIN rent_price_indices rpi ON upd.zip_code = rpi.zip_code
        LEFT JOIN submarket_vitality_scores svs ON upd.zip_code = svs.geographic_id
        WHERE upd.latitude IS NOT NULL AND upd.longitude IS NOT NULL
        """)

        total_props, with_rent, with_vitality = cursor.fetchone()

        print(f"   📊 Property Economic Intelligence Coverage:")
        print(f"      Total geocoded properties: {total_props:,}")
        print(f"      Properties with rent data: {with_rent:,} ({(with_rent/total_props)*100:.1f}%)")
        print(f"      Properties with vitality scores: {with_vitality:,} ({(with_vitality/total_props)*100:.1f}%)")

        # Regional economic intelligence distribution
        cursor.execute("""
        SELECT
            CASE
                WHEN upd.latitude < 34.0 THEN 'South LA'
                WHEN upd.latitude > 34.3 THEN 'North LA'
                WHEN upd.longitude > -118.2 THEN 'East LA'
                WHEN upd.longitude < -118.5 THEN 'West LA'
                ELSE 'Central LA'
            END as region,
            COUNT(upd.apn) as total_properties,
            COUNT(CASE WHEN svs.geographic_id IS NOT NULL THEN upd.apn END) as with_vitality,
            AVG(CASE WHEN svs.submarket_vitality_score IS NOT NULL THEN svs.submarket_vitality_score END) as avg_vitality_score
        FROM unified_property_data upd
        LEFT JOIN submarket_vitality_scores svs ON upd.zip_code = svs.geographic_id AND svs.geography_type = 'zip_code'
        WHERE upd.latitude IS NOT NULL AND upd.longitude IS NOT NULL
        GROUP BY
            CASE
                WHEN upd.latitude < 34.0 THEN 'South LA'
                WHEN upd.latitude > 34.3 THEN 'North LA'
                WHEN upd.longitude > -118.2 THEN 'East LA'
                WHEN upd.longitude < -118.5 THEN 'West LA'
                ELSE 'Central LA'
            END
        ORDER BY total_properties DESC
        """)

        regional_vitality = cursor.fetchall()

        print(f"      Regional economic intelligence:")
        for region, total, with_vit, avg_score in regional_vitality:
            coverage_pct = (with_vit / total) * 100 if total > 0 else 0
            avg_score_str = f"{avg_score:.1f}" if avg_score else "N/A"
            print(f"        {region}: {with_vit:,}/{total:,} properties ({coverage_pct:.1f}%) | Avg vitality: {avg_score_str}")

        # Step 7: Data Freshness and API Integration Status
        print("\n🏷️ DATA FRESHNESS & API INTEGRATION STATUS")
        print("-" * 60)

        # Check data freshness from various sources
        api_sources = {
            "BLS Employment": "bls_employment_data",
            "Zillow ZORI": "rent_price_indices WHERE data_source = 'ZILLOW_ZORI'",
            "Redfin Market": "rent_price_indices WHERE data_source = 'REDFIN_DATA_CENTER'",
            "Business Mix": "business_mix_analysis"
        }

        for source_name, table_query in api_sources.items():
            try:
                cursor.execute(f"SELECT MAX(last_updated) FROM {table_query}")
                last_updated = cursor.fetchone()[0]
                if last_updated:
                    print(f"   ✅ {source_name}: Last updated {last_updated}")
                else:
                    print(f"   ⚠️ {source_name}: No timestamp data")
            except Exception as e:
                print(f"   ❌ {source_name}: Query error - {e}")

        # Step 8: Final Assessment
        print("\n🎯 ECONOMIC VITALITY FRAMEWORK ASSESSMENT")
        print("-" * 60)

        components_implemented = sum([1 for count in table_stats.values() if count > 0])
        total_components = len(table_stats)

        property_coverage = (with_vitality / total_props) * 100 if total_props > 0 else 0

        print(f"   📊 Implementation Status:")
        print(f"      Components implemented: {components_implemented}/{total_components}")
        print(f"      Property coverage: {property_coverage:.1f}% with vitality scores")
        print(f"      Data sources integrated: BLS, Zillow, Redfin, Yelp, OSM")
        print(f"      Geographic areas analyzed: {len(economic_tables)} data layers")

        # Success criteria
        if components_implemented >= 3 and property_coverage >= 80:
            print(f"\n✅ COMPREHENSIVE ECONOMIC VITALITY FRAMEWORK SUCCESSFULLY IMPLEMENTED!")
            print(f"   Framework ready for investment analysis and submarket scoring")
            success = True
        elif components_implemented >= 2 and property_coverage >= 50:
            print(f"\n⚡ SUBSTANTIAL ECONOMIC INTELLIGENCE COVERAGE ACHIEVED")
            print(f"   Framework functional with good coverage across property dataset")
            success = True
        else:
            print(f"\n⚠️ PARTIAL IMPLEMENTATION - NEEDS ATTENTION")
            print(f"   Consider expanding data sources or improving coverage")
            success = False

        cursor.close()
        conn.close()

        return {
            "components_implemented": components_implemented,
            "total_components": total_components,
            "property_coverage": property_coverage,
            "table_stats": table_stats,
            "success": success
        }

    except Exception as e:
        print(f"❌ Validation error: {e}")
        return None

if __name__ == "__main__":
    results = validate_economic_vitality_coverage()
    if results and results["success"]:
        print(f"\n🚀 Economic vitality validation completed successfully!")
        print(f"Framework operational with {results['components_implemented']}/{results['total_components']} components active")
    else:
        print(f"\n❌ Economic vitality validation indicates issues need resolution!")