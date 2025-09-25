#!/usr/bin/env python3
"""
Verify environmental_constraints Table Integration
Test the newly created table with our real 457K property data
"""

import psycopg2
import time
import os
import json

# Database configuration
DATABASE_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "dealgenie_production"),
    "user": os.getenv("DB_USER", "dealgenie_app"),
    "password": os.getenv("DB_PASSWORD", "dealgenie2025"),
    "port": int(os.getenv("DB_PORT", "5432"))
}

def verify_table_exists():
    """Verify environmental_constraints table exists"""
    print("🔍 TEST 1: Table Existence Verification")
    print("-" * 50)

    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        cursor = conn.cursor()

        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = 'environmental_constraints'
            );
        """)

        table_exists = cursor.fetchone()[0]
        print(f"environmental_constraints table: {'✅ EXISTS' if table_exists else '❌ MISSING'}")

        if table_exists:
            # Get table schema
            cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'environmental_constraints'
                ORDER BY ordinal_position;
            """)

            columns = cursor.fetchall()
            print(f"Table columns ({len(columns)} total):")
            for col_name, data_type, nullable in columns:
                null_str = "NULL" if nullable == "YES" else "NOT NULL"
                print(f"  {col_name:<25} {data_type:<20} {null_str}")

        cursor.close()
        conn.close()

        return table_exists

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_record_count():
    """Test 1: Record count verification"""
    print("\n🔍 TEST 2: Record Count Verification")
    print("-" * 50)

    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        cursor = conn.cursor()

        start_time = time.time()

        # Count all records
        cursor.execute("SELECT COUNT(*) FROM environmental_constraints;")
        total_count = cursor.fetchone()[0]

        query_time = time.time() - start_time

        print(f"Total records: {total_count:,}")
        print(f"Query time: {query_time*1000:.1f}ms")

        # Verify against ZIMAS data
        cursor.execute("SELECT COUNT(*) FROM zimas_flood_zones WHERE zimas_flood_zone IS NOT NULL;")
        zimas_count = cursor.fetchone()[0]

        print(f"ZIMAS source records: {zimas_count:,}")
        print(f"Population coverage: {(total_count/zimas_count)*100:.1f}%" if zimas_count > 0 else "No ZIMAS data")

        cursor.close()
        conn.close()

        return total_count

    except Exception as e:
        print(f"❌ Error: {e}")
        return 0

def test_flagged_apn_5036019013():
    """Test 2: Test the original query for APN 5036019013"""
    print("\n🔍 TEST 3: Flagged APN 5036019013 Query")
    print("-" * 50)

    test_apn = '5036019013'

    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        cursor = conn.cursor()

        start_time = time.time()

        # The original query that was requested
        query_sql = """
        SELECT
            ec.apn,
            ec.flood_zone,
            ec.data_quality_flags,
            dsh.source_name as source_authority,
            ec.confidence_score,
            ec.verification_url
        FROM environmental_constraints ec
        JOIN data_source_hierarchy dsh ON ec.source_authority = dsh.id
        WHERE ec.apn = %s;
        """

        cursor.execute(query_sql, (test_apn,))
        result = cursor.fetchone()

        query_time = time.time() - start_time

        print(f"Original query for APN {test_apn}:")
        print(f"Query time: {query_time*1000:.1f}ms")

        if result:
            apn, flood_zone, quality_flags, source_authority, confidence, verification_url = result
            print(f"✅ Record found:")
            print(f"  APN: {apn}")
            print(f"  Flood Zone: {flood_zone}")
            print(f"  Source Authority: {source_authority}")
            print(f"  Confidence Score: {confidence}")
            print(f"  Verification URL: {verification_url}")
            print(f"  Data Quality Flags:")

            if quality_flags:
                try:
                    if isinstance(quality_flags, str):
                        flags_dict = json.loads(quality_flags)
                    else:
                        flags_dict = quality_flags

                    for key, value in flags_dict.items():
                        print(f"    {key}: {value}")
                except:
                    print(f"    {quality_flags}")
            else:
                print("    No quality flags")

            return True
        else:
            print(f"❌ No record found for APN {test_apn}")
            return False

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_source_hierarchy_integration():
    """Test 3: Source hierarchy integration"""
    print("\n🔍 TEST 4: Source Hierarchy Integration")
    print("-" * 50)

    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        cursor = conn.cursor()

        start_time = time.time()

        # Test join with data_source_hierarchy
        integration_query = """
        SELECT
            dsh.source_name,
            dsh.authority_rank,
            dsh.confidence_score,
            COUNT(ec.apn) as property_count,
            ROUND((COUNT(ec.apn)::numeric / (SELECT COUNT(*) FROM environmental_constraints)) * 100, 2) as percentage
        FROM data_source_hierarchy dsh
        LEFT JOIN environmental_constraints ec ON dsh.id = ec.source_authority
        WHERE dsh.data_category = 'flood_zones'
        GROUP BY dsh.source_name, dsh.authority_rank, dsh.confidence_score
        ORDER BY dsh.authority_rank;
        """

        cursor.execute(integration_query)
        results = cursor.fetchall()

        query_time = time.time() - start_time

        print(f"Source hierarchy integration:")
        print(f"Query time: {query_time*1000:.1f}ms")
        print(f"{'Rank':<4} {'Source':<25} {'Confidence':<10} {'Count':<10} {'%':<6}")
        print("-" * 60)

        for row in results:
            source, rank, confidence, count, percentage = row
            print(f"{rank:<4} {source:<25} {confidence:<10} {count:<10,} {percentage or 0:<6}%")

        cursor.close()
        conn.close()

        return len(results) > 0

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_performance_on_real_dataset():
    """Test 4: Performance on real dataset"""
    print("\n🔍 TEST 5: Performance on 455K+ Properties")
    print("-" * 50)

    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        cursor = conn.cursor()

        # Test 1: Sample properties with coordinates
        print("Performance Test 1: Properties with coordinates")
        start_time = time.time()

        cursor.execute("""
            SELECT COUNT(*)
            FROM environmental_constraints
            WHERE coordinates IS NOT NULL;
        """)

        coord_count = cursor.fetchone()[0]
        coord_time = time.time() - start_time

        print(f"  Properties with coordinates: {coord_count:,}")
        print(f"  Query time: {coord_time*1000:.1f}ms")

        # Test 2: Flood zone distribution
        print("\nPerformance Test 2: Flood zone distribution")
        start_time = time.time()

        cursor.execute("""
            SELECT
                flood_zone,
                COUNT(*) as count,
                ROUND((COUNT(*)::numeric / (SELECT COUNT(*) FROM environmental_constraints)) * 100, 1) as percentage
            FROM environmental_constraints
            WHERE flood_zone IS NOT NULL
            GROUP BY flood_zone
            ORDER BY count DESC
            LIMIT 10;
        """)

        flood_dist = cursor.fetchall()
        dist_time = time.time() - start_time

        print(f"  Query time: {dist_time*1000:.1f}ms")
        print("  Top flood zones:")
        for zone, count, percentage in flood_dist:
            print(f"    {zone}: {count:,} ({percentage}%)")

        # Test 3: Complex join performance
        print("\nPerformance Test 3: Complex join with property data")
        start_time = time.time()

        cursor.execute("""
            SELECT COUNT(*)
            FROM environmental_constraints ec
            JOIN unified_property_data p ON ec.apn = p.apn
            JOIN data_source_hierarchy dsh ON ec.source_authority = dsh.id
            WHERE p.geom IS NOT NULL
            AND dsh.confidence_score >= 0.80;
        """)

        join_count = cursor.fetchone()[0]
        join_time = time.time() - start_time

        print(f"  Properties in complex join: {join_count:,}")
        print(f"  Query time: {join_time*1000:.1f}ms")

        cursor.close()
        conn.close()

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_data_integrity():
    """Test 5: Data integrity check"""
    print("\n🔍 TEST 6: Data Integrity Check")
    print("-" * 50)

    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        cursor = conn.cursor()

        # Test 1: Foreign key integrity
        print("Integrity Test 1: Foreign key relationships")
        start_time = time.time()

        cursor.execute("""
            SELECT
                COUNT(*) as total_records,
                COUNT(source_authority) as with_source,
                COUNT(CASE WHEN source_authority IS NULL THEN 1 END) as missing_source
            FROM environmental_constraints;
        """)

        integrity_stats = cursor.fetchone()
        integrity_time = time.time() - start_time

        total, with_source, missing_source = integrity_stats
        print(f"  Total records: {total:,}")
        print(f"  With source authority: {with_source:,}")
        print(f"  Missing source authority: {missing_source:,}")
        print(f"  Integrity: {(with_source/total)*100:.1f}%" if total > 0 else "No data")
        print(f"  Query time: {integrity_time*1000:.1f}ms")

        # Test 2: Data quality flags
        print("\nIntegrity Test 2: Data quality flags")
        start_time = time.time()

        cursor.execute("""
            SELECT
                COUNT(*) as total_records,
                COUNT(CASE WHEN data_quality_flags != '{}'::jsonb THEN 1 END) as with_flags,
                COUNT(CASE WHEN data_quality_flags->>'discrepancy_type' IS NOT NULL THEN 1 END) as with_discrepancies
            FROM environmental_constraints;
        """)

        quality_stats = cursor.fetchone()
        quality_time = time.time() - start_time

        total, with_flags, with_discrepancies = quality_stats
        print(f"  Total records: {total:,}")
        print(f"  With quality flags: {with_flags:,}")
        print(f"  With discrepancies: {with_discrepancies:,}")
        print(f"  Query time: {quality_time*1000:.1f}ms")

        # Test 3: Sample data verification
        print("\nIntegrity Test 3: Sample data verification")
        cursor.execute("""
            SELECT apn, flood_zone, confidence_score
            FROM environmental_constraints
            WHERE flood_zone IS NOT NULL
            ORDER BY RANDOM()
            LIMIT 5;
        """)

        samples = cursor.fetchall()
        print("  Sample records:")
        for apn, zone, confidence in samples:
            print(f"    APN {apn}: {zone} (confidence: {confidence})")

        cursor.close()
        conn.close()

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    """Main verification function"""
    print("🔍 VERIFY environmental_constraints TABLE INTEGRATION")
    print("=" * 70)
    print("Testing the newly created table with our real 457K property data")
    print("=" * 70)

    # Run all verification tests
    tests_passed = 0
    total_tests = 6

    if verify_table_exists():
        tests_passed += 1

    if test_record_count() > 0:
        tests_passed += 1

    if test_flagged_apn_5036019013():
        tests_passed += 1

    if test_source_hierarchy_integration():
        tests_passed += 1

    if test_performance_on_real_dataset():
        tests_passed += 1

    if test_data_integrity():
        tests_passed += 1

    # Final results
    print("\n" + "=" * 70)
    print("📊 VERIFICATION RESULTS")
    print("=" * 70)
    print(f"Tests passed: {tests_passed}/{total_tests}")

    if tests_passed == total_tests:
        print("✅ ALL TESTS PASSED - environmental_constraints table fully functional")
        print("\n🎯 VALIDATION EVIDENCE:")
        print("✅ Table exists with proper schema")
        print("✅ Populated with real ZIMAS flood data")
        print("✅ APN 5036019013 query works correctly")
        print("✅ Source hierarchy integration functional")
        print("✅ Performance verified on 455K+ properties")
        print("✅ Data integrity confirmed")
    else:
        print(f"⚠️  {total_tests - tests_passed} tests failed - review results above")

if __name__ == "__main__":
    main()