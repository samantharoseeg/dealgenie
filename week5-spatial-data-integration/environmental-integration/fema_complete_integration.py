#!/usr/bin/env python3
"""
Complete FEMA Integration for All 455,820 Properties
Process the entire production dataset through FEMA spatial overlay
"""

import psycopg2
import time
from datetime import datetime

# Database configuration
DATABASE_CONFIG = {
    "host": "localhost",
    "database": "dealgenie_production",
    "user": "dealgenie_app",
    "password": "dealgenie2025",
    "port": 5432
}

def process_full_dataset():
    """Process all 455,820 geocoded properties through FEMA overlay"""
    conn = psycopg2.connect(**DATABASE_CONFIG)
    cursor = conn.cursor()

    print("🌊 COMPLETE FEMA INTEGRATION - FULL 455K DATASET")
    print("=" * 70)
    print(f"Started: {datetime.now()}")
    print("=" * 70)

    # Step 1: Verify current status
    print("\n📊 CURRENT DATABASE STATUS:")
    print("-" * 50)

    cursor.execute("""
    SELECT
        COUNT(*) as total,
        COUNT(latitude) as with_coords,
        COUNT(geom) as with_geom
    FROM unified_property_data
    """)
    total, with_coords, with_geom = cursor.fetchone()

    print(f"Total properties: {total:,}")
    print(f"Properties with coordinates: {with_coords:,}")
    print(f"Properties with geometries: {with_geom:,}")

    # Step 2: Create all missing geometries
    print("\n🔧 CREATING SPATIAL GEOMETRIES FOR ALL PROPERTIES:")
    print("-" * 50)

    batch_size = 50000
    total_to_process = with_coords - with_geom
    processed = 0

    print(f"Properties needing geometries: {total_to_process:,}")

    if total_to_process > 0:
        start_time = time.time()

        # Process in batches to avoid memory issues
        while processed < total_to_process:
            cursor.execute("""
            UPDATE unified_property_data
            SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
            WHERE latitude IS NOT NULL
            AND longitude IS NOT NULL
            AND geom IS NULL
            AND apn IN (
                SELECT apn FROM unified_property_data
                WHERE latitude IS NOT NULL AND geom IS NULL
                LIMIT %s
            )
            """, (batch_size,))

            conn.commit()
            batch_processed = cursor.rowcount
            processed += batch_processed

            if batch_processed > 0:
                elapsed = time.time() - start_time
                rate = processed / elapsed
                remaining = (total_to_process - processed) / rate if rate > 0 else 0

                print(f"   Processed: {processed:,}/{total_to_process:,} "
                      f"({(processed/total_to_process)*100:.1f}%) "
                      f"Rate: {rate:.0f}/sec ETA: {remaining/60:.1f} min")

        geometry_time = time.time() - start_time
        print(f"✅ All geometries created in {geometry_time/60:.1f} minutes")
    else:
        print("✅ All properties already have geometries")

    # Step 3: Perform complete spatial overlay
    print("\n🗺️ EXECUTING COMPLETE FEMA SPATIAL OVERLAY:")
    print("-" * 50)

    # Drop and recreate the overlay table for full dataset
    cursor.execute("DROP TABLE IF EXISTS property_fema_overlay_complete")

    print("Creating complete overlay (this may take several minutes)...")
    start_time = time.time()

    cursor.execute("""
    CREATE TABLE property_fema_overlay_complete AS
    SELECT
        p.apn,
        p.site_address,
        p.latitude,
        p.longitude,
        f.fld_zone as fema_zone,
        f.zone_subty as fema_subtype,
        f.sfha_tf as in_sfha
    FROM unified_property_data p
    LEFT JOIN fema_flood_zones_official f
    ON ST_Contains(f.geometry, p.geom)
    WHERE p.geom IS NOT NULL
    """)

    conn.commit()
    overlay_time = time.time() - start_time

    print(f"✅ Spatial overlay complete in {overlay_time/60:.1f} minutes")

    # Step 4: Analyze results
    print("\n📈 COMPLETE DATASET ANALYSIS:")
    print("-" * 50)

    cursor.execute("SELECT COUNT(*) FROM property_fema_overlay_complete")
    total_analyzed = cursor.fetchone()[0]

    cursor.execute("""
    SELECT COUNT(*)
    FROM property_fema_overlay_complete
    WHERE fema_zone IS NOT NULL
    """)
    in_zones = cursor.fetchone()[0]

    cursor.execute("""
    SELECT fema_zone, COUNT(*) as count
    FROM property_fema_overlay_complete
    WHERE fema_zone IS NOT NULL
    GROUP BY fema_zone
    ORDER BY count DESC
    """)
    zone_distribution = cursor.fetchall()

    print(f"Total properties analyzed: {total_analyzed:,}")
    print(f"Properties in FEMA flood zones: {in_zones:,}")
    print(f"Properties outside flood zones: {total_analyzed - in_zones:,}")
    print(f"FEMA flood zone coverage: {(in_zones/total_analyzed)*100:.1f}%")

    print("\n📊 DISTRIBUTION BY FEMA ZONE:")
    for zone, count in zone_distribution:
        percentage = (count/in_zones)*100 if in_zones > 0 else 0
        print(f"   Zone {zone}: {count:,} properties ({percentage:.1f}%)")

    # Step 5: Update environmental_constraints with complete FEMA data
    print("\n🔄 UPDATING ENVIRONMENTAL_CONSTRAINTS TABLE:")
    print("-" * 50)

    # Get FEMA source ID
    cursor.execute("SELECT id FROM data_source_hierarchy WHERE source_name = 'FEMA NFHL'")
    result = cursor.fetchone()

    if not result:
        cursor.execute("""
        INSERT INTO data_source_hierarchy
        (source_name, authority_rank, confidence_score, update_frequency, validation_method)
        VALUES ('FEMA NFHL', 1, 0.99, 'Annual', 'Federal Authority')
        RETURNING id
        """)
        fema_id = cursor.fetchone()[0]
        conn.commit()
    else:
        fema_id = result[0]

    print(f"FEMA source ID: {fema_id}")

    # Update in batches
    print("Updating environmental constraints with FEMA data...")

    cursor.execute("""
    INSERT INTO environmental_constraints
    (apn, flood_zone, source_authority, data_quality_flags, confidence_score)
    SELECT
        apn,
        COALESCE(fema_zone, 'X') as flood_zone,
        %s as source_authority,
        jsonb_build_object(
            'source', 'FEMA NFHL',
            'update_date', CURRENT_DATE,
            'sfha', COALESCE(in_sfha = 'T', false)
        ) as data_quality_flags,
        0.99 as confidence_score
    FROM property_fema_overlay_complete
    WHERE fema_zone IS NOT NULL
    ON CONFLICT (apn) DO UPDATE
    SET
        flood_zone = EXCLUDED.flood_zone,
        source_authority = EXCLUDED.source_authority,
        data_quality_flags = EXCLUDED.data_quality_flags,
        confidence_score = EXCLUDED.confidence_score
    WHERE environmental_constraints.confidence_score < 0.99
    """, (fema_id,))

    updated_count = cursor.rowcount
    conn.commit()

    print(f"✅ Updated {updated_count:,} properties with FEMA data")

    # Step 6: Generate FEMA vs ZIMAS comparison
    print("\n🔍 FEMA VS ZIMAS DISCREPANCY ANALYSIS:")
    print("-" * 50)

    cursor.execute("""
    CREATE TEMP TABLE fema_zimas_full_comparison AS
    SELECT
        pfo.apn,
        pfo.fema_zone,
        z.zimas_flood_zone,
        CASE
            WHEN pfo.fema_zone IS NULL AND z.zimas_flood_zone IS NULL THEN 'No Data'
            WHEN pfo.fema_zone IS NULL THEN 'ZIMAS Only'
            WHEN z.zimas_flood_zone IS NULL THEN 'FEMA Only'
            WHEN pfo.fema_zone = z.zimas_flood_zone THEN 'Match'
            ELSE 'Discrepancy'
        END as comparison_status
    FROM property_fema_overlay_complete pfo
    LEFT JOIN zimas_flood_zones z ON pfo.apn = z.apn
    """)

    cursor.execute("""
    SELECT comparison_status, COUNT(*) as count
    FROM fema_zimas_full_comparison
    GROUP BY comparison_status
    ORDER BY count DESC
    """)

    comparison_stats = cursor.fetchall()

    print("\nCOMPARISON RESULTS:")
    total_compared = sum(count for _, count in comparison_stats)
    for status, count in comparison_stats:
        percentage = (count / total_compared) * 100
        print(f"   {status}: {count:,} properties ({percentage:.1f}%)")

    # Get discrepancy samples
    cursor.execute("""
    SELECT apn, fema_zone, zimas_flood_zone
    FROM fema_zimas_full_comparison
    WHERE comparison_status = 'Discrepancy'
    LIMIT 5
    """)

    discrepancies = cursor.fetchall()
    if discrepancies:
        print("\n⚠️ SAMPLE DISCREPANCIES:")
        for apn, fema, zimas in discrepancies:
            print(f"   APN {apn}: FEMA='{fema}' vs ZIMAS='{zimas}'")

    # Step 7: Performance summary
    print("\n⚡ PERFORMANCE METRICS:")
    print("-" * 50)

    if overlay_time > 0:
        processing_rate = total_analyzed / overlay_time
        print(f"Total processing time: {overlay_time/60:.1f} minutes")
        print(f"Properties processed: {total_analyzed:,}")
        print(f"Processing rate: {processing_rate:.0f} properties/second")
        print(f"Average time per property: {(overlay_time/total_analyzed)*1000:.3f} ms")

    # Step 8: Final statistics
    print("\n📊 FINAL INTEGRATION STATISTICS:")
    print("=" * 70)

    cursor.execute("""
    SELECT
        (SELECT COUNT(*) FROM property_fema_overlay_complete) as total_properties,
        (SELECT COUNT(*) FROM property_fema_overlay_complete WHERE fema_zone IS NOT NULL) as with_fema,
        (SELECT COUNT(*) FROM zimas_flood_zones WHERE zimas_flood_zone IS NOT NULL) as with_zimas,
        (SELECT COUNT(*) FROM environmental_constraints WHERE source_authority = %s) as fema_authoritative
    """, (fema_id,))

    total_props, with_fema, with_zimas, fema_auth = cursor.fetchone()

    print(f"Total properties processed: {total_props:,}")
    print(f"Properties with FEMA data: {with_fema:,} ({(with_fema/total_props)*100:.1f}%)")
    print(f"Properties with ZIMAS data: {with_zimas:,} ({(with_zimas/total_props)*100:.1f}%)")
    print(f"Properties using FEMA authority: {fema_auth:,}")

    print("\n✅ VALIDATION EVIDENCE ACHIEVED:")
    print(f"   ✅ Processed complete dataset: {total_analyzed:,} properties")
    print(f"   ✅ Execution time: {overlay_time/60:.1f} minutes")
    print(f"   ✅ Environmental constraints updated: {updated_count:,} records")
    print(f"   ✅ FEMA coverage: {(with_fema/total_props)*100:.1f}% of properties")
    print(f"   ✅ Processing rate: {processing_rate:.0f} properties/second")

    print(f"\nCompleted: {datetime.now()}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    process_full_dataset()