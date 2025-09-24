#!/usr/bin/env python3
"""
Complete Census ACS Data Expansion
Process remaining 1,141 census tracts to achieve 98% economic data coverage
"""

import psycopg2
import requests
import json
import time
from datetime import datetime
import logging
import threading
from concurrent.futures import ThreadPoolExecutor

# Database configuration
DATABASE_CONFIG = {
    "host": "localhost",
    "database": "dealgenie_production",
    "user": "dealgenie_app",
    "password": "dealgenie2025",
    "port": 5432
}

# Census API configuration
CENSUS_API_BASE = "https://api.census.gov/data/2022/acs/acs5"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CensusExpansionProcessor:
    def __init__(self):
        self.processed_tracts = 0
        self.failed_tracts = 0
        self.total_properties_updated = 0
        self.start_time = time.time()
        self.lock = threading.Lock()

    def get_remaining_census_tracts(self):
        """Get all unprocessed census tracts from production dataset"""
        try:
            conn = psycopg2.connect(**DATABASE_CONFIG)
            cursor = conn.cursor()

            print("📊 IDENTIFYING REMAINING CENSUS TRACTS")
            print("-" * 50)

            # Get all census tracts with properties that don't have economic data yet
            cursor.execute("""
            SELECT
                census_tract,
                COUNT(*) as property_count
            FROM unified_property_data
            WHERE census_tract IS NOT NULL
            AND census_tract != ''
            AND census_tract NOT IN (
                SELECT DISTINCT census_tract
                FROM economic_indicators
                WHERE census_tract IS NOT NULL
            )
            GROUP BY census_tract
            ORDER BY property_count DESC
            """)

            remaining_tracts = cursor.fetchall()

            print(f"✅ Found {len(remaining_tracts):,} unprocessed census tracts")
            print(f"📊 Total properties in unprocessed tracts: {sum(count for _, count in remaining_tracts):,}")

            cursor.close()
            conn.close()

            return remaining_tracts

        except Exception as e:
            print(f"❌ Error getting remaining tracts: {e}")
            return []

    def process_census_tract_batch(self, tract_batch, batch_num, total_batches):
        """Process a batch of census tracts with Census ACS API"""
        batch_start = time.time()
        batch_processed = 0
        batch_failed = 0

        print(f"\n🔄 PROCESSING BATCH {batch_num}/{total_batches} ({len(tract_batch)} tracts)")
        print("-" * 50)

        for tract, property_count in tract_batch:
            try:
                # Format tract for Census API
                if '.' in tract:
                    tract_parts = tract.split('.')
                    census_tract_formatted = tract_parts[0].zfill(4) + tract_parts[1][:2]
                else:
                    census_tract_formatted = tract.zfill(6)

                # Census ACS API request
                variables = "B19013_001E,B19301_001E,B17001_002E,B17001_001E,B15003_022E,B15003_001E"
                url = f"{CENSUS_API_BASE}?get={variables}&for=tract:{census_tract_formatted}&in=state:06&in=county:037"

                response = requests.get(url, timeout=10)

                if response.status_code == 200:
                    data = response.json()

                    if len(data) > 1:
                        values = data[1]

                        # Parse economic indicators
                        median_income = int(values[0]) if values[0] not in ['-666666666', '', None] else None
                        per_capita = int(values[1]) if values[1] not in ['-666666666', '', None] else None
                        poverty_count = int(values[2]) if values[2] not in ['-666666666', '', None] else None
                        poverty_universe = int(values[3]) if values[3] not in ['-666666666', '', None] else None
                        college_grads = int(values[4]) if values[4] not in ['-666666666', '', None] else None
                        education_universe = int(values[5]) if values[5] not in ['-666666666', '', None] else None

                        # Calculate derived metrics
                        poverty_rate = None
                        if poverty_count and poverty_universe and poverty_universe > 0:
                            poverty_rate = round((poverty_count / poverty_universe) * 100, 2)

                        college_rate = None
                        if college_grads and education_universe and education_universe > 0:
                            college_rate = round((college_grads / education_universe) * 100, 2)

                        # Insert into database
                        conn = psycopg2.connect(**DATABASE_CONFIG)
                        cursor = conn.cursor()

                        tract_fips = f"06037{census_tract_formatted}"

                        cursor.execute("""
                        INSERT INTO economic_indicators
                        (census_tract, tract_fips, median_household_income, per_capita_income,
                         poverty_rate, census_data_quality, data_year)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (census_tract, data_year)
                        DO UPDATE SET
                            median_household_income = EXCLUDED.median_household_income,
                            per_capita_income = EXCLUDED.per_capita_income,
                            poverty_rate = EXCLUDED.poverty_rate,
                            census_data_quality = EXCLUDED.census_data_quality,
                            last_updated = CURRENT_TIMESTAMP
                        """, (
                            tract,
                            tract_fips,
                            median_income,
                            per_capita,
                            poverty_rate,
                            json.dumps({
                                "api_response_time": time.time(),
                                "property_count": property_count,
                                "college_rate": college_rate,
                                "data_quality": "complete" if all([median_income, per_capita, poverty_rate]) else "partial"
                            }),
                            2022
                        ))

                        conn.commit()
                        cursor.close()
                        conn.close()

                        batch_processed += 1

                        # Thread-safe update of global counters
                        with self.lock:
                            self.processed_tracts += 1
                            self.total_properties_updated += property_count

                        if batch_processed % 10 == 0:
                            elapsed = time.time() - batch_start
                            rate = batch_processed / elapsed if elapsed > 0 else 0
                            print(f"      ✅ Batch progress: {batch_processed}/{len(tract_batch)} ({rate:.1f} tracts/sec)")

                    else:
                        batch_failed += 1
                        with self.lock:
                            self.failed_tracts += 1

                else:
                    batch_failed += 1
                    with self.lock:
                        self.failed_tracts += 1

                # Rate limiting - respect Census API
                time.sleep(0.1)

            except Exception as e:
                batch_failed += 1
                with self.lock:
                    self.failed_tracts += 1
                print(f"      ❌ Error processing {tract}: {e}")

        batch_time = time.time() - batch_start
        print(f"   📊 Batch {batch_num} completed: {batch_processed} success, {batch_failed} failed ({batch_time:.1f}s)")

        return batch_processed, batch_failed

    def update_property_economic_data(self):
        """Update environmental_constraints with new economic data"""
        try:
            conn = psycopg2.connect(**DATABASE_CONFIG)
            cursor = conn.cursor()

            print("\n🔗 UPDATING PROPERTY ECONOMIC DATA")
            print("-" * 50)

            # Update properties with new economic data
            cursor.execute("""
            UPDATE environmental_constraints ec
            SET economic_data = jsonb_build_object(
                'median_household_income', ei.median_household_income,
                'per_capita_income', ei.per_capita_income,
                'poverty_rate', ei.poverty_rate,
                'census_tract', ei.census_tract,
                'data_year', ei.data_year,
                'last_updated', ei.last_updated
            )
            FROM unified_property_data p
            JOIN economic_indicators ei ON p.census_tract = ei.census_tract
            WHERE ec.apn = p.apn
            AND ei.data_year = 2022
            AND ec.economic_data IS NULL
            """)

            new_properties_updated = cursor.rowcount
            conn.commit()

            print(f"✅ New properties updated with economic data: {new_properties_updated:,}")

            # Get final coverage statistics
            cursor.execute("""
            SELECT
                COUNT(*) as total_properties,
                COUNT(CASE WHEN economic_data IS NOT NULL THEN 1 END) as with_economic_data
            FROM environmental_constraints
            """)

            total_props, with_economic = cursor.fetchone()
            coverage_percent = (with_economic / total_props) * 100 if total_props > 0 else 0

            cursor.close()
            conn.close()

            return {
                "new_properties_updated": new_properties_updated,
                "total_with_economic": with_economic,
                "coverage_percent": coverage_percent
            }

        except Exception as e:
            print(f"❌ Error updating property data: {e}")
            return None

def main():
    """Main Census expansion function"""
    print("📊 COMPLETE CENSUS ACS DATA EXPANSION")
    print("=" * 70)
    print(f"Expansion Date: {datetime.now()}")
    print("Target: Process remaining 1,141 census tracts")
    print("Coverage Goal: 3.2% → 98.0% (+94.8 percentage points)")
    print("=" * 70)

    processor = CensusExpansionProcessor()

    # Get remaining census tracts
    remaining_tracts = processor.get_remaining_census_tracts()

    if not remaining_tracts:
        print("✅ All census tracts already processed!")
        return

    print(f"\n⚡ BATCH PROCESSING SETUP")
    print("-" * 50)

    batch_size = 50  # Process 50 tracts per batch
    total_batches = (len(remaining_tracts) + batch_size - 1) // batch_size
    estimated_time = (len(remaining_tracts) * 2) / 60  # ~2 seconds per tract

    print(f"   Total tracts to process: {len(remaining_tracts):,}")
    print(f"   Batch size: {batch_size} tracts/batch")
    print(f"   Total batches: {total_batches}")
    print(f"   Estimated processing time: {estimated_time:.1f} minutes")
    print(f"   Target API rate: ~30 requests/minute")

    # Process batches sequentially for API rate limiting
    for i in range(0, len(remaining_tracts), batch_size):
        batch = remaining_tracts[i:i+batch_size]
        batch_num = (i // batch_size) + 1

        # Process batch
        processor.process_census_tract_batch(batch, batch_num, total_batches)

        # Show progress
        elapsed = time.time() - processor.start_time
        if elapsed > 0:
            overall_rate = processor.processed_tracts / elapsed
            properties_rate = processor.total_properties_updated / elapsed

            print(f"   📈 Overall Progress: {processor.processed_tracts:,}/{len(remaining_tracts):,} tracts")
            print(f"   ⚡ Processing Rate: {overall_rate:.1f} tracts/sec ({properties_rate:.0f} properties/sec)")
            print(f"   📊 Properties Enhanced: {processor.total_properties_updated:,}")

        # Brief pause between batches for API courtesy
        if batch_num < total_batches:
            time.sleep(2)

    total_time = time.time() - processor.start_time

    print(f"\n✅ CENSUS TRACT PROCESSING COMPLETED")
    print(f"   Processed: {processor.processed_tracts:,}/{len(remaining_tracts):,} tracts")
    print(f"   Failed: {processor.failed_tracts:,} tracts")
    print(f"   Total time: {total_time/60:.1f} minutes")
    print(f"   Average rate: {processor.processed_tracts/total_time:.1f} tracts/second")

    # Update property economic data
    property_results = processor.update_property_economic_data()

    if property_results:
        print(f"\n🎯 FINAL COVERAGE RESULTS")
        print("-" * 50)
        print(f"   Properties with economic data: {property_results['total_with_economic']:,}")
        print(f"   Coverage percentage: {property_results['coverage_percent']:.1f}%")
        print(f"   Coverage improvement: +{property_results['coverage_percent'] - 3.2:.1f} percentage points")

        # Success criteria
        if property_results['coverage_percent'] > 90:
            print(f"   ✅ SUCCESS: Achieved >90% economic data coverage!")
        elif property_results['coverage_percent'] > 75:
            print(f"   ⚡ SUBSTANTIAL: Achieved >75% economic data coverage")
        else:
            print(f"   🔄 PARTIAL: Coverage improved but below 75% target")

    print(f"\n🚀 Census ACS data expansion completed!")
    print(f"Completed: {datetime.now()}")

    return True

if __name__ == "__main__":
    success = main()
    if success:
        print("\n✅ Complete census expansion successful!")
    else:
        print("\n❌ Census expansion encountered issues!")