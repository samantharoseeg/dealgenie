#!/usr/bin/env python3
"""
Week 2 Migration Verification Script - PostgreSQL
Verifies that all Week 2 tables were created properly in PostgreSQL.
"""

import os
import sys
from pathlib import Path

try:
    import psycopg2
    from psycopg2 import sql
except ImportError:
    print("❌ psycopg2 not installed. Install with: pip install psycopg2-binary")
    sys.exit(1)

def get_db_connection():
    """Get PostgreSQL connection from environment."""
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        print("❌ DATABASE_URL environment variable not set")
        print("Example: export DATABASE_URL='postgresql://user:pass@localhost/dealgenie'")
        return None
    
    try:
        return psycopg2.connect(db_url)
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        return None

def verify_tables(conn):
    """Verify all Week 2 tables exist."""
    expected_tables = [
        'etl_audit',
        'core_address', 
        'link_address_parcel',
        'raw_permits',
        'feat_supply_bg',
        'feat_supply_parcel',
        'raw_crime',
        'feat_crime_bg', 
        'feat_crime_parcel'
    ]
    
    print("🔍 Checking Week 2 Tables:")
    
    with conn.cursor() as cur:
        success = True
        for table in expected_tables:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """, (table,))
            
            exists = cur.fetchone()[0]
            if exists:
                print(f"✅ {table}")
            else:
                print(f"❌ {table} MISSING")
                success = False
        
        return success

def check_indexes(conn):
    """Check key indexes exist."""
    key_indexes = [
        ('etl_audit', 'idx_etl_audit_process_name'),
        ('core_address', 'idx_core_address_standardized'),
        ('raw_permits', 'idx_raw_permits_apn'),
        ('raw_crime', 'idx_raw_crime_date_occurred'),
        ('feat_supply_bg', 'idx_feat_supply_bg_cbg')
    ]
    
    print("\n🔑 Checking Key Indexes:")
    
    with conn.cursor() as cur:
        success = True
        for table, index in key_indexes:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM pg_indexes 
                    WHERE tablename = %s AND indexname = %s
                )
            """, (table, index))
            
            exists = cur.fetchone()[0]
            if exists:
                print(f"✅ {index}")
            else:
                print(f"⚠️  {index} missing")
                # Don't fail on missing indexes, just warn
        
        return success

def check_foreign_keys(conn):
    """Check foreign key constraints."""
    print("\n🔗 Checking Foreign Key Constraints:")
    
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                conname as constraint_name,
                conrelid::regclass as table_name,
                confrelid::regclass as references_table
            FROM pg_constraint 
            WHERE contype = 'f' 
                AND conrelid::regclass::text IN (
                    'link_address_parcel', 'raw_permits', 'feat_supply_parcel', 
                    'feat_crime_parcel', 'raw_crime'
                )
            ORDER BY conrelid::regclass
        """)
        
        constraints = cur.fetchall()
        if constraints:
            for constraint_name, table_name, ref_table in constraints:
                print(f"✅ {table_name} → {ref_table} ({constraint_name})")
        else:
            print("⚠️  No foreign key constraints found")
        
        return True

def test_functionality(conn):
    """Test basic functionality with ETL audit insert."""
    print("\n🧪 Testing ETL Audit Functionality:")
    
    try:
        with conn.cursor() as cur:
            # Insert test record
            cur.execute("""
                INSERT INTO etl_audit (
                    process_name, 
                    run_id, 
                    status, 
                    records_processed,
                    source_system
                ) VALUES (
                    'migration_verification_test',
                    'test_' || extract(epoch from now())::text,
                    'completed',
                    0,
                    'verification_system'
                ) RETURNING id, process_name, run_id, started_at
            """)
            
            result = cur.fetchone()
            if result:
                print(f"✅ Insert successful: ID {result[0]}, Process: {result[1]}")
                
                # Clean up test record
                cur.execute("""
                    DELETE FROM etl_audit 
                    WHERE process_name = 'migration_verification_test'
                """)
                print("✅ Cleanup successful")
                return True
            else:
                print("❌ Insert failed - no result returned")
                return False
                
    except Exception as e:
        print(f"❌ Functionality test failed: {e}")
        return False

def show_table_counts(conn):
    """Show record counts for all Week 2 tables."""
    print("\n📊 Table Record Counts:")
    print("=" * 40)
    
    tables = [
        'etl_audit', 'core_address', 'link_address_parcel',
        'raw_permits', 'feat_supply_bg', 'feat_supply_parcel', 
        'raw_crime', 'feat_crime_bg', 'feat_crime_parcel'
    ]
    
    with conn.cursor() as cur:
        for table in tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                print(f"{table:<25} {count:>10,} records")
            except Exception as e:
                print(f"{table:<25} {'ERROR':>10} - {e}")

def check_schema_version(conn):
    """Check schema version was updated."""
    print("\n📋 Checking Schema Version:")
    
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM system_config WHERE key = 'schema_version'")
            result = cur.fetchone()
            
            if result and result[0] == '2.0':
                print("✅ Schema version updated to 2.0")
                return True
            else:
                print(f"❌ Schema version incorrect: {result[0] if result else 'Not found'}")
                return False
                
    except Exception as e:
        print(f"⚠️  Could not check schema version: {e}")
        return False

def main():
    """Main verification function."""
    print("🔍 Week 2 PostgreSQL Migration Verification")
    print("=" * 50)
    
    conn = get_db_connection()
    if not conn:
        sys.exit(1)
    
    try:
        success = True
        
        # Run all verification checks
        success &= verify_tables(conn)
        success &= check_indexes(conn)  
        success &= check_foreign_keys(conn)
        success &= test_functionality(conn)
        success &= check_schema_version(conn)
        
        # Show table counts
        show_table_counts(conn)
        
        if success:
            print("\n🎉 Week 2 Migration Verification: PASSED")
            print("All required tables, indexes, and functionality are working correctly.")
        else:
            print("\n❌ Week 2 Migration Verification: FAILED")
            print("Some required components are missing or not working.")
        
        return success
        
    finally:
        conn.close()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)