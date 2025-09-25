#!/usr/bin/env python3
"""
Week 2 Migration Verification Script
Verifies that all Week 2 tables were created properly with correct structure.
"""

import sqlite3
import sys
from pathlib import Path

def verify_migration():
    """Verify Week 2 migration was applied correctly."""
    db_path = "data/dealgenie.db"
    
    if not Path(db_path).exists():
        print("❌ Database not found at data/dealgenie.db")
        return False
    
    print("🔍 Verifying Week 2 Migration...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Expected Week 2 tables
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
    
    success = True
    
    # Check each table exists
    for table in expected_tables:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
        if cursor.fetchone():
            print(f"✅ Table '{table}' exists")
        else:
            print(f"❌ Table '{table}' missing")
            success = False
    
    # Check schema version
    cursor.execute("SELECT value FROM system_config WHERE key='schema_version'")
    result = cursor.fetchone()
    if result and result[0] == '2.0':
        print("✅ Schema version updated to 2.0")
    else:
        print("❌ Schema version not updated correctly")
        success = False
    
    # Check some key indexes exist
    key_indexes = [
        'idx_etl_audit_process_name',
        'idx_core_address_standardized', 
        'idx_raw_permits_apn',
        'idx_feat_supply_bg_cbg',
        'idx_raw_crime_date_occurred'
    ]
    
    for index in key_indexes:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name=?", (index,))
        if cursor.fetchone():
            print(f"✅ Index '{index}' exists")
        else:
            print(f"⚠️  Index '{index}' missing (may be normal)")
    
    # Test a sample insert into etl_audit
    try:
        cursor.execute('''
            INSERT INTO etl_audit (process_name, run_id, status, records_processed)
            VALUES ('migration_test', 'test_001', 'completed', 0)
        ''')
        conn.commit()
        print("✅ ETL audit table insert test successful")
        
        # Clean up test record
        cursor.execute("DELETE FROM etl_audit WHERE run_id = 'test_001'")
        conn.commit()
        
    except Exception as e:
        print(f"❌ ETL audit table insert test failed: {e}")
        success = False
    
    conn.close()
    
    if success:
        print("\n🎉 Week 2 Migration Verification: PASSED")
        print("All required tables, indexes, and functionality are working correctly.")
        return True
    else:
        print("\n❌ Week 2 Migration Verification: FAILED") 
        print("Some required components are missing or not working.")
        return False

def show_table_counts():
    """Show record counts for all tables."""
    print("\n📊 Current Table Record Counts:")
    print("=" * 40)
    
    conn = sqlite3.connect("data/dealgenie.db")
    cursor = conn.cursor()
    
    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cursor.fetchall()]
    
    for table in tables:
        try:
            safe = table.replace('"', '""')
            cursor.execute(f'SELECT COUNT(*) FROM "{safe}"')
            count = cursor.fetchone()[0]
            print(f"{table:<25} {count:>10,} records")
        except Exception as e:
            print(f"{table:<25} {'ERROR':>10}")
    
    conn.close()

if __name__ == "__main__":
    success = verify_migration()
    show_table_counts()
    
    sys.exit(0 if success else 1)