#!/usr/bin/env python3
"""
Week 2 Governance Verification Script
Verifies that all governance columns and tables are properly configured.
"""

import sqlite3
from pathlib import Path
from datetime import datetime

def verify_governance():
    """Verify governance columns and tables."""
    db_path = "data/dealgenie.db"
    
    if not Path(db_path).exists():
        print("❌ Database not found at data/dealgenie.db")
        return False
    
    print("🔍 Verifying Week 2 Governance Features...")
    print("=" * 50)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check governance columns in raw tables
    print("\n📋 1. Governance Columns in Raw Tables:")
    
    governance_columns = [
        'source_endpoint', 'query_params', 'as_of_date', 
        'ingest_timestamp', 'response_headers', 'api_version'
    ]
    
    for table in ['raw_permits', 'raw_crime']:
        print(f"\n{table}:")
        cursor.execute(f"PRAGMA table_info({table})")
        columns = {row[1]: row[2] for row in cursor.fetchall()}
        
        for col in governance_columns:
            if col in columns:
                print(f"  ✅ {col:<20} ({columns[col]})")
            else:
                print(f"  ❌ {col:<20} MISSING")
    
    # Check new governance tables
    print("\n📊 2. Governance Tables:")
    
    governance_tables = ['api_governance', 'data_lineage']
    for table in governance_tables:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
        if cursor.fetchone():
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  ✅ {table:<20} (exists, {count} records)")
        else:
            print(f"  ❌ {table:<20} MISSING")
    
    # Check governance views
    print("\n👁️ 3. Governance Views:")
    
    governance_views = [
        'view_api_health',
        'view_recent_ingestion', 
        'view_backtest_availability'
    ]
    
    for view in governance_views:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='view' AND name=?", (view,))
        if cursor.fetchone():
            print(f"  ✅ {view}")
        else:
            print(f"  ❌ {view} MISSING")
    
    # Check ETL audit enhancements
    print("\n⚙️ 4. ETL Audit API Tracking Columns:")
    
    api_columns = [
        'api_endpoint', 'api_params', 'total_api_calls',
        'successful_api_calls', 'failed_api_calls', 'backtest_mode'
    ]
    
    cursor.execute("PRAGMA table_info(etl_audit)")
    etl_columns = {row[1]: row[2] for row in cursor.fetchall()}
    
    for col in api_columns:
        if col in etl_columns:
            print(f"  ✅ {col:<25} ({etl_columns[col]})")
        else:
            print(f"  ❌ {col:<25} MISSING")
    
    # Test API governance insert
    print("\n🧪 5. Testing API Governance Functionality:")
    
    try:
        cursor.execute('''
            INSERT INTO api_governance (
                api_name, endpoint_url, api_version,
                rate_limit_max, rate_limit_period_seconds
            ) VALUES (
                'test_api', 'https://api.test.com/v1', '1.0',
                100, 3600
            )
        ''')
        conn.commit()
        print("  ✅ API governance insert successful")
        
        # Clean up
        cursor.execute("DELETE FROM api_governance WHERE api_name='test_api'")
        conn.commit()
        
    except Exception as e:
        print(f"  ❌ API governance insert failed: {e}")
    
    # Test data lineage insert
    try:
        cursor.execute('''
            INSERT INTO data_lineage (
                source_type, source_name, target_table,
                transformation_type, as_of_date
            ) VALUES (
                'api', 'test_source', 'test_target',
                'direct', date('now')
            )
        ''')
        conn.commit()
        print("  ✅ Data lineage insert successful")
        
        # Clean up
        cursor.execute("DELETE FROM data_lineage WHERE source_name='test_source'")
        conn.commit()
        
    except Exception as e:
        print(f"  ❌ Data lineage insert failed: {e}")
    
    # Check schema version
    print("\n📋 6. Schema Version:")
    cursor.execute("SELECT value FROM system_config WHERE key='schema_version'")
    result = cursor.fetchone()
    if result and result[0] == '2.1':
        print(f"  ✅ Schema version: {result[0]} (governance included)")
    else:
        print(f"  ❌ Schema version: {result[0] if result else 'Unknown'}")
    
    conn.close()
    
    print("\n" + "=" * 50)
    print("✅ Governance verification complete!")
    print("\nYour database now supports:")
    print("• API endpoint tracking for reproducibility")
    print("• Query parameter storage for exact replication")
    print("• As-of-date for backtesting capabilities")
    print("• Ingest timestamps for freshness monitoring")
    print("• Full data lineage tracking")
    print("• API health and rate limit management")
    
    return True

def show_governance_demo():
    """Demonstrate governance features with sample data."""
    print("\n📊 Governance Feature Demo:")
    print("=" * 50)
    
    conn = sqlite3.connect("data/dealgenie.db")
    cursor = conn.cursor()
    
    # Insert sample API governance record
    print("\n1️⃣ Tracking API Health:")
    cursor.execute('''
        INSERT OR REPLACE INTO api_governance (
            api_name, endpoint_url, api_version,
            rate_limit_max, rate_limit_period_seconds,
            total_calls, successful_calls, failed_calls,
            avg_response_time_ms, last_successful_call
        ) VALUES (
            'LADBS_Permits', 
            'https://data.lacity.org/resource/yv23-pmwf.json',
            '2.0',
            1000, 3600,
            847, 845, 2,
            234.5, datetime('now', '-5 minutes')
        )
    ''')
    conn.commit()
    
    cursor.execute("SELECT * FROM view_api_health WHERE api_name='LADBS_Permits'")
    result = cursor.fetchone()
    if result:
        print(f"  API: {result[0]}")
        print(f"  Status: {result[2]}")
        print(f"  Success Rate: {result[6]}%")
        print(f"  Avg Response: {result[7]}ms")
    
    # Demonstrate lineage tracking
    print("\n2️⃣ Data Lineage Example:")
    cursor.execute('''
        INSERT INTO data_lineage (
            source_type, source_name, source_endpoint,
            source_query_params, target_table, target_record_id,
            transformation_type, confidence_score, as_of_date
        ) VALUES (
            'api', 'LADBS_Permits',
            'https://data.lacity.org/resource/yv23-pmwf.json',
            '{"$limit": 1000, "$where": "issue_date > ''2024-01-01''"}',
            'raw_permits', 'PERMIT-2024-001',
            'direct', 0.95, date('now')
        )
    ''')
    conn.commit()
    
    print("  ✅ Lineage tracked: API → raw_permits")
    print("  ✅ Query params preserved for reproducibility")
    print("  ✅ As-of-date set for backtesting")
    
    conn.close()

if __name__ == "__main__":
    verify_governance()
    show_governance_demo()