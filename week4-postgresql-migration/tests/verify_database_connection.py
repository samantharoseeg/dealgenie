#!/usr/bin/env python3
"""
VERIFY DATABASE CONNECTION - MANDATORY CHECK
"""

import psycopg2

POSTGRESQL_CONFIG = {
    "host": "localhost",
    "database": "dealgenie_production",
    "user": "dealgenie_app",
    "password": "dealgenie2025",
    "port": 5432
}

def verify_database():
    print("1. MANDATORY DATABASE VERIFICATION:")
    print("-" * 50)

    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor()

        print("Command: SELECT COUNT(*) FROM unified_property_data;")
        cursor.execute("SELECT COUNT(*) FROM unified_property_data;")
        count = cursor.fetchone()[0]
        print(f"Result: {count}")

        if count == 457768:
            print("✅ CONFIRMED: 457,768 properties in database")
        else:
            print(f"❌ ERROR: Expected 457,768, found {count}")

        cursor.close()
        conn.close()
        return count == 457768
    except Exception as e:
        print(f"❌ DATABASE CONNECTION ERROR: {e}")
        return False

if __name__ == "__main__":
    success = verify_database()
    exit(0 if success else 1)