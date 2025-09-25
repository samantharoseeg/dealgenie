#!/usr/bin/env python3
"""
DealGenie Database Migration Runner
Applies database migrations in order and tracks applied migrations.
Supports both SQLite and PostgreSQL databases.
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import hashlib

# Try to import both database drivers
try:
    import sqlite3
    SQLITE_AVAILABLE = True
except ImportError:
    SQLITE_AVAILABLE = False

try:
    import psycopg2
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

class MigrationRunner:
    def __init__(self, db_config=None, migrations_dir: str = "db/migrations"):
        """Initialize migration runner with database configuration.
        
        Args:
            db_config: Either a path string for SQLite or DATABASE_URL for PostgreSQL
            migrations_dir: Directory containing migration files
        """
        self.migrations_dir = Path(migrations_dir)
        
        # Determine database type and configuration
        if db_config is None:
            # Default to SQLite
            db_config = "data/dealgenie.db"
        
        if db_config.startswith(('postgresql://', 'postgres://')):
            if not POSTGRES_AVAILABLE:
                raise ImportError("psycopg2 required for PostgreSQL support")
            self.db_type = 'postgresql'
            self.db_url = db_config
        else:
            if not SQLITE_AVAILABLE:
                raise ImportError("sqlite3 required for SQLite support")
            self.db_type = 'sqlite'
            self.db_path = db_config
        
        self.ensure_migration_tracking()
    
    def get_connection(self):
        """Get database connection based on database type."""
        if self.db_type == 'postgresql':
            return psycopg2.connect(self.db_url)
        else:
            return sqlite3.connect(self.db_path)
    
    def ensure_migration_tracking(self):
        """Ensure migration tracking table exists."""
        if self.db_type == 'postgresql':
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS schema_migrations (
                            id SERIAL PRIMARY KEY,
                            migration_file VARCHAR(255) NOT NULL UNIQUE,
                            applied_at TIMESTAMPTZ DEFAULT NOW(),
                            execution_time_ms INTEGER,
                            success BOOLEAN DEFAULT TRUE,
                            error_message TEXT,
                            checksum VARCHAR(64)
                        )
                    ''')
                    conn.commit()
        else:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    migration_file VARCHAR(255) NOT NULL UNIQUE,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    execution_time_ms INTEGER,
                    success BOOLEAN DEFAULT TRUE,
                    error_message TEXT,
                    checksum TEXT
                )
            ''')
            conn.commit()
            conn.close()
    
    def get_applied_migrations(self) -> set:
        """Get list of already applied migrations."""
        if self.db_type == 'postgresql':
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT migration_file FROM schema_migrations WHERE success = TRUE")
                    return {row[0] for row in cursor.fetchall()}
        else:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT migration_file FROM schema_migrations WHERE success = TRUE")
            applied = {row[0] for row in cursor.fetchall()}
            conn.close()
            return applied
    
    def get_pending_migrations(self) -> list:
        """Get list of pending migrations in order."""
        if not self.migrations_dir.exists():
            print(f"⚠️  Migrations directory {self.migrations_dir} does not exist")
            return []
        
        applied = self.get_applied_migrations()
        all_migrations = []
        
        # Find all .sql files in migrations directory
        for file_path in self.migrations_dir.glob("*.sql"):
            if file_path.name not in applied:
                all_migrations.append(file_path)
        
        # Sort by filename (assumes numbered prefixes like 001_, 002_)
        all_migrations.sort(key=lambda x: x.name)
        return all_migrations
    
    def calculate_checksum(self, file_path: Path) -> str:
        """Calculate simple checksum of migration file."""
        import hashlib
        content = file_path.read_text()
        return hashlib.md5(content.encode()).hexdigest()[:8]
    
    def apply_migration(self, migration_file: Path) -> bool:
        """Apply a single migration file."""
        print(f"📁 Applying migration: {migration_file.name}")
        
        start_time = datetime.now()
        
        try:
            # Read migration file
            sql_content = migration_file.read_text()
            checksum = self.calculate_checksum(migration_file)
            
            # Apply migration
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Execute migration SQL
            cursor.executescript(sql_content)
            
            # Record successful migration
            execution_time = int((datetime.now() - start_time).total_seconds() * 1000)
            cursor.execute('''
                INSERT INTO schema_migrations 
                (migration_file, applied_at, execution_time_ms, success, checksum)
                VALUES (?, ?, ?, ?, ?)
            ''', (migration_file.name, datetime.now(), execution_time, True, checksum))
            
            conn.commit()
            conn.close()
            
            print(f"✅ Migration {migration_file.name} applied successfully ({execution_time}ms)")
            return True
            
        except Exception as e:
            # Record failed migration
            try:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO schema_migrations 
                    (migration_file, applied_at, success, error_message)
                    VALUES (?, ?, ?, ?)
                ''', (migration_file.name, datetime.now(), False, str(e)))
                conn.commit()
                conn.close()
            except:
                pass  # If we can't even log the failure, continue
            
            print(f"❌ Migration {migration_file.name} failed: {e}")
            return False
    
    def run_migrations(self, target_migration: str = None) -> bool:
        """Run all pending migrations or up to target migration."""
        pending = self.get_pending_migrations()
        
        if not pending:
            print("✅ No pending migrations - database is up to date")
            return True
        
        print(f"📋 Found {len(pending)} pending migration(s):")
        for migration in pending:
            print(f"   - {migration.name}")
        
        # Filter to target migration if specified
        if target_migration:
            filtered = []
            for migration in pending:
                filtered.append(migration)
                if migration.name == target_migration:
                    break
            pending = filtered
        
        print(f"\n🚀 Applying {len(pending)} migration(s)...")
        
        success_count = 0
        for migration in pending:
            if self.apply_migration(migration):
                success_count += 1
            else:
                print(f"❌ Stopping migration run due to failure in {migration.name}")
                break
        
        if success_count == len(pending):
            print(f"\n🎉 All {success_count} migration(s) applied successfully!")
            return True
        else:
            print(f"\n⚠️  Applied {success_count}/{len(pending)} migrations")
            return False
    
    def show_status(self):
        """Show current migration status."""
        applied = self.get_applied_migrations()
        pending = self.get_pending_migrations()
        
        print("📊 Database Migration Status")
        print("=" * 40)
        print(f"Database: {self.db_path}")
        print(f"Migrations Directory: {self.migrations_dir}")
        print(f"Applied Migrations: {len(applied)}")
        print(f"Pending Migrations: {len(pending)}")
        
        if applied:
            print("\n✅ Applied:")
            for migration in sorted(applied):
                print(f"   - {migration}")
        
        if pending:
            print("\n⏳ Pending:")
            for migration in pending:
                print(f"   - {migration.name}")
        else:
            print("\n✅ Database is up to date!")
    
    def rollback_last(self):
        """Rollback the last applied migration (if supported)."""
        print("⚠️  Rollback functionality not implemented - SQLite doesn't support easy rollbacks")
        print("   Consider restoring from backup or manually reverting changes")

def main():
    """Main CLI interface for migration runner."""
    if len(sys.argv) < 2:
        print("Usage: python db/run_migration.py [command] [options]")
        print("Commands:")
        print("  status  - Show migration status")
        print("  run     - Run all pending migrations")
        print("  run [migration_file] - Run migrations up to specified file")
        return
    
    command = sys.argv[1]
    runner = MigrationRunner()
    
    if command == "status":
        runner.show_status()
    
    elif command == "run":
        target = sys.argv[2] if len(sys.argv) > 2 else None
        success = runner.run_migrations(target)
        sys.exit(0 if success else 1)
    
    elif command == "rollback":
        runner.rollback_last()
    
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

if __name__ == "__main__":
    main()