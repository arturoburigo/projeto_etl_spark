#!/usr/bin/env python3
"""
Script to configure SQL Server database
Creates database, schema and configures connection based on environment variables
"""

import os
import pyodbc
import time
import sys
from typing import Optional

class SQLServerSetup:
    def __init__(self):
        """Initialize SQL Server configuration with environment variables"""
        self.server = os.getenv('DB_SERVER', 'localhost')
        self.port = os.getenv('DB_PORT', '1433')
        self.username = os.getenv('DB_USERNAME', 'sa')
        self.password = os.getenv('DB_PASSWORD', 'satc@2025')
        self.database = os.getenv('DB_DATABASE', 'etl_entregas_db')
        self.schema = os.getenv('DB_SCHEMA', 'entregas')
        
        # Connection string for master (database creation)
        self.master_conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.server},{self.port};"
            f"DATABASE=master;"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"TrustServerCertificate=yes;"
        )
        
        # Connection string for specific database
        self.db_conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.server},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"TrustServerCertificate=yes;"
        )

    def wait_for_server(self, timeout: int = 60) -> bool:
        """Wait for SQL Server to become available"""
        print(f"Waiting for SQL Server at {self.server}:{self.port}...")
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                conn = pyodbc.connect(self.master_conn_str, timeout=5)
                conn.close()
                print("✅ SQL Server is available!")
                return True
            except Exception as e:
                print(f"⏳ Trying to connect... ({int(time.time() - start_time)}s)")
                time.sleep(2)
        
        print(f"❌ Timeout connecting to SQL Server after {timeout}s")
        return False

    def database_exists(self) -> bool:
        """Check if database already exists"""
        try:
            with pyodbc.connect(self.master_conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM sys.databases WHERE name = ?", 
                    (self.database,)
                )
                return cursor.fetchone()[0] > 0
        except Exception as e:
            print(f"❌ Error checking database: {e}")
            return False

    def create_database(self) -> bool:
        """Create database if it doesn't exist"""
        try:
            if self.database_exists():
                print(f"✅ Database '{self.database}' already exists")
                return True
            
            print(f"🔨 Creating database '{self.database}'...")
            with pyodbc.connect(self.master_conn_str) as conn:
                conn.autocommit = True
                cursor = conn.cursor()
                
                # Create database
                cursor.execute(f"CREATE DATABASE [{self.database}]")
                print(f"✅ Database '{self.database}' created successfully!")
                return True
                
        except Exception as e:
            print(f"❌ Error creating database: {e}")
            return False

    def schema_exists(self) -> bool:
        """Check if schema already exists"""
        try:
            with pyodbc.connect(self.db_conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM sys.schemas WHERE name = ?", 
                    (self.schema,)
                )
                return cursor.fetchone()[0] > 0
        except Exception as e:
            print(f"❌ Error checking schema: {e}")
            return False

    def create_schema(self) -> bool:
        """Create schema if it doesn't exist"""
        try:
            if self.schema_exists():
                print(f"✅ Schema '{self.schema}' already exists")
                return True
            
            print(f"🔨 Creating schema '{self.schema}'...")
            with pyodbc.connect(self.db_conn_str) as conn:
                conn.autocommit = True
                cursor = conn.cursor()
                
                # Create schema
                cursor.execute(f"CREATE SCHEMA [{self.schema}]")
                print(f"✅ Schema '{self.schema}' created successfully!")
                return True
                
        except Exception as e:
            print(f"❌ Error creating schema: {e}")
            return False

    def set_default_schema(self) -> bool:
        """Set default schema for user"""
        try:
            print(f"🔧 Configuring default schema '{self.schema}' for user '{self.username}'...")
            with pyodbc.connect(self.db_conn_str) as conn:
                conn.autocommit = True
                cursor = conn.cursor()
                
                # Set default schema for user
                cursor.execute(f"ALTER USER [{self.username}] WITH DEFAULT_SCHEMA = [{self.schema}]")
                print(f"✅ Default schema configured!")
                return True
                
        except Exception as e:
            print(f"⚠️ Warning when configuring default schema: {e}")
            # Not critical if it fails
            return True

    def test_connection(self) -> bool:
        """Test final connection"""
        try:
            print("🧪 Testing final connection...")
            with pyodbc.connect(self.db_conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT @@VERSION, DB_NAME(), SCHEMA_NAME()")
                result = cursor.fetchone()
                print(f"✅ Connection OK!")
                print(f"   Database: {result[1]}")
                print(f"   Current schema: {result[2]}")
                return True
        except Exception as e:
            print(f"❌ Connection test error: {e}")
            return False

    def setup(self) -> bool:
        """Execute complete configuration process"""
        print("🚀 Starting SQL Server configuration...")
        print(f"📋 Settings:")
        print(f"   Server: {self.server}:{self.port}")
        print(f"   Database: {self.database}")
        print(f"   Schema: {self.schema}")
        print(f"   User: {self.username}")
        print("-" * 50)
        
        # Wait for server
        if not self.wait_for_server():
            return False
        
        # Create database
        if not self.create_database():
            return False
        
        # Wait a bit for database to become available
        time.sleep(2)
        
        # Create schema
        if not self.create_schema():
            return False
        
        # Configure default schema
        self.set_default_schema()
        # Test connection
        if not self.test_connection():
            return False
        
        print("\n" + "=" * 50)
        print("✅ CONFIGURATION COMPLETED SUCCESSFULLY!")
        print("=" * 50)
        print(f"🔗 Connection string:")
        print(f"   {self.db_conn_str}")
        print(f"\n📝 To use in your applications:")
        print(f"   DATABASE_URL=mssql+pyodbc://{self.username}:{self.password}@{self.server}:{self.port}/{self.database}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes")
        
        return True

def main():
    """Main function"""
    print("🐋 SQL Server Docker Setup Script")
    print("=" * 50)
    
    # Check if dependencies are installed
    try:
        import pyodbc
    except ImportError:
        print("❌ pyodbc is not installed!")
        print("💡 Run: pip install pyodbc")
        sys.exit(1)
    
    # Execute configuration
    setup = SQLServerSetup()
    
    if setup.setup():
        print("\n🎉 Setup completed! Your database is ready to use.")
        sys.exit(0)
    else:
        print("\n💥 Configuration failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()