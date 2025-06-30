#!/usr/bin/env python3
"""
Main setup script for the ETL project
Runs all setup scripts in the correct order:
1. create_schema_and_columns.py - Creates database and schema
2. create_tables.py - Creates all tables
3. faker_data.py - Generates sample data
"""

import os
import sys
import subprocess
import time
from typing import List, Tuple

class SetupRunner:
    def __init__(self):
        """Initialize the setup runner"""
        self.scripts = [
            ("create_schema_and_columns.py", "Database and Schema Setup"),
            ("create_tables.py", "Table Creation"),
            ("faker_data.py", "Sample Data Generation")
        ]
        self.current_dir = os.path.dirname(os.path.abspath(__file__))
        
    def check_dependencies(self) -> bool:
        """Check if required dependencies are installed"""
        print("🔍 Checking dependencies...")
        
        try:
            import pyodbc
            print("✅ pyodbc is installed")
        except ImportError:
            print("❌ pyodbc is not installed!")
            print("💡 Please run: pip install pyodbc")
            return False
            
        return True
    
    def load_environment_variables(self) -> bool:
        """Load environment variables from .env file if it exists"""
        env_file = os.path.join(os.path.dirname(self.current_dir), '.env')
        
        if os.path.exists(env_file):
            print("📋 Loading environment variables from .env file...")
            try:
                with open(env_file, 'r') as f:
                    for line in f:
                        if '=' in line and not line.startswith('#'):
                            key, value = line.strip().split('=', 1)
                            os.environ[key] = value
                print("✅ Environment variables loaded")
                return True
            except Exception as e:
                print(f"⚠️ Warning: Could not load .env file: {e}")
                return False
        else:
            print("ℹ️ No .env file found, using default environment variables")
            return True
    
    def run_script(self, script_name: str, description: str) -> bool:
        """Run a single Python script"""
        script_path = os.path.join(self.current_dir, script_name)
        
        if not os.path.exists(script_path):
            print(f"❌ Script not found: {script_path}")
            return False
        
        print(f"\n{'='*60}")
        print(f"🚀 Running: {description}")
        print(f"📁 Script: {script_name}")
        print(f"{'='*60}")
        
        try:
            # Run the script with subprocess
            result = subprocess.run(
                [sys.executable, script_path],
                cwd=self.current_dir,
                capture_output=False,
                text=True,
                env=os.environ.copy()
            )
            
            if result.returncode == 0:
                print(f"\n✅ {description} completed successfully!")
                return True
            else:
                print(f"\n❌ {description} failed with return code: {result.returncode}")
                return False
                
        except Exception as e:
            print(f"\n❌ Error running {script_name}: {e}")
            return False
    
    def show_summary(self, results: List[Tuple[str, bool]]) -> None:
        """Show a summary of all operations"""
        print(f"\n{'='*60}")
        print("📊 SETUP SUMMARY")
        print(f"{'='*60}")
        
        success_count = 0
        for description, success in results:
            status = "✅ SUCCESS" if success else "❌ FAILED"
            print(f"{status} - {description}")
            if success:
                success_count += 1
        
        print(f"\n📈 Results: {success_count}/{len(results)} operations completed successfully")
        
        if success_count == len(results):
            print("\n🎉 ALL SETUP OPERATIONS COMPLETED SUCCESSFULLY!")
            print("💡 Your ETL project is ready to use!")
        else:
            print(f"\n⚠️ {len(results) - success_count} operation(s) failed")
            print("💡 Please check the error messages above and try again")
    
    def run_all(self) -> bool:
        """Run all setup scripts in the correct order"""
        print("🏗️ ETL PROJECT SETUP")
        print("="*60)
        print("This script will set up your complete ETL environment:")
        print("1. Create database and schema")
        print("2. Create all necessary tables")
        print("3. Generate sample data (200,000 records)")
        print("\n⚠️ This process will take several minutes...")
        
        # Ask for confirmation
        response = input("\nDo you want to continue? (yes/no): ")
        if response.lower() not in ['yes', 'y', 'sim', 's']:
            print("❌ Setup cancelled by user")
            return False
        
        # Check dependencies
        if not self.check_dependencies():
            return False
        
        # Load environment variables
        if not self.load_environment_variables():
            return False
        
        print(f"\n📁 Working directory: {self.current_dir}")
        print(f"🐍 Python executable: {sys.executable}")
        
        # Run all scripts
        results = []
        start_time = time.time()
        
        for script_name, description in self.scripts:
            success = self.run_script(script_name, description)
            results.append((description, success))
            
            if not success:
                print(f"\n💥 Setup failed at: {description}")
                print("💡 Please fix the error and try again")
                break
            
            # Small delay between scripts
            if script_name != self.scripts[-1][0]:  # Not the last script
                print("\n⏳ Waiting 3 seconds before next step...")
                time.sleep(3)
        
        # Show summary
        self.show_summary(results)
        
        elapsed_time = time.time() - start_time
        print(f"\n⏱️ Total execution time: {elapsed_time:.1f} seconds")
        
        return all(success for _, success in results)

def main():
    """Main function"""
    runner = SetupRunner()
    
    try:
        success = runner.run_all()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n❌ Setup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 