# 🔐 Environment Variables Configuration

## 📋 Overview

This document describes all the environment variables needed to configure and run the ETL project with Apache Spark and Azure Data Lake.

---

## 📁 `.env.example` File

Create a `.env` file in the project root based on the template below:

```bash
# =============================================================================
# ETL PROJECT WITH APACHE SPARK & AZURE DATA LAKE
# Environment Variables Configuration File
# =============================================================================

# =============================================================================
# AZURE DATA LAKE STORAGE
# =============================================================================

# Azure storage account name
ADLS_ACCOUNT_NAME=yourstorageaccount

# Data Lake Containers (Medallion Architecture)
ADLS_FILE_SYSTEM_NAME=landing
ADLS_BRONZE_CONTAINER_NAME=bronze
ADLS_SILVER_CONTAINER_NAME=silver
ADLS_GOLD_CONTAINER_NAME=gold

# SAS Token for Azure Storage access
# Generate a new token with permissions: read, add, create, write, delete, list, update, process, tag, filter, setimmutability
# Valid for: blob, file, queue, table
ADLS_SAS_TOKEN="sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupitfx&se=2024-12-31T23:59:59Z&st=2024-01-01T00:00:00Z&spr=https&sig=YOUR_SAS_SIGNATURE_HERE"

# =============================================================================
# SQL SERVER DATABASE
# =============================================================================

# SQL Server (Azure SQL Database or local SQL Server)
SQL_SERVER=your-server.database.windows.net

# Database name
SQL_DATABASE=LogisticsDB

# Database schema
SQL_SCHEMA=dbo

# Access credentials
SQL_USERNAME=admin
SQL_PASSWORD=YourSecurePassword123!

# =============================================================================
# APACHE SPARK CONFIGURATION
# =============================================================================

# Spark Driver Memory
SPARK_DRIVER_MEMORY=4g

# Spark Executor Memory
SPARK_EXECUTOR_MEMORY=4g

# Number of cores per Executor
SPARK_EXECUTOR_CORES=2

# Number of Executor instances
SPARK_EXECUTOR_INSTANCES=2

# Number of partitions for shuffle operations
SPARK_SQL_SHUFFLE_PARTITIONS=200

# Default parallelism
SPARK_DEFAULT_PARALLELISM=8

# =============================================================================
# AIRFLOW CONFIGURATION
# =============================================================================

# Airflow environment (development, staging, production)
AIRFLOW_ENV=development

# Timezone
AIRFLOW_TIMEZONE=America/New_York

# Email for notifications (optional)
AIRFLOW_ADMIN_EMAIL=admin@company.com

# =============================================================================
# LOGGING & MONITORING
# =============================================================================

# Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
LOG_LEVEL=INFO

# Directory for logs
LOG_DIR=/tmp/logs

# Enable verbose Spark logging
SPARK_VERBOSE_LOGGING=false

# =============================================================================
# PERFORMANCE TUNING
# =============================================================================

# Target file size for Delta Lake (in bytes)
DELTA_TARGET_FILE_SIZE=134217728

# Enable automatic optimizations
DELTA_AUTO_OPTIMIZE=true
DELTA_AUTO_COMPACT=true

# Network timeout (in seconds)
NETWORK_TIMEOUT=800

# =============================================================================
# DEVELOPMENT & TESTING
# =============================================================================

# Development mode (enables extra logs and debug configurations)
DEV_MODE=true

# Use synthetic data (for development/testing)
USE_SYNTHETIC_DATA=false

# Number of records for synthetic data
SYNTHETIC_DATA_RECORDS=10000

# =============================================================================
# SECURITY
# =============================================================================

# Encryption key for sensitive data (optional)
ENCRYPTION_KEY=your-32-character-key-here

# Enable SSL/TLS for connections
ENABLE_SSL=true

# =============================================================================
# BUSINESS CONFIGURATION
# =============================================================================

# Logistics business specific configurations

# Business timezone
BUSINESS_TIMEZONE=America/New_York

# Business start time (HH:MM format)
BUSINESS_START_TIME=06:00

# Business end time (HH:MM format)
BUSINESS_END_TIME=22:00

# Business days (1=Monday, 7=Sunday)
BUSINESS_DAYS=1,2,3,4,5,6

# =============================================================================
# ALERTS AND NOTIFICATIONS
# =============================================================================

# Email for critical alerts
ALERT_EMAIL=alerts@company.com

# Slack webhook URL for notifications (optional)
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK

# Performance alert threshold
PERFORMANCE_ALERT_THRESHOLD=90

# =============================================================================
# BACKUP AND RECOVERY
# =============================================================================

# Enable automatic backup
ENABLE_AUTO_BACKUP=true

# Backup retention (in days)
BACKUP_RETENTION_DAYS=30

# Backup directory
BACKUP_DIR=/tmp/backups
```

---

## 🔧 Step-by-Step Configuration

### **1. File Creation**

```bash
# Copy template
cp .env.example .env

# Edit with your configurations
nano .env  # or vim .env, code .env, etc.
```

### **2. Azure Data Lake Storage**

#### **Create Storage Account**

```bash
# Login to Azure
az login

# Create Resource Group
az group create --name rg-etl-project --location eastus

# Create Storage Account
az storage account create \
    --name yourstorageaccount \
    --resource-group rg-etl-project \
    --location eastus \
    --sku Standard_LRS \
    --kind StorageV2 \
    --hierarchical-namespace true
```

#### **Create Containers**

```bash
# Create containers for Medallion Architecture
az storage container create --name landing --account-name yourstorageaccount
az storage container create --name bronze --account-name yourstorageaccount
az storage container create --name silver --account-name yourstorageaccount
az storage container create --name gold --account-name yourstorageaccount
```

#### **Generate SAS Token**

```bash
# Generate SAS Token valid for 1 year
az storage account generate-sas \
    --account-name yourstorageaccount \
    --account-key $(az storage account keys list --account-name yourstorageaccount --query '[0].value' -o tsv) \
    --expiry 2024-12-31T23:59:59Z \
    --permissions racwdlupitfx \
    --resource-types sco \
    --services bfqt
```

### **3. SQL Server Configuration**

#### **Azure SQL Database**

```bash
# Create SQL Server
az sql server create \
    --name your-sql-server \
    --resource-group rg-etl-project \
    --location eastus \
    --admin-user admin \
    --admin-password YourSecurePassword123!

# Create Database
az sql db create \
    --resource-group rg-etl-project \
    --server your-sql-server \
    --name LogisticsDB \
    --service-objective Basic
```

#### **Configure Firewall**

```bash
# Allow Azure access
az sql server firewall-rule create \
    --resource-group rg-etl-project \
    --server your-sql-server \
    --name AllowAzureServices \
    --start-ip-address 0.0.0.0 \
    --end-ip-address 0.0.0.0

# Allow your local IP
az sql server firewall-rule create \
    --resource-group rg-etl-project \
    --server your-sql-server \
    --name AllowMyIP \
    --start-ip-address YOUR_IP \
    --end-ip-address YOUR_IP
```

---

## 📊 Environment-Specific Configurations

### **Local Development**

```bash
# Limited resources for development
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g
SPARK_EXECUTOR_CORES=1
SPARK_EXECUTOR_INSTANCES=1
SPARK_SQL_SHUFFLE_PARTITIONS=50

# Verbose logs for debugging
LOG_LEVEL=DEBUG
SPARK_VERBOSE_LOGGING=true
DEV_MODE=true
```

### **Staging/Testing**

```bash
# Medium resources for testing
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g
SPARK_EXECUTOR_CORES=2
SPARK_EXECUTOR_INSTANCES=2
SPARK_SQL_SHUFFLE_PARTITIONS=200

# Moderate logs
LOG_LEVEL=INFO
DEV_MODE=false
```

### **Production**

```bash
# Maximized resources for production
SPARK_DRIVER_MEMORY=8g
SPARK_EXECUTOR_MEMORY=8g
SPARK_EXECUTOR_CORES=4
SPARK_EXECUTOR_INSTANCES=4
SPARK_SQL_SHUFFLE_PARTITIONS=400

# Optimized logs
LOG_LEVEL=WARNING
SPARK_VERBOSE_LOGGING=false
DEV_MODE=false

# Enhanced security
ENABLE_SSL=true
ENCRYPTION_KEY=your-secure-32-char-key
```

---

## 🔒 Security

### **Credential Protection**

```bash
# Never commit the .env file
echo ".env" >> .gitignore

# Use system environment variables in production
export ADLS_SAS_TOKEN="your_token_here"
export SQL_PASSWORD="your_password_here"
```

### **Credential Rotation**

```bash
# Generate new SAS Token monthly
az storage account generate-sas \
    --account-name yourstorageaccount \
    --account-key $(az storage account keys list --account-name yourstorageaccount --query '[0].value' -o tsv) \
    --expiry $(date -d "+1 month" +%Y-%m-%dT23:59:59Z) \
    --permissions racwdlupitfx \
    --resource-types sco \
    --services bfqt
```

---

## 🧪 Configuration Validation

### **Test Script**

```python
#!/usr/bin/env python3
"""
Script to validate environment variable configuration
"""

import os
from dotenv import load_dotenv

def validate_config():
    load_dotenv()
    
    required_vars = [
        'ADLS_ACCOUNT_NAME',
        'ADLS_SAS_TOKEN',
        'SQL_SERVER',
        'SQL_DATABASE',
        'SQL_USERNAME',
        'SQL_PASSWORD'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing variables: {', '.join(missing_vars)}")
        return False
    
    print("✅ All required variables are configured")
    return True

def test_azure_connection():
    """Test Azure Storage connection"""
    try:
        from azure.storage.blob import BlobServiceClient
        
        account_name = os.getenv('ADLS_ACCOUNT_NAME')
        sas_token = os.getenv('ADLS_SAS_TOKEN')
        
        client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=sas_token
        )
        
        # List containers
        containers = list(client.list_containers())
        print(f"✅ Azure connection OK. Containers found: {len(containers)}")
        return True
        
    except Exception as e:
        print(f"❌ Azure connection error: {e}")
        return False

def test_sql_connection():
    """Test SQL Server connection"""
    try:
        from sqlalchemy import create_engine
        from urllib.parse import quote_plus
        
        server = os.getenv('SQL_SERVER')
        database = os.getenv('SQL_DATABASE')
        username = os.getenv('SQL_USERNAME')
        password = quote_plus(os.getenv('SQL_PASSWORD'))
        
        conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
        engine = create_engine(conn_str)
        
        # Simple test
        with engine.connect() as conn:
            result = conn.execute("SELECT 1 as test")
            print("✅ SQL Server connection OK")
            return True
            
    except Exception as e:
        print(f"❌ SQL Server connection error: {e}")
        return False

if __name__ == "__main__":
    print("🔍 Validating configuration...")
    
    if validate_config():
        print("\n🧪 Testing connections...")
        test_azure_connection()
        test_sql_connection()
    
    print("\n✅ Validation complete!")
```

### **Run Validation**

```bash
# Save as validate_config.py and run
python validate_config.py
```

---

## 📝 Troubleshooting

### **Common Issues**

#### **1. Invalid SAS Token**

```bash
# Error: "Server failed to authenticate the request"
# Solution: Generate new SAS token

az storage account generate-sas \
    --account-name yourstorageaccount \
    --account-key $(az storage account keys list --account-name yourstorageaccount --query '[0].value' -o tsv) \
    --expiry 2024-12-31T23:59:59Z \
    --permissions racwdlupitfx \
    --resource-types sco \
    --services bfqt
```

#### **2. SQL Server Connection Denied**

```bash
# Error: "Login failed for user"
# Solution: Check firewall and credentials

# List firewall rules
az sql server firewall-rule list \
    --resource-group rg-etl-project \
    --server your-sql-server

# Add your IP
az sql server firewall-rule create \
    --resource-group rg-etl-project \
    --server your-sql-server \
    --name AllowMyIP \
    --start-ip-address $(curl -s ifconfig.me) \
    --end-ip-address $(curl -s ifconfig.me)
```

#### **3. Spark Memory Error**

```bash
# Error: "OutOfMemoryError"
# Solution: Reduce memory configurations

SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g
SPARK_SQL_SHUFFLE_PARTITIONS=50
```

---

## 📚 References

- [Azure Storage SAS Tokens](https://docs.microsoft.com/en-us/azure/storage/common/storage-sas-overview)
- [Azure SQL Database](https://docs.microsoft.com/en-us/azure/azure-sql/database/)
- [Apache Spark Configuration](https://spark.apache.org/docs/latest/configuration.html)
- [Apache Airflow Configuration](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html)

---

!!! warning "Important"
    Never commit real credentials to the repository. Always use the local `.env` file or system environment variables in production.