# Installation Guide

## Prerequisites

Before starting the installation, ensure you have the following components installed:

### **Required Software**
- Python >= 3.10
- Docker Desktop (for running Airflow)
- Azure CLI (for configuring Data Lake)
- Poetry (for dependency management)
- Git

### **Cloud Resources**
- Azure subscription with the following services:
  - Azure Data Lake Storage Gen2
  - Azure SQL Database (optional, or use local SQL Server)

## Step-by-Step Installation

### 1. **Clone the Repository**

```bash
git clone https://github.com/arturoburigo/projeto_etl_spark.git
cd projeto_etl_spark
```

### 2. **Install Python Dependencies**

```bash
# Install Poetry (if not already installed)
curl -sSL https://install.python-poetry.org | python3 -

# Install project dependencies
poetry install

# Activate the virtual environment
poetry shell
```

### 3. **Start SQL Server with Pre-built Data**

```bash
docker run --platform linux/amd64 \
  -e "ACCEPT_EULA=Y" \
  -e "SA_PASSWORD=satc@2025" \
  -p 1433:1433 \
  --name etl-deliveries-db \
  -d arturoburigo/mssql-etl-deliveries-db:latest
```

### 4. **Configure Azure Resources**

#### **Login to Azure**
```bash
az login
```

#### **Create Resource Group**
```bash
az group create --name rg-etl-project --location eastus
```

#### **Deploy Infrastructure with Terraform**
```bash
cd iac

# Initialize Terraform
terraform init

# Review the execution plan
terraform plan

# Apply the infrastructure
terraform apply
```

### 5. **Configure Environment Variables**

Create a `.env` file in the `astro` folder:

```bash
cd astro
cp .env.example .env
```

Edit the `.env` file with your Azure credentials:

```bash
# Azure Data Lake
ADLS_ACCOUNT_NAME=yourstorageaccount
ADLS_SAS_TOKEN="your_sas_token_here"

# SQL Server
SQL_SERVER=localhost
SQL_DATABASE=LogisticsDB
SQL_USERNAME=sa
SQL_PASSWORD=satc@2025

# Spark Configuration
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g
```

### 6. **Start Apache Airflow**

```bash
# From the astro directory
cd astro

# Start Airflow
astro dev start
```

This command will:
- Build the Airflow Docker images
- Start all necessary containers
- Initialize the Airflow database
- Create default admin user

### 7. **Access Airflow UI**

Once started, access the Airflow web interface:
- URL: http://localhost:8080
- Username: `admin`
- Password: `admin`

## Verify Installation

### **Check Connections**

1. Navigate to Admin → Connections in Airflow UI
2. Verify the following connections exist:
   - `azure_data_lake_default`
   - `sql_server_default`

### **Test DAG**

1. Go to the DAGs page
2. Find `medallion_architecture_etl`
3. Toggle the DAG to "On"
4. Click "Trigger DAG" to run manually

### **Verify Azure Resources**

```bash
# List storage containers
az storage container list \
  --account-name yourstorageaccount \
  --query "[].name" \
  --output table
```

You should see:
- landing
- bronze
- silver
- gold

## Common Installation Issues

### **Docker Issues**

If Docker Desktop is not running:
```bash
# Start Docker Desktop (macOS/Windows)
# Or on Linux:
sudo systemctl start docker
```

### **Port Conflicts**

If port 8080 is already in use:
```bash
# Stop the conflicting service or change Airflow port in docker-compose.yml
```

### **Permission Issues**

```bash
# Fix permission issues on Linux/macOS
sudo chown -R $USER:$USER .
chmod -R 755 .
```

### **Azure Authentication**

If Azure CLI authentication fails:
```bash
# Clear cached credentials
az account clear

# Login again
az login
```

## Next Steps

After successful installation:

1. **Run Test Pipeline**: Execute the sample DAG to verify everything works
2. **Configure Monitoring**: Set up alerts and monitoring dashboards
3. **Review Documentation**: Read the architecture and pipeline documentation
4. **Customize Pipeline**: Adapt the pipeline to your specific needs

## Troubleshooting

For additional help:
- Check the [Troubleshooting Guide](troubleshooting.md)
- Review [FAQ](faq.md)
- Open an issue on [GitHub](https://github.com/arturoburigo/projeto_etl_spark/issues)