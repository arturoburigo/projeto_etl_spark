# 🚀 Quick Start

## 📋 Prerequisites

Before getting started, make sure you have the following items installed on your system:

### 🛠️ **Essential Tools**

=== "Windows"
    ```powershell
    # Install Python 3.10+
    winget install Python.Python.3.10
    
    # Install Docker Desktop
    winget install Docker.DockerDesktop
    
    # Install Azure CLI
    winget install Microsoft.AzureCLI
    
    # Install Git
    winget install Git.Git
    ```

=== "macOS"
    ```bash
    # Install Homebrew (if not already installed)
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    
    # Install Python 3.10+
    brew install python@3.10
    
    # Install Docker Desktop
    brew install --cask docker
    
    # Install Azure CLI
    brew install azure-cli
    
    # Install Git
    brew install git
    ```

=== "Linux (Ubuntu/Debian)"
    ```bash
    # Update repositories
    sudo apt update
    
    # Install Python 3.10+
    sudo apt install python3.10 python3.10-pip python3.10-venv
    
    # Install Docker
    curl -fsSL https://get.docker.com -o get-docker.sh
    sh get-docker.sh
    
    # Install Azure CLI
    curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
    
    # Install Git
    sudo apt install git
    ```

### 📦 **Poetry (Dependency Manager)**

```bash
# Install Poetry
curl -sSL https://install.python-poetry.org | python3 -

# Add to PATH (add to your .bashrc/.zshrc)
export PATH="$HOME/.local/bin:$PATH"

# Verify installation
poetry --version
```

---

## ⚡ 5-Minute Installation

### **1. Clone the Repository**

```bash
git clone https://github.com/arturoburigo/projeto_etl_spark.git
cd projeto_etl_spark
```

### **2. Start the SQL Server with Pre-built Data**

```bash
docker run --platform linux/amd64 -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=satc@2025" -p 1433:1433 --name etl-deliveries-db -d arturoburigo/mssql-etl-deliveries-db:latest
```

### **3. Set up Azure Resources**

- Create a Microsoft/Azure account with access to paid resources
- In the [Azure Portal](https://portal.azure.com/), create a workspace following the [Microsoft documentation](https://learn.microsoft.com/en-us/azure/databricks/getting-started/)
- During this process, you will create a **resource group**. Save the resource group name as it will be used in the next step

### **4. Configure Azure**

```bash
# Login to Azure
az login
```

### **5. Configure Terraform**

In the file [`/iac/variables.tf`](https://github.com/arturoburigo/projeto_etl_spark/blob/iac/variables.tf), modify the following variable by adding the **resource group** you created previously.

### **6. Deploy the Cloud Environment**

```bash
cd iac
terraform init
terraform apply
```

### **7. Verify Azure Resources**

Check the [Azure Portal](https://portal.azure.com/) for the **MS SQL Server**, **MS SQL Database**, and **ADLS Gen2** containing the containers `landing-zone`, `bronze`, `silver`, and `gold` that were created in the previous step.

### **8. Generate SAS Token**

In the [Azure Portal](https://portal.azure.com/), generate a **SAS TOKEN** for the `landing-zone` container following this [documentation](https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens?tabs=Containers#create-sas-tokens-in-the-azure-portal).

### **9. Configure Environment Variables**

Create a `.env` file in the `astro` folder and fill in the variables with your Azure credentials and SAS token.

**Example `.env` configuration:**
```bash
# Azure Data Lake
ADLS_ACCOUNT_NAME=yourstorageaccount
ADLS_FILE_SYSTEM_NAME=landing
ADLS_BRONZE_CONTAINER_NAME=bronze
ADLS_SILVER_CONTAINER_NAME=silver
ADLS_GOLD_CONTAINER_NAME=gold
ADLS_SAS_TOKEN="sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupitfx&se=2024-12-31T23:59:59Z&st=2024-01-01T00:00:00Z&spr=https&sig=YOUR_SAS_SIGNATURE"

# SQL Server
SQL_SERVER=your-server.database.windows.net
SQL_DATABASE=LogisticsDB
SQL_SCHEMA=dbo
SQL_USERNAME=admin
SQL_PASSWORD=YourSecurePassword123!

# Spark Configuration (optional)
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g
SPARK_EXECUTOR_CORES=2
```

### **10. Set up Python Environment**

```bash
# Activate Poetry environment
poetry env activate
poetry install
```

### **11. Start Airflow**

```bash
# Navigate to Airflow directory
cd astro

# Start Airflow environment
astro dev start
```

!!! tip "Tip"
    The `astro dev start` command may take a few minutes on first run as it needs to download Docker images.

### **12. Access the Web Interface**

After complete initialization, access:

- 🌐 **Airflow UI**: [http://localhost:8080](http://localhost:8080)
- 👤 **Credentials**: `admin` / `admin`

---

## 🎯 First Execution

### **1. Check Connections**

In Airflow UI, go to **Admin → Connections** and verify the connections are configured:

- ✅ `azure_data_lake_default`
- ✅ `sql_server_default`

### **2. Execute the Pipeline**

1. Navigate to **DAGs** in the Airflow interface
2. Find the DAG `Medallion Architecture - ETL`
3. Click the **▶️ Trigger DAG** button
4. Monitor execution in the **Graph View** tab

### **3. Verify Results**

After successful execution, you should see:

- ✅ **Landing Zone**: CSV files in the `landing` container
- ✅ **Bronze Layer**: Delta tables in the `bronze` container
- ✅ **Silver Layer**: Clean data in the `silver` container
- ✅ **Gold Layer**: Dimensional model in the `gold` container

---

## 🔧 Advanced Configuration

### **Azure Data Lake Setup**

If you don't have an Azure Data Lake configured yet:

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

# Create containers
az storage container create --name landing --account-name yourstorageaccount
az storage container create --name bronze --account-name yourstorageaccount
az storage container create --name silver --account-name yourstorageaccount
az storage container create --name gold --account-name yourstorageaccount

# Generate SAS Token (valid for 1 year)
az storage account generate-sas \
    --account-name yourstorageaccount \
    --account-key $(az storage account keys list --account-name yourstorageaccount --query '[0].value' -o tsv) \
    --expiry 2024-12-31T23:59:59Z \
    --permissions racwdlup \
    --resource-types sco \
    --services bfqt
```

---

## 🧪 Quick Test

### **SQL Server Connection Test**

```bash
# Inside Poetry environment
poetry shell

# Run connection test
python astro/tests/test_sqlserver_connection.py
```

### **DAG Test**

```bash
# DAG syntax test
python astro/dags/main.py

# Unit test
pytest astro/tests/test_dag_example.py
```

### **Spark Test**

```python
# Open Python interactive shell
python

# Test Spark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("quick_test") \
    .getOrCreate()

# Create test DataFrame
df = spark.createDataFrame([(1, "test"), (2, "spark")], ["id", "name"])
df.show()

spark.stop()
```

---

## 📊 Monitoring

### **Airflow Logs**

```bash
# View real-time logs
astro dev logs

# Service-specific logs
astro dev logs scheduler
astro dev logs webserver
```

### **Monitoring Interfaces**

- 📊 **Airflow UI**: [http://localhost:8080](http://localhost:8080)
- 📈 **Flower (Celery)**: [http://localhost:5555](http://localhost:5555)
- 🗄️ **Postgres**: [http://localhost:5432](http://localhost:5432)

---

## 🚨 Troubleshooting

### **Common Issues**

#### **1. Azure Connection Error**

```bash
# Check connectivity
az storage blob list --container-name landing --account-name yourstorageaccount

# Test SAS Token
curl "https://yourstorageaccount.blob.core.windows.net/landing?sv=2022-11-02&ss=bfqt..."
```

#### **2. Spark Memory Error**

Edit the `.env` file:
```bash
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g
```

#### **3. Airflow Won't Start**

```bash
# Stop all services
astro dev stop

# Clean containers
docker system prune -f

# Restart
astro dev start
```

#### **4. Permission Issues**

```bash
# Linux/macOS - Adjust permissions
sudo chown -R $USER:$USER .
chmod -R 755 .
```

### **Useful Commands**

```bash
# Check container status
docker ps

# View logs from specific container
docker logs <container_id>

# Restart Airflow only
astro dev restart

# Execute command inside container
astro dev bash
```

---

## 📚 Next Steps

Now that you have the environment running, explore:

1. 📖 **[Architecture](architecture.md)** - Understand how the system works
2. 🔧 **[ETL Pipeline](etl_pipeline.md)** - Data processing details
3. 📊 **[KPIs and Metrics](kpis_metrics.md)** - Calculated indicators
4. 🎛️ **[Airflow](airflow.md)** - Advanced orchestration
5. 🧪 **[Tests](tests.md)** - How to test the system

---

## 🆘 Need Help?

- 📖 **Documentation**: [docs/](../index.md)
- 🐛 **Issues**: [GitHub Issues](https://github.com/arturoburigo/projeto_etl_spark/issues)
- 💬 **Discussions**: [GitHub Discussions](https://github.com/arturoburigo/projeto_etl_spark/discussions)

---

!!! success "Congratulations! 🎉"
    You have successfully configured the ETL project with Apache Spark and Azure Data Lake. The pipeline is ready to process data at scale!