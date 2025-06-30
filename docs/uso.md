# How to Use

## Running the ETL Pipeline

### **1. Start the Environment**

```bash
# Navigate to the Airflow directory
cd astro

# Start Airflow
astro dev start

# Check container status
docker ps
```

### **2. Configure Connections**

Access Airflow UI at http://localhost:8080 and configure:

#### **Azure Data Lake Connection**
1. Go to Admin → Connections
2. Click "Add a new record"
3. Fill in:
   - Connection Id: `azure_data_lake_default`
   - Connection Type: `wasb`
   - Host: `yourstorageaccount.blob.core.windows.net`
   - Extra: `{"sas_token": "your_sas_token"}`

#### **SQL Server Connection**
1. Go to Admin → Connections
2. Click "Add a new record"
3. Fill in:
   - Connection Id: `sql_server_default`
   - Connection Type: `mssql`
   - Host: `localhost` or `your-server.database.windows.net`
   - Schema: `LogisticsDB`
   - Login: `sa` or `admin`
   - Password: Your password

### **3. Execute the Pipeline**

#### **Manual Execution**
1. Navigate to DAGs page
2. Find `medallion_architecture_etl`
3. Toggle the DAG to "On"
4. Click "Trigger DAG"

#### **Scheduled Execution**
The DAG is configured to run daily at 2 AM automatically once enabled.

## Working with Spark Scripts

### **Running Spark Jobs Locally**

```bash
# Activate Poetry environment
poetry shell

# Run a Spark transformation
spark-submit data/spark_transformations/bronze_processing.py \
  --input-path /path/to/input \
  --output-path /path/to/output
```

### **Testing Spark Code**

```python
# Create a test Spark session
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("test_etl") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()

# Load test data
df = spark.read.csv("test_data.csv", header=True)

# Apply transformations
df_transformed = df.filter(df.amount > 0)

# Verify results
df_transformed.show()
```

## Data Quality Monitoring

### **Viewing Data Quality Metrics**

1. Access Airflow UI
2. Navigate to the DAG run
3. Click on task instances
4. View logs for quality metrics

### **Custom Quality Checks**

```python
def data_quality_check(df):
    """Custom data quality validation"""
    
    # Check for nulls
    null_counts = df.select([
        sum(col(c).isNull().cast("int")).alias(c) 
        for c in df.columns
    ]).collect()[0].asDict()
    
    # Check for duplicates
    duplicate_count = df.count() - df.dropDuplicates().count()
    
    # Business rule validation
    invalid_amounts = df.filter(df.amount < 0).count()
    
    return {
        "null_counts": null_counts,
        "duplicate_count": duplicate_count,
        "invalid_amounts": invalid_amounts
    }
```

## Querying Processed Data

### **Accessing Delta Tables**

```python
# Read from Bronze layer
df_bronze = spark.read.format("delta").load("wasbs://bronze@account.blob.core.windows.net/customers")

# Read from Silver layer
df_silver = spark.read.format("delta").load("wasbs://silver@account.blob.core.windows.net/customers")

# Read from Gold layer
df_gold = spark.read.format("delta").load("wasbs://gold@account.blob.core.windows.net/dim_customer")
```

### **Time Travel Queries**

```python
# Read data as of specific timestamp
df_history = spark.read \
    .format("delta") \
    .option("timestampAsOf", "2024-01-01") \
    .load("path/to/delta/table")

# Read specific version
df_version = spark.read \
    .format("delta") \
    .option("versionAsOf", "5") \
    .load("path/to/delta/table")
```

## Performance Tuning

### **Monitoring Spark Jobs**

```python
# Enable Spark UI
spark.conf.set("spark.ui.enabled", "true")
spark.conf.set("spark.ui.port", "4040")

# Access at http://localhost:4040 during job execution
```

### **Optimizing Queries**

```python
# Use broadcast joins for small tables
from pyspark.sql.functions import broadcast

df_large.join(broadcast(df_small), "key")

# Partition data for better performance
df.write \
    .partitionBy("year", "month") \
    .format("delta") \
    .save("output_path")

# Cache frequently used DataFrames
df_frequent.cache()
df_frequent.count()  # Force materialization
```

## Troubleshooting Common Issues

### **Pipeline Failures**

1. Check Airflow logs:
   ```bash
   astro dev logs
   ```

2. View specific task logs in UI:
   - Click on failed task
   - View "Log" tab

3. Common fixes:
   - Verify connections are configured
   - Check Azure SAS token hasn't expired
   - Ensure sufficient Spark resources

### **Data Quality Issues**

```python
# Add detailed logging
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_with_logging(df):
    logger.info(f"Input records: {df.count()}")
    
    df_processed = apply_transformations(df)
    
    logger.info(f"Output records: {df_processed.count()}")
    logger.info(f"Schema: {df_processed.schema}")
    
    return df_processed
```

### **Performance Issues**

```python
# Profile your Spark job
df.explain(True)  # Show execution plan

# Monitor memory usage
spark.sparkContext.statusTracker().getExecutorInfos()

# Adjust partitions
df.repartition(200)  # Increase parallelism
df.coalesce(10)     # Reduce partitions
```

## Best Practices

### **Development Workflow**

1. **Test locally first**
   ```bash
   # Run with sample data
   spark-submit --master local[*] your_script.py
   ```

2. **Use version control**
   ```bash
   git add .
   git commit -m "feat: add new transformation"
   git push origin feature/new-transformation
   ```

3. **Document your code**
   ```python
   def transform_customer_data(df):
       """
       Transform customer data for silver layer.
       
       Args:
           df: Input DataFrame with raw customer data
           
       Returns:
           DataFrame with cleaned and standardized customer data
       """
       pass
   ```

### **Production Considerations**

- Enable monitoring and alerting
- Implement proper error handling
- Use configuration files for parameters
- Maintain data lineage documentation
- Regular backup of critical data

## Advanced Usage

### **Custom Operators**

```python
# astro/plugins/custom_operators.py
from airflow.models import BaseOperator

class DataQualityOperator(BaseOperator):
    def __init__(self, table_name, quality_checks, **kwargs):
        super().__init__(**kwargs)
        self.table_name = table_name
        self.quality_checks = quality_checks
    
    def execute(self, context):
        # Implement quality check logic
        pass
```

### **Dynamic DAG Generation**

```python
# Generate DAGs for multiple tables
tables = ['customers', 'orders', 'products']

for table in tables:
    dag_id = f'etl_{table}'
    
    dag = DAG(
        dag_id,
        default_args=default_args,
        schedule_interval='@daily'
    )
    
    globals()[dag_id] = dag
```