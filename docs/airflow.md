# Apache Airflow and Orchestration

## Overview

Apache Airflow is the orchestration engine for our ETL pipeline, managing the scheduling, monitoring, and execution of data processing tasks.

## Main DAG: `medallion_architecture_etl`

The primary DAG orchestrates the complete data flow through the Medallion architecture:

```python
@dag(
    dag_id="medallion_architecture_etl",
    schedule="0 2 * * *",  # Daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "production"]
)
def etl_pipeline():
    # Extract data from SQL Server
    extract_task = extract_from_sqlserver()
    
    # Process Bronze Layer
    bronze_task = process_bronze_layer()
    
    # Process Silver Layer
    silver_task = process_silver_layer()
    
    # Process Gold Layer
    gold_task = process_gold_layer()
    
    # Define dependencies
    extract_task >> bronze_task >> silver_task >> gold_task
```

## Key Features

### **Scheduling**
- Daily runs at 2 AM
- No backfill (catchup=False)
- Timezone-aware scheduling

### **Error Handling**
- Automatic retry (3 attempts)
- Exponential backoff
- Email notifications on failure

### **Monitoring**
- Real-time execution tracking
- Performance metrics
- Log aggregation

## Configuration

### **Connection Setup**

```python
# Azure Data Lake Connection
Connection(
    conn_id="azure_data_lake_default",
    conn_type="wasb",
    host="yourstorageaccount.blob.core.windows.net",
    extra={
        "sas_token": "your_sas_token"
    }
)

# SQL Server Connection
Connection(
    conn_id="sql_server_default",
    conn_type="mssql",
    host="your-server.database.windows.net",
    schema="LogisticsDB",
    login="admin",
    password="your_password"
)
```

### **Variables**

```python
# Airflow Variables
Variable.set("environment", "production")
Variable.set("spark_config", json.dumps({
    "driver_memory": "4g",
    "executor_memory": "4g"
}))
```

## Best Practices

### **DAG Design**
- Keep DAGs simple and focused
- Use task groups for related operations
- Implement proper error handling
- Add meaningful documentation

### **Performance**
- Use connection pooling
- Implement incremental processing
- Optimize task parallelism
- Monitor resource usage

### **Testing**
- Unit test individual tasks
- Integration test complete DAGs
- Validate data quality checks
- Test error scenarios

## Tests

Tests are located in `astro/tests/` and include:
- DAG validation tests
- Task unit tests
- Integration tests
- Performance benchmarks