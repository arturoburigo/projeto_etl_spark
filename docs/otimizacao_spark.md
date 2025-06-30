# ⚡ Apache Spark Optimization

## 📋 Overview

Optimizing Apache Spark connection and configuration is crucial for achieving maximum performance in data processing. This guide presents best practices and optimized configurations for the ETL project.

---

## 🔧 Optimized Connection Configurations

### **Optimized SparkSession**

```python
def create_optimized_spark_session(app_name="etl_project_spark"):
    """
    Creates an optimized SparkSession for the ETL project
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", 
                "io.delta:delta-core_2.12:2.3.0,"
                "org.apache.hadoop:hadoop-azure:3.3.6,"
                "org.apache.hadoop:hadoop-common:3.3.6,"
                "com.microsoft.azure:azure-storage:8.6.6") \
        \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        \
        .config("spark.driver.memory", "4g") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.driver.cores", "2") \
        \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.instances", "2") \
        \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
        \
        .config("spark.default.parallelism", "8") \
        .config("spark.sql.shuffle.partitions", "200") \
        \
        .config("spark.network.timeout", "800s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.storage.blockManagerSlaveTimeoutMs", "600s") \
        \
        .getOrCreate()
```

---

## 🚀 Environment-Specific Optimizations

### **Local Development**

```python
def spark_config_development():
    """Optimized configuration for local development"""
    return {
        # Limited resources for development
        "spark.driver.memory": "2g",
        "spark.executor.memory": "2g", 
        "spark.executor.cores": "1",
        "spark.executor.instances": "1",
        
        # Reduced parallelism
        "spark.default.parallelism": "4",
        "spark.sql.shuffle.partitions": "50",
        
        # More verbose logs for debugging
        "spark.sql.adaptive.logLevel": "INFO",
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": "/tmp/spark-events",
        
        # Local checkpoint
        "spark.sql.streaming.checkpointLocation": "/tmp/spark-checkpoints"
    }
```

### **Production/Azure**

```python
def spark_config_production():
    """Optimized configuration for production on Azure"""
    return {
        # Maximized resources
        "spark.driver.memory": "8g",
        "spark.driver.maxResultSize": "4g",
        "spark.executor.memory": "8g",
        "spark.executor.cores": "4", 
        "spark.executor.instances": "4",
        
        # High parallelism
        "spark.default.parallelism": "16",
        "spark.sql.shuffle.partitions": "400",
        
        # Network optimizations for Azure
        "spark.network.timeout": "1200s",
        "spark.sql.broadcastTimeout": "1200s",
        
        # Delta Lake optimizations
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",
        
        # Optimized cache
        "spark.sql.cache.serializer": "org.apache.spark.sql.execution.columnar.InMemoryTableScanExec",
        "spark.sql.columnVector.offheap.enabled": "true"
    }
```

---

## ☁️ Optimized Azure Integration

### **Azure Storage Configuration**

```python
def configure_azure_storage(spark, account_name, sas_token, containers):
    """
    Configures optimized connection with Azure Storage
    """
    # Azure base configuration
    spark.conf.set(f"fs.azure.account.auth.type.{account_name}.dfs.core.windows.net", "SAS")
    spark.conf.set(f"fs.azure.sas.token.provider.type.{account_name}.dfs.core.windows.net", 
                   "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    
    # Configure SAS token for each container
    for container in containers:
        spark.conf.set(f"fs.azure.sas.{container}.{account_name}.blob.core.windows.net", sas_token)
    
    # Azure-specific optimizations
    spark.conf.set("fs.azure.io.retry.max.retries", "10")
    spark.conf.set("fs.azure.io.retry.backoff.interval", "3s")
    spark.conf.set("fs.azure.block.size", "268435456")  # 256MB
    spark.conf.set("fs.azure.write.request.size", "67108864")  # 64MB
    
    # Optimized buffer
    spark.conf.set("fs.azure.read.request.size", "67108864")  # 64MB
    spark.conf.set("fs.azure.account.keyprovider.{account_name}.dfs.core.windows.net", 
                   "org.apache.hadoop.fs.azurebfs.services.SimpleKeyProvider")

# Usage example
spark = create_optimized_spark_session()
configure_azure_storage(spark, "youraccount", "your_sas_token", 
                       ["landing", "bronze", "silver", "gold"])
```

### **Azure Connection Pool**

```python
class AzureConnectionPool:
    """Reusable connection pool for Azure"""
    
    def __init__(self, account_name, sas_token, max_connections=10):
        self.account_name = account_name
        self.sas_token = sas_token
        self.max_connections = max_connections
        self._pool = []
        self._active_connections = 0
    
    def get_blob_client(self, container_name):
        """Gets blob client from pool"""
        if self._pool and self._active_connections < self.max_connections:
            return self._pool.pop()
        
        from azure.storage.blob import BlobServiceClient
        client = BlobServiceClient(
            account_url=f"https://{self.account_name}.blob.core.windows.net",
            credential=self.sas_token
        )
        self._active_connections += 1
        return client.get_container_client(container_name)
    
    def return_client(self, client):
        """Returns client to pool"""
        if len(self._pool) < self.max_connections:
            self._pool.append(client)
        self._active_connections -= 1
```

---

## 🔄 Performance Optimizations

### **Adaptive Query Execution (AQE)**

```python
def enable_aqe_optimizations(spark):
    """Enables Adaptive Query Execution optimizations"""
    
    # Main AQE
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    # Join Optimization
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
    
    # Local Shuffle Reader
    spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
    
    # Broadcast Join Threshold
    spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "50MB")
    
    # Coalesce Partitions
    spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "200")
    
    # Advisory Partition Size
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
```

### **Smart Cache**

```python
class SmartCache:
    """Smart cache for frequently accessed DataFrames"""
    
    def __init__(self, spark):
        self.spark = spark
        self.cached_dfs = {}
        self.access_count = {}
    
    def cache_if_beneficial(self, df, key, threshold=2):
        """Cache DataFrame if frequently accessed"""
        self.access_count[key] = self.access_count.get(key, 0) + 1
        
        if self.access_count[key] >= threshold and key not in self.cached_dfs:
            # Cache with optimized storage level
            df.cache()
            df.count()  # Force materialization
            self.cached_dfs[key] = df
            logger.info(f"DataFrame {key} cached after {self.access_count[key]} accesses")
        
        return self.cached_dfs.get(key, df)
    
    def clear_cache(self):
        """Clear cache for all DataFrames"""
        for key, df in self.cached_dfs.items():
            df.unpersist()
        self.cached_dfs.clear()
        self.access_count.clear()

# Usage example
cache_manager = SmartCache(spark)
df_customers = cache_manager.cache_if_beneficial(df_customers, "customers")
```

### **Optimized Partitioning**

```python
def optimize_partitioning(df, partition_cols, target_partition_size="128MB"):
    """
    Optimizes partitioning based on data size
    """
    # Calculate ideal number of partitions
    df_size_bytes = df.rdd.map(lambda x: len(str(x))).sum()
    target_size_bytes = int(target_partition_size.replace("MB", "")) * 1024 * 1024
    optimal_partitions = max(1, df_size_bytes // target_size_bytes)
    
    # Repartition if necessary
    current_partitions = df.rdd.getNumPartitions()
    if current_partitions != optimal_partitions:
        if partition_cols:
            df = df.repartition(optimal_partitions, *partition_cols)
        else:
            df = df.repartition(optimal_partitions)
    
    return df

# Usage example
df_deliveries_optimized = optimize_partitioning(
    df_deliveries, 
    ["year", "month"], 
    target_partition_size="256MB"
)
```

---

## 💾 I/O Optimizations

### **Optimized Reading**

```python
def read_delta_optimized(spark, path, filters=None, columns=None):
    """
    Optimized reading of Delta tables
    """
    reader = spark.read.format("delta")
    
    # Predicate pushdown
    if filters:
        for filter_expr in filters:
            reader = reader.filter(filter_expr)
    
    # Column pruning
    if columns:
        df = reader.load(path).select(*columns)
    else:
        df = reader.load(path)
    
    # Z-Order optimization hint
    if hasattr(df, 'hint'):
        df = df.hint("Z_ORDER", ["processing_date"])
    
    return df

# Usage example
df_deliveries = read_delta_optimized(
    spark,
    "wasbs://silver@account.blob.core.windows.net/deliveries",
    filters=["delivery_date >= '2024-01-01'"],
    columns=["delivery_id", "freight_value", "delivery_status"]
)
```

### **Optimized Writing**

```python
def write_delta_optimized(df, path, partition_cols=None, mode="overwrite"):
    """
    Optimized writing for Delta tables
    """
    writer = df.write.format("delta").mode(mode)
    
    # Optimization configurations
    writer = writer.option("overwriteSchema", "true") \
                   .option("mergeSchema", "true") \
                   .option("optimizeWrite", "true") \
                   .option("autoCompact", "true")
    
    # Partitioning if specified
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    
    # Configure target file size
    spark.conf.set("spark.databricks.delta.targetFileSize", "134217728")  # 128MB
    
    writer.save(path)
    
    # Post-write optimization
    spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (processing_date)")

# Usage example
write_delta_optimized(
    df_silver,
    "wasbs://silver@account.blob.core.windows.net/customers",
    partition_cols=["year", "month"]
)
```

---

## 🔍 Performance Monitoring

### **Spark Metrics**

```python
class SparkMetricsCollector:
    """Collects Spark performance metrics"""
    
    def __init__(self, spark):
        self.spark = spark
        self.metrics = {}
    
    def collect_job_metrics(self, job_description):
        """Collects metrics for a specific job"""
        sc = self.spark.sparkContext
        
        # Basic metrics
        self.metrics[job_description] = {
            "active_jobs": len(sc.statusTracker().getActiveJobIds()),
            "active_stages": len(sc.statusTracker().getActiveStageIds()),
            "executors": len(sc.statusTracker().getExecutorInfos()),
            "total_cores": sum(e.totalCores for e in sc.statusTracker().getExecutorInfos()),
            "memory_used": sum(e.memoryUsed for e in sc.statusTracker().getExecutorInfos()),
            "memory_total": sum(e.maxMemory for e in sc.statusTracker().getExecutorInfos())
        }
    
    def log_performance_summary(self):
        """Log performance summary"""
        for job, metrics in self.metrics.items():
            memory_usage_pct = (metrics["memory_used"] / metrics["memory_total"]) * 100
            logger.info(f"""
            Job: {job}
            Executors: {metrics["executors"]}
            Cores: {metrics["total_cores"]}
            Memory Usage: {memory_usage_pct:.1f}%
            """)

# Usage example
metrics_collector = SparkMetricsCollector(spark)
metrics_collector.collect_job_metrics("bronze_processing")
```

### **Performance Alerts**

```python
def setup_performance_alerts(spark):
    """Sets up performance alerts"""
    
    def check_memory_usage():
        sc = spark.sparkContext
        executors = sc.statusTracker().getExecutorInfos()
        
        for executor in executors:
            if executor.maxMemory > 0:
                usage_pct = (executor.memoryUsed / executor.maxMemory) * 100
                if usage_pct > 90:
                    logger.warning(f"Executor {executor.executorId} using {usage_pct:.1f}% of memory")
    
    def check_failed_tasks():
        sc = spark.sparkContext
        for stage_id in sc.statusTracker().getActiveStageIds():
            stage_info = sc.statusTracker().getStageInfo(stage_id)
            if stage_info and stage_info.numFailedTasks > 0:
                logger.error(f"Stage {stage_id} has {stage_info.numFailedTasks} failed tasks")
    
    # Run checks periodically
    import threading
    import time
    
    def monitor():
        while True:
            check_memory_usage()
            check_failed_tasks()
            time.sleep(30)  # Check every 30 seconds
    
    monitor_thread = threading.Thread(target=monitor, daemon=True)
    monitor_thread.start()
```

---

## 🛠️ Workload-Specific Configuration

### **ETL Batch Processing**

```python
def spark_config_etl_batch():
    """Optimized configuration for batch ETL processing"""
    return {
        # High memory for large volume processing
        "spark.executor.memory": "8g",
        "spark.driver.memory": "4g",
        
        # More partitions for parallelism
        "spark.sql.shuffle.partitions": "400",
        "spark.default.parallelism": "200",
        
        # Higher timeout for long jobs
        "spark.network.timeout": "1800s",
        "spark.sql.broadcastTimeout": "1800s",
        
        # Compression to save I/O
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.sql.orc.compression.codec": "snappy",
        
        # Serialization optimizations
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryo.unsafe": "true",
        "spark.kryo.registrationRequired": "false"
    }
```

### **Real-time Streaming**

```python
def spark_config_streaming():
    """Optimized configuration for streaming"""
    return {
        # Lower memory, more executors
        "spark.executor.memory": "2g",
        "spark.executor.cores": "2",
        "spark.executor.instances": "8",
        
        # Low latency
        "spark.streaming.blockInterval": "50ms",
        "spark.streaming.receiver.maxRate": "10000",
        
        # Frequent checkpoint
        "spark.sql.streaming.checkpointLocation": "/tmp/streaming-checkpoints",
        "spark.sql.streaming.minBatchesToRetain": "5",
        
        # Smaller buffer for low latency
        "spark.sql.shuffle.partitions": "100"
    }
```

---

## 📊 Benchmark and Tuning

### **Benchmark Script**

```python
def benchmark_spark_config(spark, df, operation_name):
    """Runs benchmark for a Spark operation"""
    import time
    
    start_time = time.time()
    
    # Force execution with count()
    if hasattr(df, 'count'):
        result_count = df.count()
    else:
        result_count = "N/A"
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    # Collect Spark metrics
    sc = spark.sparkContext
    executors = sc.statusTracker().getExecutorInfos()
    
    metrics = {
        "operation": operation_name,
        "execution_time": execution_time,
        "record_count": result_count,
        "records_per_second": result_count / execution_time if execution_time > 0 else 0,
        "num_executors": len(executors),
        "total_memory": sum(e.maxMemory for e in executors),
        "memory_used": sum(e.memoryUsed for e in executors)
    }
    
    logger.info(f"""
    Benchmark Results for {operation_name}:
    - Execution Time: {execution_time:.2f}s
    - Records Processed: {result_count:,}
    - Records/Second: {metrics['records_per_second']:,.0f}
    - Memory Usage: {(metrics['memory_used']/metrics['total_memory']*100):.1f}%
    """)
    
    return metrics

# Usage example
benchmark_spark_config(spark, df_bronze_processing, "Bronze Layer Processing")
```

---

## ⚙️ Dynamic Configuration

```python
class DynamicSparkConfig:
    """Dynamic configuration based on environment and workload"""
    
    def __init__(self):
        self.configs = {}
    
    def auto_configure(self, data_size_gb, operation_type="etl"):
        """Automatic configuration based on data size"""
        
        if data_size_gb < 1:
            # Small data
            memory_per_executor = "2g"
            num_executors = 1
            shuffle_partitions = 50
        elif data_size_gb < 10:
            # Medium data
            memory_per_executor = "4g"
            num_executors = 2
            shuffle_partitions = 200
        else:
            # Large data
            memory_per_executor = "8g"
            num_executors = 4
            shuffle_partitions = 400
        
        self.configs = {
            "spark.executor.memory": memory_per_executor,
            "spark.executor.instances": str(num_executors),
            "spark.sql.shuffle.partitions": str(shuffle_partitions),
            "spark.default.parallelism": str(shuffle_partitions // 2)
        }
        
        return self.configs
    
    def apply_to_spark(self, spark):
        """Apply configurations to SparkSession"""
        for key, value in self.configs.items():
            spark.conf.set(key, value)

# Usage example
config_manager = DynamicSparkConfig()
optimal_config = config_manager.auto_configure(data_size_gb=5.2)
config_manager.apply_to_spark(spark)
```

---

These optimizations can significantly improve your Spark pipeline performance, especially when processing large volumes of data in Azure Data Lake. Remember to always benchmark configurations in your specific environment to find ideal values.