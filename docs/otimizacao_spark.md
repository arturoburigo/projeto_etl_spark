# ⚡ Otimização do Apache Spark

## 📋 Visão Geral

A otimização da conexão e configuração do Apache Spark é crucial para obter máxima performance no processamento de dados. Este guia apresenta as melhores práticas e configurações otimizadas para o projeto ETL.

---

## 🔧 Configurações de Conexão Otimizadas

### **SparkSession Otimizada**

```python
def create_optimized_spark_session(app_name="projeto_etl_spark"):
    """
    Cria uma SparkSession otimizada para o projeto ETL
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

## 🚀 Otimizações por Ambiente

### **Desenvolvimento Local**

```python
def spark_config_development():
    """Configuração otimizada para desenvolvimento local"""
    return {
        # Recursos limitados para desenvolvimento
        "spark.driver.memory": "2g",
        "spark.executor.memory": "2g", 
        "spark.executor.cores": "1",
        "spark.executor.instances": "1",
        
        # Paralelismo reduzido
        "spark.default.parallelism": "4",
        "spark.sql.shuffle.partitions": "50",
        
        # Logs mais verbosos para debug
        "spark.sql.adaptive.logLevel": "INFO",
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": "/tmp/spark-events",
        
        # Checkpoint local
        "spark.sql.streaming.checkpointLocation": "/tmp/spark-checkpoints"
    }
```

### **Produção/Azure**

```python
def spark_config_production():
    """Configuração otimizada para produção no Azure"""
    return {
        # Recursos maximizados
        "spark.driver.memory": "8g",
        "spark.driver.maxResultSize": "4g",
        "spark.executor.memory": "8g",
        "spark.executor.cores": "4", 
        "spark.executor.instances": "4",
        
        # Paralelismo alto
        "spark.default.parallelism": "16",
        "spark.sql.shuffle.partitions": "400",
        
        # Otimizações de rede para Azure
        "spark.network.timeout": "1200s",
        "spark.sql.broadcastTimeout": "1200s",
        
        # Delta Lake otimizações
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",
        
        # Cache otimizado
        "spark.sql.cache.serializer": "org.apache.spark.sql.execution.columnar.InMemoryTableScanExec",
        "spark.sql.columnVector.offheap.enabled": "true"
    }
```

---

## ☁️ Integração com Azure Otimizada

### **Configuração Azure Storage**

```python
def configure_azure_storage(spark, account_name, sas_token, containers):
    """
    Configura conexão otimizada com Azure Storage
    """
    # Configuração base do Azure
    spark.conf.set(f"fs.azure.account.auth.type.{account_name}.dfs.core.windows.net", "SAS")
    spark.conf.set(f"fs.azure.sas.token.provider.type.{account_name}.dfs.core.windows.net", 
                   "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    
    # Configurar SAS token para cada container
    for container in containers:
        spark.conf.set(f"fs.azure.sas.{container}.{account_name}.blob.core.windows.net", sas_token)
    
    # Otimizações específicas do Azure
    spark.conf.set("fs.azure.io.retry.max.retries", "10")
    spark.conf.set("fs.azure.io.retry.backoff.interval", "3s")
    spark.conf.set("fs.azure.block.size", "268435456")  # 256MB
    spark.conf.set("fs.azure.write.request.size", "67108864")  # 64MB
    
    # Buffer otimizado
    spark.conf.set("fs.azure.read.request.size", "67108864")  # 64MB
    spark.conf.set("fs.azure.account.keyprovider.{account_name}.dfs.core.windows.net", 
                   "org.apache.hadoop.fs.azurebfs.services.SimpleKeyProvider")

# Exemplo de uso
spark = create_optimized_spark_session()
configure_azure_storage(spark, "seuaccount", "seu_sas_token", 
                       ["landing", "bronze", "silver", "gold"])
```

### **Pool de Conexões Azure**

```python
class AzureConnectionPool:
    """Pool de conexões reutilizáveis para Azure"""
    
    def __init__(self, account_name, sas_token, max_connections=10):
        self.account_name = account_name
        self.sas_token = sas_token
        self.max_connections = max_connections
        self._pool = []
        self._active_connections = 0
    
    def get_blob_client(self, container_name):
        """Obtém cliente blob do pool"""
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
        """Retorna cliente para o pool"""
        if len(self._pool) < self.max_connections:
            self._pool.append(client)
        self._active_connections -= 1
```

---

## 🔄 Otimizações de Performance

### **Adaptive Query Execution (AQE)**

```python
def enable_aqe_optimizations(spark):
    """Habilita otimizações do Adaptive Query Execution"""
    
    # AQE Principal
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    # Otimização de Joins
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

### **Cache Inteligente**

```python
class SmartCache:
    """Cache inteligente para DataFrames frequentemente acessados"""
    
    def __init__(self, spark):
        self.spark = spark
        self.cached_dfs = {}
        self.access_count = {}
    
    def cache_if_beneficial(self, df, key, threshold=2):
        """Cache DataFrame se for acessado frequentemente"""
        self.access_count[key] = self.access_count.get(key, 0) + 1
        
        if self.access_count[key] >= threshold and key not in self.cached_dfs:
            # Cache com storage level otimizado
            df.cache()
            df.count()  # Força materialização
            self.cached_dfs[key] = df
            logger.info(f"DataFrame {key} foi cacheado após {self.access_count[key]} acessos")
        
        return self.cached_dfs.get(key, df)
    
    def clear_cache(self):
        """Limpa cache de todos os DataFrames"""
        for key, df in self.cached_dfs.items():
            df.unpersist()
        self.cached_dfs.clear()
        self.access_count.clear()

# Exemplo de uso
cache_manager = SmartCache(spark)
df_clientes = cache_manager.cache_if_beneficial(df_clientes, "clientes")
```

### **Particionamento Otimizado**

```python
def optimize_partitioning(df, partition_cols, target_partition_size="128MB"):
    """
    Otimiza particionamento baseado no tamanho dos dados
    """
    # Calcular número ideal de partições
    df_size_bytes = df.rdd.map(lambda x: len(str(x))).sum()
    target_size_bytes = int(target_partition_size.replace("MB", "")) * 1024 * 1024
    optimal_partitions = max(1, df_size_bytes // target_size_bytes)
    
    # Reparticionar se necessário
    current_partitions = df.rdd.getNumPartitions()
    if current_partitions != optimal_partitions:
        if partition_cols:
            df = df.repartition(optimal_partitions, *partition_cols)
        else:
            df = df.repartition(optimal_partitions)
    
    return df

# Exemplo de uso
df_entregas_optimized = optimize_partitioning(
    df_entregas, 
    ["ano", "mes"], 
    target_partition_size="256MB"
)
```

---

## 💾 Otimizações de I/O

### **Leitura Otimizada**

```python
def read_delta_optimized(spark, path, filters=None, columns=None):
    """
    Leitura otimizada de tabelas Delta
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
        df = df.hint("Z_ORDER", ["data_processamento"])
    
    return df

# Exemplo de uso
df_entregas = read_delta_optimized(
    spark,
    "wasbs://silver@account.blob.core.windows.net/entregas",
    filters=["data_entrega >= '2024-01-01'"],
    columns=["id_entrega", "valor_frete", "status_entrega"]
)
```

### **Escrita Otimizada**

```python
def write_delta_optimized(df, path, partition_cols=None, mode="overwrite"):
    """
    Escrita otimizada para tabelas Delta
    """
    writer = df.write.format("delta").mode(mode)
    
    # Configurações de otimização
    writer = writer.option("overwriteSchema", "true") \
                   .option("mergeSchema", "true") \
                   .option("optimizeWrite", "true") \
                   .option("autoCompact", "true")
    
    # Particionamento se especificado
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    
    # Configurar tamanho de arquivo alvo
    spark.conf.set("spark.databricks.delta.targetFileSize", "134217728")  # 128MB
    
    writer.save(path)
    
    # Otimização pós-escrita
    spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (data_processamento)")

# Exemplo de uso
write_delta_optimized(
    df_silver,
    "wasbs://silver@account.blob.core.windows.net/clientes",
    partition_cols=["ano", "mes"]
)
```

---

## 🔍 Monitoramento de Performance

### **Métricas de Spark**

```python
class SparkMetricsCollector:
    """Coleta métricas de performance do Spark"""
    
    def __init__(self, spark):
        self.spark = spark
        self.metrics = {}
    
    def collect_job_metrics(self, job_description):
        """Coleta métricas de um job específico"""
        sc = self.spark.sparkContext
        
        # Métricas básicas
        self.metrics[job_description] = {
            "active_jobs": len(sc.statusTracker().getActiveJobIds()),
            "active_stages": len(sc.statusTracker().getActiveStageIds()),
            "executors": len(sc.statusTracker().getExecutorInfos()),
            "total_cores": sum(e.totalCores for e in sc.statusTracker().getExecutorInfos()),
            "memory_used": sum(e.memoryUsed for e in sc.statusTracker().getExecutorInfos()),
            "memory_total": sum(e.maxMemory for e in sc.statusTracker().getExecutorInfos())
        }
    
    def log_performance_summary(self):
        """Log resumo de performance"""
        for job, metrics in self.metrics.items():
            memory_usage_pct = (metrics["memory_used"] / metrics["memory_total"]) * 100
            logger.info(f"""
            Job: {job}
            Executors: {metrics["executors"]}
            Cores: {metrics["total_cores"]}
            Memory Usage: {memory_usage_pct:.1f}%
            """)

# Exemplo de uso
metrics_collector = SparkMetricsCollector(spark)
metrics_collector.collect_job_metrics("bronze_processing")
```

### **Alertas de Performance**

```python
def setup_performance_alerts(spark):
    """Configura alertas de performance"""
    
    def check_memory_usage():
        sc = spark.sparkContext
        executors = sc.statusTracker().getExecutorInfos()
        
        for executor in executors:
            if executor.maxMemory > 0:
                usage_pct = (executor.memoryUsed / executor.maxMemory) * 100
                if usage_pct > 90:
                    logger.warning(f"Executor {executor.executorId} usando {usage_pct:.1f}% da memória")
    
    def check_failed_tasks():
        sc = spark.sparkContext
        for stage_id in sc.statusTracker().getActiveStageIds():
            stage_info = sc.statusTracker().getStageInfo(stage_id)
            if stage_info and stage_info.numFailedTasks > 0:
                logger.error(f"Stage {stage_id} tem {stage_info.numFailedTasks} tasks falhadas")
    
    # Executar verificações periodicamente
    import threading
    import time
    
    def monitor():
        while True:
            check_memory_usage()
            check_failed_tasks()
            time.sleep(30)  # Verificar a cada 30 segundos
    
    monitor_thread = threading.Thread(target=monitor, daemon=True)
    monitor_thread.start()
```

---

## 🛠️ Configuração por Tipo de Workload

### **ETL Batch Processing**

```python
def spark_config_etl_batch():
    """Configuração otimizada para processamento ETL em lote"""
    return {
        # Memória alta para processamento de grandes volumes
        "spark.executor.memory": "8g",
        "spark.driver.memory": "4g",
        
        # Mais partições para paralelismo
        "spark.sql.shuffle.partitions": "400",
        "spark.default.parallelism": "200",
        
        # Timeout maior para jobs longos
        "spark.network.timeout": "1800s",
        "spark.sql.broadcastTimeout": "1800s",
        
        # Compressão para economizar I/O
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.sql.orc.compression.codec": "snappy",
        
        # Otimizações de serialização
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryo.unsafe": "true",
        "spark.kryo.registrationRequired": "false"
    }
```

### **Real-time Streaming**

```python
def spark_config_streaming():
    """Configuração otimizada para streaming"""
    return {
        # Memória menor, mais executors
        "spark.executor.memory": "2g",
        "spark.executor.cores": "2",
        "spark.executor.instances": "8",
        
        # Baixa latência
        "spark.streaming.blockInterval": "50ms",
        "spark.streaming.receiver.maxRate": "10000",
        
        # Checkpoint frequente
        "spark.sql.streaming.checkpointLocation": "/tmp/streaming-checkpoints",
        "spark.sql.streaming.minBatchesToRetain": "5",
        
        # Buffer menor para baixa latência
        "spark.sql.shuffle.partitions": "100"
    }
```

---

## 📊 Benchmark e Tuning

### **Script de Benchmark**

```python
def benchmark_spark_config(spark, df, operation_name):
    """Executa benchmark de uma operação Spark"""
    import time
    
    start_time = time.time()
    
    # Força execução com count()
    if hasattr(df, 'count'):
        result_count = df.count()
    else:
        result_count = "N/A"
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    # Coleta métricas do Spark
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

# Exemplo de uso
benchmark_spark_config(spark, df_bronze_processing, "Bronze Layer Processing")
```

---

## ⚙️ Configuração Dinâmica

```python
class DynamicSparkConfig:
    """Configuração dinâmica baseada no ambiente e workload"""
    
    def __init__(self):
        self.configs = {}
    
    def auto_configure(self, data_size_gb, operation_type="etl"):
        """Configuração automática baseada no tamanho dos dados"""
        
        if data_size_gb < 1:
            # Dados pequenos
            memory_per_executor = "2g"
            num_executors = 1
            shuffle_partitions = 50
        elif data_size_gb < 10:
            # Dados médios
            memory_per_executor = "4g"
            num_executors = 2
            shuffle_partitions = 200
        else:
            # Dados grandes
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
        """Aplica configurações ao SparkSession"""
        for key, value in self.configs.items():
            spark.conf.set(key, value)

# Exemplo de uso
config_manager = DynamicSparkConfig()
optimal_config = config_manager.auto_configure(data_size_gb=5.2)
config_manager.apply_to_spark(spark)
```

---

Essas otimizações podem melhorar significativamente a performance do seu pipeline Spark, especialmente quando processando grandes volumes de dados no Azure Data Lake. Lembre-se de sempre fazer benchmark das configurações em seu ambiente específico para encontrar os valores ideais. 