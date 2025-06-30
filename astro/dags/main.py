from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import os
from dotenv import load_dotenv
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_date, current_timestamp, trim, upper, lower, regexp_replace, col, to_date, to_timestamp, when, expr
from pyspark.sql.types import DateType, TimestampType, IntegerType, DoubleType, FloatType, LongType, StringType
import findspark
import sys
import importlib.util

# Add the include directory to the Python path to import the layer modules
sys.path.append('/usr/local/airflow/include')

findspark.init()

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global Spark session that will be shared across all tasks
spark_session = None

def get_or_create_spark_session():
    """Create a single Spark session that will be used throughout the entire pipeline"""
    global spark_session
    
    if spark_session is None:
        logger.info("Creating Spark session for the entire pipeline...")
        spark_session = SparkSession.builder \
            .appName("etl_pipeline") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-azure:3.3.6,org.apache.hadoop:hadoop-common:3.3.6,com.microsoft.azure:azure-storage:8.6.6") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        logger.info("Spark session created successfully")
    
    return spark_session

def stop_spark_session():
    """Stop the global Spark session"""
    global spark_session
    if spark_session is not None:
        logger.info("Stopping Spark session...")
        spark_session.stop()
        spark_session = None
        logger.info("Spark session stopped")

def load_layer_module(module_path, module_name):
    """Dynamically load a Python module from a file path"""
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

@dag(
    dag_id="Medallion Architecture - ETL",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mssql", "adls", "bronze", "etl", "azure"],
    description="DAG para extrair dados do SQL Server, carregar no ADLS e processar na camada Bronze"
)
def airflow_pipeline():

    @task()
    def landing_zone_pipeline():
        load_dotenv()

        # Azure Data Lake
        account_name = os.getenv("ADLS_ACCOUNT_NAME")
        file_system_name = os.getenv("ADLS_FILE_SYSTEM_NAME")
        directory_name = "/"  # Usando diretório raiz
        sas_token = os.getenv("ADLS_SAS_TOKEN")

        # SQL Server
        server = os.getenv("SQL_SERVER")
        database = os.getenv("SQL_DATABASE")
        schema = os.getenv("SQL_SCHEMA")
        username = os.getenv("SQL_USERNAME")
        password = quote_plus(os.getenv("SQL_PASSWORD"))

        try:
            # Criar conexão com SQL Server com parâmetros adicionais
            conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes&Connection_Timeout=60&ConnectRetryCount=3&ConnectRetryInterval=10&ApplicationIntent=ReadWrite&MultiSubnetFailover=Yes"
            engine = create_engine(conn_str, pool_timeout=60, pool_recycle=3600, pool_pre_ping=True)
            logger.info("Conexão com SQL Server estabelecida com sucesso")

            # Cliente ADLS
            service_client = DataLakeServiceClient(
                account_url=f"https://{account_name}.dfs.core.windows.net",
                credential=sas_token
            )
            logger.info("Cliente ADLS criado com sucesso")

            # Obter sistema de arquivos e diretório
            file_system_client = service_client.get_file_system_client(file_system_name)
            directory_client = file_system_client.get_directory_client(directory_name)

            # Obter lista de tabelas
            query_tables = f"SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = '{schema}'"
            df_tables = pd.read_sql(query_tables, engine)
            logger.info(f"Encontradas {len(df_tables)} tabelas no schema {schema}")

            # Processar cada tabela
            for _, row in df_tables.iterrows():
                table_name = row["table_name"]
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                file_name = f"{table_name}_{timestamp}.csv"
                
                # Extrair dados da tabela
                df = pd.read_sql(f"SELECT * FROM {schema}.{table_name}", engine)
                
                # Upload para ADLS
                file_client = directory_client.get_file_client(file_name)
                file_client.upload_data(df.to_csv(index=False).encode(), overwrite=True)
                logger.info(f"Dados da tabela '{table_name}' carregados com sucesso para {file_name}")

            logger.info("Processo de extração e upload concluído com sucesso")

        except Exception as e:
            logger.error(f"Erro durante a execução: {str(e)}")
            raise

    @task()
    def bronze_pipeline():
        """
        Task 2: Call the bronze layer Python script
        """
        try:
            # Get the shared Spark session
            spark = get_or_create_spark_session()
            
            # Configure SAS tokens for the Spark session
            load_dotenv()
            account_name = os.getenv("ADLS_ACCOUNT_NAME")
            landing_container = os.getenv("ADLS_FILE_SYSTEM_NAME")
            bronze_container = os.getenv("ADLS_BRONZE_CONTAINER_NAME", "bronze")
            sas_token = os.getenv("ADLS_SAS_TOKEN").replace('"', '')
            
            spark.conf.set(f"fs.azure.sas.{landing_container}.{account_name}.blob.core.windows.net", sas_token)
            spark.conf.set(f"fs.azure.sas.{bronze_container}.{account_name}.blob.core.windows.net", sas_token)
            
            # Load and call the bronze layer module
            bronze_module = load_layer_module("/usr/local/airflow/include/bronze_layer.py", "bronze_layer")
            bronze_module.copy_csvs_to_bronze(spark)
            
            logger.info("Bronze layer processing completed successfully")
            
        except Exception as e:
            logger.error(f"Erro durante a execução do bronze pipeline: {str(e)}")
            raise
        
    @task()
    def silver_pipeline():
        """
        Task 3: Call the silver layer Python script
        """
        try:
            # Get the shared Spark session
            spark = get_or_create_spark_session()
            
            # Configure SAS tokens for the Spark session
            load_dotenv()
            account_name = os.getenv("ADLS_ACCOUNT_NAME")
            bronze_container = os.getenv("ADLS_BRONZE_CONTAINER_NAME")
            silver_container = os.getenv("ADLS_SILVER_CONTAINER_NAME")
            sas_token = os.getenv("ADLS_SAS_TOKEN").replace('"', '')
            
            spark.conf.set(f"fs.azure.sas.{bronze_container}.{account_name}.blob.core.windows.net", sas_token)
            spark.conf.set(f"fs.azure.sas.{silver_container}.{account_name}.blob.core.windows.net", sas_token)
            
            # Load and call the silver layer module
            silver_module = load_layer_module("/usr/local/airflow/include/silver_layer.py", "silver_layer")
            silver_module.process_bronze_to_silver(spark)
            
            logger.info("Silver layer processing completed successfully")
            
        except Exception as e:
            logger.error(f"Erro durante a execução do silver pipeline: {str(e)}")
            raise

    @task()
    def gold_pipeline():
        """
        Task 4: Call the gold layer Python script
        """
        try:
            # Get the shared Spark session
            spark = get_or_create_spark_session()
            
            # Configure SAS tokens for the Spark session
            load_dotenv()
            account_name = os.getenv("ADLS_ACCOUNT_NAME")
            silver_container = os.getenv("ADLS_SILVER_CONTAINER_NAME")
            gold_container = os.getenv("ADLS_GOLD_CONTAINER_NAME")
            sas_token = os.getenv("ADLS_SAS_TOKEN").replace('"', '')
            
            spark.conf.set(f"fs.azure.sas.{silver_container}.{account_name}.blob.core.windows.net", sas_token)
            spark.conf.set(f"fs.azure.sas.{gold_container}.{account_name}.blob.core.windows.net", sas_token)
            
            # Load and call the gold layer module
            gold_module = load_layer_module("/usr/local/airflow/include/gold_layer.py", "gold_layer")
            gold_module.run_gold_pipeline(spark)
            
            logger.info("Gold layer processing completed successfully")
            
        except Exception as e:
            logger.error(f"Erro durante a execução do gold pipeline: {str(e)}")
            raise

    @task()
    def cleanup_spark_session():
        """
        Task 5: Cleanup task to stop the Spark session at the end of the pipeline
        """
        try:
            stop_spark_session()
            logger.info("Pipeline completed and Spark session cleaned up successfully")
        except Exception as e:
            logger.error(f"Error during Spark session cleanup: {str(e)}")
            raise

    # Definir dependências do pipeline
    landing_task = landing_zone_pipeline()
    bronze_task = bronze_pipeline()
    silver_task = silver_pipeline()
    gold_task = gold_pipeline()
    cleanup_task = cleanup_spark_session()
    
    # Definir ordem de execução
    landing_task >> bronze_task >> silver_task >> gold_task >> cleanup_task

dag = airflow_pipeline() 