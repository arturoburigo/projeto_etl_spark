import os
import logging
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_date, current_timestamp

# Configuração do logging para exibir informações no console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

# Recupera as variáveis de ambiente necessárias para conexão com o Azure Blob Storage
ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
LANDING_CONTAINER_NAME = os.getenv("ADLS_FILE_SYSTEM_NAME")
BRONZE_CONTAINER_NAME = os.getenv("ADLS_BRONZE_CONTAINER_NAME")
SAS_TOKEN = os.getenv("ADLS_SAS_TOKEN").replace('"', '')

# Cria o cliente de serviço do Blob Storage usando a URL da conta e o SAS Token
blob_service_client = BlobServiceClient(
    account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
    credential=SAS_TOKEN
)

# Cria clientes para os containers de origem (landing) e destino (bronze)
landing_container_client = blob_service_client.get_container_client(LANDING_CONTAINER_NAME)
bronze_container_client = blob_service_client.get_container_client(BRONZE_CONTAINER_NAME)

# Se o container bronze não existir, cria ele
try:
    bronze_container_client.create_container()
    logging.info(f"Container '{BRONZE_CONTAINER_NAME}' created.")
except Exception as e:
    # A exceção mais comum aqui é o container já existir, o que não é um erro.
    logging.warning(f"Could not create container '{BRONZE_CONTAINER_NAME}'. It might already exist.")
    pass

# Cria SparkSession com as configurações corretas para Delta Lake e Azure
# Esta é a parte crucial que foi corrigida
spark = SparkSession.builder \
    .appName("bronze_layer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-azure:3.3.6,org.apache.hadoop:hadoop-common:3.3.6,com.microsoft.azure:azure-storage:8.6.6") \
    .getOrCreate()


# Função que copia arquivos CSV do container landing para o container bronze
def copy_csvs_to_bronze():
    # Configura credenciais SAS para acesso via Spark
    spark.conf.set(f"fs.azure.sas.{LANDING_CONTAINER_NAME}.{ACCOUNT_NAME}.blob.core.windows.net", SAS_TOKEN)
    spark.conf.set(f"fs.azure.sas.{BRONZE_CONTAINER_NAME}.{ACCOUNT_NAME}.blob.core.windows.net", SAS_TOKEN)
    
    # Lista arquivos CSV no container landing
    blobs = landing_container_client.list_blobs()
    csv_blobs = [b for b in blobs if b.name.endswith('.csv')]
    logging.info(f"Found {len(csv_blobs)} CSV files in landing container.")

    for blob in csv_blobs:
        try:
            source_path = f"wasbs://{LANDING_CONTAINER_NAME}@{ACCOUNT_NAME}.blob.core.windows.net/{blob.name}"
            file_name = os.path.basename(blob.name)
            table_name = os.path.splitext(file_name)[0]
            
            logging.info(f"Processing file: {file_name}")
            
            # Lê CSV no Spark DataFrame, inferindo o schema
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
            
            # Adiciona colunas de metadata
            df = df.withColumn("processing_date", current_date()) \
                   .withColumn("processing_timestamp", current_timestamp()) \
                   .withColumn("source_file_name", lit(file_name))
            
            # Define o caminho de destino Delta no container bronze
            dest_path = f"wasbs://{BRONZE_CONTAINER_NAME}@{ACCOUNT_NAME}.blob.core.windows.net/{table_name}"
            
            # Escreve em formato Delta Lake, sobrescrevendo se já existir
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(dest_path)
            logging.info(f"Successfully written Delta table for {file_name} to bronze at {dest_path}")
        except Exception as e:
            logging.error(f"Failed to process file {blob.name}. Error: {e}")


    logging.info("All CSV files processed and written to bronze as Delta tables.")

# Executa a função principal se o script for chamado diretamente
if __name__ == "__main__":
    copy_csvs_to_bronze()
    spark.stop()
    logging.info("Spark session stopped.")
