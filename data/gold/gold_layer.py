import os
import logging
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, max as spark_max, min as spark_min, lit, current_timestamp

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Carrega .env
load_dotenv()

# Variáveis Azure
ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
SILVER_CONTAINER_NAME = os.getenv("ADLS_SILVER_CONTAINER_NAME")
GOLD_CONTAINER_NAME = os.getenv("ADLS_GOLD_CONTAINER_NAME")
SAS_TOKEN = os.getenv("ADLS_SAS_TOKEN").replace('"', '')

# Blob Service
blob_service_client = BlobServiceClient(
    account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
    credential=SAS_TOKEN
)

gold_container_client = blob_service_client.get_container_client(GOLD_CONTAINER_NAME)

# Cria container GOLD se não existir
try:
    gold_container_client.create_container()
    logging.info(f"Container '{GOLD_CONTAINER_NAME}' criado.")
except Exception:
    logging.warning(f"Container '{GOLD_CONTAINER_NAME}' já existe ou não foi possível criar.")

# Spark Session
spark = SparkSession.builder \
    .appName("gold_layer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-azure:3.3.6,org.apache.hadoop:hadoop-common:3.3.6,com.microsoft.azure:azure-storage:8.6.6") \
    .getOrCreate()

spark.conf.set(f"fs.azure.sas.{SILVER_CONTAINER_NAME}.{ACCOUNT_NAME}.blob.core.windows.net", SAS_TOKEN)
spark.conf.set(f"fs.azure.sas.{GOLD_CONTAINER_NAME}.{ACCOUNT_NAME}.blob.core.windows.net", SAS_TOKEN)

def load_silver_table(table_name):
    path = f"wasbs://{SILVER_CONTAINER_NAME}@{ACCOUNT_NAME}.blob.core.windows.net/{table_name}"
    logging.info(f"Lendo tabela Silver: {table_name}")
    return spark.read.format("delta").load(path)

def save_gold_table(df, table_name):
    path = f"wasbs://{GOLD_CONTAINER_NAME}@{ACCOUNT_NAME}.blob.core.windows.net/{table_name}"
    logging.info(f"Salvando tabela Gold: {table_name}")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)

def process_gold():
    """
    Exemplo de processo Gold para análises agregadas e enriquecidas.
    """

    # 1. Clientes + Total de entregas e valor total de frete por cliente
    clientes = load_silver_table("clientes")
    entregas = load_silver_table("entregas")

    entregas_agg = entregas.groupBy("DOCUMENTO").agg(
        count("*").alias("TOTAL_ENTREGAS"),
        spark_sum("VALOR_FRETE").alias("VALOR_TOTAL_FRETE"),
        spark_sum("PESO_KG").alias("PESO_TOTAL_KG")
    )

    clientes_entregas = clientes.join(entregas_agg, clientes.DOCUMENTO == entregas_agg.DOCUMENTO, "left") \
                                .drop(entregas_agg.DOCUMENTO)

    clientes_entregas = clientes_entregas.fillna({"TOTAL_ENTREGAS": 0, "VALOR_TOTAL_FRETE": 0.0, "PESO_TOTAL_KG": 0.0})

    clientes_entregas = clientes_entregas.withColumn("_GOLD_INGESTION_TIMESTAMP", current_timestamp())

    save_gold_table(clientes_entregas, "clientes_analises")

    logging.info("Tabela 'clientes_analises' processada com sucesso.")

    # 2. Motoristas + total de viagens + status ativos
    motoristas = load_silver_table("motoristas")

    viagens = entregas.groupBy("NOME_MOTORISTA").agg(
        count("*").alias("TOTAL_VIAGENS"),
        spark_sum("VALOR_FRETE").alias("VALOR_TOTAL_FRETE_VIAGENS")
    )

    motoristas_analise = motoristas.join(viagens, motoristas.NOME == viagens.NOME_MOTORISTA, "left") \
                                   .drop(viagens.NOME_MOTORISTA)

    motoristas_analise = motoristas_analise.fillna({"TOTAL_VIAGENS": 0, "VALOR_TOTAL_FRETE_VIAGENS": 0.0})

    motoristas_analise = motoristas_analise.withColumn("_GOLD_INGESTION_TIMESTAMP", current_timestamp())

    save_gold_table(motoristas_analise, "motoristas_analises")

    logging.info("Tabela 'motoristas_analises' processada com sucesso.")

    # 3. Veículos + total km rodados + status operacional
    veiculos = load_silver_table("veiculos")

    rotas = load_silver_table("rotas")

    entregas_rotas = entregas.join(rotas, entregas.NOME_ROTA == rotas.NOME_ROTA, "left")

    km_por_veiculo = entregas_rotas.groupBy("PLACA").agg(
        spark_sum("DISTANCIA").alias("KM_TOTAL_RODADOS"),
        count("*").alias("TOTAL_ENTREGAS")
    )

    veiculos_analise = veiculos.join(km_por_veiculo, "PLACA", "left")

    veiculos_analise = veiculos_analise.fillna({"KM_TOTAL_RODADOS": 0, "TOTAL_ENTREGAS": 0})

    veiculos_analise = veiculos_analise.withColumn("_GOLD_INGESTION_TIMESTAMP", current_timestamp())

    save_gold_table(veiculos_analise, "veiculos_analises")

    logging.info("Tabela 'veiculos_analises' processada com sucesso.")

    # 4. Análise geral de entregas por status e mês/ano
    from pyspark.sql.functions import date_format

    entregas_status = entregas.withColumn("ANO", date_format(col("DATA_INICIO"), "yyyy")) \
                              .withColumn("MES", date_format(col("DATA_INICIO"), "MM")) \
                              .groupBy("ANO", "MES", "STATUS_ENTREGA") \
                              .agg(
                                  count("*").alias("TOTAL_ENTREGAS"),
                                  spark_sum("VALOR_FRETE").alias("VALOR_TOTAL_FRETE"),
                                  spark_sum("PESO_KG").alias("PESO_TOTAL_KG")
                              ) \
                              .orderBy("ANO", "MES")

    entregas_status = entregas_status.withColumn("_GOLD_INGESTION_TIMESTAMP", current_timestamp())

    save_gold_table(entregas_status, "entregas_status_analise")

    logging.info("Tabela 'entregas_status_analise' processada com sucesso.")

if __name__ == "__main__":
    try:
        process_gold()
        logging.info("Pipeline GOLD executado com sucesso!")
    except Exception as e:
        logging.error(f"Erro na execução do pipeline GOLD: {str(e)}")
    finally:
        spark.stop()
        logging.info("Sessão Spark finalizada.")
