import os
import logging
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, max as spark_max, min as spark_min, lit, current_timestamp, date_format

# =======================
# Logging e Variáveis
# =======================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

load_dotenv()

ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
SILVER_CONTAINER_NAME = os.getenv("ADLS_SILVER_CONTAINER_NAME")
GOLD_CONTAINER_NAME = os.getenv("ADLS_GOLD_CONTAINER_NAME")
SAS_TOKEN = os.getenv("ADLS_SAS_TOKEN").replace('"', '')

# =======================
# Conexão Azure Blob
# =======================
blob_service_client = BlobServiceClient(
    account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
    credential=SAS_TOKEN
)

gold_container_client = blob_service_client.get_container_client(GOLD_CONTAINER_NAME)

try:
    gold_container_client.create_container()
    logging.info(f"Container '{GOLD_CONTAINER_NAME}' criado.")
except Exception:
    logging.warning(f"Container '{GOLD_CONTAINER_NAME}' já existe ou não foi possível criar.")

# =======================
# Spark Session
# =======================
spark = SparkSession.builder \
    .appName("gold_layer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-azure:3.3.6,org.apache.hadoop:hadoop-common:3.3.6,com.microsoft.azure:azure-storage:8.6.6") \
    .getOrCreate()

spark.conf.set(f"fs.azure.sas.{SILVER_CONTAINER_NAME}.{ACCOUNT_NAME}.blob.core.windows.net", SAS_TOKEN)
spark.conf.set(f"fs.azure.sas.{GOLD_CONTAINER_NAME}.{ACCOUNT_NAME}.blob.core.windows.net", SAS_TOKEN)

# =======================
# Funções auxiliares
# =======================
def load_silver_table(table_name):
    path = f"wasbs://{SILVER_CONTAINER_NAME}@{ACCOUNT_NAME}.blob.core.windows.net/{table_name}"
    logging.info(f"Lendo tabela Silver: {table_name}")
    return spark.read.format("delta").load(path)

def save_gold_table(df, table_name):
    path = f"wasbs://{GOLD_CONTAINER_NAME}@{ACCOUNT_NAME}.blob.core.windows.net/{table_name}"
    logging.info(f"Salvando tabela Gold: {table_name}")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)

# ================================
# Funções de Validação de Dados
# ================================
def verificar_nulos(df, colunas_criticas):
    for coluna in colunas_criticas:
        nulos = df.filter(f"{coluna} IS NULL").count()
        if nulos > 0:
            logging.warning(f"🚨 Existem {nulos} valores nulos na coluna '{coluna}'")
        else:
            logging.info(f"✅ Coluna '{coluna}' sem valores nulos.")

def verificar_duplicatas(df, chave):
    total = df.count()
    distintos = df.dropDuplicates([chave]).count()
    if total != distintos:
        logging.warning(f"🚨 Existem {total - distintos} registros duplicados na chave '{chave}'")
    else:
        logging.info(f"✅ Nenhuma duplicata encontrada na chave '{chave}'")

def verificar_volume(df, minimo):
    total = df.count()
    if total < minimo:
        logging.warning(f"🚨 Volume abaixo do esperado! Total: {total}, Esperado: {minimo}")
    else:
        logging.info(f"✅ Volume ok. Total de registros: {total}")

# ================================
# Processo GOLD
# ================================
def process_gold():

    clientes = load_silver_table("clientes")
    entregas = load_silver_table("entregas")

    # 🔸 Clientes + Entregas
    entregas_agg = entregas.groupBy("DOCUMENTO").agg(
        count("*").alias("TOTAL_ENTREGAS"),
        spark_sum("VALOR_FRETE").alias("VALOR_TOTAL_FRETE"),
        spark_sum("PESO_KG").alias("PESO_TOTAL_KG")
    )

    clientes_entregas = clientes.join(entregas_agg, clientes.DOCUMENTO == entregas_agg.DOCUMENTO, "left") \
                                .drop(entregas_agg.DOCUMENTO)

    clientes_entregas = clientes_entregas.fillna({"TOTAL_ENTREGAS": 0, "VALOR_TOTAL_FRETE": 0.0, "PESO_TOTAL_KG": 0.0})
    clientes_entregas = clientes_entregas.withColumn("_GOLD_INGESTION_TIMESTAMP", current_timestamp())

    # 🔍 Testes
    verificar_nulos(clientes_entregas, ["DOCUMENTO", "TOTAL_ENTREGAS", "VALOR_TOTAL_FRETE"])
    verificar_duplicatas(clientes_entregas, "DOCUMENTO")
    verificar_volume(clientes_entregas, minimo=10)

    save_gold_table(clientes_entregas, "clientes_analises")
    logging.info("Tabela 'clientes_analises' processada com sucesso.")

    # 🔸 Motoristas
    motoristas = load_silver_table("motoristas")
    viagens = entregas.groupBy("NOME_MOTORISTA").agg(
        count("*").alias("TOTAL_VIAGENS"),
        spark_sum("VALOR_FRETE").alias("VALOR_TOTAL_FRETE_VIAGENS")
    )

    motoristas_analise = motoristas.join(viagens, motoristas.NOME == viagens.NOME_MOTORISTA, "left") \
                                   .drop(viagens.NOME_MOTORISTA)

    motoristas_analise = motoristas_analise.fillna({"TOTAL_VIAGENS": 0, "VALOR_TOTAL_FRETE_VIAGENS": 0.0})
    motoristas_analise = motoristas_analise.withColumn("_GOLD_INGESTION_TIMESTAMP", current_timestamp())

    verificar_nulos(motoristas_analise, ["NOME", "TOTAL_VIAGENS"])
    verificar_duplicatas(motoristas_analise, "NOME")
    verificar_volume(motoristas_analise, minimo=5)

    save_gold_table(motoristas_analise, "motoristas_analises")
    logging.info("Tabela 'motoristas_analises' processada com sucesso.")

    # 🔸 Veículos
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

    verificar_nulos(veiculos_analise, ["PLACA", "TOTAL_ENTREGAS"])
    verificar_duplicatas(veiculos_analise, "PLACA")
    verificar_volume(veiculos_analise, minimo=5)

    save_gold_table(veiculos_analise, "veiculos_analises")
    logging.info("Tabela 'veiculos_analises' processada com sucesso.")

    # Entregas Status
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

    verificar_nulos(entregas_status, ["ANO", "MES", "STATUS_ENTREGA"])
    verificar_volume(entregas_status, minimo=5)

    save_gold_table(entregas_status, "entregas_status_analise")
    logging.info("Tabela 'entregas_status_analise' processada com sucesso.")

# =======================
# Executar Pipeline
# =======================
if __name__ == "__main__":
    try:
        process_gold()
        logging.info("Pipeline GOLD executado com sucesso!")
    except Exception as e:
        logging.error(f"Erro na execução do pipeline GOLD: {str(e)}")
    finally:
        spark.stop()
        logging.info("Sessão Spark finalizada.")
