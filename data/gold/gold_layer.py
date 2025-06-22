import os
import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, col, lit, monotonically_increasing_id,
    year, month, dayofmonth, quarter, weekofyear, dayofweek,
    date_format, to_date, expr, concat, when, sum, avg, count
)
from delta.tables import DeltaTable

# Configuração do Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Variáveis de Conexão do Azure Data Lake Storage
ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
SILVER_CONTAINER_NAME = os.getenv("ADLS_SILVER_CONTAINER_NAME")
GOLD_CONTAINER_NAME = os.getenv("ADLS_GOLD_CONTAINER_NAME")
SAS_TOKEN = os.getenv("ADLS_SAS_TOKEN", "").replace('"', '')

# Inicialização da Sessão Spark
def get_spark_session():
    return SparkSession.builder \
        .appName("gold_layer_dimensional") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-azure:3.3.6,org.apache.hadoop:hadoop-common:3.3.6") \
        .getOrCreate()

spark = get_spark_session()
spark.conf.set(f"fs.azure.sas.{SILVER_CONTAINER_NAME}.{ACCOUNT_NAME}.blob.core.windows.net", SAS_TOKEN)
spark.conf.set(f"fs.azure.sas.{GOLD_CONTAINER_NAME}.{ACCOUNT_NAME}.blob.core.windows.net", SAS_TOKEN)

# --- Funções Auxiliares ---

def get_silver_path(table_name):
    return f"wasbs://{SILVER_CONTAINER_NAME}@{ACCOUNT_NAME}.blob.core.windows.net/{table_name}"

def get_gold_path(table_name):
    return f"wasbs://{GOLD_CONTAINER_NAME}@{ACCOUNT_NAME}.blob.core.windows.net/{table_name}"

def load_silver_table(table_name):
    path = get_silver_path(table_name)
    logging.info(f"Lendo tabela da camada Silver: {table_name} de {path}")
    return spark.read.format("delta").load(path)

def save_gold_table(df, table_name):
    path = get_gold_path(table_name)
    logging.info(f"Salvando tabela na camada Gold: {table_name} em {path}")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)

def merge_dimension(df_updates, table_name, merge_key):
    """
    Executa um merge (SCD Tipo 1) em uma tabela de dimensão.
    Cria a tabela se ela não existir.
    """
    gold_path = get_gold_path(table_name)
    logging.info(f"Executando merge para a dimensão: {table_name}")

    df_updates = df_updates.withColumn("_GOLD_INGESTION_TIMESTAMP", current_timestamp())

    if not DeltaTable.isDeltaTable(spark, gold_path):
        logging.info(f"Tabela {table_name} não encontrada. Criando nova tabela.")
        df_updates.write.format("delta").save(gold_path)
    else:
        delta_table = DeltaTable.forPath(spark, gold_path)
        delta_table.alias("target") \
            .merge(
                df_updates.alias("source"),
                f"target.{merge_key} = source.{merge_key}"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    logging.info(f"Merge para a dimensão {table_name} concluído.")

# --- Processamento das Dimensões ---

def process_dim_data():
    logging.info("Processando Dim_Data")
    # Criar uma tabela de datas de 2020 a 2030
    df = spark.sql("SELECT sequence(to_date('2020-01-01'), to_date('2030-12-31'), interval 1 day) as dates") \
        .withColumn("data_completa", expr("explode(dates)"))

    dim_data = df.select(
        col("data_completa"),
        date_format(col("data_completa"), "yyyyMMdd").cast("int").alias("data_key"),
        year(col("data_completa")).alias("ano"),
        quarter(col("data_completa")).alias("trimestre"),
        concat(lit("T"), quarter(col("data_completa"))).alias("nome_trimestre"),
        month(col("data_completa")).alias("mes"),
        date_format(col("data_completa"), "MMMM").alias("nome_mes"),
        dayofmonth(col("data_completa")).alias("dia_do_mes"),
        dayofweek(col("data_completa")).alias("dia_da_semana"),
        date_format(col("data_completa"), "E").alias("nome_dia_da_semana"),
        weekofyear(col("data_completa")).alias("semana_do_ano"),
        (dayofweek(col("data_completa")).isin([1, 7])).cast("boolean").alias("eh_fim_de_semana")
    ).withColumn("eh_feriado", lit(False)) \
     .withColumn("feriado_nome", lit(None).cast("string"))

    save_gold_table(dim_data, "Dim_Data")
    logging.info("Dim_Data processada com sucesso.")

def process_dimensions():
    logging.info("Iniciando processamento das dimensões.")
    process_dim_data()

    # Dim_Cliente
    df_clientes_silver = load_silver_table("clientes")
    df_dim_cliente = df_clientes_silver.select(
        col("ID_CLIENTE").alias("id_cliente_origem"),
        col("NOME").alias("nome_cliente"),
        col("TIPO_CLIENTE").alias("tipo_cliente"),
        col("DOCUMENTO").alias("cpf_cnpj"),
        col("EMAIL").alias("email"),
        col("TELEFONE").alias("telefone"),
        col("ENDERECO").alias("endereco"),
        col("CIDADE").alias("cidade"),
        col("ESTADO").alias("estado"),
        col("CEP").alias("cep"),
        col("DATA_CADASTRO").alias("data_cadastro")
    )
    merge_dimension(df_dim_cliente, "Dim_Cliente", "id_cliente_origem")

    # Dim_Motorista
    df_motoristas_silver = load_silver_table("motoristas")
    df_dim_motorista = df_motoristas_silver.select(
        col("ID_MOTORISTA").alias("id_motorista_origem"),
        col("NOME").alias("nome_motorista"),
        col("CPF"),
        col("CNH").alias("numero_cnh"),
        col("DATA_NASCIMENTO").alias("data_nascimento"),
        col("TELEFONE").alias("telefone"),
        col("EMAIL").alias("email"),
        col("ATIVO").alias("status_ativo"),
        col("DATA_CONTRATACAO").alias("data_contratacao")
    )
    merge_dimension(df_dim_motorista, "Dim_Motorista", "id_motorista_origem")

    # Dim_Veiculo
    df_veiculos_silver = load_silver_table("veiculos")
    df_dim_veiculo = df_veiculos_silver.select(
        col("ID_VEICULO").alias("id_veiculo_origem"),
        col("PLACA").alias("placa"),
        col("MODELO").alias("modelo"),
        col("MARCA").alias("marca"),
        col("ANO").alias("ano_fabricacao"),
        col("CAPACIDADE_KG").alias("capacidade_carga_kg"),
        col("TIPO_VEICULO").alias("tipo_veiculo"),
        col("STATUS").alias("status_operacional")
    )
    merge_dimension(df_dim_veiculo, "Dim_Veiculo", "id_veiculo_origem")

    # Dim_Tipo_Carga
    df_cargas_silver = load_silver_table("tipos_carga")
    df_dim_carga = df_cargas_silver.select(
        col("ID_TIPO_CARGA").alias("id_tipo_carga_origem"),
        col("NOME_TIPO").alias("nome_tipo"),
        col("DESCRICAO").alias("descricao_tipo"),
        col("REFRIGERADO").alias("requer_refrigeracao"),
        col("PESO_MEDIO_KG").alias("peso_medio_kg")
    )
    merge_dimension(df_dim_carga, "Dim_Tipo_Carga", "id_tipo_carga_origem")

    # Dim_Rota
    df_rotas_silver = load_silver_table("rotas")
    df_dim_rota = df_rotas_silver.select(
        col("ID_ROTA").alias("id_rota_origem"),
        col("NOME_ROTA").alias("nome_rota"),
        col("ORIGEM").alias("origem"),
        col("DESTINO").alias("destino"),
        col("DISTANCIA_KM").alias("distancia_km"),
        col("TEMPO_ESTIMADO_H").alias("tempo_estimado_horas")
    )
    merge_dimension(df_dim_rota, "Dim_Rota", "id_rota_origem")

    logging.info("Processamento de todas as dimensões concluído.")

# --- Processamento das Fatos ---

def process_facts():
    logging.info("Iniciando processamento das tabelas Fato.")

    # Carregar dimensões para lookup
    dim_veiculo = spark.read.format("delta").load(get_gold_path("Dim_Veiculo")).withColumn("id_veiculo_key", monotonically_increasing_id())
    dim_motorista = spark.read.format("delta").load(get_gold_path("Dim_Motorista")).withColumn("id_motorista_key", monotonically_increasing_id())
    dim_cliente = spark.read.format("delta").load(get_gold_path("Dim_Cliente")).withColumn("id_cliente_key", monotonically_increasing_id())
    dim_rota = spark.read.format("delta").load(get_gold_path("Dim_Rota")).withColumn("id_rota_key", monotonically_increasing_id())
    dim_tipo_carga = spark.read.format("delta").load(get_gold_path("Dim_Tipo_Carga")).withColumn("id_tipo_carga_key", monotonically_increasing_id())
    dim_data = spark.read.format("delta").load(get_gold_path("Dim_Data"))

    # Fato_Entregas
    logging.info("Processando Fato_Entregas.")
    fato_entregas_silver = load_silver_table("entregas")
    fato_entregas = fato_entregas_silver \
        .join(dim_veiculo, fato_entregas_silver.ID_VEICULO == dim_veiculo.id_veiculo_origem, "left") \
        .join(dim_motorista, fato_entregas_silver.ID_MOTORISTA == dim_motorista.id_motorista_origem, "left") \
        .join(dim_cliente.alias("rem"), fato_entregas_silver.ID_CLIENTE_REMETENTE == col("rem.id_cliente_origem"), "left") \
        .join(dim_cliente.alias("dest"), fato_entregas_silver.ID_CLIENTE_DESTINATARIO == col("dest.id_cliente_origem"), "left") \
        .join(dim_rota, fato_entregas_silver.ID_ROTA == dim_rota.id_rota_origem, "left") \
        .join(dim_tipo_carga, fato_entregas_silver.ID_TIPO_CARGA == dim_tipo_carga.id_tipo_carga_origem, "left") \
        .join(dim_data.alias("d_ini"), to_date(fato_entregas_silver.DATA_INICIO) == col("d_ini.data_completa"), "left") \
        .join(dim_data.alias("d_prev"), to_date(fato_entregas_silver.DATA_PREVISAO) == col("d_prev.data_completa"), "left") \
        .join(dim_data.alias("d_fim"), to_date(fato_entregas_silver.DATA_FIM) == col("d_fim.data_completa"), "left") \
        .select(
            col("id_veiculo_key"),
            col("id_motorista_key"),
            col("rem.id_cliente_key").alias("id_cliente_remetente_key"),
            col("dest.id_cliente_key").alias("id_cliente_destinatario_key"),
            col("id_rota_key"),
            col("id_tipo_carga_key"),
            col("d_ini.data_key").alias("data_inicio_entrega_key"),
            col("d_prev.data_key").alias("data_previsao_fim_entrega_key"),
            col("d_fim.data_key").alias("data_fim_real_entrega_key"),
            col("ID_ENTREGA").alias("id_entrega_degenerada"),
            col("STATUS_ENTREGA").alias("status_entrega"),
            col("VALOR_FRETE").alias("valor_frete"),
            col("PESO_KG").alias("peso_carga_kg")
        ).withColumn("_GOLD_INGESTION_TIMESTAMP", current_timestamp())
    save_gold_table(fato_entregas, "Fato_Entregas")

    # Outras tabelas Fato (Coletas, Manutencoes, etc.) seguiriam o mesmo padrão de join e select.
    # Por brevidade, o código para elas não está totalmente expandido aqui, mas a lógica é idêntica.
    logging.info("Processamento das tabelas Fato concluído.")

# --- KPIs e Métricas ---
def process_kpis_and_metrics():
    logging.info("Calculando KPIs e Métricas.")
    fato_entregas = spark.read.format("delta").load(get_gold_path("Fato_Entregas"))
    dim_data = spark.read.format("delta").load(get_gold_path("Dim_Data"))

    # KPI 1: Percentual de Entregas no Prazo (On-Time Delivery)
    kpi_otd = fato_entregas.withColumn("on_time",
        (col("data_fim_real_entrega_key") <= col("data_previsao_fim_entrega_key")) | (col("data_previsao_fim_entrega_key").isNull())
    ).agg(
        (sum(when(col("on_time") == True, 1).otherwise(0)) / count("*") * 100).alias("percentual_entregas_no_prazo")
    )
    save_gold_table(kpi_otd, "kpi_percentual_entregas_no_prazo")

    # KPI 2: Custo Médio de Frete por Rota
    dim_rota = spark.read.format("delta").load(get_gold_path("Dim_Rota"))
    kpi_custo_rota = fato_entregas.join(dim_rota, "id_rota_key") \
        .groupBy("nome_rota", "origem", "destino") \
        .agg(avg("valor_frete").alias("custo_medio_frete"))
    save_gold_table(kpi_custo_rota, "kpi_custo_medio_frete_por_rota")

    # KPI 3: Total de Entregas por Tipo de Veículo
    dim_veiculo = spark.read.format("delta").load(get_gold_path("Dim_Veiculo"))
    kpi_entregas_veiculo = fato_entregas.join(dim_veiculo, "id_veiculo_key") \
        .groupBy("tipo_veiculo") \
        .agg(count("*").alias("total_entregas"))
    save_gold_table(kpi_entregas_veiculo, "kpi_total_entregas_por_tipo_veiculo")

    # KPI 4: Valor Total de Frete por Cliente
    dim_cliente = spark.read.format("delta").load(get_gold_path("Dim_Cliente"))
    kpi_valor_cliente = fato_entregas.join(dim_cliente, fato_entregas.id_cliente_remetente_key == dim_cliente.id_cliente_key) \
        .groupBy("nome_cliente") \
        .agg(sum("valor_frete").alias("valor_total_frete"))
    save_gold_table(kpi_valor_cliente, "kpi_valor_total_frete_por_cliente")

    # Métrica 1: Total de Entregas Mensal
    metrica_entregas_mes = fato_entregas.join(dim_data, fato_entregas.data_inicio_entrega_key == dim_data.data_key) \
        .groupBy("ano", "mes") \
        .agg(count("*").alias("total_entregas")) \
        .orderBy("ano", "mes")
    save_gold_table(metrica_entregas_mes, "metrica_total_entregas_mensal")

    # Métrica 2: Peso Total Transportado por Mês
    metrica_peso_mes = fato_entregas.join(dim_data, fato_entregas.data_inicio_entrega_key == dim_data.data_key) \
        .groupBy("ano", "mes") \
        .agg(sum("peso_carga_kg").alias("peso_total_kg")) \
        .orderBy("ano", "mes")
    save_gold_table(metrica_peso_mes, "metrica_peso_total_transportado_mensal")

    logging.info("Cálculo de KPIs e Métricas concluído.")

if __name__ == "__main__":
    try:
        process_dimensions()
        process_facts()
        process_kpis_and_metrics()
        logging.info("Pipeline GOLD dimensional executado com sucesso!")
    except Exception as e:
        logging.error(f"Erro na execução do pipeline GOLD: {e}", exc_info=True)
        raise e
    finally:
        spark.stop()
        logging.info("Sessão Spark finalizada.")
