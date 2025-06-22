import os
import logging
import re
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, col, lit, monotonically_increasing_id,
    year, month, dayofmonth, quarter, weekofyear, dayofweek,
    date_format, to_date, expr, concat, when, sum, avg, count
)
from delta.tables import DeltaTable
from azure.storage.blob import BlobServiceClient

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

def find_latest_silver_path(table_name_prefix):
    """
    Encontra o caminho mais recente para uma tabela na camada Silver,
    considerando os sufixos de timestamp e a capitalização.
    """
    logging.info(f"Procurando pelo caminho mais recente da tabela '{table_name_prefix}' na camada Silver.")
    try:
        account_url = f"https://{ACCOUNT_NAME}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=SAS_TOKEN)
        container_client = blob_service_client.get_container_client(SILVER_CONTAINER_NAME)

        candidate_folders = set()
        for blob in container_client.list_blobs():
            # Filtra para garantir que estamos olhando para o prefixo correto, ignorando o caso
            if blob.name.lower().startswith(table_name_prefix.lower()):
                folder_name = blob.name.split('/')[0]
                candidate_folders.add(folder_name)

        if not candidate_folders:
            logging.warning(f"Nenhuma pasta encontrada com o prefixo '{table_name_prefix}' no container '{SILVER_CONTAINER_NAME}'.")
            return None

        latest_folder = None
        latest_timestamp = None
        regex = re.compile(r'(\d{8}_\d{6})$')

        for folder in candidate_folders:
            match = regex.search(folder)
            if match:
                timestamp_str = match.group(1)
                try:
                    current_timestamp = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
                    if latest_timestamp is None or current_timestamp > latest_timestamp:
                        latest_timestamp = current_timestamp
                        latest_folder = folder
                except ValueError:
                    continue
        
        if latest_folder:
            logging.info(f"Pasta mais recente encontrada para '{table_name_prefix}': {latest_folder}")
            return latest_folder
        else:
            # Fallback para pastas sem timestamp
            for folder in candidate_folders:
                if folder.lower() == table_name_prefix.lower():
                    logging.info(f"Nenhuma pasta com timestamp encontrada. Usando correspondência exata: {folder}")
                    return folder
            logging.warning(f"Nenhuma pasta com sufixo de data/hora válido encontrada para '{table_name_prefix}'. Retornando a primeira encontrada como fallback.")
            return list(candidate_folders)[0] if candidate_folders else None

    except Exception as e:
        logging.error(f"Erro ao listar blobs no container '{SILVER_CONTAINER_NAME}': {e}", exc_info=True)
        raise

def create_container_if_not_exists(account_name, container_name, sas_token):
    """Cria um container no Azure Blob Storage se ele não existir."""
    if not container_name:
        logging.error("Nome do container de destino (Gold) não definido. Verifique a variável de ambiente ADLS_GOLD_CONTAINER_NAME.")
        raise ValueError("Nome do container Gold não pode ser nulo.")
    try:
        account_url = f"https://{account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=sas_token)
        container_client = blob_service_client.get_container_client(container_name)

        if not container_client.exists():
            logging.info(f"Container '{container_name}' não existe. Criando...")
            container_client.create_container()
            logging.info(f"Container '{container_name}' criado com sucesso.")
        else:
            logging.info(f"Container '{container_name}' já existe.")
    except Exception as e:
        logging.error(f"Falha ao criar ou verificar o container '{container_name}': {e}", exc_info=True)
        raise

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
    latest_folder_name = find_latest_silver_path(table_name)
    
    if not latest_folder_name:
        error_msg = f"ERRO: Nenhuma pasta de origem para a tabela '{table_name}' foi encontrada no container Silver '{SILVER_CONTAINER_NAME}'."
        logging.error(error_msg)
        logging.error("Verifique se o pipeline da camada Silver foi executado e se os nomes das pastas estão no formato esperado (ex: Tabela_YYYYMMDD_HHMMSS).")
        raise FileNotFoundError(error_msg)

    path = get_silver_path(latest_folder_name)
    logging.info(f"Lendo tabela da camada Silver: {table_name} (pasta: {latest_folder_name}) de {path}")
    try:
        return spark.read.format("delta").load(path)
    except Exception as e:
        if 'Path does not exist' in str(e):
            logging.error(f"ERRO: A tabela de origem '{table_name}' (pasta: {latest_folder_name}) não foi encontrada no caminho: {path}")
            logging.error("Verifique se o pipeline da camada Silver foi executado com sucesso antes de rodar o pipeline da Gold.")
            raise FileNotFoundError(f"Tabela de origem não encontrada na camada Silver: {path}") from e
        else:
            logging.error(f"Erro inesperado ao ler a tabela '{table_name}' (pasta: {latest_folder_name}) da camada Silver.", exc_info=True)
            raise e

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
        col("IDENTIFICADOR_CLIENTE").alias("id_cliente_origem"),
        col("NOME_CLIENTE").alias("nome_cliente"),
        col("TIPO_CLIENTE").alias("tipo_cliente"),
        col("DOCUMENTO_CPF_CNPJ").alias("cpf_cnpj"),
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
        col("IDENTIFICADOR_MOTORISTA").alias("id_motorista_origem"),
        col("NOME_MOTORISTA").alias("nome_motorista"),
        col("CPF"),
        col("NUMERO_CNH").alias("numero_cnh"),
        col("DATA_NASCIMENTO").alias("data_nascimento"),
        col("TELEFONE").alias("telefone"),
        col("EMAIL").alias("email"),
        col("STATUS_ATIVO").alias("status_ativo"),
        col("DATA_CONTRATACAO").alias("data_contratacao")
    )
    merge_dimension(df_dim_motorista, "Dim_Motorista", "id_motorista_origem")

    # Dim_Veiculo
    df_veiculos_silver = load_silver_table("veiculos")
    df_dim_veiculo = df_veiculos_silver.select(
        col("IDENTIFICADOR_VEICULO").alias("id_veiculo_origem"),
        col("PLACA").alias("placa"),
        col("MODELO").alias("modelo"),
        col("MARCA").alias("marca"),
        col("ANO_FABRICACAO").alias("ano_fabricacao"),
        col("CAPACIDADE_CARGA_KG").alias("capacidade_carga_kg"),
        col("TIPO_VEICULO").alias("tipo_veiculo"),
        col("STATUS_OPERACIONAL").alias("status_operacional")
    )
    merge_dimension(df_dim_veiculo, "Dim_Veiculo", "id_veiculo_origem")

    # Dim_Tipo_Carga
    df_cargas_silver = load_silver_table("tipos_carga")
    df_dim_carga = df_cargas_silver.select(
        col("IDENTIFICADOR_TIPO_CARGA").alias("id_tipo_carga_origem"),
        col("NOME_TIPO").alias("nome_tipo"),
        col("DESCRICAO_TIPO").alias("descricao_tipo"),
        col("REQUER_REFRIGERACAO").alias("requer_refrigeracao"),
        col("PESO_MEDIO_KG").alias("peso_medio_kg")
    )
    merge_dimension(df_dim_carga, "Dim_Tipo_Carga", "id_tipo_carga_origem")

    # Dim_Rota
    df_rotas_silver = load_silver_table("rotas")
    df_dim_rota = df_rotas_silver.select(
        col("IDENTIFICADOR_ROTA").alias("id_rota_origem"),
        col("NOME_ROTA").alias("nome_rota"),
        col("ORIGEM").alias("origem"),
        col("DESTINO").alias("destino"),
        col("DISTANCIA_KM").alias("distancia_km"),
        col("TEMPO_ESTIMADO_HORAS").alias("tempo_estimado_horas")
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
    dim_rota = spark.read.format("delta").load(get_gold_path("Dim_Rota"))
    dim_tipo_carga = spark.read.format("delta").load(get_gold_path("Dim_Tipo_Carga")).withColumn("id_tipo_carga_key", monotonically_increasing_id())
    dim_data = spark.read.format("delta").load(get_gold_path("Dim_Data"))

    # Fato_Entregas
    logging.info("Processando Fato_Entregas.")
    fato_entregas_silver = load_silver_table("entregas")
    # Use col("NOME_COLUNA") para acessar colunas, não atributo direto
    fato_entregas = fato_entregas_silver \
        .join(dim_veiculo, col("IDENTIFICADOR_VEICULO") == dim_veiculo.id_veiculo_origem, "left") \
        .join(dim_motorista, col("IDENTIFICADOR_MOTORISTA") == dim_motorista.id_motorista_origem, "left") \
        .join(dim_cliente.alias("rem"), col("IDENTIFICADOR_CLIENTE_REMETENTE") == col("rem.id_cliente_origem"), "left") \
        .join(dim_cliente.alias("dest"), col("IDENTIFICADOR_CLIENTE_DESTINATARIO") == col("dest.id_cliente_origem"), "left") \
        .join(dim_rota, col("IDENTIFICADOR_ROTA") == dim_rota.id_rota_origem, "left") \
        .join(dim_tipo_carga, col("IDENTIFICADOR_TIPO_CARGA") == dim_tipo_carga.id_tipo_carga_origem, "left") \
        .join(dim_data.alias("d_ini"), to_date(col("DATA_INICIO_ENTREGA")) == col("d_ini.data_completa"), "left") \
        .join(dim_data.alias("d_prev"), to_date(col("DATA_PREVISAO_FIM_ENTREGA")) == col("d_prev.data_completa"), "left") \
        .join(dim_data.alias("d_fim"), to_date(col("DATA_FIM_REAL_ENTREGA")) == col("d_fim.data_completa"), "left") \
        .select(
            col("id_veiculo_key"),
            col("id_motorista_key"),
            col("rem.id_cliente_key").alias("id_cliente_remetente_key"),
            col("dest.id_cliente_key").alias("id_cliente_destinatario_key"),
            col("id_tipo_carga_key"),
            col("d_ini.data_key").alias("data_inicio_entrega_key"),
            col("d_prev.data_key").alias("data_previsao_fim_entrega_key"),
            col("d_fim.data_key").alias("data_fim_real_entrega_key"),
            col("IDENTIFICADOR_ENTREGA").alias("id_entrega_degenerada"),
            col("STATUS_ENTREGA").alias("status_entrega"),
            col("VALOR_FRETE").alias("valor_frete"),
            col("PESO_CARGA_KG").alias("peso_carga_kg"),
            # Adicionando chaves naturais para joins robustos nos KPIs
            col("IDENTIFICADOR_VEICULO").alias("id_veiculo_origem"),
            col("IDENTIFICADOR_MOTORISTA").alias("id_motorista_origem"),
            col("IDENTIFICADOR_CLIENTE_REMETENTE").alias("id_cliente_remetente_origem"),
            col("IDENTIFICADOR_CLIENTE_DESTINATARIO").alias("id_cliente_destinatario_origem"),
            col("IDENTIFICADOR_ROTA").alias("id_rota_origem"),
            col("IDENTIFICADOR_TIPO_CARGA").alias("id_tipo_carga_origem")
        ).withColumn("_GOLD_INGESTION_TIMESTAMP", current_timestamp())
    save_gold_table(fato_entregas, "Fato_Entregas")

    # Fato_Coletas
    logging.info("Processando Fato_Coletas.")
    fato_coletas_silver = load_silver_table("coletas")
    fato_entregas_gold = spark.read.format("delta").load(get_gold_path("Fato_Entregas"))
    
    # A FK em Fato_Coletas aponta para a dimensão degenerada em Fato_Entregas
    fato_coletas = fato_coletas_silver \
        .join(fato_entregas_gold, fato_coletas_silver.IDENTIFICADOR_ENTREGA == fato_entregas_gold.id_entrega_degenerada, "left") \
        .join(dim_data, to_date(fato_coletas_silver.DATA_HORA_COLETA) == dim_data.data_completa, "left") \
        .select(
            col("id_entrega_degenerada").alias("id_entrega_key"),
            col("data_key").alias("data_hora_coleta_key"),
            col("IDENTIFICADOR_COLETA").alias("id_coleta_degenerada"),
            col("STATUS_COLETA").alias("status_coleta"),
            col("ENDERECO_COLETA").alias("endereco_coleta"),
            col("OBSERVACOES").alias("observacoes"),
            lit(1).alias("quantidade_coletas")
        ).withColumn("_GOLD_INGESTION_TIMESTAMP", current_timestamp())
    save_gold_table(fato_coletas, "Fato_Coletas")    # Fato_Rotas
    logging.info("Processando Fato_Rotas.")
    rotas_df = load_silver_table("rotas")
    fato_rotas = rotas_df.alias("r").join(dim_rota.alias("dr"), col("r.IDENTIFICADOR_ROTA") == col("dr.id_rota_origem"), "left") \
        .select(
            col("dr.id_rota_origem").alias("id_rota_key"),
            col("r.IDENTIFICADOR_ROTA").alias("id_rota_origem"),
            col("r.DISTANCIA_KM").alias("distancia_km"),
            col("r.TEMPO_ESTIMADO_HORAS").alias("tempo_estimado_horas")
        ).withColumn("_GOLD_INGESTION_TIMESTAMP", current_timestamp())
    save_gold_table(fato_rotas, "Fato_Rotas")    # Fato_Manutencoes
    logging.info("Processando Fato_Manutencoes.")
    fato_manutencoes_silver = load_silver_table("manutencoes")
    fato_manutencoes = fato_manutencoes_silver \
        .join(dim_veiculo, col("IDENTIFICADOR_VEICULO") == dim_veiculo.id_veiculo_origem, "left") \
        .join(dim_data.alias("d_manut"), to_date(col("DATA_MANUTENCAO")) == col("d_manut.data_completa"), "left") \
        .select(
            col("id_veiculo_key"),
            col("d_manut.data_key").alias("data_manutencao_key"),
            col("IDENTIFICADOR_MANUTENCAO").alias("id_manutencao_degenerada"),
            col("TIPO_MANUTENCAO").alias("tipo_manutencao"),
            col("DESCRICAO_SERVICO").alias("descricao_servico"),
            col("CUSTO_MANUTENCAO").alias("custo_manutencao"),
            col("TEMPO_PARADO_HORAS").alias("tempo_parado_horas"),
            # Adicionando chaves naturais
            col("IDENTIFICADOR_VEICULO").alias("id_veiculo_origem"),
            col("IDENTIFICADOR_MANUTENCAO").alias("id_manutencao_origem")
        ).withColumn("_GOLD_INGESTION_TIMESTAMP", current_timestamp())
    save_gold_table(fato_manutencoes, "Fato_Manutencoes")

    # Fato_Abastecimentos
    logging.info("Processando Fato_Abastecimentos.")
    fato_abastecimentos_silver = load_silver_table("abastecimentos")
    fato_abastecimentos = fato_abastecimentos_silver \
        .join(dim_veiculo, col("IDENTIFICADOR_VEICULO") == dim_veiculo.id_veiculo_origem, "left") \
        .join(dim_data.alias("d_abast"), to_date(col("DATA_ABASTECIMENTO")) == col("d_abast.data_completa"), "left") \
        .select(
            col("id_veiculo_key"),
            col("d_abast.data_key").alias("data_abastecimento_key"),
            col("IDENTIFICADOR_ABASTECIMENTO").alias("id_abastecimento_degenerada"),
            col("TIPO_COMBUSTIVEL").alias("tipo_combustivel"),
            col("LITROS").alias("litros"),
            col("VALOR_TOTAL").alias("valor_total"),
            # Adicionando chaves naturais
            col("IDENTIFICADOR_VEICULO").alias("id_veiculo_origem")
        ).withColumn("_GOLD_INGESTION_TIMESTAMP", current_timestamp())
    save_gold_table(fato_abastecimentos, "Fato_Abastecimentos")

    # Fato_Multas
    logging.info("Processando Fato_Multas.")
    fato_multas_silver = load_silver_table("multas")
    fato_multas = fato_multas_silver \
        .join(dim_veiculo, col("IDENTIFICADOR_VEICULO") == dim_veiculo.id_veiculo_origem, "left") \
        .join(dim_motorista, col("IDENTIFICADOR_MOTORISTA") == dim_motorista.id_motorista_origem, "left") \
        .join(dim_data.alias("d_multa"), to_date(col("DATA_MULTA")) == col("d_multa.data_completa"), "left") \
        .select(
            col("id_veiculo_key"),
            col("id_motorista_key"),
            col("d_multa.data_key").alias("data_multa_key"),
            col("IDENTIFICADOR_MULTA").alias("id_multa_degenerada"),
            col("LOCAL_MULTA").alias("local_multa"),
            col("DESCRICAO_INFRACAO").alias("descricao_infracao"),
            col("STATUS_PAGAMENTO").alias("status_pagamento"),
            col("VALOR_MULTA").alias("valor_multa"),
            # Adicionando chaves naturais
            col("IDENTIFICADOR_VEICULO").alias("id_veiculo_origem"),
            col("IDENTIFICADOR_MOTORISTA").alias("id_motorista_origem")
        ).withColumn("_GOLD_INGESTION_TIMESTAMP", current_timestamp())
    save_gold_table(fato_multas, "Fato_Multas")

    logging.info("Processamento de todas as tabelas Fato concluído.")

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
    kpi_custo_rota = fato_entregas.join(dim_rota, fato_entregas["id_rota_origem"] == dim_rota["id_rota_origem"], "left") \
        .groupBy(dim_rota["nome_rota"], dim_rota["origem"], dim_rota["destino"]) \
        .agg(avg("valor_frete").alias("custo_medio_frete"))
    save_gold_table(kpi_custo_rota, "kpi_custo_medio_frete_por_rota")

    # KPI 3: Total de Entregas por Tipo de Veículo
    dim_veiculo = spark.read.format("delta").load(get_gold_path("Dim_Veiculo"))
    kpi_entregas_veiculo = fato_entregas.join(dim_veiculo, fato_entregas["id_veiculo_origem"] == dim_veiculo["id_veiculo_origem"], "left") \
        .groupBy(dim_veiculo["tipo_veiculo"]) \
        .agg(count("*").alias("total_entregas"))
    save_gold_table(kpi_entregas_veiculo, "kpi_total_entregas_por_tipo_veiculo")

    # KPI 4: Valor Total de Frete por Cliente
    dim_cliente = spark.read.format("delta").load(get_gold_path("Dim_Cliente"))
    kpi_valor_cliente = fato_entregas.join(dim_cliente, fato_entregas["id_cliente_remetente_origem"] == dim_cliente["id_cliente_origem"], "left") \
        .groupBy(dim_cliente["nome_cliente"]) \
        .agg(sum("valor_frete").alias("valor_total_frete"))
    save_gold_table(kpi_valor_cliente, "kpi_valor_total_frete_por_cliente")

    # Métrica 1: Total de Entregas Mensal
    metrica_entregas_mes = fato_entregas.join(dim_data, fato_entregas["data_inicio_entrega_key"] == dim_data["data_key"], "left") \
        .groupBy(dim_data["ano"], dim_data["mes"]) \
        .agg(count("*").alias("total_entregas")) \
        .orderBy("ano", "mes")
    save_gold_table(metrica_entregas_mes, "metrica_total_entregas_mensal")

    # Métrica 2: Peso Total Transportado por Mês
    metrica_peso_mes = fato_entregas.join(dim_data, fato_entregas["data_inicio_entrega_key"] == dim_data["data_key"], "left") \
        .groupBy(dim_data["ano"], dim_data["mes"]) \
        .agg(sum("peso_carga_kg").alias("peso_total_kg")) \
        .orderBy("ano", "mes")
    save_gold_table(metrica_peso_mes, "metrica_peso_total_transportado_mensal")

    logging.info("Cálculo de KPIs e Métricas concluído.")

if __name__ == "__main__":
    try:
        create_container_if_not_exists(ACCOUNT_NAME, GOLD_CONTAINER_NAME, SAS_TOKEN)
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
