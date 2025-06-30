import os
import logging
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_date, current_timestamp, trim, upper, lower, regexp_replace
from pyspark.sql.types import DateType, TimestampType, IntegerType, DoubleType, FloatType, LongType, StringType
from pyspark.sql.functions import col, to_date, to_timestamp, when

# Configuração de logging definida para nível INFO para capturar todas as mensagens relevantes
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

# Variáveis de ambiente necessárias para conexão com Azure Blob Storage
ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
BRONZE_CONTAINER_NAME = os.getenv("ADLS_BRONZE_CONTAINER_NAME")
SILVER_CONTAINER_NAME = os.getenv("ADLS_SILVER_CONTAINER_NAME")
SAS_TOKEN = os.getenv("ADLS_SAS_TOKEN").replace('"', '')

# Cria o BlobServiceClient usando a URL da conta e o Token SAS
blob_service_client = BlobServiceClient(
    account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
    credential=SAS_TOKEN
)

# Cria clientes de container para silver e bronze
silver_container_client = blob_service_client.get_container_client(SILVER_CONTAINER_NAME)
bronze_container_client = blob_service_client.get_container_client(BRONZE_CONTAINER_NAME)

# Se o container silver não existir, cria ele
try:
    silver_container_client.create_container()
    logging.info(f"Container '{SILVER_CONTAINER_NAME}' criado.")
except Exception as e:
    logging.warning(f"Container '{SILVER_CONTAINER_NAME}' já existe ou não foi possível criar.")
    pass

# Dicionário de mapeamento de colunas para o contexto de logística/transporte
COLUMN_MAPPING = {
    'CD_': 'CODIGO_',
    'TP_': 'TIPO_', 
    'VL_': 'VALOR_',
    'DT_': 'DATA_',
    'HR_': 'HORA_',
    'NM_': 'NOME_',
    'DS_': 'DESCRICAO_',
    'NR_': 'NUMERO_',
    'ID_': 'IDENTIFICADOR_',
    'QT_': 'QUANTIDADE_',
    'PC_': 'PRECO_',
    'ST_': 'STATUS_',
    'END_': 'ENDERECO_',
    'KM_': 'QUILOMETROS_',
    'CPF_': 'DOCUMENTO_CPF_',
    'CNPJ_': 'DOCUMENTO_CNPJ_',
    'CEP_': 'CODIGO_POSTAL_'
}

# Mapeamentos específicos para colunas exatas
SPECIFIC_COLUMN_MAPPING = {
    # Clientes
    'nome_cliente': 'NOME_CLIENTE',
    'cpf_cnpj': 'DOCUMENTO',
    'email': 'EMAIL',
    'telefone': 'TELEFONE',
    'endereco': 'ENDERECO',
    'cidade': 'CIDADE',
    'estado': 'UF',
    'cep': 'CODIGO_POSTAL',
    'data_cadastro': 'DATA_CADASTRO',
    
    # Motoristas
    'nome_motorista': 'NOME',
    'cpf': 'CPF',
    'numero_cnh': 'CNH',
    'data_nascimento': 'DATA_NASCIMENTO',
    'data_contratacao': 'DATA_CONTRATACAO',
    'status_ativo': 'ATIVO',
    
    # Veículos
    'placa': 'PLACA',
    'modelo': 'MODELO',
    'marca': 'MARCA',
    'ano_fabricacao': 'ANO',
    'capacidade_carga_kg': 'CAPACIDADE_KG',
    'tipo_veiculo': 'CATEGORIA',
    'status_operacional': 'STATUS',
    
    # Entregas/Coletas
    'data_inicio_entrega': 'DATA_INICIO',
    'data_previsao_fim_entrega': 'DATA_PREVISAO',
    'data_fim_real_entrega': 'DATA_CONCLUSAO',
    'status_entrega': 'STATUS',
    'valor_frete': 'VALOR',
    'peso_carga_kg': 'PESO_KG',
    'data_hora_coleta': 'DATA_COLETA',
    
    # Rotas
    'nome_rota': 'NOME_ROTA',
    'origem': 'ORIGEM',
    'destino': 'DESTINO',
    'distancia_km': 'DISTANCIA',
    'tempo_estimado_horas': 'TEMPO_ESTIMADO',
    
    # Manutenções e Abastecimentos
    'data_manutencao': 'DATA',
    'tipo_manutencao': 'TIPO',
    'descricao_servico': 'DESCRICAO',
    'custo_manutencao': 'CUSTO',
    'tempo_parado_horas': 'TEMPO_PARADO',
    'data_abastecimento': 'DATA',
    'litros': 'VOLUME',
    'valor_total': 'VALOR',
    'tipo_combustivel': 'COMBUSTIVEL',
    
    # Multas
    'data_multa': 'DATA',
    'local_multa': 'LOCAL',
    'descricao_infracao': 'INFRACAO',
    'valor_multa': 'VALOR',
    'status_pagamento': 'STATUS_PAGAMENTO'
}


def standardize_column_names(df, table_name):
    """
    Padroniza nomes de colunas:
    - Converte para maiúsculas
    - Aplica mapeamentos de prefixos/sufixos
    - Remove colunas de metadados bronze
    - Adiciona colunas de metadados silver
    """
    logging.info(f"Padronizando nomes de colunas para {table_name}")
    logging.info(f"Colunas originais: {df.columns}")
    
    # Renomeia colunas seguindo o padrão de prefixos/sufixos
    for old_col in df.columns:
        new_col = old_col.upper()
        
        # Aplica mapeamentos de prefixos/sufixos
        for old_pattern, new_pattern in COLUMN_MAPPING.items():
            new_col = new_col.replace(old_pattern.upper(), new_pattern.upper())
        
        if old_col != new_col:
            df = df.withColumnRenamed(old_col, new_col)
            logging.info(f"Coluna renomeada: {old_col} -> {new_col}")
    
    # Aplica mapeamentos específicos para colunas exatas
    for old_name, new_name in SPECIFIC_COLUMN_MAPPING.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
            logging.info(f"Coluna específica renomeada: {old_name} -> {new_name}")
    
    # Adiciona metadados da camada silver
    df = df.withColumn("_SILVER_INGESTION_TIMESTAMP", current_timestamp()) \
           .withColumn("_SOURCE_TABLE", lit(table_name))
    
    logging.info(f"Colunas finais: {df.columns}")
    return df


def apply_data_quality_transformations(df, table_name):
    """
    Aplica transformações de qualidade de dados:
    - Remove duplicatas
    - Conversões de tipo
    - Limpeza de strings
    - Converte strings para maiúsculas
    """
    logging.info(f"Aplicando transformações de qualidade para {table_name}")
    initial_count = df.count()
    logging.info(f"Registros iniciais: {initial_count}")
    
    # 1. Remove duplicatas
    df = df.dropDuplicates()
    after_dedup_count = df.count()
    logging.info(f"Após remoção de duplicatas: {after_dedup_count} (removidas: {initial_count - after_dedup_count})")
    
    # 2. Converte todas as colunas string para maiúsculas
    for col_name in df.columns:
        if dict(df.dtypes)[col_name] == "string":
            df = df.withColumn(col_name, upper(col(col_name)))
    
    # 3. Converte colunas de string date/timestamp para tipos adequados
    for col_name, data_type in df.dtypes:
        if data_type == "string":
            # Verifica se a coluna contém datas
            if any(date_hint in col_name.lower() for date_hint in ["data_", "dt_"]):
                try:
                    df = df.withColumn(col_name, to_date(col(col_name)))
                except:
                    logging.warning(f"Não foi possível converter {col_name} para data")
            
            # Verifica se a coluna contém timestamps
            elif any(ts_hint in col_name.lower() for ts_hint in ["hora", "timestamp"]):
                try:
                    df = df.withColumn(col_name, to_timestamp(col(col_name)))
                except:
                    logging.warning(f"Não foi possível converter {col_name} para timestamp")
    
    # 4. Limpa espaços em branco das colunas string restantes
    string_columns = [c for c, t in df.dtypes if t == "string"]
    for col_name in string_columns:
        df = df.withColumn(col_name, trim(col(col_name)))
    
    final_count = df.count()
    logging.info(f"Registros finais após limpeza: {final_count}")
    
    return df


def apply_business_rules(df, table_name):
    """
    Aplica regras de negócio específicas por tabela com base nos requisitos
    definidos no esquema relacional
    """
    logging.info(f"Aplicando regras de negócio para tabela {table_name}")
    
    # Extrai o nome base da tabela (Remove sufixos de data)
    base_table_name = table_name.split('_')[0].lower() if '_202' in table_name else table_name.lower()
    
    # Aplica regras específicas por tabela
    if "clientes" in base_table_name:
        logging.info("Aplicando regras para tabela de CLIENTES")
        
        # Regra: tipo_cliente deve ser 'Pessoa Física' ou 'Pessoa Jurídica'
        if "tipo_cliente" in df.columns:
            df = df.withColumn("tipo_cliente", 
                when(col("tipo_cliente").isin(["PESSOA FÍSICA", "PESSOA FISICA"]), "Pessoa Física")
                .when(col("tipo_cliente").isin(["PESSOA JURÍDICA", "PESSOA JURIDICA", "CNPJ"]), "Pessoa Jurídica")
                .otherwise(col("tipo_cliente")))
            
        # Regra: padroniza CPF/CNPJ (remove caracteres especiais)
        if "cpf_cnpj" in df.columns:
            df = df.withColumn("cpf_cnpj", regexp_replace(col("cpf_cnpj"), "[^0-9]", ""))
        
        # Regra: validação de e-mail
        if "email" in df.columns:
            # Converte emails para minúsculos
            df = df.withColumn("email", lower(trim(col("email"))))
        
    elif "motoristas" in base_table_name:
        logging.info("Aplicando regras para tabela de MOTORISTAS")
        
        # Regra: padronização de CPF
        if "cpf" in df.columns:
            df = df.withColumn("cpf", regexp_replace(col("cpf"), "[^0-9]", ""))
        
        # Regra: padronização de CNH
        if "numero_cnh" in df.columns:
            df = df.withColumn("numero_cnh", regexp_replace(col("numero_cnh"), "[^0-9]", ""))
        
        # Regra: status_ativo como booleano
        if "status_ativo" in df.columns:
            df = df.withColumn("status_ativo", 
                when(col("status_ativo").isin(["SIM", "VERDADEIRO", "1", "ATIVO", "TRUE", True]), True)
                .otherwise(False))
    
    elif "veiculos" in base_table_name:
        logging.info("Aplicando regras para tabela de VEICULOS")
        
        # Regra: padronização de placas (remove espaços e hífens)
        if "placa" in df.columns:
            df = df.withColumn("placa", regexp_replace(col("placa"), "[ -]", ""))
        
        # Regra: padronização de tipo_veiculo
        if "tipo_veiculo" in df.columns:
            df = df.withColumn("tipo_veiculo", 
                when(col("tipo_veiculo").like("%CAMINH%"), "Caminhão")
                .when(col("tipo_veiculo").like("%VAN%"), "Van")
                .when(col("tipo_veiculo").like("%UTILIT%"), "Utilitário")
                .when(col("tipo_veiculo").like("%CARR%"), "Carro")
                .otherwise(col("tipo_veiculo")))
        
        # Regra: padronização de status_operacional
        if "status_operacional" in df.columns:
            df = df.withColumn("status_operacional", 
                when(col("status_operacional").like("%DISPON%"), "Disponível")
                .when(col("status_operacional").like("%VIAG%"), "Em Viagem")
                .when(col("status_operacional").like("%MANUT%"), "Em Manutenção")
                .when(col("status_operacional").like("%INAT%"), "Inativo")
                .otherwise(col("status_operacional")))
    
    elif "entregas" in base_table_name:
        logging.info("Aplicando regras para tabela de ENTREGAS")
        
        # Regra: padronização de status_entrega
        if "status_entrega" in df.columns:
            df = df.withColumn("status_entrega", 
                when(col("status_entrega").like("%AGEND%"), "Agendada")
                .when(col("status_entrega").like("%TRANSITO%") | col("status_entrega").like("%TRANS%"), "Em Trânsito")
                .when(col("status_entrega").like("%ENTREG%"), "Entregue")
                .when(col("status_entrega").like("%ATRAS%"), "Atrasada")
                .when(col("status_entrega").like("%CANCEL%"), "Cancelada")
                .when(col("status_entrega").like("%PROBL%"), "Problema")
                .otherwise(col("status_entrega")))
        
        # Regra: conversão de valores para decimal
        if "valor_frete" in df.columns:
            df = df.withColumn("valor_frete", col("valor_frete").cast(DoubleType()))
        
        if "peso_carga_kg" in df.columns:
            df = df.withColumn("peso_carga_kg", col("peso_carga_kg").cast(DoubleType()))
    
    elif "coletas" in base_table_name:
        logging.info("Aplicando regras para tabela de COLETAS")
        
        # Regra: padronização de status_coleta
        if "status_coleta" in df.columns:
            df = df.withColumn("status_coleta", 
                when(col("status_coleta").like("%AGEND%"), "Agendada")
                .when(col("status_coleta").like("%REALIZ%"), "Realizada")
                .when(col("status_coleta").like("%CANCEL%"), "Cancelada")
                .when(col("status_coleta").like("%PROBL%"), "Problema")
                .otherwise(col("status_coleta")))
    
    elif "manutencoes" in base_table_name or "manutencao" in base_table_name:
        logging.info("Aplicando regras para tabela de MANUTENCOES")
        
        # Regra: padronização de tipo_manutencao
        if "tipo_manutencao" in df.columns:
            df = df.withColumn("tipo_manutencao", 
                when(col("tipo_manutencao").like("%PREV%"), "Preventiva")
                .when(col("tipo_manutencao").like("%CORRET%"), "Corretiva")
                .when(col("tipo_manutencao").like("%PRED%"), "Preditiva")
                .otherwise(col("tipo_manutencao")))
        
        # Regra: conversão de valores para decimal
        if "custo_manutencao" in df.columns:
            df = df.withColumn("custo_manutencao", col("custo_manutencao").cast(DoubleType()))
    
    elif "abastecimentos" in base_table_name:
        logging.info("Aplicando regras para tabela de ABASTECIMENTOS")
        
        # Regra: conversão de valores para decimal
        if "litros" in df.columns:
            df = df.withColumn("litros", col("litros").cast(DoubleType()))
        
        if "valor_total" in df.columns:
            df = df.withColumn("valor_total", col("valor_total").cast(DoubleType()))
        
        # Regra: padronização de tipo_combustivel
        if "tipo_combustivel" in df.columns:
            df = df.withColumn("tipo_combustivel", 
                when(col("tipo_combustivel").like("%GASOL%"), "Gasolina")
                .when(col("tipo_combustivel").like("%ETAN%"), "Etanol")
                .when(col("tipo_combustivel").like("%DIES%"), "Diesel")
                .when(col("tipo_combustivel").like("%GAS%"), "Gás")
                .otherwise(col("tipo_combustivel")))
    
    elif "multas" in base_table_name:
        logging.info("Aplicando regras para tabela de MULTAS")
        
        # Regra: padronização de status_pagamento
        if "status_pagamento" in df.columns:
            df = df.withColumn("status_pagamento", 
                when(col("status_pagamento").like("%PEND%"), "Pendente")
                .when(col("status_pagamento").like("%PAG%"), "Paga")
                .when(col("status_pagamento").like("%RECOR%"), "Recorrida")
                .otherwise(col("status_pagamento")))
        
        # Regra: conversão de valores para decimal
        if "valor_multa" in df.columns:
            df = df.withColumn("valor_multa", col("valor_multa").cast(DoubleType()))
    
    elif "tipos_carga" in base_table_name or "carga" in base_table_name:
        logging.info("Aplicando regras para tabela de TIPOS_CARGA")
        
        # Regra: requer_refrigeracao como booleano
        if "requer_refrigeracao" in df.columns:
            df = df.withColumn("requer_refrigeracao", 
                when(col("requer_refrigeracao").isin(["SIM", "VERDADEIRO", "1", "TRUE", True]), True)
                .otherwise(False))
        
        # Regra: conversão de valores para decimal
        if "peso_medio_kg" in df.columns:
            df = df.withColumn("peso_medio_kg", col("peso_medio_kg").cast(DoubleType()))
    
    elif "rotas" in base_table_name:
        logging.info("Aplicando regras para tabela de ROTAS")
        
        # Regra: conversão de valores para decimal
        if "distancia_km" in df.columns:
            df = df.withColumn("distancia_km", col("distancia_km").cast(DoubleType()))
        
        if "tempo_estimado_horas" in df.columns:
            df = df.withColumn("tempo_estimado_horas", col("tempo_estimado_horas").cast(DoubleType()))
    
    return df


def process_bronze_to_silver(spark=None):
    """
    Função principal que processa todas as tabelas da camada bronze para silver
    
    Args:
        spark: Optional SparkSession. If None, creates a new one.
    """
    # Use provided Spark session or create a new one
    if spark is None:
        spark = SparkSession.builder \
            .appName("silver_layer") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-azure:3.3.6,org.apache.hadoop:hadoop-common:3.3.6,com.microsoft.azure:azure-storage:8.6.6") \
            .getOrCreate()
    
    # Configura tokens SAS para o Spark
    spark.conf.set(f"fs.azure.sas.{BRONZE_CONTAINER_NAME}.{ACCOUNT_NAME}.blob.core.windows.net", SAS_TOKEN)
    spark.conf.set(f"fs.azure.sas.{SILVER_CONTAINER_NAME}.{ACCOUNT_NAME}.blob.core.windows.net", SAS_TOKEN)
    
    # Lista tabelas no container bronze
    blobs_list = list(bronze_container_client.list_blobs())
    
    # Agrupa blobs por tabela
    tables = {}
    for blob in blobs_list:
        if '/' in blob.name:
            table_name = blob.name.split('/')[0]
            if table_name not in tables:
                tables[table_name] = []
            tables[table_name].append(blob.name)
    
    logging.info(f"Encontradas {len(tables)} tabelas no container bronze: {list(tables.keys())}")
    
    # Processa cada tabela
    for table_name in tables:
        try:
            logging.info(f"Iniciando processamento da tabela: {table_name}")
            
            # Caminho de origem na camada bronze
            source_path = f"wasbs://{BRONZE_CONTAINER_NAME}@{ACCOUNT_NAME}.blob.core.windows.net/{table_name}"
            
            # Lê a tabela Delta da camada bronze
            df = spark.read.format("delta").load(source_path)
            logging.info(f"Tabela {table_name} carregada com {df.count()} registros e {len(df.columns)} colunas")
            
            # Mostra alguns registros da tabela bronze antes das transformações
            logging.info(f"\n=== DADOS DA TABELA BRONZE: {table_name} ===")
            df.show(5, truncate=False)
            
            # Aplica transformações da camada silver
            # 1. Padronização de nomes de colunas
            df_silver = standardize_column_names(df, table_name)
            
            # 2. Transformações de qualidade de dados (incluindo remoção de duplicatas)
            df_silver = apply_data_quality_transformations(df_silver, table_name)
            
            # 3. Regras de negócio específicas
            df_silver = apply_business_rules(df_silver, table_name)
            
            # Define caminho de destino
            dest_path = f"wasbs://{SILVER_CONTAINER_NAME}@{ACCOUNT_NAME}.blob.core.windows.net/{table_name}"
            
            # Mostra alguns registros da tabela silver após as transformações
            logging.info(f"\n=== DADOS DA TABELA SILVER: {table_name} ===")
            df_silver.show(5, truncate=False)
            
            # Escreve na camada Silver
            df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(dest_path)
            
            logging.info(f"Tabela {table_name} processada com sucesso para camada silver")
            logging.info(f"Registros processados: {df_silver.count()}")
            logging.info(f"Colunas finais: {df_silver.columns}")
            
        except Exception as e:
            logging.error(f"Erro ao processar tabela {table_name}: {str(e)}")
            continue
    
    logging.info("Processamento de todas as tabelas concluído")

# Executa as funções principais
if __name__ == "__main__":
    try:
        # Processa Bronze para Silver
        process_bronze_to_silver()
        
        logging.info("Pipeline Bronze para Silver executado com sucesso!")
        
    except Exception as e:
        logging.error(f"Erro na execução principal: {str(e)}")
        raise
    # Note: We don't stop the Spark session here as it's managed by the main DAG