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

findspark.init()

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dag(
    dag_id="sqlserver_to_bronze_adls",
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
        Task 2: Processa CSVs da landing zone para a camada Bronze usando PySpark
        """
        load_dotenv()
        
        # Variáveis de ambiente
        account_name = os.getenv("ADLS_ACCOUNT_NAME")
        landing_container = os.getenv("ADLS_FILE_SYSTEM_NAME")
        bronze_container = os.getenv("ADLS_BRONZE_CONTAINER_NAME", "bronze")
        sas_token = os.getenv("ADLS_SAS_TOKEN").replace('"', '')
        
        try:
            # Cliente Blob Storage
            blob_service_client = BlobServiceClient(
                account_url=f"https://{account_name}.blob.core.windows.net",
                credential=sas_token
            )
            
            # Criar container Bronze se não existir
            landing_container_client = blob_service_client.get_container_client(landing_container)
            bronze_container_client = blob_service_client.get_container_client(bronze_container)
            try:
                bronze_container_client.create_container()
                logger.info(f"Container '{bronze_container}' criado")
            except:
                logger.info(f"Container '{bronze_container}' já existe")
            
            # Criar SparkSession com Delta Lake e Azure configurations
            spark = SparkSession.builder \
                .appName("bronze_layer") \
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
            
            logger.info("SparkSession criado com sucesso")
            
            # Credentials SAS configured for Spark to access Azure Blob Storage
            spark.conf.set(f"fs.azure.sas.{landing_container}.{account_name}.blob.core.windows.net", sas_token)
            spark.conf.set(f"fs.azure.sas.{bronze_container}.{account_name}.blob.core.windows.net", sas_token)
            
            # List CSV files in the landing container
            blobs = landing_container_client.list_blobs()
            csv_blobs = [b for b in blobs if b.name.endswith('.csv')]
            logger.info(f"Found {len(csv_blobs)} CSV files in landing container.")

            for blob in csv_blobs:
                try:
                    source_path = f"wasbs://{landing_container}@{account_name}.blob.core.windows.net/{blob.name}"
                    file_name = os.path.basename(blob.name)
                    table_name = os.path.splitext(file_name)[0]
                    
                    logger.info(f"Processing file: {file_name}")
                    
                    # Read CSV into Spark DataFrame, inferring the schema
                    df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
                    
                    # Add metadata columns for processing date, timestamp, and source file name
                    df = df.withColumn("processing_date", current_date()) \
                        .withColumn("processing_timestamp", current_timestamp()) \
                        .withColumn("source_file_name", lit(file_name))
                    
                    # Set the destination path for the Delta table in the bronze container
                    dest_path = f"wasbs://{bronze_container}@{account_name}.blob.core.windows.net/{table_name}"
                    
                    # Writing the DataFrame to Delta format in the bronze container
                    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(dest_path)
                    logger.info(f"Successfully written Delta table for {file_name} to bronze at {dest_path}")
                except Exception as e:
                    logger.error(f"Failed to process file {blob.name}. Error: {e}")

            logger.info("All CSV files processed and written to bronze as Delta tables.")
            spark.stop()
            logger.info("Spark session stopped.")
            
        except Exception as e:
            logger.error(f"Erro durante a execução do bronze pipeline: {str(e)}")
            raise
        
    @task()
    def silver_pipeline():
        """
        Task 3: Processa dados da camada Bronze para a camada Silver usando PySpark
        """
        load_dotenv()
        
        # Variáveis de ambiente
        account_name = os.getenv("ADLS_ACCOUNT_NAME")
        bronze_container = os.getenv("ADLS_BRONZE_CONTAINER_NAME")
        silver_container = os.getenv("ADLS_SILVER_CONTAINER_NAME")
        sas_token = os.getenv("ADLS_SAS_TOKEN").replace('"', '')
        
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
            logger.info(f"Padronizando nomes de colunas para {table_name}")
            logger.info(f"Colunas originais: {df.columns}")
            
            # Renomeia colunas seguindo o padrão de prefixos/sufixos
            for old_col in df.columns:
                new_col = old_col.upper()
                
                # Aplica mapeamentos de prefixos/sufixos
                for old_pattern, new_pattern in COLUMN_MAPPING.items():
                    new_col = new_col.replace(old_pattern.upper(), new_pattern.upper())
                
                if old_col != new_col:
                    df = df.withColumnRenamed(old_col, new_col)
                    logger.info(f"Coluna renomeada: {old_col} -> {new_col}")
            
            # Aplica mapeamentos específicos para colunas exatas
            for old_name, new_name in SPECIFIC_COLUMN_MAPPING.items():
                if old_name in df.columns:
                    df = df.withColumnRenamed(old_name, new_name)
                    logger.info(f"Coluna específica renomeada: {old_name} -> {new_name}")
            
            # Adiciona metadados da camada silver
            df = df.withColumn("_SILVER_INGESTION_TIMESTAMP", current_timestamp()) \
                   .withColumn("_SOURCE_TABLE", lit(table_name))
            
            logger.info(f"Colunas finais: {df.columns}")
            return df

        def apply_data_quality_transformations(df, table_name):
            """
            Aplica transformações de qualidade de dados:
            - Remove duplicatas
            - Converte tipos
            - Limpeza de strings
            - Converte strings para maiúsculas
            """
            logger.info(f"Aplicando transformações de qualidade para {table_name}")
            initial_count = df.count()
            logger.info(f"Registros iniciais: {initial_count}")
            
            # 1. Remove duplicatas
            df = df.dropDuplicates()
            after_dedup_count = df.count()
            logger.info(f"Após remoção de duplicatas: {after_dedup_count} (removidas: {initial_count - after_dedup_count})")
            
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
                            logger.warning(f"Não foi possível converter {col_name} para data")
                    
                    # Verifica se a coluna contém timestamps
                    elif any(ts_hint in col_name.lower() for ts_hint in ["hora", "timestamp"]):
                        try:
                            df = df.withColumn(col_name, to_timestamp(col(col_name)))
                        except:
                            logger.warning(f"Não foi possível converter {col_name} para timestamp")
            
            # 4. Limpa espaços em branco das colunas string restantes
            string_columns = [c for c, t in df.dtypes if t == "string"]
            for col_name in string_columns:
                df = df.withColumn(col_name, trim(col(col_name)))
            
            final_count = df.count()
            logger.info(f"Registros finais após limpeza: {final_count}")
            
            return df

        def apply_business_rules(df, table_name):
            """
            Aplica regras de negócio específicas por tabela com base nos requisitos
            definidos no esquema relacional
            """
            logger.info(f"Aplicando regras de negócio para tabela {table_name}")
            
            # Extrai o nome base da tabela (Remove sufixos de data)
            base_table_name = table_name.split('_')[0].lower() if '_202' in table_name else table_name.lower()
            
            # Aplica regras específicas por tabela
            if "clientes" in base_table_name:
                logger.info("Aplicando regras para tabela de CLIENTES")
                
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
                logger.info("Aplicando regras para tabela de MOTORISTAS")
                
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
                logger.info("Aplicando regras para tabela de VEICULOS")
                
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
                logger.info("Aplicando regras para tabela de ENTREGAS")
                
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
                logger.info("Aplicando regras para tabela de COLETAS")
                
                # Regra: padronização de status_coleta
                if "status_coleta" in df.columns:
                    df = df.withColumn("status_coleta", 
                        when(col("status_coleta").like("%AGEND%"), "Agendada")
                        .when(col("status_coleta").like("%REALIZ%"), "Realizada")
                        .when(col("status_coleta").like("%CANCEL%"), "Cancelada")
                        .when(col("status_coleta").like("%PROBL%"), "Problema")
                        .otherwise(col("status_coleta")))
            
            elif "manutencoes" in base_table_name or "manutencao" in base_table_name:
                logger.info("Aplicando regras para tabela de MANUTENCOES")
                
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
                logger.info("Aplicando regras para tabela de ABASTECIMENTOS")
                
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
                logger.info("Aplicando regras para tabela de MULTAS")
                
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
                logger.info("Aplicando regras para tabela de TIPOS_CARGA")
                
                # Regra: requer_refrigeracao como booleano
                if "requer_refrigeracao" in df.columns:
                    df = df.withColumn("requer_refrigeracao", 
                        when(col("requer_refrigeracao").isin(["SIM", "VERDADEIRO", "1", "TRUE", True]), True)
                        .otherwise(False))
                
                # Regra: conversão de valores para decimal
                if "peso_medio_kg" in df.columns:
                    df = df.withColumn("peso_medio_kg", col("peso_medio_kg").cast(DoubleType()))
            
            elif "rotas" in base_table_name:
                logger.info("Aplicando regras para tabela de ROTAS")
                
                # Regra: conversão de valores para decimal
                if "distancia_km" in df.columns:
                    df = df.withColumn("distancia_km", col("distancia_km").cast(DoubleType()))
                
                if "tempo_estimado_horas" in df.columns:
                    df = df.withColumn("tempo_estimado_horas", col("tempo_estimado_horas").cast(DoubleType()))
            
            return df

        try:
            # Cliente Blob Storage
            blob_service_client = BlobServiceClient(
                account_url=f"https://{account_name}.blob.core.windows.net",
                credential=sas_token
            )
            
            # Criar container Silver se não existir
            bronze_container_client = blob_service_client.get_container_client(bronze_container)
            silver_container_client = blob_service_client.get_container_client(silver_container)
            
            try:
                silver_container_client.create_container()
                logger.info(f"Container '{silver_container}' criado com sucesso")
            except:
                logger.info(f"Container '{silver_container}' já existe")
            
            # Criar SparkSession com Delta Lake e Azure configurations
            spark = SparkSession.builder \
                .appName("silver_layer") \
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
            
            logger.info("SparkSession criado com sucesso")
            
            # Configura tokens SAS para o Spark
            spark.conf.set(f"fs.azure.sas.{bronze_container}.{account_name}.blob.core.windows.net", sas_token)
            spark.conf.set(f"fs.azure.sas.{silver_container}.{account_name}.blob.core.windows.net", sas_token)
            
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
            
            logger.info(f"Encontradas {len(tables)} tabelas no container bronze: {list(tables.keys())}")
            
            # Processa cada tabela
            for table_name in tables:
                try:
                    logger.info(f"Iniciando processamento da tabela: {table_name}")
                    
                    # Caminho de origem na camada bronze
                    source_path = f"wasbs://{bronze_container}@{account_name}.blob.core.windows.net/{table_name}"
                    
                    # Lê a tabela Delta da camada bronze
                    df = spark.read.format("delta").load(source_path)
                    logger.info(f"Tabela {table_name} carregada com {df.count()} registros e {len(df.columns)} colunas")
                    
                    # Mostra alguns registros da tabela bronze antes das transformações
                    logger.info(f"\n=== DADOS DA TABELA BRONZE: {table_name} ===")
                    df.show(5, truncate=False)
                    
                    # Aplica transformações da camada silver
                    # 1. Padronização de nomes de colunas
                    df_silver = standardize_column_names(df, table_name)
                    
                    # 2. Transformações de qualidade de dados (incluindo remoção de duplicatas)
                    df_silver = apply_data_quality_transformations(df_silver, table_name)
                    
                    # 3. Regras de negócio específicas
                    df_silver = apply_business_rules(df_silver, table_name)
                    
                    # Define caminho de destino
                    dest_path = f"wasbs://{silver_container}@{account_name}.blob.core.windows.net/{table_name}"
                    
                    # Mostra alguns registros da tabela silver após as transformações
                    logger.info(f"\n=== DADOS DA TABELA SILVER: {table_name} ===")
                    df_silver.show(5, truncate=False)
                    
                    # Escreve na camada Silver
                    df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(dest_path)
                    
                    logger.info(f"Tabela {table_name} processada com sucesso para camada silver")
                    logger.info(f"Registros processados: {df_silver.count()}")
                    logger.info(f"Colunas finais: {df_silver.columns}")
                    
                except Exception as e:
                    logger.error(f"Erro ao processar tabela {table_name}: {str(e)}")
                    continue
            
            logger.info("Processamento de todas as tabelas concluído")
            spark.stop()
            logger.info("Spark session stopped.")

        except Exception as e:
            logger.error(f"Erro durante a execução do silver pipeline: {str(e)}")
            raise
    @task()
    def gold_pipeline():
        """
        Task 4: Processa dados da camada Silver para a camada Gold usando PySpark (Modelo Dimensional)
        """
        load_dotenv()
        
        # Configurar logs do Spark para reduzir verbosidade
        import logging
        logging.getLogger("py4j").setLevel(logging.WARNING)
        logging.getLogger("pyspark").setLevel(logging.WARNING)
        logging.getLogger("org.apache.spark").setLevel(logging.WARNING)
        logging.getLogger("org.apache.hadoop").setLevel(logging.WARNING)
        logging.getLogger("org.apache.ivy").setLevel(logging.WARNING)
        
        # Variáveis de ambiente
        account_name = os.getenv("ADLS_ACCOUNT_NAME")
        silver_container = os.getenv("ADLS_SILVER_CONTAINER_NAME")
        gold_container = os.getenv("ADLS_GOLD_CONTAINER_NAME")
        sas_token = os.getenv("ADLS_SAS_TOKEN").replace('"', '')
        
        def find_latest_silver_path(table_name_prefix):
            """
            Encontra o caminho mais recente para uma tabela na camada Silver,
            considerando os sufixos de timestamp e a capitalização.
            """
            logger.info(f"Procurando pelo caminho mais recente da tabela '{table_name_prefix}' na camada Silver.")
            try:
                account_url = f"https://{account_name}.blob.core.windows.net"
                blob_service_client = BlobServiceClient(account_url=account_url, credential=sas_token)
                container_client = blob_service_client.get_container_client(silver_container)

                candidate_folders = set()
                for blob in container_client.list_blobs():
                    # Filtra para garantir que estamos olhando para o prefixo correto, ignorando o caso
                    if blob.name.lower().startswith(table_name_prefix.lower()):
                        folder_name = blob.name.split('/')[0]
                        candidate_folders.add(folder_name)

                if not candidate_folders:
                    logger.warning(f"Nenhuma pasta encontrada com o prefixo '{table_name_prefix}' no container '{silver_container}'.")
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
                    logger.info(f"Pasta mais recente encontrada para '{table_name_prefix}': {latest_folder}")
                    return latest_folder
                else:
                    # Fallback para pastas sem timestamp
                    for folder in candidate_folders:
                        if folder.lower() == table_name_prefix.lower():
                            logger.info(f"Nenhuma pasta com timestamp encontrada. Usando correspondência exata: {folder}")
                            return folder
                    logger.warning(f"Nenhuma pasta com sufixo de data/hora válido encontrada para '{table_name_prefix}'. Retornando a primeira encontrada como fallback.")
                    return list(candidate_folders)[0] if candidate_folders else None

            except Exception as e:
                logger.error(f"Erro ao listar blobs no container '{silver_container}': {e}", exc_info=True)
                raise

        def create_container_if_not_exists(account_name, container_name, sas_token):
            """Cria um container no Azure Blob Storage se ele não existir."""
            if not container_name:
                logger.error("Nome do container de destino (Gold) não definido. Verifique a variável de ambiente ADLS_GOLD_CONTAINER_NAME.")
                raise ValueError("Nome do container Gold não pode ser nulo.")
            try:
                account_url = f"https://{account_name}.blob.core.windows.net"
                blob_service_client = BlobServiceClient(account_url=account_url, credential=sas_token)
                container_client = blob_service_client.get_container_client(container_name)

                if not container_client.exists():
                    logger.info(f"Container '{container_name}' não existe. Criando...")
                    container_client.create_container()
                    logger.info(f"Container '{container_name}' criado com sucesso.")
                else:
                    logger.info(f"Container '{container_name}' já existe.")
            except Exception as e:
                logger.error(f"Falha ao criar ou verificar o container '{container_name}': {e}", exc_info=True)
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
        spark.conf.set(f"fs.azure.sas.{silver_container}.{account_name}.blob.core.windows.net", sas_token)
        spark.conf.set(f"fs.azure.sas.{gold_container}.{account_name}.blob.core.windows.net", sas_token)

        # --- Funções Auxiliares ---

        def get_silver_path(table_name):
            return f"wasbs://{silver_container}@{account_name}.blob.core.windows.net/{table_name}"

        def get_gold_path(table_name):
            return f"wasbs://{gold_container}@{account_name}.blob.core.windows.net/{table_name}"

        def load_silver_table(table_name):
            start_time = datetime.now()
            latest_folder_name = find_latest_silver_path(table_name)
            
            if not latest_folder_name:
                error_msg = f"ERRO: Nenhuma pasta de origem para a tabela '{table_name}' foi encontrada no container Silver '{silver_container}'."
                logger.error(error_msg)
                logger.error("Verifique se o pipeline da camada Silver foi executado e se os nomes das pastas estão no formato esperado (ex: Tabela_YYYYMMDD_HHMMSS).")
                raise FileNotFoundError(error_msg)

            path = get_silver_path(latest_folder_name)
            logger.info(f"Lendo tabela da camada Silver: {table_name} (pasta: {latest_folder_name}) de {path}")
            try:
                df = spark.read.format("delta").load(path)
                end_time = datetime.now()
                load_time = (end_time - start_time).total_seconds()
                logger.info(f"Tabela {table_name} carregada em {load_time:.2f}s com {df.count()} registros")
                return df
            except Exception as e:
                if 'Path does not exist' in str(e):
                    logger.error(f"ERRO: A tabela de origem '{table_name}' (pasta: {latest_folder_name}) não foi encontrada no caminho: {path}")
                    logger.error("Verifique se o pipeline da camada Silver foi executado com sucesso antes de rodar o pipeline da Gold.")
                    raise FileNotFoundError(f"Tabela de origem não encontrada na camada Silver: {path}") from e
                else:
                    logger.error(f"Erro inesperado ao ler a tabela '{table_name}' (pasta: {latest_folder_name}) da camada Silver.", exc_info=True)
                    raise e

        def save_gold_table(df, table_name):
            start_time = datetime.now()
            path = get_gold_path(table_name)
            logger.info(f"Salvando tabela na camada Gold: {table_name} em {path}")
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
            end_time = datetime.now()
            save_time = (end_time - start_time).total_seconds()
            logger.info(f"Tabela {table_name} salva em {save_time:.2f}s")

        def merge_dimension(df_updates, table_name, merge_key):
            """
            Executa um merge (SCD Tipo 1) em uma tabela de dimensão.
            Cria a tabela se ela não existir.
            """
            start_time = datetime.now()
            gold_path = get_gold_path(table_name)
            logger.info(f"Executando merge para a dimensão: {table_name}")

            df_updates = df_updates.withColumn("_GOLD_INGESTION_TIMESTAMP", current_timestamp())

            if not DeltaTable.isDeltaTable(spark, gold_path):
                logger.info(f"Tabela {table_name} não encontrada. Criando nova tabela.")
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
            
            end_time = datetime.now()
            merge_time = (end_time - start_time).total_seconds()
            logger.info(f"Merge para a dimensão {table_name} concluído em {merge_time:.2f}s")

        # --- Processamento das Dimensões ---

        def process_dim_data():
            logger.info("Processando Dim_Data")
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
            logger.info("Dim_Data processada com sucesso.")

        def process_dimensions():
            logger.info("=== INICIANDO PROCESSAMENTO DAS DIMENSÕES ===")
            start_time_dimensions = datetime.now()
            
            # Dim_Data
            logger.info("=== PROCESSANDO Dim_Data ===")
            dim_data_start = datetime.now()
            process_dim_data()
            dim_data_end = datetime.now()
            logger.info(f"Dim_Data processada em: {(dim_data_end - dim_data_start).total_seconds():.2f} segundos")

            # Dim_Cliente
            logger.info("=== PROCESSANDO Dim_Cliente ===")
            dim_cliente_start = datetime.now()
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
            dim_cliente_end = datetime.now()
            logger.info(f"Dim_Cliente processada em: {(dim_cliente_end - dim_cliente_start).total_seconds():.2f} segundos")

            # Dim_Motorista
            logger.info("=== PROCESSANDO Dim_Motorista ===")
            dim_motorista_start = datetime.now()
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
            dim_motorista_end = datetime.now()
            logger.info(f"Dim_Motorista processada em: {(dim_motorista_end - dim_motorista_start).total_seconds():.2f} segundos")

            # Dim_Veiculo
            logger.info("=== PROCESSANDO Dim_Veiculo ===")
            dim_veiculo_start = datetime.now()
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
            dim_veiculo_end = datetime.now()
            logger.info(f"Dim_Veiculo processada em: {(dim_veiculo_end - dim_veiculo_start).total_seconds():.2f} segundos")

            # Dim_Tipo_Carga
            logger.info("=== PROCESSANDO Dim_Tipo_Carga ===")
            dim_carga_start = datetime.now()
            df_cargas_silver = load_silver_table("tipos_carga")
            df_dim_carga = df_cargas_silver.select(
                col("IDENTIFICADOR_TIPO_CARGA").alias("id_tipo_carga_origem"),
                col("NOME_TIPO").alias("nome_tipo"),
                col("DESCRICAO_TIPO").alias("descricao_tipo"),
                col("REQUER_REFRIGERACAO").alias("requer_refrigeracao"),
                col("PESO_MEDIO_KG").alias("peso_medio_kg")
            )
            merge_dimension(df_dim_carga, "Dim_Tipo_Carga", "id_tipo_carga_origem")
            dim_carga_end = datetime.now()
            logger.info(f"Dim_Tipo_Carga processada em: {(dim_carga_end - dim_carga_start).total_seconds():.2f} segundos")

            # Dim_Rota
            logger.info("=== PROCESSANDO Dim_Rota ===")
            dim_rota_start = datetime.now()
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
            dim_rota_end = datetime.now()
            logger.info(f"Dim_Rota processada em: {(dim_rota_end - dim_rota_start).total_seconds():.2f} segundos")

            end_time_dimensions = datetime.now()
            total_dimensions_time = (end_time_dimensions - start_time_dimensions).total_seconds()
            
            logger.info("=== RESUMO DE TEMPOS - DIMENSÕES ===")
            logger.info(f"Dim_Data: {(dim_data_end - dim_data_start).total_seconds():.2f}s")
            logger.info(f"Dim_Cliente: {(dim_cliente_end - dim_cliente_start).total_seconds():.2f}s")
            logger.info(f"Dim_Motorista: {(dim_motorista_end - dim_motorista_start).total_seconds():.2f}s")
            logger.info(f"Dim_Veiculo: {(dim_veiculo_end - dim_veiculo_start).total_seconds():.2f}s")
            logger.info(f"Dim_Tipo_Carga: {(dim_carga_end - dim_carga_start).total_seconds():.2f}s")
            logger.info(f"Dim_Rota: {(dim_rota_end - dim_rota_start).total_seconds():.2f}s")
            logger.info(f"Tempo total Dimensões: {total_dimensions_time:.2f}s")
            logger.info("=== FIM DO PROCESSAMENTO DAS DIMENSÕES ===")

        # --- Processamento das Fatos ---

        def process_facts():
            logger.info("=== INICIANDO PROCESSAMENTO DAS TABELAS FATO ===")
            start_time_facts = datetime.now()
            
            # Carregar dimensões para lookup
            logger.info("Carregando dimensões para lookup...")
            load_dims_start = datetime.now()
            dim_veiculo = spark.read.format("delta").load(get_gold_path("Dim_Veiculo")).withColumn("id_veiculo_key", monotonically_increasing_id())
            dim_motorista = spark.read.format("delta").load(get_gold_path("Dim_Motorista")).withColumn("id_motorista_key", monotonically_increasing_id())
            dim_cliente = spark.read.format("delta").load(get_gold_path("Dim_Cliente")).withColumn("id_cliente_key", monotonically_increasing_id())
            dim_rota = spark.read.format("delta").load(get_gold_path("Dim_Rota"))
            dim_tipo_carga = spark.read.format("delta").load(get_gold_path("Dim_Tipo_Carga")).withColumn("id_tipo_carga_key", monotonically_increasing_id())
            dim_data = spark.read.format("delta").load(get_gold_path("Dim_Data"))
            load_dims_end = datetime.now()
            logger.info(f"Dimensões carregadas em: {(load_dims_end - load_dims_start).total_seconds():.2f} segundos")

            # Fato_Entregas
            logger.info("=== PROCESSANDO Fato_Entregas ===")
            fato_entregas_start = datetime.now()
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
            fato_entregas_end = datetime.now()
            logger.info(f"Fato_Entregas processada em: {(fato_entregas_end - fato_entregas_start).total_seconds():.2f} segundos")

            # Fato_Coletas
            logger.info("=== PROCESSANDO Fato_Coletas ===")
            fato_coletas_start = datetime.now()
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
                    lit(1).alias("quantidade_coletas")
                ).withColumn("_GOLD_INGESTION_TIMESTAMP", current_timestamp())
            save_gold_table(fato_coletas, "Fato_Coletas")
            fato_coletas_end = datetime.now()
            logger.info(f"Fato_Coletas processada em: {(fato_coletas_end - fato_coletas_start).total_seconds():.2f} segundos")

            # Fato_Rotas
            logger.info("=== PROCESSANDO Fato_Rotas ===")
            fato_rotas_start = datetime.now()
            rotas_df = load_silver_table("rotas")
            fato_rotas = rotas_df.alias("r").join(dim_rota.alias("dr"), col("r.IDENTIFICADOR_ROTA") == col("dr.id_rota_origem"), "left") \
                .select(
                    col("dr.id_rota_origem").alias("id_rota_key"),
                    col("r.IDENTIFICADOR_ROTA").alias("id_rota_origem"),
                    col("r.DISTANCIA_KM").alias("distancia_km"),
                    col("r.TEMPO_ESTIMADO_HORAS").alias("tempo_estimado_horas")
                ).withColumn("_GOLD_INGESTION_TIMESTAMP", current_timestamp())
            save_gold_table(fato_rotas, "Fato_Rotas")
            fato_rotas_end = datetime.now()
            logger.info(f"Fato_Rotas processada em: {(fato_rotas_end - fato_rotas_start).total_seconds():.2f} segundos")

            # Fato_Manutencoes
            logger.info("=== PROCESSANDO Fato_Manutencoes ===")
            fato_manutencoes_start = datetime.now()
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
            fato_manutencoes_end = datetime.now()
            logger.info(f"Fato_Manutencoes processada em: {(fato_manutencoes_end - fato_manutencoes_start).total_seconds():.2f} segundos")

            # Fato_Abastecimentos
            logger.info("=== PROCESSANDO Fato_Abastecimentos ===")
            fato_abastecimentos_start = datetime.now()
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
            fato_abastecimentos_end = datetime.now()
            logger.info(f"Fato_Abastecimentos processada em: {(fato_abastecimentos_end - fato_abastecimentos_start).total_seconds():.2f} segundos")

            # Fato_Multas
            logger.info("=== PROCESSANDO Fato_Multas ===")
            fato_multas_start = datetime.now()
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
            fato_multas_end = datetime.now()
            logger.info(f"Fato_Multas processada em: {(fato_multas_end - fato_multas_start).total_seconds():.2f} segundos")

            end_time_facts = datetime.now()
            total_facts_time = (end_time_facts - start_time_facts).total_seconds()
            
            logger.info("=== RESUMO DE TEMPOS - TABELAS FATO ===")
            logger.info(f"Carregamento de dimensões: {(load_dims_end - load_dims_start).total_seconds():.2f}s")
            logger.info(f"Fato_Entregas: {(fato_entregas_end - fato_entregas_start).total_seconds():.2f}s")
            logger.info(f"Fato_Coletas: {(fato_coletas_end - fato_coletas_start).total_seconds():.2f}s")
            logger.info(f"Fato_Rotas: {(fato_rotas_end - fato_rotas_start).total_seconds():.2f}s")
            logger.info(f"Fato_Manutencoes: {(fato_manutencoes_end - fato_manutencoes_start).total_seconds():.2f}s")
            logger.info(f"Fato_Abastecimentos: {(fato_abastecimentos_end - fato_abastecimentos_start).total_seconds():.2f}s")
            logger.info(f"Fato_Multas: {(fato_multas_end - fato_multas_start).total_seconds():.2f}s")
            logger.info(f"Tempo total Tabelas Fato: {total_facts_time:.2f}s")
            logger.info("=== FIM DO PROCESSAMENTO DAS TABELAS FATO ===")

        def process_kpis_and_metrics():
            logger.info("=== INICIANDO CÁLCULO DE KPIs E MÉTRICAS ===")
            start_time_total = datetime.now()
            
            # Carregar tabelas necessárias
            logger.info("Carregando tabelas para cálculo de KPIs...")
            load_start = datetime.now()
            fato_entregas = spark.read.format("delta").load(get_gold_path("Fato_Entregas"))
            dim_data = spark.read.format("delta").load(get_gold_path("Dim_Data"))
            dim_rota = spark.read.format("delta").load(get_gold_path("Dim_Rota"))
            dim_veiculo = spark.read.format("delta").load(get_gold_path("Dim_Veiculo"))
            dim_cliente = spark.read.format("delta").load(get_gold_path("Dim_Cliente"))
            load_end = datetime.now()
            logger.info(f"Tabelas carregadas em: {(load_end - load_start).total_seconds():.2f} segundos")

            # KPI 1: Percentual de Entregas no Prazo (On-Time Delivery)
            logger.info("=== CALCULANDO KPI 1: Percentual de Entregas no Prazo ===")
            kpi1_start = datetime.now()
            kpi_otd = fato_entregas.withColumn("on_time",
                (col("data_fim_real_entrega_key") <= col("data_previsao_fim_entrega_key")) | (col("data_previsao_fim_entrega_key").isNull())
            ).agg(
                (sum(when(col("on_time") == True, 1).otherwise(0)) / count("*") * 100).alias("percentual_entregas_no_prazo")
            )
            save_gold_table(kpi_otd, "kpi_percentual_entregas_no_prazo")
            kpi1_end = datetime.now()
            logger.info(f"KPI 1 concluído em: {(kpi1_end - kpi1_start).total_seconds():.2f} segundos")

            # KPI 2: Custo Médio de Frete por Rota
            logger.info("=== CALCULANDO KPI 2: Custo Médio de Frete por Rota ===")
            kpi2_start = datetime.now()
            kpi_custo_rota = fato_entregas.join(dim_rota, fato_entregas["id_rota_origem"] == dim_rota["id_rota_origem"], "left") \
                .groupBy(dim_rota["nome_rota"], dim_rota["origem"], dim_rota["destino"]) \
                .agg(avg("valor_frete").alias("custo_medio_frete"))
            save_gold_table(kpi_custo_rota, "kpi_custo_medio_frete_por_rota")
            kpi2_end = datetime.now()
            logger.info(f"KPI 2 concluído em: {(kpi2_end - kpi2_start).total_seconds():.2f} segundos")

            # KPI 3: Total de Entregas por Tipo de Veículo
            logger.info("=== CALCULANDO KPI 3: Total de Entregas por Tipo de Veículo ===")
            kpi3_start = datetime.now()
            kpi_entregas_veiculo = fato_entregas.join(dim_veiculo, fato_entregas["id_veiculo_origem"] == dim_veiculo["id_veiculo_origem"], "left") \
                .groupBy(dim_veiculo["tipo_veiculo"]) \
                .agg(count("*").alias("total_entregas"))
            save_gold_table(kpi_entregas_veiculo, "kpi_total_entregas_por_tipo_veiculo")
            kpi3_end = datetime.now()
            logger.info(f"KPI 3 concluído em: {(kpi3_end - kpi3_start).total_seconds():.2f} segundos")

            # KPI 4: Valor Total de Frete por Cliente
            logger.info("=== CALCULANDO KPI 4: Valor Total de Frete por Cliente ===")
            kpi4_start = datetime.now()
            kpi_valor_cliente = fato_entregas.join(dim_cliente, fato_entregas["id_cliente_remetente_origem"] == dim_cliente["id_cliente_origem"], "left") \
                .groupBy(dim_cliente["nome_cliente"]) \
                .agg(sum("valor_frete").alias("valor_total_frete"))
            save_gold_table(kpi_valor_cliente, "kpi_valor_total_frete_por_cliente")
            kpi4_end = datetime.now()
            logger.info(f"KPI 4 concluído em: {(kpi4_end - kpi4_start).total_seconds():.2f} segundos")

            # Métrica 1: Total de Entregas Mensal
            logger.info("=== CALCULANDO MÉTRICA 1: Total de Entregas Mensal ===")
            metrica1_start = datetime.now()
            metrica_entregas_mes = fato_entregas.join(dim_data, fato_entregas["data_inicio_entrega_key"] == dim_data["data_key"], "left") \
                .groupBy(dim_data["ano"], dim_data["mes"]) \
                .agg(count("*").alias("total_entregas")) \
                .orderBy("ano", "mes")
            save_gold_table(metrica_entregas_mes, "metrica_total_entregas_mensal")
            metrica1_end = datetime.now()
            logger.info(f"Métrica 1 concluída em: {(metrica1_end - metrica1_start).total_seconds():.2f} segundos")

            # Métrica 2: Peso Total Transportado por Mês
            logger.info("=== CALCULANDO MÉTRICA 2: Peso Total Transportado por Mês ===")
            metrica2_start = datetime.now()
            metrica_peso_mes = fato_entregas.join(dim_data, fato_entregas["data_inicio_entrega_key"] == dim_data["data_key"], "left") \
                .groupBy(dim_data["ano"], dim_data["mes"]) \
                .agg(sum("peso_carga_kg").alias("peso_total_kg")) \
                .orderBy("ano", "mes")
            save_gold_table(metrica_peso_mes, "metrica_peso_total_transportado_mensal")
            metrica2_end = datetime.now()
            logger.info(f"Métrica 2 concluída em: {(metrica2_end - metrica2_start).total_seconds():.2f} segundos")

            end_time_total = datetime.now()
            total_time = (end_time_total - start_time_total).total_seconds()
            
            logger.info("=== RESUMO DE TEMPOS - KPIs E MÉTRICAS ===")
            logger.info(f"Carregamento de tabelas: {(load_end - load_start).total_seconds():.2f}s")
            logger.info(f"KPI 1 (Percentual Entregas no Prazo): {(kpi1_end - kpi1_start).total_seconds():.2f}s")
            logger.info(f"KPI 2 (Custo Médio por Rota): {(kpi2_end - kpi2_start).total_seconds():.2f}s")
            logger.info(f"KPI 3 (Entregas por Tipo Veículo): {(kpi3_end - kpi3_start).total_seconds():.2f}s")
            logger.info(f"KPI 4 (Valor Total por Cliente): {(kpi4_end - kpi4_start).total_seconds():.2f}s")
            logger.info(f"Métrica 1 (Entregas Mensal): {(metrica1_end - metrica1_start).total_seconds():.2f}s")
            logger.info(f"Métrica 2 (Peso Mensal): {(metrica2_end - metrica2_start).total_seconds():.2f}s")
            logger.info(f"Tempo total KPIs e Métricas: {total_time:.2f}s")
            logger.info("=== FIM DO CÁLCULO DE KPIs E MÉTRICAS ===")

        try:
            logger.info("=== INICIANDO PIPELINE GOLD DIMENSIONAL ===")
            start_time_gold_total = datetime.now()
            
            create_container_if_not_exists(account_name, gold_container, sas_token)
            
            # Processamento das Dimensões
            start_time_dimensions = datetime.now()
            process_dimensions()
            end_time_dimensions = datetime.now()
            dimensions_time = (end_time_dimensions - start_time_dimensions).total_seconds()
            logger.info(f"=== TEMPO TOTAL DIMENSÕES: {dimensions_time:.2f}s ===")
            
            # Processamento das Tabelas Fato
            start_time_facts = datetime.now()
            process_facts()
            end_time_facts = datetime.now()
            facts_time = (end_time_facts - start_time_facts).total_seconds()
            logger.info(f"=== TEMPO TOTAL TABELAS FATO: {facts_time:.2f}s ===")
            
            # Processamento de KPIs e Métricas
            start_time_kpis = datetime.now()
            process_kpis_and_metrics()
            end_time_kpis = datetime.now()
            kpis_time = (end_time_kpis - start_time_kpis).total_seconds()
            logger.info(f"=== TEMPO TOTAL KPIs E MÉTRICAS: {kpis_time:.2f}s ===")
            
            end_time_gold_total = datetime.now()
            total_gold_time = (end_time_gold_total - start_time_gold_total).total_seconds()
            
            logger.info("=== RESUMO FINAL - PIPELINE GOLD ===")
            logger.info(f"Dimensões: {dimensions_time:.2f}s ({(dimensions_time/total_gold_time)*100:.1f}%)")
            logger.info(f"Tabelas Fato: {facts_time:.2f}s ({(facts_time/total_gold_time)*100:.1f}%)")
            logger.info(f"KPIs e Métricas: {kpis_time:.2f}s ({(kpis_time/total_gold_time)*100:.1f}%)")
            logger.info(f"Tempo Total Pipeline Gold: {total_gold_time:.2f}s")
            logger.info("=== PIPELINE GOLD DIMENSIONAL EXECUTADO COM SUCESSO! ===")
            
        except Exception as e:
            logger.error(f"Erro na execução do pipeline GOLD: {e}", exc_info=True)
            raise e
        finally:
            spark.stop()
            logger.info("Sessão Spark finalizada.")

    

    # Definir dependências do pipeline
    landing_task = landing_zone_pipeline()
    bronze_task = bronze_pipeline()
    silver_task = silver_pipeline()
    gold_task = gold_pipeline()
    
    # Definir ordem de execução
    landing_task >> bronze_task >> silver_task >> gold_task

dag = airflow_pipeline() 