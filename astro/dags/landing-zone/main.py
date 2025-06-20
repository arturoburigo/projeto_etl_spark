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
from pyspark.sql.functions import lit, current_date, current_timestamp, trim, upper, lower, regexp_replace, col, to_date, to_timestamp, when
from pyspark.sql.types import DateType, TimestampType, IntegerType, DoubleType, FloatType, LongType, StringType
import findspark

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
            - Conversões de tipo
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

    # Definir dependências do pipeline
    landing_task = landing_zone_pipeline()
    bronze_task = bronze_pipeline()
    silver_task = silver_pipeline()
    
    # Definir ordem de execução
    landing_task >> bronze_task >> silver_task

dag = airflow_pipeline() 