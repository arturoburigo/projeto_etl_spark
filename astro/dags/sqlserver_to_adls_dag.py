from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import os
from dotenv import load_dotenv
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dag(
    dag_id="sqlserver_to_adls",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mssql", "adls", "elt", "azure"],
    description="DAG para extrair dados do SQL Server e carregar no Azure Data Lake Storage"
)
def sqlserver_to_adls_dag():

    @task()
    def extract_sqlserver_and_upload_azure_adls():
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

    extract_sqlserver_and_upload_azure_adls()

dag = sqlserver_to_adls_dag() 