import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError
from sqlalchemy import create_engine
import os
from urllib.parse import quote_plus
import logging
from datetime import datetime

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configurações do Azure Data Lake Storage
ADLS_ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
ADLS_FILE_SYSTEM_NAME = os.getenv("ADLS_FILE_SYSTEM_NAME")
ADLS_DIRECTORY_NAME = "/"  # "/" represents the root directory
ADLS_SAS_TOKEN = os.getenv("ADLS_SAS_TOKEN")

# Configurações do SQL Server
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_SCHEMA = os.getenv("SQL_SCHEMA")
SQL_USERNAME = os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
    
def create_sql_connection():
    """Cria conexão com o SQL Server"""
    try:
        password = quote_plus(SQL_PASSWORD)
        conn_str = f"mssql+pyodbc://{SQL_USERNAME}:{password}@{SQL_SERVER}/{SQL_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server"
        engine = create_engine(conn_str)
        return engine
    except Exception as e:
        logger.error(f"Erro ao criar conexão com SQL Server: {str(e)}")
        raise

def create_adls_client():
    """Cria cliente do Azure Data Lake Storage"""
    try:
        service_client = DataLakeServiceClient(
            account_url=f"https://{ADLS_ACCOUNT_NAME}.dfs.core.windows.net",
            credential=ADLS_SAS_TOKEN
        )
        return service_client
    except Exception as e:
        logger.error(f"Erro ao criar cliente ADLS: {str(e)}")
        raise

def get_tables(engine, schema):
    """Obtém lista de tabelas do schema especificado"""
    try:
        query = f"SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = '{schema}'"
        df_tables = pd.read_sql(query, engine)
        return df_tables
    except Exception as e:
        logger.error(f"Erro ao obter tabelas: {str(e)}")
        raise

def upload_table_to_adls(engine, schema, table_name, directory_client):
    """Extrai dados da tabela e faz upload para ADLS"""
    try:
        # Criar nome do arquivo com timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"{table_name}_{timestamp}.csv"
        
        # Extrair dados da tabela
        query = f"SELECT * FROM {schema}.{table_name}"
        df = pd.read_sql(query, engine)
        
        # Upload para ADLS
        file_client = directory_client.get_file_client(file_name)
        data = df.to_csv(index=False).encode()
        file_client.upload_data(data, overwrite=True)
        
        logger.info(f"Dados da tabela '{table_name}' carregados com sucesso para {file_name}")
    except Exception as e:
        logger.error(f"Erro ao processar tabela {table_name}: {str(e)}")
        raise

def main():
    try:
        # Criar conexão com SQL Server
        engine = create_sql_connection()
        logger.info("Conexão com SQL Server estabelecida com sucesso")

        # Criar cliente ADLS
        service_client = create_adls_client()
        logger.info("Cliente ADLS criado com sucesso")

        # Obter sistema de arquivos e diretório
        file_system_client = service_client.get_file_system_client(ADLS_FILE_SYSTEM_NAME)
        directory_client = file_system_client.get_directory_client(ADLS_DIRECTORY_NAME)

        # Criar diretório se não existir e não for a raiz
        if ADLS_DIRECTORY_NAME != "/":
            try:
                directory_client.create_directory()
                logger.info(f"Diretório '{ADLS_DIRECTORY_NAME}' criado com sucesso")
            except ResourceExistsError:
                logger.info(f"Diretório '{ADLS_DIRECTORY_NAME}' já existe")
        else:
            logger.info("Usando diretório raiz do container")

        # Obter lista de tabelas
        df_tables = get_tables(engine, SQL_SCHEMA)
        logger.info(f"Encontradas {len(df_tables)} tabelas no schema {SQL_SCHEMA}")

        # Processar cada tabela
        for index, row in df_tables.iterrows():
            table_name = row["table_name"]
            upload_table_to_adls(engine, SQL_SCHEMA, table_name, directory_client)

        logger.info("Processo de extração e upload concluído com sucesso")

    except Exception as e:
        logger.error(f"Erro durante a execução: {str(e)}")
        raise

if __name__ == "__main__":
    main() 