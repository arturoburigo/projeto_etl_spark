import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_connection():
    load_dotenv()

    # SQL Server
    server = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DATABASE")
    username = os.getenv("SQL_USERNAME")
    password = quote_plus(os.getenv("SQL_PASSWORD"))

    try:
        # Criar conexão com SQL Server com parâmetros adicionais
        conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes&Connection_Timeout=60&ConnectRetryCount=3&ConnectRetryInterval=10&ApplicationIntent=ReadWrite&MultiSubnetFailover=Yes"
        
        logger.info(f"Tentando conectar ao servidor: {server}")
        logger.info(f"Database: {database}")
        logger.info(f"Username: {username}")
        
        engine = create_engine(conn_str, pool_timeout=60, pool_recycle=3600, pool_pre_ping=True)
        
        # Testar a conexão
        with engine.connect() as connection:
            result = connection.execute(text("SELECT @@version")).fetchone()
            logger.info("Conexão estabelecida com sucesso!")
            logger.info(f"Versão do SQL Server: {result[0]}")
            
    except Exception as e:
        logger.error(f"Erro durante a conexão: {str(e)}")
        raise

if __name__ == "__main__":
    test_connection() 