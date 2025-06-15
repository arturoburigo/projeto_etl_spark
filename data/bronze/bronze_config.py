import os
from dotenv import load_dotenv

load_dotenv()

# Config Azure Data Lake Storage
def get_adls_config():
    return {
        'account_name': os.getenv("ADLS_ACCOUNT_NAME"),
        'file_system_name': os.getenv("ADLS_FILE_SYSTEM_NAME"),
        'sas_token': os.getenv("ADLS_SAS_TOKEN")
    }

# Config SQL Server
def get_sql_config():
    return {
        'server': os.getenv("SQL_SERVER"),
        'database': os.getenv("SQL_DATABASE"),
        'schema': os.getenv("SQL_SCHEMA"),
        'username': os.getenv("SQL_USERNAME"),
        'password': os.getenv("SQL_PASSWORD")
    }
