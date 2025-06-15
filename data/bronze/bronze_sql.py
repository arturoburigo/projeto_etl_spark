from sqlalchemy import create_engine
from urllib.parse import quote_plus
import pandas as pd
import logging

def create_sql_connection(sql_config):
    try:
        password = quote_plus(sql_config['password'])
        conn_str = f"mssql+pyodbc://{sql_config['username']}:{password}@{sql_config['server']}/{sql_config['database']}?driver=ODBC+Driver+17+for+SQL+Server"
        engine = create_engine(conn_str)
        return engine
    except Exception as e:
        logging.error(f"Error creating connection to SQL Server: {str(e)}")
        raise

def get_tables(engine, schema):
    try:
        query = f"SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = '{schema}'"
        df_tables = pd.read_sql(query, engine)
        return df_tables
    except Exception as e:
        logging.error(f"Error obtaining tables: {str(e)}")
        raise

def extract_table(engine, schema, table_name):
    try:
        query = f"SELECT * FROM {schema}.{table_name}"
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        logging.error(f"Error extracting data from the table {table_name}: {str(e)}")
        raise
