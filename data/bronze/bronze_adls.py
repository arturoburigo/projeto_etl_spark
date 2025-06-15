from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError
import logging
import os
from datetime import datetime
import tempfile

def create_adls_client(adls_config):
    try:
        service_client = DataLakeServiceClient(
            account_url=f"https://{adls_config['account_name']}.dfs.core.windows.net",
            credential=adls_config['sas_token']
        )
        return service_client
    except Exception as e:
        logging.error(f"Error creating ADLS client: {str(e)}")
        raise

def ensure_bronze_dir(file_system_client, bronze_dir="bronze"):
    try:
        file_system_client.create_directory(bronze_dir)
        logging.info(f"Directory '{bronze_dir}' created successfully")
    except ResourceExistsError:
        logging.info(f"Directory '{bronze_dir}' already exists")

def upload_csv_to_bronze(directory_client, df, table_name, bronze_dir="bronze"):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"{bronze_dir}/{table_name}_{timestamp}.csv"
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        df.to_parquet(tmp.name, index=False)
        tmp.close()
        with open(tmp.name, "rb") as f:
            file_client = directory_client.get_file_client(file_name)
            file_client.upload_data(f, overwrite=True)
    os.remove(tmp.name)
    logging.info(f"Table '{table_name}' saved and sent to bronze: {file_name}")
