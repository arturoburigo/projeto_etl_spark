from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError
import logging
import os

def create_adls_client(adls_config):
    try:
        service_client = DataLakeServiceClient(
            account_url=f"https://{adls_config['account_name']}.dfs.core.windows.net",
            credential=adls_config['sas_token']
        )
        return service_client
    except Exception as e:
        logging.error(f"Erro ao criar cliente ADLS: {str(e)}")
        raise

def ensure_bronze_dir(file_system_client, bronze_dir="bronze"):
    try:
        file_system_client.create_directory(bronze_dir)
        logging.info(f"Diretório '{bronze_dir}' criado com sucesso")
    except ResourceExistsError:
        logging.info(f"Diretório '{bronze_dir}' já existe")

def upload_parquet_to_bronze(directory_client, df, table_name, bronze_dir="bronze"):
    from datetime import datetime
    import tempfile
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"{bronze_dir}/{table_name}_{timestamp}.parquet"
    with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
        df.to_parquet(tmp.name, index=False)
        tmp.close()
        with open(tmp.name, "rb") as f:
            file_client = directory_client.get_file_client(file_name)
            file_client.upload_data(f, overwrite=True)
    os.remove(tmp.name)
    logging.info(f"Tabela '{table_name}' salva e enviada para bronze: {file_name}")
