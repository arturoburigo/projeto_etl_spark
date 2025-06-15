import os
import logging
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

load_dotenv()

ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
CONTAINER_NAME = os.getenv("ADLS_BRONZE_SYSTEM_NAME")
SAS_TOKEN = os.getenv("ADLS_SAS_TOKEN").replace('"', '')

# Remove aspas se existirem
if SAS_TOKEN.startswith('?'):
    SAS_TOKEN = SAS_TOKEN[1:]

BRONZE_DIR = "bronze"

# Constrói a URL de conexão
blob_service_client = BlobServiceClient(
    account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
    credential=SAS_TOKEN
)

container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# Garante que o diretório bronze existe (no Blob, é só prefixo)
def ensure_bronze_dir():
    # No Blob Storage, diretórios são virtuais, não precisa criar
    pass

def copy_csvs_to_bronze():
    blobs = container_client.list_blobs()
    csv_blobs = [b for b in blobs if b.name.endswith('.csv') and not b.name.startswith(BRONZE_DIR + '/')]
    logging.info(f"Encontrados {len(csv_blobs)} arquivos CSV na landing-zone.")
    for blob in csv_blobs:
        source_blob = blob.name
        dest_blob = f"{BRONZE_DIR}/{os.path.basename(blob.name)}"
        # Copia o blob para bronze
        copied_blob = container_client.get_blob_client(dest_blob)
        # Baixa e faz upload (pode ser feito server-side se preferir)
        data = container_client.download_blob(source_blob).readall()
        copied_blob.upload_blob(data, overwrite=True)
        logging.info(f"Arquivo {source_blob} copiado para {dest_blob}")
    logging.info("Cópia para bronze concluída!")

if __name__ == "__main__":
    ensure_bronze_dir()
    copy_csvs_to_bronze()
