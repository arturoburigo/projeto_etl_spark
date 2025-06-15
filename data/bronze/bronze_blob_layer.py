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
LANDING_CONTAINER_NAME = os.getenv("ADLS_FILE_SYSTEM_NAME") # container landing-zone
BRONZE_CONTAINER_NAME = os.getenv("ADLS_BRONZE_CONTAINER_NAME") # new container bronze
SAS_TOKEN = os.getenv("ADLS_SAS_TOKEN").replace('"', '')

# URL conection
blob_service_client = BlobServiceClient(
    account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
    credential=SAS_TOKEN
)

landing_container_client = blob_service_client.get_container_client(LANDING_CONTAINER_NAME)
bronze_container_client = blob_service_client.get_container_client(BRONZE_CONTAINER_NAME)

# If the bronze container does not exist, create it
try:
    bronze_container_client.create_container()
except Exception:
    pass

def copy_csvs_to_bronze():
    blobs = landing_container_client.list_blobs()
    csv_blobs = [b for b in blobs if b.name.endswith('.csv')]
    logging.info(f"Find {len(csv_blobs)} CSV files in landing container.")
    for blob in csv_blobs:
        source_blob = blob.name
        dest_blob = os.path.basename(blob.name)
        # Download blob from landing container
        data = landing_container_client.download_blob(source_blob).readall()
        # Upload blob to bronze container
        bronze_blob = bronze_container_client.get_blob_client(dest_blob)
        bronze_blob.upload_blob(data, overwrite=True)
        logging.info(f"File {source_blob} copied to bronze container as {dest_blob}")
    logging.info("All CSV files copied to bronze container successfully!")

if __name__ == "__main__":
    copy_csvs_to_bronze()
