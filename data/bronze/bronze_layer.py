import os
import logging
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_date, current_timestamp

# Loggin config is set to INFO level to capture all relevant messages 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load environment variables from .env file
load_dotenv()

# Enviroment variables necessary for Azure Blob Storage connection
ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
LANDING_CONTAINER_NAME = os.getenv("ADLS_FILE_SYSTEM_NAME")
BRONZE_CONTAINER_NAME = os.getenv("ADLS_BRONZE_CONTAINER_NAME")
SAS_TOKEN = os.getenv("ADLS_SAS_TOKEN").replace('"', '')

# Create the BlobServiceClient using the account URL and SAS Token
blob_service_client = BlobServiceClient(
    account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
    credential=SAS_TOKEN
)

# Create container clients for landing and bronze
landing_container_client = blob_service_client.get_container_client(LANDING_CONTAINER_NAME)
bronze_container_client = blob_service_client.get_container_client(BRONZE_CONTAINER_NAME)

# If the bronze container does not exist, create it
try:
    bronze_container_client.create_container()
    logging.info(f"Container '{BRONZE_CONTAINER_NAME}' created.")
except Exception as e:
    # If the container already exists, we log a warning instead of raising an error.
    logging.warning(f"Could not create container '{BRONZE_CONTAINER_NAME}'. It might already exist.")
    pass

# Create SparkSession with Delta Lake and Azure configurations
spark = SparkSession.builder \
    .appName("bronze_layer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-azure:3.3.6,org.apache.hadoop:hadoop-common:3.3.6,com.microsoft.azure:azure-storage:8.6.6") \
    .getOrCreate()


# Function to copy CSV files from landing container to bronze container
def copy_csvs_to_bronze():
    # Credentials SAS configured for Spark to access Azure Blob Storage
    spark.conf.set(f"fs.azure.sas.{LANDING_CONTAINER_NAME}.{ACCOUNT_NAME}.blob.core.windows.net", SAS_TOKEN)
    spark.conf.set(f"fs.azure.sas.{BRONZE_CONTAINER_NAME}.{ACCOUNT_NAME}.blob.core.windows.net", SAS_TOKEN)
    
    # List CSV files in the landing container
    blobs = landing_container_client.list_blobs()
    csv_blobs = [b for b in blobs if b.name.endswith('.csv')]
    logging.info(f"Found {len(csv_blobs)} CSV files in landing container.")

    for blob in csv_blobs:
        try:
            source_path = f"wasbs://{LANDING_CONTAINER_NAME}@{ACCOUNT_NAME}.blob.core.windows.net/{blob.name}"
            file_name = os.path.basename(blob.name)
            table_name = os.path.splitext(file_name)[0]
            
            logging.info(f"Processing file: {file_name}")
            
            # Read CSV into Spark DataFrame, inferring the schema
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
            
            # Add metadata columns for processing date, timestamp, and source file name
            df = df.withColumn("processing_date", current_date()) \
                   .withColumn("processing_timestamp", current_timestamp()) \
                   .withColumn("source_file_name", lit(file_name))
            
            # Set the destination path for the Delta table in the bronze container
            dest_path = f"wasbs://{BRONZE_CONTAINER_NAME}@{ACCOUNT_NAME}.blob.core.windows.net/{table_name}"
            
            # Writing the DataFrame to Delta format in the bronze container
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(dest_path)
            logging.info(f"Successfully written Delta table for {file_name} to bronze at {dest_path}")
        except Exception as e:
            logging.error(f"Failed to process file {blob.name}. Error: {e}")


    logging.info("All CSV files processed and written to bronze as Delta tables.")

# Execute the main function if the script is run directly
if __name__ == "__main__":
    copy_csvs_to_bronze()
    spark.stop()
    logging.info("Spark session stopped.")
