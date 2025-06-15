import logging
from bronze_config import get_adls_config
from bronze_adls import create_adls_client, ensure_bronze_dir
import pandas as pd
from io import BytesIO

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

BRONZE_DIR = "bronze"

# Novo main: bronze a partir dos CSVs já existentes na landing-zone
def main():
    try:
        adls_config = get_adls_config()
        service_client = create_adls_client(adls_config)
        file_system_client = service_client.get_file_system_client(adls_config['file_system_name'])
        ensure_bronze_dir(file_system_client, BRONZE_DIR)
        # Lista arquivos CSV na raiz do container
        paths = file_system_client.get_paths(path="", recursive=False)
        csv_files = [p.name for p in paths if p.name.endswith('.csv')]
        logging.info(f"Encontrados {len(csv_files)} arquivos CSV na landing-zone.")
        directory_client = file_system_client.get_directory_client("")
        for csv_file in csv_files:
            file_client = directory_client.get_file_client(csv_file)
            download = file_client.download_file()
            csv_bytes = download.readall()
            df = pd.read_csv(BytesIO(csv_bytes))
            table_name = csv_file.split('/')[-1].replace('.csv', '')
            from bronze_adls import upload_parquet_to_bronze
            upload_parquet_to_bronze(directory_client, df, table_name, BRONZE_DIR)
        logging.info("Bronze concluído com sucesso!")
    except Exception as e:
        logging.error(f"Erro durante a execução: {str(e)}")
        raise

if __name__ == "__main__":
    main()
