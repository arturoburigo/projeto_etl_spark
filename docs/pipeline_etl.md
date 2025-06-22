# Pipeline ETL

## Etapas:
1. **Extração:** SQL Server
2. **Carga:** Azure Data Lake (camada landing)
3. **Transformação:** Spark com Delta Lake e Iceberg
4. **Carga Final:** Camada gold

## Ferramentas:
- Airflow (orquestração)
- Spark (processamento)
- Azure Data Lake (armazenamento)