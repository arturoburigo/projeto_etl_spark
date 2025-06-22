# Airflow e Orquestração

## DAG principal: `sqlserver_to_adls_dag.py`
- Extrai dados do SQL Server
- Move dados para o Azure Data Lake
- Pode ser agendado ou executado manualmente

## Testes
- Localizados em `astro/tests/`