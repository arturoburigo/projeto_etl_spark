# Como Usar

## Executando o pipeline:

1. Configure as credenciais do Azure.
2. Inicie o Airflow com `docker-compose up`.
3. Acesse o Airflow em `localhost:8080`.
4. Execute o DAG `sqlserver_to_adls_dag`.

## Executando scripts Spark:
```bash
spark-submit data/create_schema_and_columns.py
```