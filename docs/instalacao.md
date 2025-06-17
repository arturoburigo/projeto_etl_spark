# Instalação

## Pré-requisitos
- Python >= 3.10
- Docker (para rodar o Airflow)
- Azure CLI (para configurar o Data Lake)

## Instalação dos pacotes
Execute:

```bash
poetry install
```

## Subindo o Airflow
```bash
cd astro
docker-compose up -d
```