
#  Projeto ETL com Apache Spark

Este projeto realiza a **extração de dados de um banco SQL Server**, armazena os dados em um **Data Lake na Azure** e realiza o processamento distribuído com **Apache Spark**, orquestrado por **Apache Airflow**.

---

##  Objetivos

-  Extrair dados do **SQL Server**
-  Armazenar dados no **Azure Data Lake**
-  Processar os dados com **Apache Spark**, usando **Delta Lake** e **Apache Iceberg**
-  Automatizar o pipeline com **Apache Airflow**

---

##  Tecnologias Utilizadas

- Python >= 3.10
- Apache Spark
- Apache Airflow (via Docker)
- Azure Data Lake
- Delta Lake & Apache Iceberg
- Poetry (gerenciador de dependências)
- Docker & Docker Compose

---

##  Instalação

### Pré-requisitos

- [Python 3.10+](https://www.python.org/downloads/)
- [Docker](https://www.docker.com/)
- [Azure CLI](https://learn.microsoft.com/pt-br/cli/azure/install-azure-cli)
- [Poetry](https://python-poetry.org/docs/#installation)

### Instale as dependências do projeto

```bash
poetry install
```

---

##  Como Usar

### 1. Subindo o Apache Airflow

```bash
cd astro
docker-compose up -d
```

Acesse o Airflow: [http://localhost:8080](http://localhost:8080)

### 2. Executando o pipeline

- Configure as **credenciais do Azure**
- Inicie o Airflow
- Execute o DAG: `sqlserver_to_adls_dag`

### 3. Rodando scripts Spark

```bash
spark-submit data/create_schema_and_columns.py
```

---

##  Estrutura do Projeto

```text
├── astro/              # Ambiente Airflow
│   ├── dags/           # DAGs do Airflow
│   ├── tests/          # Testes dos DAGs
│   └── Dockerfile      # Imagem do Airflow
├── data/               # Scripts auxiliares (Spark)
├── pyproject.toml      # Dependências gerenciadas pelo Poetry
├── poetry.lock         # Lockfile de dependências
├── comandos.txt        # Comandos úteis
└── README.md           # Documentação do projeto
```

---

##  Orquestração com Airflow

- DAG principal: `sqlserver_to_adls_dag.py`
- Responsável por:
  - Extração de dados do SQL Server
  - Movimentação para o Azure Data Lake
- Pode ser executado manualmente ou agendado

---

##  Testes

- Localizados em: `astro/tests/`
- Testes dos DAGs e componentes do pipeline

---

##  Pipeline ETL

### Etapas:

1. **Extração**: dados oriundos do SQL Server
2. **Carga Inicial**: camada *landing* no Azure Data Lake
3. **Transformação**: processamento com Spark usando Delta Lake e Iceberg
4. **Carga Final**: camada *gold*

### Ferramentas:

- **Airflow** → orquestração do pipeline
- **Spark** → processamento distribuído
- **Azure Data Lake** → armazenamento escalável

---

##  Autores

- [Arturo Burigo](https://github.com/arturoburigo)
- [Luiz Bezerra](https://github.com/bezerraluiz)
- [Gabriel Morona](https://github.com/M0rona)
- [Maria Laura](https://github.com/laura27241)
- [Amanda Dimas](https://github.com/amandadimas)

---

##  Licença

Este projeto está licenciado sob os termos da **MIT License**. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.
