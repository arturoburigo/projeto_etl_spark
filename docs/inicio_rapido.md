# 🚀 Início Rápido

## 📋 Pré-requisitos

Antes de começar, certifique-se de ter os seguintes itens instalados em seu sistema:

### 🛠️ **Ferramentas Essenciais**

=== "Windows"
    ```powershell
    # Instalar Python 3.10+
    winget install Python.Python.3.10
    
    # Instalar Docker Desktop
    winget install Docker.DockerDesktop
    
    # Instalar Azure CLI
    winget install Microsoft.AzureCLI
    
    # Instalar Git
    winget install Git.Git
    ```

=== "macOS"
    ```bash
    # Instalar Homebrew (se não tiver)
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    
    # Instalar Python 3.10+
    brew install python@3.10
    
    # Instalar Docker Desktop
    brew install --cask docker
    
    # Instalar Azure CLI
    brew install azure-cli
    
    # Instalar Git
    brew install git
    ```

=== "Linux (Ubuntu/Debian)"
    ```bash
    # Atualizar repositórios
    sudo apt update
    
    # Instalar Python 3.10+
    sudo apt install python3.10 python3.10-pip python3.10-venv
    
    # Instalar Docker
    curl -fsSL https://get.docker.com -o get-docker.sh
    sh get-docker.sh
    
    # Instalar Azure CLI
    curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
    
    # Instalar Git
    sudo apt install git
    ```

### 📦 **Poetry (Gerenciador de Dependências)**

```bash
# Instalar Poetry
curl -sSL https://install.python-poetry.org | python3 -

# Adicionar ao PATH (adicione ao seu .bashrc/.zshrc)
export PATH="$HOME/.local/bin:$PATH"

# Verificar instalação
poetry --version
```

---

## ⚡ Instalação em 5 Minutos

### **1. Clone o Repositório**

```bash
git clone https://github.com/seu-usuario/projeto_etl_spark.git
cd projeto_etl_spark
```

### **2. Configure o Ambiente Python**

```bash
# Criar ambiente virtual com Poetry
poetry install

# Ativar o ambiente virtual
poetry shell
```

### **3. Configure as Variáveis de Ambiente**

```bash
# Copiar arquivo de exemplo
cp .env.example .env

# Editar com suas credenciais
nano .env  # ou vim .env, ou qualquer editor de sua preferência
```

**Exemplo de configuração `.env`:**
```bash
# Azure Data Lake
ADLS_ACCOUNT_NAME=seuaccountstorage
ADLS_FILE_SYSTEM_NAME=landing
ADLS_BRONZE_CONTAINER_NAME=bronze
ADLS_SILVER_CONTAINER_NAME=silver
ADLS_GOLD_CONTAINER_NAME=gold
ADLS_SAS_TOKEN="sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupitfx&se=2024-12-31T23:59:59Z&st=2024-01-01T00:00:00Z&spr=https&sig=SUA_ASSINATURA_SAS"

# SQL Server
SQL_SERVER=seu-servidor.database.windows.net
SQL_DATABASE=LogisticaDB
SQL_SCHEMA=dbo
SQL_USERNAME=admin
SQL_PASSWORD=SuaSenhaSegura123!

# Spark Configuration (opcional)
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g
SPARK_EXECUTOR_CORES=2
```

### **4. Inicie o Airflow**

```bash
# Navegar para o diretório do Airflow
cd astro

# Iniciar o ambiente Airflow
astro dev start
```

!!! tip "Dica"
    O comando `astro dev start` pode demorar alguns minutos na primeira execução, pois precisa baixar as imagens Docker.

### **5. Acesse a Interface Web**

Após a inicialização completa, acesse:

- 🌐 **Airflow UI**: [http://localhost:8080](http://localhost:8080)
- 👤 **Credenciais**: `admin` / `admin`

---

## 🎯 Primeira Execução

### **1. Verifique as Conexões**

No Airflow UI, vá para **Admin → Connections** e verifique se as conexões estão configuradas:

- ✅ `azure_data_lake_default`
- ✅ `sql_server_default`

### **2. Execute o Pipeline**

1. Navegue para **DAGs** na interface do Airflow
2. Encontre a DAG `sqlserver_to_bronze_adls`
3. Clique no botão **▶️ Trigger DAG**
4. Monitore a execução na aba **Graph View**

### **3. Verifique os Resultados**

Após a execução bem-sucedida, você deve ver:

- ✅ **Landing Zone**: Arquivos CSV no container `landing`
- ✅ **Bronze Layer**: Tabelas Delta no container `bronze`
- ✅ **Silver Layer**: Dados limpos no container `silver`
- ✅ **Gold Layer**: Modelo dimensional no container `gold`

---

## 🔧 Configuração Avançada

### **Azure Data Lake Setup**

Se você ainda não tem um Azure Data Lake configurado:

```bash
# Login no Azure
az login

# Criar Resource Group
az group create --name rg-projeto-etl --location brazilsouth

# Criar Storage Account
az storage account create \
    --name seuaccountstorage \
    --resource-group rg-projeto-etl \
    --location brazilsouth \
    --sku Standard_LRS \
    --kind StorageV2 \
    --hierarchical-namespace true

# Criar containers
az storage container create --name landing --account-name seuaccountstorage
az storage container create --name bronze --account-name seuaccountstorage
az storage container create --name silver --account-name seuaccountstorage
az storage container create --name gold --account-name seuaccountstorage

# Gerar SAS Token (válido por 1 ano)
az storage account generate-sas \
    --account-name seuaccountstorage \
    --account-key $(az storage account keys list --account-name seuaccountstorage --query '[0].value' -o tsv) \
    --expiry 2024-12-31T23:59:59Z \
    --permissions racwdlup \
    --resource-types sco \
    --services bfqt
```

### **SQL Server Setup (Opcional)**

Se você quiser usar dados de exemplo:

```bash
# Navegar para o diretório de dados
cd data

# Executar script de criação de tabelas
python create_tables.py

# Gerar dados sintéticos
python faker_data.py
```

---

## 🧪 Teste Rápido

### **Teste de Conexão SQL Server**

```bash
# Dentro do ambiente Poetry
poetry shell

# Executar teste de conexão
python astro/tests/test_sqlserver_connection.py
```

### **Teste de DAG**

```bash
# Teste de sintaxe da DAG
python astro/dags/main.py

# Teste unitário
pytest astro/tests/test_dag_example.py
```

### **Teste de Spark**

```python
# Abrir Python interativo
python

# Testar Spark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("teste_rapido") \
    .getOrCreate()

# Criar DataFrame de teste
df = spark.createDataFrame([(1, "teste"), (2, "spark")], ["id", "nome"])
df.show()

spark.stop()
```

---

## 📊 Monitoramento

### **Logs do Airflow**

```bash
# Ver logs em tempo real
astro dev logs

# Logs específicos de um serviço
astro dev logs scheduler
astro dev logs webserver
```

### **Interface de Monitoramento**

- 📊 **Airflow UI**: [http://localhost:8080](http://localhost:8080)
- 📈 **Flower (Celery)**: [http://localhost:5555](http://localhost:5555)
- 🗄️ **Postgres**: [http://localhost:5432](http://localhost:5432)

---

## 🚨 Troubleshooting

### **Problemas Comuns**

#### **1. Erro de Conexão com Azure**

```bash
# Verificar conectividade
az storage blob list --container-name landing --account-name seuaccountstorage

# Testar SAS Token
curl "https://seuaccountstorage.blob.core.windows.net/landing?sv=2022-11-02&ss=bfqt..."
```

#### **2. Erro de Memória no Spark**

Edite o arquivo `.env`:
```bash
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g
```

#### **3. Airflow não Inicia**

```bash
# Parar todos os serviços
astro dev stop

# Limpar containers
docker system prune -f

# Reiniciar
astro dev start
```

#### **4. Problemas de Permissão**

```bash
# Linux/macOS - Ajustar permissões
sudo chown -R $USER:$USER .
chmod -R 755 .
```

### **Comandos Úteis**

```bash
# Verificar status dos containers
docker ps

# Ver logs de um container específico
docker logs <container_id>

# Reiniciar apenas o Airflow
astro dev restart

# Executar comando dentro do container
astro dev bash
```

---

## 📚 Próximos Passos

Agora que você tem o ambiente funcionando, explore:

1. 📖 **[Arquitetura](arquitetura.md)** - Entenda como o sistema funciona
2. 🔧 **[Pipeline ETL](pipeline_etl.md)** - Detalhes do processamento de dados
3. 📊 **[KPIs e Métricas](kpis_metricas.md)** - Indicadores calculados
4. 🎛️ **[Airflow](airflow.md)** - Orquestração avançada
5. 🧪 **[Testes](testes.md)** - Como testar o sistema

---

## 🆘 Precisa de Ajuda?

- 📖 **Documentação**: [docs/](../index.md)
- 🐛 **Issues**: [GitHub Issues](https://github.com/seu-usuario/projeto_etl_spark/issues)
- 💬 **Discussões**: [GitHub Discussions](https://github.com/seu-usuario/projeto_etl_spark/discussions)
- 📧 **Email**: contato@projeto-etl.com

---

!!! success "Parabéns! 🎉"
    Você configurou com sucesso o projeto ETL com Apache Spark e Azure Data Lake. O pipeline está pronto para processar dados em larga escala! 