# 🔐 Configuração de Variáveis de Ambiente

## 📋 Visão Geral

Este documento descreve todas as variáveis de ambiente necessárias para configurar e executar o projeto ETL com Apache Spark e Azure Data Lake.

---

## 📁 Arquivo `.env.example`

Crie um arquivo `.env` na raiz do projeto baseado no template abaixo:

```bash
# =============================================================================
# PROJETO ETL COM APACHE SPARK & AZURE DATA LAKE
# Arquivo de Configuração de Variáveis de Ambiente
# =============================================================================

# =============================================================================
# AZURE DATA LAKE STORAGE
# =============================================================================

# Nome da conta de armazenamento Azure
ADLS_ACCOUNT_NAME=seuaccountstorage

# Containers do Data Lake (Medallion Architecture)
ADLS_FILE_SYSTEM_NAME=landing
ADLS_BRONZE_CONTAINER_NAME=bronze
ADLS_SILVER_CONTAINER_NAME=silver
ADLS_GOLD_CONTAINER_NAME=gold

# Token SAS para acesso ao Azure Storage
# Gere um novo token com permissões: read, add, create, write, delete, list, update, process, tag, filter, setimmutability
# Válido para: blob, file, queue, table
ADLS_SAS_TOKEN="sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupitfx&se=2024-12-31T23:59:59Z&st=2024-01-01T00:00:00Z&spr=https&sig=SUA_ASSINATURA_SAS_AQUI"

# =============================================================================
# SQL SERVER DATABASE
# =============================================================================

# Servidor SQL Server (Azure SQL Database ou SQL Server local)
SQL_SERVER=seu-servidor.database.windows.net

# Nome do banco de dados
SQL_DATABASE=LogisticaDB

# Schema do banco de dados
SQL_SCHEMA=dbo

# Credenciais de acesso
SQL_USERNAME=admin
SQL_PASSWORD=SuaSenhaSegura123!

# =============================================================================
# APACHE SPARK CONFIGURATION
# =============================================================================

# Memória do Driver Spark
SPARK_DRIVER_MEMORY=4g

# Memória dos Executors Spark
SPARK_EXECUTOR_MEMORY=4g

# Número de cores por Executor
SPARK_EXECUTOR_CORES=2

# Número de instâncias de Executors
SPARK_EXECUTOR_INSTANCES=2

# Número de partições para shuffle operations
SPARK_SQL_SHUFFLE_PARTITIONS=200

# Paralelismo padrão
SPARK_DEFAULT_PARALLELISM=8

# =============================================================================
# AIRFLOW CONFIGURATION
# =============================================================================

# Ambiente do Airflow (development, staging, production)
AIRFLOW_ENV=development

# Timezone
AIRFLOW_TIMEZONE=America/Sao_Paulo

# Email para notificações (opcional)
AIRFLOW_ADMIN_EMAIL=admin@empresa.com

# =============================================================================
# LOGGING & MONITORING
# =============================================================================

# Nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
LOG_LEVEL=INFO

# Diretório para logs
LOG_DIR=/tmp/logs

# Habilitar logs detalhados do Spark
SPARK_VERBOSE_LOGGING=false

# =============================================================================
# PERFORMANCE TUNING
# =============================================================================

# Tamanho do arquivo alvo para Delta Lake (em bytes)
DELTA_TARGET_FILE_SIZE=134217728

# Habilitar otimizações automáticas
DELTA_AUTO_OPTIMIZE=true
DELTA_AUTO_COMPACT=true

# Timeout de rede (em segundos)
NETWORK_TIMEOUT=800

# =============================================================================
# DEVELOPMENT & TESTING
# =============================================================================

# Modo de desenvolvimento (habilita logs extras e configurações de debug)
DEV_MODE=true

# Usar dados sintéticos (para desenvolvimento/testes)
USE_SYNTHETIC_DATA=false

# Número de registros para dados sintéticos
SYNTHETIC_DATA_RECORDS=10000

# =============================================================================
# SECURITY
# =============================================================================

# Chave de criptografia para dados sensíveis (opcional)
ENCRYPTION_KEY=sua-chave-de-32-caracteres-aqui

# Habilitar SSL/TLS para conexões
ENABLE_SSL=true

# =============================================================================
# BUSINESS CONFIGURATION
# =============================================================================

# Configurações específicas do negócio de logística

# Fuso horário para operações de negócio
BUSINESS_TIMEZONE=America/Sao_Paulo

# Horário de início das operações (formato HH:MM)
BUSINESS_START_TIME=06:00

# Horário de fim das operações (formato HH:MM)
BUSINESS_END_TIME=22:00

# Dias úteis (1=Segunda, 7=Domingo)
BUSINESS_DAYS=1,2,3,4,5,6

# =============================================================================
# ALERTAS E NOTIFICAÇÕES
# =============================================================================

# Email para alertas críticos
ALERT_EMAIL=alertas@empresa.com

# Webhook para notificações Slack (opcional)
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK

# Threshold para alertas de performance
PERFORMANCE_ALERT_THRESHOLD=90

# =============================================================================
# BACKUP E RECOVERY
# =============================================================================

# Habilitar backup automático
ENABLE_AUTO_BACKUP=true

# Retenção de backups (em dias)
BACKUP_RETENTION_DAYS=30

# Diretório de backup
BACKUP_DIR=/tmp/backups
```

---

## 🔧 Configuração Passo a Passo

### **1. Criação do Arquivo**

```bash
# Copiar template
cp .env.example .env

# Editar com suas configurações
nano .env  # ou vim .env, code .env, etc.
```

### **2. Azure Data Lake Storage**

#### **Criar Storage Account**

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
```

#### **Criar Containers**

```bash
# Criar containers para Medallion Architecture
az storage container create --name landing --account-name seuaccountstorage
az storage container create --name bronze --account-name seuaccountstorage
az storage container create --name silver --account-name seuaccountstorage
az storage container create --name gold --account-name seuaccountstorage
```

#### **Gerar SAS Token**

```bash
# Gerar SAS Token válido por 1 ano
az storage account generate-sas \
    --account-name seuaccountstorage \
    --account-key $(az storage account keys list --account-name seuaccountstorage --query '[0].value' -o tsv) \
    --expiry 2024-12-31T23:59:59Z \
    --permissions racwdlupitfx \
    --resource-types sco \
    --services bfqt
```

### **3. SQL Server Configuration**

#### **Azure SQL Database**

```bash
# Criar SQL Server
az sql server create \
    --name seu-sql-server \
    --resource-group rg-projeto-etl \
    --location brazilsouth \
    --admin-user admin \
    --admin-password SuaSenhaSegura123!

# Criar Database
az sql db create \
    --resource-group rg-projeto-etl \
    --server seu-sql-server \
    --name LogisticaDB \
    --service-objective Basic
```

#### **Configurar Firewall**

```bash
# Permitir acesso do Azure
az sql server firewall-rule create \
    --resource-group rg-projeto-etl \
    --server seu-sql-server \
    --name AllowAzureServices \
    --start-ip-address 0.0.0.0 \
    --end-ip-address 0.0.0.0

# Permitir seu IP local
az sql server firewall-rule create \
    --resource-group rg-projeto-etl \
    --server seu-sql-server \
    --name AllowMyIP \
    --start-ip-address SEU_IP \
    --end-ip-address SEU_IP
```

---

## 📊 Configurações por Ambiente

### **Desenvolvimento Local**

```bash
# Recursos limitados para desenvolvimento
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g
SPARK_EXECUTOR_CORES=1
SPARK_EXECUTOR_INSTANCES=1
SPARK_SQL_SHUFFLE_PARTITIONS=50

# Logs verbosos para debug
LOG_LEVEL=DEBUG
SPARK_VERBOSE_LOGGING=true
DEV_MODE=true
```

### **Staging/Teste**

```bash
# Recursos médios para testes
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g
SPARK_EXECUTOR_CORES=2
SPARK_EXECUTOR_INSTANCES=2
SPARK_SQL_SHUFFLE_PARTITIONS=200

# Logs moderados
LOG_LEVEL=INFO
DEV_MODE=false
```

### **Produção**

```bash
# Recursos maximizados para produção
SPARK_DRIVER_MEMORY=8g
SPARK_EXECUTOR_MEMORY=8g
SPARK_EXECUTOR_CORES=4
SPARK_EXECUTOR_INSTANCES=4
SPARK_SQL_SHUFFLE_PARTITIONS=400

# Logs otimizados
LOG_LEVEL=WARNING
SPARK_VERBOSE_LOGGING=false
DEV_MODE=false

# Segurança reforçada
ENABLE_SSL=true
ENCRYPTION_KEY=sua-chave-segura-de-32-chars
```

---

## 🔒 Segurança

### **Proteção de Credenciais**

```bash
# Nunca commite o arquivo .env
echo ".env" >> .gitignore

# Use variáveis de ambiente do sistema em produção
export ADLS_SAS_TOKEN="seu_token_aqui"
export SQL_PASSWORD="sua_senha_aqui"
```

### **Rotação de Credenciais**

```bash
# Gerar novo SAS Token mensalmente
az storage account generate-sas \
    --account-name seuaccountstorage \
    --account-key $(az storage account keys list --account-name seuaccountstorage --query '[0].value' -o tsv) \
    --expiry $(date -d "+1 month" +%Y-%m-%dT23:59:59Z) \
    --permissions racwdlupitfx \
    --resource-types sco \
    --services bfqt
```

---

## 🧪 Validação da Configuração

### **Script de Teste**

```python
#!/usr/bin/env python3
"""
Script para validar configuração das variáveis de ambiente
"""

import os
from dotenv import load_dotenv

def validate_config():
    load_dotenv()
    
    required_vars = [
        'ADLS_ACCOUNT_NAME',
        'ADLS_SAS_TOKEN',
        'SQL_SERVER',
        'SQL_DATABASE',
        'SQL_USERNAME',
        'SQL_PASSWORD'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Variáveis faltando: {', '.join(missing_vars)}")
        return False
    
    print("✅ Todas as variáveis obrigatórias estão configuradas")
    return True

def test_azure_connection():
    """Testa conexão com Azure Storage"""
    try:
        from azure.storage.blob import BlobServiceClient
        
        account_name = os.getenv('ADLS_ACCOUNT_NAME')
        sas_token = os.getenv('ADLS_SAS_TOKEN')
        
        client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=sas_token
        )
        
        # Listar containers
        containers = list(client.list_containers())
        print(f"✅ Conexão Azure OK. Containers encontrados: {len(containers)}")
        return True
        
    except Exception as e:
        print(f"❌ Erro na conexão Azure: {e}")
        return False

def test_sql_connection():
    """Testa conexão com SQL Server"""
    try:
        from sqlalchemy import create_engine
        from urllib.parse import quote_plus
        
        server = os.getenv('SQL_SERVER')
        database = os.getenv('SQL_DATABASE')
        username = os.getenv('SQL_USERNAME')
        password = quote_plus(os.getenv('SQL_PASSWORD'))
        
        conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
        engine = create_engine(conn_str)
        
        # Teste simples
        with engine.connect() as conn:
            result = conn.execute("SELECT 1 as test")
            print("✅ Conexão SQL Server OK")
            return True
            
    except Exception as e:
        print(f"❌ Erro na conexão SQL Server: {e}")
        return False

if __name__ == "__main__":
    print("🔍 Validando configuração...")
    
    if validate_config():
        print("\n🧪 Testando conexões...")
        test_azure_connection()
        test_sql_connection()
    
    print("\n✅ Validação concluída!")
```

### **Executar Validação**

```bash
# Salvar como validate_config.py e executar
python validate_config.py
```

---

## 📝 Troubleshooting

### **Problemas Comuns**

#### **1. Token SAS Inválido**

```bash
# Erro: "Server failed to authenticate the request"
# Solução: Gerar novo token SAS

az storage account generate-sas \
    --account-name seuaccountstorage \
    --account-key $(az storage account keys list --account-name seuaccountstorage --query '[0].value' -o tsv) \
    --expiry 2024-12-31T23:59:59Z \
    --permissions racwdlupitfx \
    --resource-types sco \
    --services bfqt
```

#### **2. Conexão SQL Server Negada**

```bash
# Erro: "Login failed for user"
# Solução: Verificar firewall e credenciais

# Listar regras de firewall
az sql server firewall-rule list \
    --resource-group rg-projeto-etl \
    --server seu-sql-server

# Adicionar seu IP
az sql server firewall-rule create \
    --resource-group rg-projeto-etl \
    --server seu-sql-server \
    --name AllowMyIP \
    --start-ip-address $(curl -s ifconfig.me) \
    --end-ip-address $(curl -s ifconfig.me)
```

#### **3. Erro de Memória Spark**

```bash
# Erro: "OutOfMemoryError"
# Solução: Reduzir configurações de memória

SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g
SPARK_SQL_SHUFFLE_PARTITIONS=50
```

---

## 📚 Referências

- [Azure Storage SAS Tokens](https://docs.microsoft.com/en-us/azure/storage/common/storage-sas-overview)
- [Azure SQL Database](https://docs.microsoft.com/en-us/azure/azure-sql/database/)
- [Apache Spark Configuration](https://spark.apache.org/docs/latest/configuration.html)
- [Apache Airflow Configuration](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html)

---

!!! warning "Importante"
    Nunca commite credenciais reais no repositório. Use sempre o arquivo `.env` local ou variáveis de ambiente do sistema em produção. 