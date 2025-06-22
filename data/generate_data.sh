#!/bin/bash

echo "🎲 Gerador de Dados Faker - Sistema de Entregas"
echo "================================================"

# Verificar se Python está disponível
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 não encontrado!"
    exit 1
fi

# Verificar se o script Python existe
if [ ! -f "faker_data.py" ]; then
    echo "❌ Arquivo faker_data.py não encontrado!"
    echo "💡 Certifique-se de que o arquivo está no mesmo diretório"
    exit 1
fi

# Carregar variáveis de ambiente se existir arquivo .env
if [ -f .env ]; then
    echo "📋 Carregando variáveis de ambiente..."
    export $(cat .env | grep -v '^#' | grep -v '^$' | xargs)
fi

# Verificar se container está rodando
echo "🔍 Verificando container SQL Server..."
if [ $(docker ps -f "name=etl_entregas" -f "status=running" -q | wc -l) -eq 0 ]; then
    echo "⚠️  Container 'etl_entregas' não está rodando"
    echo "💡 Inicie o container com: docker start etl_entregas"
    echo "   Ou execute: ./setup.sh"
    exit 1
fi

echo "✅ Container está rodando!"

# Verificar se as tabelas existem
echo "🔍 Verificando se as tabelas existem..."
python3 -c "
import pyodbc
import os
try:
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={os.getenv(\"DB_SERVER\", \"localhost\")},{os.getenv(\"DB_PORT\", \"1433\")};DATABASE={os.getenv(\"DB_DATABASE\", \"etl_entregas_db\")};UID={os.getenv(\"DB_USERNAME\", \"sa\")};PWD={os.getenv(\"DB_PASSWORD\", \"satc@2025\")};TrustServerCertificate=yes;'
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute(\"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?\", (os.getenv('DB_SCHEMA', 'entregas'),))
    table_count = cursor.fetchone()[0]
    conn.close()
    if table_count < 10:
        print(f'⚠️  Apenas {table_count} tabelas encontradas!')
        print('💡 Execute primeiro: ./create_tables.sh')
        exit(1)
    else:
        print(f'✅ {table_count} tabelas encontradas!')
except Exception as e:
    print(f'❌ Erro ao conectar: {e}')
    exit(1)
"

if [ $? -ne 0 ]; then
    exit 1
fi

echo ""
echo "📋 IMPORTANTE:"
echo "   • Este processo irá gerar 200.000 registros"
echo "   • Pode levar alguns minutos para concluir"
echo "   • Dados existentes serão REMOVIDOS"
echo ""

# Perguntar confirmação se não foi passado --no-confirm
if [[ "$1" != "--no-confirm" ]]; then
    read -p "Deseja continuar? (s/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Ss]$ ]]; then
        echo "❌ Operação cancelada"
        exit 0
    fi
fi

# Executar geração de dados
echo ""
echo "🚀 Iniciando geração de dados..."
echo "⏱️  Isso pode levar alguns minutos..."
echo ""

start_time=$(date +%s)

python3 faker_data.py "$@"

# Verificar resultado
if [ $? -eq 0 ]; then
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    
    echo ""
    echo "🎉 DADOS GERADOS COM SUCESSO!"
    echo "============================="
    echo "⏱️  Tempo total: ${duration} segundos"
    echo ""
    echo "📊 O que foi criado:"
    echo "   • 5.000 clientes (PF e PJ)"
    echo "   • 800 motoristas"
    echo "   • 1.200 veículos"
    echo "   • 50 tipos de carga"
    echo "   • 150 rotas"
    echo "   • 120.000 entregas"
    echo "   • 50.000 coletas"
    echo "   • 15.000 manutenções"
    echo "   • 7.000 abastecimentos"
    echo "   • 800 multas"
    echo ""
    echo "💡 Próximos passos:"
    echo "   • Use ferramentas de BI para análise"
    echo "   • Conecte com Power BI, Tableau, etc."
    echo "   • Desenvolva aplicações usando os dados"
    echo ""
    echo "🔗 String de conexão:"
    echo "   Host: localhost:1433"
    echo "   Database: ${DB_DATABASE:-etl_entregas_db}"
    echo "   Schema: ${DB_SCHEMA:-entregas}"
    echo "   User: ${DB_USERNAME:-sa}"
else
    echo ""
    echo "❌ FALHA NA GERAÇÃO DOS DADOS!"
    echo "💡 Verifique os logs acima para mais detalhes"
    echo ""
    echo "🔧 Comandos para diagnóstico:"
    echo "   docker logs etl_entregas                    # Logs do SQL Server"
    echo "   python3 create_tables.py                    # Recriar tabelas"
    echo "   python3 faker_data.py --no-clear           # Tentar sem limpar"
    exit 1
fi