#!/bin/bash

echo "🏗️  Criador de Tabelas - Sistema de Entregas"
echo "============================================="

# Verificar se Python está disponível
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 não encontrado!"
    exit 1
fi

# Verificar se o script Python existe
if [ ! -f "create_tables.py" ]; then
    echo "❌ Arquivo create_tables.py não encontrado!"
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
echo ""

# Executar script Python
echo "🚀 Executando criação das tabelas..."
python3 create_tables.py "$@"

# Verificar resultado
if [ $? -eq 0 ]; then
    echo ""
    echo "🎉 Tabelas criadas com sucesso!"
    echo ""
    echo "📋 Próximos passos:"
    echo "   - Suas tabelas estão prontas no schema 'entregas'"
    echo "   - Você pode começar a inserir dados"
    echo "   - Use qualquer ferramenta de BI/ETL para conectar"
    echo ""
    echo "🔧 Comandos úteis:"
    echo "   python3 create_tables.py --recreate  # Recriar todas as tabelas"
    echo "   docker exec -it etl_entregas sqlcmd -S localhost -U sa -P 'satc@2025'"
else
    echo ""
    echo "❌ Falha na criação das tabelas!"
    echo "💡 Verifique os logs acima para mais detalhes"
    exit 1
fi