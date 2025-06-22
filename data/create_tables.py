#!/usr/bin/env python3
"""
Script para criar tabelas do sistema de entregas
Cria todas as tabelas necessárias no schema configurado
"""

import os
import pyodbc
import sys
from typing import List, Dict

class TableCreator:
    def __init__(self):
        """Inicializa configuração do SQL Server com variáveis de ambiente"""
        self.server = os.getenv('DB_SERVER', 'localhost')
        self.port = os.getenv('DB_PORT', '1433')
        self.username = os.getenv('DB_USERNAME', 'sa')
        self.password = os.getenv('DB_PASSWORD', 'satc@2025')
        self.database = os.getenv('DB_DATABASE', 'etl_entregas_db')
        self.schema = os.getenv('DB_SCHEMA', 'entregas')
        
        # String de conexão para o database
        self.db_conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.server},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"TrustServerCertificate=yes;"
        )

    def get_table_scripts(self) -> List[Dict[str, str]]:
        """Retorna lista com scripts SQL das tabelas na ordem correta de criação"""
        return [
            {
                "name": "Clientes",
                "sql": f"""
                CREATE TABLE [{self.schema}].[Clientes] (
                    id_cliente INT PRIMARY KEY IDENTITY(1,1),
                    nome_cliente VARCHAR(255) NOT NULL,
                    tipo_cliente VARCHAR(50) CHECK (tipo_cliente IN ('Pessoa Física', 'Pessoa Jurídica')),
                    cpf_cnpj VARCHAR(18) UNIQUE,
                    email VARCHAR(100),
                    telefone VARCHAR(20),
                    endereco VARCHAR(255),
                    cidade VARCHAR(100),
                    estado VARCHAR(50),
                    cep VARCHAR(10),
                    data_cadastro DATETIME DEFAULT GETDATE()
                )
                """
            },
            {
                "name": "Motoristas",
                "sql": f"""
                CREATE TABLE [{self.schema}].[Motoristas] (
                    id_motorista INT PRIMARY KEY IDENTITY(1,1),
                    nome_motorista VARCHAR(255) NOT NULL,
                    cpf VARCHAR(14) UNIQUE NOT NULL,
                    numero_cnh VARCHAR(20) UNIQUE NOT NULL,
                    data_nascimento DATE,
                    telefone VARCHAR(20),
                    email VARCHAR(100),
                    status_ativo BIT DEFAULT 1,
                    data_contratacao DATE
                )
                """
            },
            {
                "name": "Veiculos",
                "sql": f"""
                CREATE TABLE [{self.schema}].[Veiculos] (
                    id_veiculo INT PRIMARY KEY IDENTITY(1,1),
                    placa VARCHAR(10) UNIQUE NOT NULL,
                    modelo VARCHAR(100) NOT NULL,
                    marca VARCHAR(100),
                    ano_fabricacao INT,
                    capacidade_carga_kg DECIMAL(10, 2),
                    tipo_veiculo VARCHAR(50) CHECK (tipo_veiculo IN ('Caminhão', 'Van', 'Utilitário', 'Carro')),
                    status_operacional VARCHAR(50) CHECK (status_operacional IN ('Disponível', 'Em Viagem', 'Em Manutenção', 'Inativo'))
                )
                """
            },
            {
                "name": "Tipos_Carga",
                "sql": f"""
                CREATE TABLE [{self.schema}].[Tipos_Carga] (
                    id_tipo_carga INT PRIMARY KEY IDENTITY(1,1),
                    nome_tipo VARCHAR(100) NOT NULL UNIQUE,
                    descricao_tipo VARCHAR(MAX),
                    requer_refrigeracao BIT DEFAULT 0,
                    peso_medio_kg DECIMAL(10, 2)
                )
                """
            },
            {
                "name": "Rotas",
                "sql": f"""
                CREATE TABLE [{self.schema}].[Rotas] (
                    id_rota INT PRIMARY KEY IDENTITY(1,1),
                    nome_rota VARCHAR(255) NOT NULL,
                    origem VARCHAR(255) NOT NULL,
                    destino VARCHAR(255) NOT NULL,
                    distancia_km DECIMAL(10, 2),
                    tempo_estimado_horas DECIMAL(5, 2)
                )
                """
            },
            {
                "name": "Entregas",
                "sql": f"""
                CREATE TABLE [{self.schema}].[Entregas] (
                    id_entrega INT PRIMARY KEY IDENTITY(1,1),
                    id_veiculo INT NOT NULL,
                    id_motorista INT NOT NULL,
                    id_cliente_remetente INT NOT NULL,
                    id_cliente_destinatario INT NOT NULL,
                    id_rota INT,
                    id_tipo_carga INT NOT NULL,
                    data_inicio_entrega DATETIME NOT NULL,
                    data_previsao_fim_entrega DATETIME,
                    data_fim_real_entrega DATETIME,
                    status_entrega VARCHAR(50) NOT NULL CHECK (status_entrega IN ('Agendada', 'Em Trânsito', 'Entregue', 'Atrasada', 'Cancelada', 'Problema')),
                    valor_frete DECIMAL(10, 2) NOT NULL,
                    peso_carga_kg DECIMAL(10, 2),
                    CONSTRAINT FK_Entregas_Veiculo FOREIGN KEY (id_veiculo) REFERENCES [{self.schema}].[Veiculos](id_veiculo),
                    CONSTRAINT FK_Entregas_Motorista FOREIGN KEY (id_motorista) REFERENCES [{self.schema}].[Motoristas](id_motorista),
                    CONSTRAINT FK_Entregas_ClienteRemetente FOREIGN KEY (id_cliente_remetente) REFERENCES [{self.schema}].[Clientes](id_cliente),
                    CONSTRAINT FK_Entregas_ClienteDestinatario FOREIGN KEY (id_cliente_destinatario) REFERENCES [{self.schema}].[Clientes](id_cliente),
                    CONSTRAINT FK_Entregas_Rota FOREIGN KEY (id_rota) REFERENCES [{self.schema}].[Rotas](id_rota),
                    CONSTRAINT FK_Entregas_TipoCarga FOREIGN KEY (id_tipo_carga) REFERENCES [{self.schema}].[Tipos_Carga](id_tipo_carga)
                )
                """
            },
            {
                "name": "Coletas",
                "sql": f"""
                CREATE TABLE [{self.schema}].[Coletas] (
                    id_coleta INT PRIMARY KEY IDENTITY(1,1),
                    id_entrega INT NOT NULL UNIQUE,
                    data_hora_coleta DATETIME NOT NULL,
                    endereco_coleta VARCHAR(255),
                    status_coleta VARCHAR(50) CHECK (status_coleta IN ('Agendada', 'Realizada', 'Cancelada', 'Problema')),
                    observacoes VARCHAR(MAX),
                    CONSTRAINT FK_Coletas_Entrega FOREIGN KEY (id_entrega) REFERENCES [{self.schema}].[Entregas](id_entrega)
                )
                """
            },
            {
                "name": "Manutencoes_Veiculo",
                "sql": f"""
                CREATE TABLE [{self.schema}].[Manutencoes_Veiculo] (
                    id_manutencao INT PRIMARY KEY IDENTITY(1,1),
                    id_veiculo INT NOT NULL,
                    data_manutencao DATETIME NOT NULL,
                    tipo_manutencao VARCHAR(50) NOT NULL CHECK (tipo_manutencao IN ('Preventiva', 'Corretiva', 'Preditiva')),
                    descricao_servico VARCHAR(MAX),
                    custo_manutencao DECIMAL(10, 2) NOT NULL,
                    tempo_parado_horas DECIMAL(5, 2),
                    CONSTRAINT FK_Manutencoes_Veiculo FOREIGN KEY (id_veiculo) REFERENCES [{self.schema}].[Veiculos](id_veiculo)
                )
                """
            },
            {
                "name": "Abastecimentos",
                "sql": f"""
                CREATE TABLE [{self.schema}].[Abastecimentos] (
                    id_abastecimento INT PRIMARY KEY IDENTITY(1,1),
                    id_veiculo INT NOT NULL,
                    data_abastecimento DATETIME NOT NULL,
                    litros DECIMAL(10, 2) NOT NULL,
                    valor_total DECIMAL(10, 2) NOT NULL,
                    tipo_combustivel VARCHAR(50),
                    CONSTRAINT FK_Abastecimentos_Veiculo FOREIGN KEY (id_veiculo) REFERENCES [{self.schema}].[Veiculos](id_veiculo)
                )
                """
            },
            {
                "name": "Multas_Transito",
                "sql": f"""
                CREATE TABLE [{self.schema}].[Multas_Transito] (
                    id_multa INT PRIMARY KEY IDENTITY(1,1),
                    id_veiculo INT NOT NULL,
                    id_motorista INT,
                    data_multa DATETIME NOT NULL,
                    local_multa VARCHAR(255),
                    descricao_infracao VARCHAR(MAX),
                    valor_multa DECIMAL(10, 2) NOT NULL,
                    status_pagamento VARCHAR(50) CHECK (status_pagamento IN ('Pendente', 'Paga', 'Recorrida')),
                    CONSTRAINT FK_Multas_Veiculo FOREIGN KEY (id_veiculo) REFERENCES [{self.schema}].[Veiculos](id_veiculo),
                    CONSTRAINT FK_Multas_Motorista FOREIGN KEY (id_motorista) REFERENCES [{self.schema}].[Motoristas](id_motorista)
                )
                """
            }
        ]

    def table_exists(self, table_name: str) -> bool:
        """Verifica se a tabela já existe no schema"""
        try:
            with pyodbc.connect(self.db_conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                """, (self.schema, table_name))
                return cursor.fetchone()[0] > 0
        except Exception as e:
            print(f"❌ Erro ao verificar tabela {table_name}: {e}")
            return False

    def create_table(self, table_info: Dict[str, str]) -> bool:
        """Cria uma tabela específica"""
        table_name = table_info["name"]
        table_sql = table_info["sql"]
        
        try:
            if self.table_exists(table_name):
                print(f"⚠️  Tabela '{table_name}' já existe - pulando")
                return True
            
            print(f"🔨 Criando tabela '{table_name}'...")
            with pyodbc.connect(self.db_conn_str) as conn:
                conn.autocommit = True
                cursor = conn.cursor()
                cursor.execute(table_sql)
                print(f"✅ Tabela '{table_name}' criada com sucesso!")
                return True
                
        except Exception as e:
            print(f"❌ Erro ao criar tabela '{table_name}': {e}")
            return False

    def drop_all_tables(self) -> bool:
        """Remove todas as tabelas (em ordem reversa devido às FK)"""
        try:
            print("🗑️  Removendo tabelas existentes...")
            
            # Lista das tabelas em ordem reversa (devido às foreign keys)
            tables_to_drop = [
                "Multas_Transito", "Abastecimentos", "Manutencoes_Veiculo", 
                "Coletas", "Entregas", "Rotas", "Tipos_Carga", 
                "Veiculos", "Motoristas", "Clientes"
            ]
            
            with pyodbc.connect(self.db_conn_str) as conn:
                conn.autocommit = True
                cursor = conn.cursor()
                
                for table in tables_to_drop:
                    try:
                        if self.table_exists(table):
                            cursor.execute(f"DROP TABLE [{self.schema}].[{table}]")
                            print(f"🗑️  Tabela '{table}' removida")
                    except Exception as e:
                        print(f"⚠️  Erro ao remover tabela '{table}': {e}")
                
            print("✅ Limpeza concluída!")
            return True
            
        except Exception as e:
            print(f"❌ Erro na limpeza: {e}")
            return False

    def create_all_tables(self, recreate: bool = False) -> bool:
        """Cria todas as tabelas do sistema"""
        print("🏗️  Iniciando criação das tabelas...")
        print(f"📋 Schema: {self.schema}")
        print(f"📋 Database: {self.database}")
        print("-" * 50)
        
        # Se recreate=True, remove todas as tabelas primeiro
        if recreate:
            if not self.drop_all_tables():
                return False
            print()
        
        # Obter scripts das tabelas
        tables = self.get_table_scripts()
        
        # Criar cada tabela
        success_count = 0
        for table_info in tables:
            if self.create_table(table_info):
                success_count += 1
            print()  # Linha em branco para separar
        
        # Resumo final
        total_tables = len(tables)
        print("=" * 50)
        if success_count == total_tables:
            print("✅ TODAS AS TABELAS CRIADAS COM SUCESSO!")
            print(f"📊 {success_count}/{total_tables} tabelas criadas")
            self.show_tables_summary()
            return True
        else:
            print(f"⚠️  CRIAÇÃO PARCIAL: {success_count}/{total_tables} tabelas criadas")
            return False

    def show_tables_summary(self):
        """Mostra resumo das tabelas criadas"""
        try:
            print("\n📋 RESUMO DAS TABELAS CRIADAS:")
            print("-" * 50)
            
            with pyodbc.connect(self.db_conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT TABLE_NAME, 
                           (SELECT COUNT(*) 
                            FROM INFORMATION_SCHEMA.COLUMNS 
                            WHERE TABLE_SCHEMA = t.TABLE_SCHEMA 
                            AND TABLE_NAME = t.TABLE_NAME) as TOTAL_COLUMNS
                    FROM INFORMATION_SCHEMA.TABLES t
                    WHERE TABLE_SCHEMA = ?
                    ORDER BY TABLE_NAME
                """, (self.schema,))
                
                tables = cursor.fetchall()
                for i, (table_name, column_count) in enumerate(tables, 1):
                    print(f"{i:2d}. {table_name:<25} ({column_count} colunas)")
                
                print(f"\n✅ Total: {len(tables)} tabelas no schema '{self.schema}'")
                
        except Exception as e:
            print(f"❌ Erro ao listar tabelas: {e}")

    def test_database_connection(self) -> bool:
        """Testa a conexão com o banco"""
        try:
            print("🧪 Testando conexão com o banco...")
            with pyodbc.connect(self.db_conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT DB_NAME(), @@VERSION")
                result = cursor.fetchone()
                print(f"✅ Conectado ao database: {result[0]}")
                return True
        except Exception as e:
            print(f"❌ Erro de conexão: {e}")
            return False

def main():
    """Função principal"""
    print("🏗️  Criador de Tabelas - Sistema de Entregas")
    print("=" * 50)
    
    # Verificar argumentos
    recreate = "--recreate" in sys.argv or "-r" in sys.argv
    
    if recreate:
        print("🔄 Modo RECREATE ativado - tabelas serão removidas e recriadas")
        response = input("⚠️  Tem certeza? Digite 'sim' para continuar: ")
        if response.lower() != 'sim':
            print("❌ Operação cancelada")
            sys.exit(0)
        print()
    
    # Verificar dependências
    try:
        import pyodbc
    except ImportError:
        print("❌ pyodbc não está instalado!")
        print("💡 Execute: pip install pyodbc")
        sys.exit(1)
    
    # Criar instância e executar
    creator = TableCreator()
    
    # Testar conexão
    if not creator.test_database_connection():
        print("💡 Verifique se:")
        print("   - O container SQL Server está rodando")
        print("   - As variáveis de ambiente estão corretas")
        print("   - O database e schema existem")
        sys.exit(1)
    
    print()
    
    # Criar tabelas
    if creator.create_all_tables(recreate=recreate):
        print("\n🎉 Processo concluído com sucesso!")
        print("💡 Suas tabelas estão prontas para uso!")
        sys.exit(0)
    else:
        print("\n💥 Falha na criação das tabelas!")
        sys.exit(1)

if __name__ == "__main__":
    main()