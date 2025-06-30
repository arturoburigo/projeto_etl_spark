#!/usr/bin/env python3
"""
Script to create delivery system tables
Creates all necessary tables in the configured schema
"""

import os
import pyodbc
import sys
from typing import List, Dict

class TableCreator:
    def __init__(self):
        """Initialize SQL Server configuration with environment variables"""
        self.server = os.getenv('DB_SERVER', 'localhost')
        self.port = os.getenv('DB_PORT', '1433')
        self.username = os.getenv('DB_USERNAME', 'sa')
        self.password = os.getenv('DB_PASSWORD', 'satc@2025')
        self.database = os.getenv('DB_DATABASE', 'etl_entregas_db')
        self.schema = os.getenv('DB_SCHEMA', 'entregas')
        
        # Connection string for database
        self.db_conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.server},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"TrustServerCertificate=yes;"
        )

    def get_table_scripts(self) -> List[Dict[str, str]]:
        """Returns list with SQL scripts for tables in correct creation order"""
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
        """Check if table already exists in schema"""
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
            print(f"❌ Error checking table {table_name}: {e}")
            return False

    def create_table(self, table_info: Dict[str, str]) -> bool:
        """Create a specific table"""
        table_name = table_info["name"]
        table_sql = table_info["sql"]
        
        try:
            if self.table_exists(table_name):
                print(f"⚠️  Table '{table_name}' already exists - skipping")
                return True
            
            print(f"🔨 Creating table '{table_name}'...")
            with pyodbc.connect(self.db_conn_str) as conn:
                conn.autocommit = True
                cursor = conn.cursor()
                cursor.execute(table_sql)
                print(f"✅ Table '{table_name}' created successfully!")
                return True
                
        except Exception as e:
            print(f"❌ Error creating table '{table_name}': {e}")
            return False

    def drop_all_tables(self) -> bool:
        """Remove all tables (in reverse order due to FK constraints)"""
        try:
            print("🗑️  Removing existing tables...")
            
            # List of tables in reverse order (due to foreign keys)
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
                            print(f"🗑️  Table '{table}' removed")
                    except Exception as e:
                        print(f"⚠️  Error removing table '{table}': {e}")
                
            print("✅ Cleanup completed!")
            return True
            
        except Exception as e:
            print(f"❌ Error during cleanup: {e}")
            return False

    def create_all_tables(self, recreate: bool = False) -> bool:
        """Create all system tables"""
        print("🏗️  Starting table creation...")
        print(f"📋 Schema: {self.schema}")
        print(f"📋 Database: {self.database}")
        print("-" * 50)
        
        # If recreate=True, remove all tables first
        if recreate:
            if not self.drop_all_tables():
                return False
            print()
        
        # Get table scripts
        tables = self.get_table_scripts()
        
        # Create each table
        success_count = 0
        for table_info in tables:
            if self.create_table(table_info):
                success_count += 1
            print()  # Blank line to separate
        
        # Final summary
        total_tables = len(tables)
        print("=" * 50)
        if success_count == total_tables:
            print("✅ ALL TABLES CREATED SUCCESSFULLY!")
            print(f"📊 {success_count}/{total_tables} tables created")
            self.show_tables_summary()
            return True
        else:
            print(f"⚠️  PARTIAL CREATION: {success_count}/{total_tables} tables created")
            return False

    def show_tables_summary(self):
        """Show summary of created tables"""
        try:
            print("\n📋 SUMMARY OF CREATED TABLES:")
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
                    print(f"{i:2d}. {table_name:<25} ({column_count} columns)")
                
                print(f"\n✅ Total: {len(tables)} tables in schema '{self.schema}'")
                
        except Exception as e:
            print(f"❌ Error listing tables: {e}")

    def test_database_connection(self) -> bool:
        """Test database connection"""
        try:
            print("🧪 Testing database connection...")
            with pyodbc.connect(self.db_conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT DB_NAME(), @@VERSION")
                result = cursor.fetchone()
                print(f"✅ Connected to database: {result[0]}")
                return True
        except Exception as e:
            print(f"❌ Connection error: {e}")
            return False

def main():
    """Main function"""
    print("🏗️  Table Creator - Delivery System")
    print("=" * 50)
    
    # Check arguments
    recreate = "--recreate" in sys.argv or "-r" in sys.argv
    
    if recreate:
        print("🔄 RECREATE mode activated - tables will be removed and recreated")
        response = input("⚠️  Are you sure? Type 'yes' to continue: ")
        if response.lower() != 'yes':
            print("❌ Operation cancelled")
            sys.exit(0)
        print()
    
    # Check dependencies
    try:
        import pyodbc
    except ImportError:
        print("❌ pyodbc is not installed!")
        print("💡 Run: pip install pyodbc")
        sys.exit(1)
    
    # Create instance and execute
    creator = TableCreator()
    
    # Test connection
    if not creator.test_database_connection():
        print("💡 Please check if:")
        print("   - SQL Server container is running")
        print("   - Environment variables are correct")
        print("   - Database and schema exist")
        sys.exit(1)
    
    print()
    
    # Create tables
    if creator.create_all_tables(recreate=recreate):
        print("\n🎉 Process completed successfully!")
        print("💡 Your tables are ready to use!")
        sys.exit(0)
    else:
        print("\n💥 Failed to create tables!")
        sys.exit(1)

if __name__ == "__main__":
    main()