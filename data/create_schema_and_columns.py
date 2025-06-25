#!/usr/bin/env python3
"""
Script para configurar banco de dados SQL Server
Cria database, schema e configura conexão baseado em variáveis de ambiente
"""

import os
import pyodbc
import time
import sys
from typing import Optional

class SQLServerSetup:
    def __init__(self):
        """Inicializa configuração do SQL Server com variáveis de ambiente"""
        self.server = os.getenv('DB_SERVER', 'localhost')
        self.port = os.getenv('DB_PORT', '1433')
        self.username = os.getenv('DB_USERNAME', 'sa')
        self.password = os.getenv('DB_PASSWORD', 'satc@2025')
        self.database = os.getenv('DB_DATABASE', 'etl_entregas_db')
        self.schema = os.getenv('DB_SCHEMA', 'entregas')
        
        # String de conexão para master (criação do database)
        self.master_conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.server},{self.port};"
            f"DATABASE=master;"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"TrustServerCertificate=yes;"
        )
        
        # String de conexão para o database específico
        self.db_conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.server},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"TrustServerCertificate=yes;"
        )

    def wait_for_server(self, timeout: int = 60) -> bool:
        """Aguarda o servidor SQL Server ficar disponível"""
        print(f"Aguardando SQL Server em {self.server}:{self.port}...")
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                conn = pyodbc.connect(self.master_conn_str, timeout=5)
                conn.close()
                print("✅ SQL Server está disponível!")
                return True
            except Exception as e:
                print(f"⏳ Tentando conectar... ({int(time.time() - start_time)}s)")
                time.sleep(2)
        
        print(f"❌ Timeout ao conectar com SQL Server após {timeout}s")
        return False

    def database_exists(self) -> bool:
        """Verifica se o database já existe"""
        try:
            with pyodbc.connect(self.master_conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM sys.databases WHERE name = ?", 
                    (self.database,)
                )
                return cursor.fetchone()[0] > 0
        except Exception as e:
            print(f"❌ Erro ao verificar database: {e}")
            return False

    def create_database(self) -> bool:
        """Cria o database se não existir"""
        try:
            if self.database_exists():
                print(f"✅ Database '{self.database}' já existe")
                return True
            
            print(f"🔨 Criando database '{self.database}'...")
            with pyodbc.connect(self.master_conn_str) as conn:
                conn.autocommit = True
                cursor = conn.cursor()
                
                # Criar database
                cursor.execute(f"CREATE DATABASE [{self.database}]")
                print(f"✅ Database '{self.database}' criado com sucesso!")
                return True
                
        except Exception as e:
            print(f"❌ Erro ao criar database: {e}")
            return False

    def schema_exists(self) -> bool:
        """Verifica se o schema já existe"""
        try:
            with pyodbc.connect(self.db_conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM sys.schemas WHERE name = ?", 
                    (self.schema,)
                )
                return cursor.fetchone()[0] > 0
        except Exception as e:
            print(f"❌ Erro ao verificar schema: {e}")
            return False

    def create_schema(self) -> bool:
        """Cria o schema se não existir"""
        try:
            if self.schema_exists():
                print(f"✅ Schema '{self.schema}' já existe")
                return True
            
            print(f"🔨 Criando schema '{self.schema}'...")
            with pyodbc.connect(self.db_conn_str) as conn:
                conn.autocommit = True
                cursor = conn.cursor()
                
                # Criar schema
                cursor.execute(f"CREATE SCHEMA [{self.schema}]")
                print(f"✅ Schema '{self.schema}' criado com sucesso!")
                return True
                
        except Exception as e:
            print(f"❌ Erro ao criar schema: {e}")
            return False

    def set_default_schema(self) -> bool:
        """Define o schema padrão para o usuário"""
        try:
            print(f"🔧 Configurando schema padrão '{self.schema}' para usuário '{self.username}'...")
            with pyodbc.connect(self.db_conn_str) as conn:
                conn.autocommit = True
                cursor = conn.cursor()
                
                # Definir schema padrão para o usuário
                cursor.execute(f"ALTER USER [{self.username}] WITH DEFAULT_SCHEMA = [{self.schema}]")
                print(f"✅ Schema padrão configurado!")
                return True
                
        except Exception as e:
            print(f"⚠️  Aviso ao configurar schema padrão: {e}")
            # Não é crítico se falhar
            return True

    def test_connection(self) -> bool:
        """Testa a conexão final"""
        try:
            print("🧪 Testando conexão final...")
            with pyodbc.connect(self.db_conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT @@VERSION, DB_NAME(), SCHEMA_NAME()")
                result = cursor.fetchone()
                print(f"✅ Conexão OK!")
                print(f"   Database: {result[1]}")
                print(f"   Schema atual: {result[2]}")
                return True
        except Exception as e:
            print(f"❌ Erro no teste de conexão: {e}")
            return False

    def setup(self) -> bool:
        """Executa todo o processo de configuração"""
        print("🚀 Iniciando configuração do SQL Server...")
        print(f"📋 Configurações:")
        print(f"   Servidor: {self.server}:{self.port}")
        print(f"   Database: {self.database}")
        print(f"   Schema: {self.schema}")
        print(f"   Usuário: {self.username}")
        print("-" * 50)
        
        # Aguardar servidor
        if not self.wait_for_server():
            return False
        
        # Criar database
        if not self.create_database():
            return False
        
        # Aguardar um pouco para o database ficar disponível
        time.sleep(2)
        
        # Criar schema
        if not self.create_schema():
            return False
        
        # Configurar schema padrão
        self.set_default_schema()
        # Testar conexão
        if not self.test_connection():
            return False
        
        print("\n" + "=" * 50)
        print("✅ CONFIGURAÇÃO CONCLUÍDA COM SUCESSO!")
        print("=" * 50)
        print(f"🔗 String de conexão:")
        print(f"   {self.db_conn_str}")
        print(f"\n📝 Para usar em suas aplicações:")
        print(f"   DATABASE_URL=mssql+pyodbc://{self.username}:{self.password}@{self.server}:{self.port}/{self.database}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes")
        
        return True

def main():
    """Função principal"""
    print("🐋 SQL Server Docker Setup Script")
    print("=" * 50)
    
    # Verificar se as dependências estão instaladas
    try:
        import pyodbc
    except ImportError:
        print("❌ pyodbc não está instalado!")
        print("💡 Execute: pip install pyodbc")
        sys.exit(1)
    
    # Executar configuração
    setup = SQLServerSetup()
    
    if setup.setup():
        print("\n🎉 Setup concluído! Seu banco está pronto para uso.")
        sys.exit(0)
    else:
        print("\n💥 Falha na configuração!")
        sys.exit(1)

if __name__ == "__main__":
    main()