#!/usr/bin/env python3
"""
Faker data generator for delivery system
Creates 200,000 records logically distributed across tables
"""

import os
import pyodbc
import random
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import time

# Simulating Faker with realistic Brazilian data
class BrazilianFaker:
    def __init__(self):
        # Common Brazilian names
        self.first_names = [
            'João', 'Maria', 'José', 'Ana', 'Carlos', 'Francisca', 'Paulo', 'Antônia',
            'Pedro', 'Luiza', 'Manoel', 'Lúcia', 'Francisco', 'Helena', 'Ricardo',
            'Sandra', 'Fernando', 'Carla', 'Roberto', 'Mariana', 'Marcos', 'Juliana',
            'Antonio', 'Patricia', 'Rafael', 'Adriana', 'Daniel', 'Cristina', 'Eduardo',
            'Fernanda', 'Gabriel', 'Camila', 'Lucas', 'Beatriz', 'Bruno', 'Larissa',
            'Diego', 'Tatiana', 'Rodrigo', 'Renata', 'Felipe', 'Vanessa', 'Gustavo',
            'Priscila', 'Leonardo', 'Amanda', 'Thiago', 'Gabriela', 'André', 'Débora'
        ]
        
        self.last_names = [
            'Silva', 'Santos', 'Oliveira', 'Souza', 'Rodrigues', 'Ferreira', 'Alves',
            'Pereira', 'Lima', 'Gomes', 'Costa', 'Ribeiro', 'Martins', 'Carvalho',
            'Almeida', 'Lopes', 'Soares', 'Fernandes', 'Vieira', 'Barbosa', 'Rocha',
            'Dias', 'Monteiro', 'Cardoso', 'Reis', 'Araújo', 'Nascimento', 'Freitas',
            'Nunes', 'Moreira', 'Correia', 'Castro', 'Pinto', 'Teixeira', 'Ramos'
        ]
        
        # Brazilian cities
        self.cities = [
            'São Paulo', 'Rio de Janeiro', 'Brasília', 'Salvador', 'Fortaleza',
            'Belo Horizonte', 'Manaus', 'Curitiba', 'Recife', 'Porto Alegre',
            'Belém', 'Goiânia', 'Guarulhos', 'Campinas', 'São Luís', 'Maceió',
            'Duque de Caxias', 'Natal', 'Teresina', 'São Bernardo do Campo',
            'Nova Iguaçu', 'João Pessoa', 'Santo André', 'Osasco', 'Jaboatão',
            'Contagem', 'São José dos Campos', 'Ribeirão Preto', 'Sorocaba',
            'Uberlândia', 'Cuiabá', 'Aracaju', 'Feira de Santana', 'Joinville',
            'Florianópolis', 'Vitória', 'Blumenau', 'Londrina', 'Maringá',
            'Caxias do Sul', 'Pelotas', 'Canoas', 'São Vicente', 'Franca'
        ]
        
        # Brazilian states
        self.states = [
            'SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'GO', 'PE', 'CE',
            'PB', 'ES', 'RN', 'AL', 'MT', 'MS', 'DF', 'SE', 'AM', 'RO',
            'AC', 'AP', 'RR', 'PA', 'TO', 'MA', 'PI'
        ]
        
        # Brazilian vehicle models
        self.vehicle_models = [
            'Sprinter', 'Daily', 'Ducato', 'Master', 'HR', 'Bongo', 'Accelo',
            'Atego', 'Cargo', 'Constellation', 'Axor', 'Actros', 'FH',
            'VM', 'Meteor', 'Worker', 'Delivery', 'Volksbus', 'Agrale',
            'Ford Cargo', 'Volvo FH', 'Scania R', 'Mercedes Axor'
        ]
        
        self.vehicle_brands = [
            'Mercedes-Benz', 'Iveco', 'Volkswagen', 'Ford', 'Volvo',
            'Scania', 'Hyundai', 'Agrale', 'MAN'
        ]
        
        # Cargo types
        self.cargo_types = [
            'Electronics', 'Clothing', 'Food', 'Medicines', 'Furniture',
            'Cosmetics', 'Books', 'Beverages', 'Cleaning Products', 'Toys',
            'Tools', 'Auto Parts', 'Construction Materials', 'Fabrics',
            'Chemical Products', 'Equipment', 'Documents', 'Footwear'
        ]

    def name(self) -> str:
        return f"{random.choice(self.first_names)} {random.choice(self.last_names)}"
    
    def company_name(self) -> str:
        suffixes = ['Ltda', 'S/A', 'ME', 'EPP', 'EIRELI']
        business_types = ['Commerce', 'Industry', 'Services', 'Distributor', 'Wholesale']
        return f"{random.choice(self.last_names)} {random.choice(business_types)} {random.choice(suffixes)}"
    
    def cpf(self) -> str:
        return f"{random.randint(100, 999)}.{random.randint(100, 999)}.{random.randint(100, 999)}-{random.randint(10, 99)}"
    
    def cnpj(self) -> str:
        return f"{random.randint(10, 99)}.{random.randint(100, 999)}.{random.randint(100, 999)}/0001-{random.randint(10, 99)}"
    
    def email(self, name: str) -> str:
        domains = ['gmail.com', 'hotmail.com', 'yahoo.com.br', 'outlook.com', 'empresa.com.br']
        clean_name = name.lower().replace(' ', '.').replace('ã', 'a').replace('ç', 'c')
        return f"{clean_name}@{random.choice(domains)}"
    
    def phone(self) -> str:
        return f"({random.randint(11, 99)}) {random.randint(90000, 99999)}-{random.randint(1000, 9999)}"
    
    def address(self) -> str:
        street_types = ['Street', 'Avenue', 'Square', 'Alley', 'Lane']
        return f"{random.choice(street_types)} {random.choice(self.last_names)}, {random.randint(1, 9999)}"
    
    def city(self) -> str:
        return random.choice(self.cities)
    
    def state(self) -> str:
        return random.choice(self.states)
    
    def zip_code(self) -> str:
        return f"{random.randint(10000, 99999)}-{random.randint(100, 999)}"
    
    def vehicle_plate(self) -> str:
        # Old Brazilian and Mercosur plates
        if random.choice([True, False]):
            # Old plate
            return f"{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}-{random.randint(1000, 9999)}"
        else:
            # Mercosur plate
            return f"{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{random.randint(1, 9)}{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{random.randint(10, 99)}"
    
    def cnh(self) -> str:
        return f"{random.randint(10000000000, 99999999999)}"
    
    def date_between(self, start_date: datetime, end_date: datetime) -> datetime:
        time_between = end_date - start_date
        days_between = time_between.days
        random_days = random.randint(0, days_between)
        return start_date + timedelta(days=random_days)

class DataGenerator:
    def __init__(self):
        """Initialize data generator"""
        self.server = os.getenv('DB_SERVER', 'localhost')
        self.port = os.getenv('DB_PORT', '1433')
        self.username = os.getenv('DB_USERNAME', 'sa')
        self.password = os.getenv('DB_PASSWORD', 'satc@2025')
        self.database = os.getenv('DB_DATABASE', 'etl_entregas_db')
        self.schema = os.getenv('DB_SCHEMA', 'entregas')
        
        self.db_conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.server},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"TrustServerCertificate=yes;"
        )
        
        self.faker = BrazilianFaker()
        
        # Data distribution configuration (total: 200,000)
        self.table_sizes = {
            'Clientes': 5000,           # 5,000 customers
            'Motoristas': 800,          # 800 drivers  
            'Veiculos': 1200,           # 1,200 vehicles
            'Tipos_Carga': 50,          # 50 cargo types
            'Rotas': 150,               # 150 routes
            'Entregas': 120000,         # 120,000 deliveries (main)
            'Coletas': 50000,           # 50,000 pickups (not every delivery has pickup)
            'Manutencoes_Veiculo': 15000, # 15,000 maintenance records
            'Abastecimentos': 7000,     # 7,000 fuel records  
            'Multas_Transito': 800      # 800 traffic tickets
        }
        
        # Generated IDs for relationships
        self.generated_ids = {}

    def clear_tables(self) -> bool:
        """Clear all tables in correct order"""
        try:
            print("🗑️  Clearing existing data...")
            
            tables_order = [
                "Multas_Transito", "Abastecimentos", "Manutencoes_Veiculo",
                "Coletas", "Entregas", "Rotas", "Tipos_Carga", 
                "Veiculos", "Motoristas", "Clientes"
            ]
            
            with pyodbc.connect(self.db_conn_str) as conn:
                conn.autocommit = True
                cursor = conn.cursor()
                
                for table in tables_order:
                    cursor.execute(f"DELETE FROM [{self.schema}].[{table}]")
                    cursor.execute(f"DBCC CHECKIDENT('[{self.schema}].[{table}]', RESEED, 0)")
                    print(f"🗑️  Table {table} cleared")
            
            print("✅ Cleanup completed!")
            return True
            
        except Exception as e:
            print(f"❌ Error during cleanup: {e}")
            return False

    def generate_clientes(self) -> bool:
        """Generate data for Clientes table"""
        try:
            print(f"👥 Generating {self.table_sizes['Clientes']} customers...")
            
            with pyodbc.connect(self.db_conn_str) as conn:
                cursor = conn.cursor()
                
                for i in range(self.table_sizes['Clientes']):
                    tipo = random.choice(['Pessoa Física', 'Pessoa Jurídica'])
                    
                    if tipo == 'Pessoa Física':
                        nome = self.faker.name()
                        cpf_cnpj = self.faker.cpf()
                    else:
                        nome = self.faker.company_name()
                        cpf_cnpj = self.faker.cnpj()
                    
                    email = self.faker.email(nome)
                    telefone = self.faker.phone()
                    endereco = self.faker.address()
                    cidade = self.faker.city()
                    estado = self.faker.state()
                    cep = self.faker.zip_code()
                    
                    cursor.execute(f"""
                        INSERT INTO [{self.schema}].[Clientes] 
                        (nome_cliente, tipo_cliente, cpf_cnpj, email, telefone, endereco, cidade, estado, cep)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (nome, tipo, cpf_cnpj, email, telefone, endereco, cidade, estado, cep))
                    
                    if (i + 1) % 1000 == 0:
                        conn.commit()
                        print(f"   📝 {i + 1} customers inserted...")
                
                conn.commit()
                
                # Get generated IDs
                cursor.execute(f"SELECT id_cliente FROM [{self.schema}].[Clientes]")
                self.generated_ids['clientes'] = [row[0] for row in cursor.fetchall()]
                
            print(f"✅ {self.table_sizes['Clientes']} customers created!")
            return True
            
        except Exception as e:
            print(f"❌ Error generating customers: {e}")
            return False

    def generate_motoristas(self) -> bool:
        """Generate data for Motoristas table"""
        try:
            print(f"🚗 Generating {self.table_sizes['Motoristas']} drivers...")
            
            with pyodbc.connect(self.db_conn_str) as conn:
                cursor = conn.cursor()
                
                for i in range(self.table_sizes['Motoristas']):
                    nome = self.faker.name()
                    cpf = self.faker.cpf()
                    cnh = self.faker.cnh()
                    data_nascimento = self.faker.date_between(
                        datetime(1960, 1, 1), 
                        datetime(2000, 12, 31)
                    )
                    telefone = self.faker.phone()
                    email = self.faker.email(nome)
                    status_ativo = random.choice([1, 1, 1, 0])  # 75% active
                    data_contratacao = self.faker.date_between(
                        datetime(2015, 1, 1),
                        datetime.now()
                    )
                    
                    cursor.execute(f"""
                        INSERT INTO [{self.schema}].[Motoristas] 
                        (nome_motorista, cpf, numero_cnh, data_nascimento, telefone, email, status_ativo, data_contratacao)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (nome, cpf, cnh, data_nascimento, telefone, email, status_ativo, data_contratacao))
                
                conn.commit()
                
                # Get generated IDs
                cursor.execute(f"SELECT id_motorista FROM [{self.schema}].[Motoristas]")
                self.generated_ids['motoristas'] = [row[0] for row in cursor.fetchall()]
                
            print(f"✅ {self.table_sizes['Motoristas']} drivers created!")
            return True
            
        except Exception as e:
            print(f"❌ Error generating drivers: {e}")
            return False

    def generate_veiculos(self) -> bool:
        """Generate data for Veiculos table"""
        try:
            print(f"🚛 Generating {self.table_sizes['Veiculos']} vehicles...")
            
            with pyodbc.connect(self.db_conn_str) as conn:
                cursor = conn.cursor()
                
                for i in range(self.table_sizes['Veiculos']):
                    placa = self.faker.vehicle_plate()
                    modelo = random.choice(self.faker.vehicle_models)
                    marca = random.choice(self.faker.vehicle_brands)
                    ano = random.randint(2010, 2024)
                    
                    tipo_veiculo = random.choice(['Caminhão', 'Van', 'Utilitário', 'Carro'])
                    
                    # Capacity based on type
                    if tipo_veiculo == 'Caminhão':
                        capacidade = random.uniform(5000, 25000)
                    elif tipo_veiculo == 'Van':
                        capacidade = random.uniform(800, 3000)
                    elif tipo_veiculo == 'Utilitário':
                        capacidade = random.uniform(500, 1500)
                    else:  # Carro
                        capacidade = random.uniform(200, 600)
                    
                    status = random.choice(['Disponível', 'Em Viagem', 'Em Manutenção', 'Inativo'])
                    
                    cursor.execute(f"""
                        INSERT INTO [{self.schema}].[Veiculos] 
                        (placa, modelo, marca, ano_fabricacao, capacidade_carga_kg, tipo_veiculo, status_operacional)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (placa, modelo, marca, ano, capacidade, tipo_veiculo, status))
                
                conn.commit()
                
                # Get generated IDs
                cursor.execute(f"SELECT id_veiculo FROM [{self.schema}].[Veiculos]")
                self.generated_ids['veiculos'] = [row[0] for row in cursor.fetchall()]
                
            print(f"✅ {self.table_sizes['Veiculos']} vehicles created!")
            return True
            
        except Exception as e:
            print(f"❌ Error generating vehicles: {e}")
            return False

    def generate_tipos_carga(self) -> bool:
        """Generate data for Tipos_Carga table"""
        try:
            print(f"📦 Generating {self.table_sizes['Tipos_Carga']} cargo types...")
            
            with pyodbc.connect(self.db_conn_str) as conn:
                cursor = conn.cursor()
                
                for i, tipo in enumerate(self.faker.cargo_types[:self.table_sizes['Tipos_Carga']]):
                    descricao = f"Specialized transport of {tipo.lower()}"
                    refrigeracao = 1 if tipo in ['Food', 'Medicines', 'Beverages'] else 0
                    peso_medio = random.uniform(10, 1000)
                    
                    cursor.execute(f"""
                        INSERT INTO [{self.schema}].[Tipos_Carga] 
                        (nome_tipo, descricao_tipo, requer_refrigeracao, peso_medio_kg)
                        VALUES (?, ?, ?, ?)
                    """, (tipo, descricao, refrigeracao, peso_medio))
                
                conn.commit()
                
                # Get generated IDs
                cursor.execute(f"SELECT id_tipo_carga FROM [{self.schema}].[Tipos_Carga]")
                self.generated_ids['tipos_carga'] = [row[0] for row in cursor.fetchall()]
                
            print(f"✅ {self.table_sizes['Tipos_Carga']} cargo types created!")
            return True
            
        except Exception as e:
            print(f"❌ Error generating cargo types: {e}")
            return False

    def generate_rotas(self) -> bool:
        """Generate data for Rotas table"""
        try:
            print(f"🗺️  Generating {self.table_sizes['Rotas']} routes...")
            
            with pyodbc.connect(self.db_conn_str) as conn:
                cursor = conn.cursor()
                
                for i in range(self.table_sizes['Rotas']):
                    origem = self.faker.city()
                    destino = self.faker.city()
                    
                    # Avoid origin equal to destination
                    while destino == origem:
                        destino = self.faker.city()
                    
                    nome_rota = f"{origem} → {destino}"
                    distancia = random.uniform(50, 2000)  # km
                    tempo_estimado = distancia / random.uniform(60, 80)  # hours
                    
                    cursor.execute(f"""
                        INSERT INTO [{self.schema}].[Rotas] 
                        (nome_rota, origem, destino, distancia_km, tempo_estimado_horas)
                        VALUES (?, ?, ?, ?, ?)
                    """, (nome_rota, origem, destino, distancia, tempo_estimado))
                
                conn.commit()
                
                # Get generated IDs
                cursor.execute(f"SELECT id_rota FROM [{self.schema}].[Rotas]")
                self.generated_ids['rotas'] = [row[0] for row in cursor.fetchall()]
                
            print(f"✅ {self.table_sizes['Rotas']} routes created!")
            return True
            
        except Exception as e:
            print(f"❌ Error generating routes: {e}")
            return False

    def generate_entregas(self) -> bool:
        """Generate data for Entregas table (main table)"""
        try:
            print(f"🚚 Generating {self.table_sizes['Entregas']} deliveries...")
            
            batch_size = 5000
            total_entregas = self.table_sizes['Entregas']
            
            with pyodbc.connect(self.db_conn_str) as conn:
                cursor = conn.cursor()
                
                for batch_start in range(0, total_entregas, batch_size):
                    batch_end = min(batch_start + batch_size, total_entregas)
                    
                    for i in range(batch_start, batch_end):
                        id_veiculo = random.choice(self.generated_ids['veiculos'])
                        id_motorista = random.choice(self.generated_ids['motoristas'])
                        id_cliente_remetente = random.choice(self.generated_ids['clientes'])
                        id_cliente_destinatario = random.choice(self.generated_ids['clientes'])
                        
                        # Avoid same customer as sender and recipient
                        while id_cliente_destinatario == id_cliente_remetente:
                            id_cliente_destinatario = random.choice(self.generated_ids['clientes'])
                        
                        id_rota = random.choice(self.generated_ids['rotas']) if random.random() > 0.1 else None
                        id_tipo_carga = random.choice(self.generated_ids['tipos_carga'])
                        
                        # Dates
                        data_inicio = self.faker.date_between(
                            datetime(2023, 1, 1),
                            datetime.now()
                        )
                        
                        data_previsao_fim = data_inicio + timedelta(hours=random.randint(2, 48))
                        
                        # Status and real end date
                        status_options = ['Agendada', 'Em Trânsito', 'Entregue', 'Atrasada', 'Cancelada', 'Problema']
                        weights = [10, 15, 60, 10, 3, 2]  # Weights for realistic distribution
                        status = random.choices(status_options, weights=weights)[0]
                        
                        if status == 'Entregue':
                            # 70% delivered on time, 30% with delay
                            if random.random() < 0.7:
                                data_fim_real = data_previsao_fim - timedelta(hours=random.randint(0, 4))
                            else:
                                data_fim_real = data_previsao_fim + timedelta(hours=random.randint(1, 24))
                        elif status in ['Agendada', 'Em Trânsito']:
                            data_fim_real = None
                        else:
                            data_fim_real = data_previsao_fim + timedelta(hours=random.randint(1, 12))
                        
                        valor_frete = random.uniform(50, 2000)
                        peso_carga = random.uniform(10, 5000)
                        
                        cursor.execute(f"""
                            INSERT INTO [{self.schema}].[Entregas] 
                            (id_veiculo, id_motorista, id_cliente_remetente, id_cliente_destinatario, 
                             id_rota, id_tipo_carga, data_inicio_entrega, data_previsao_fim_entrega, 
                             data_fim_real_entrega, status_entrega, valor_frete, peso_carga_kg)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (id_veiculo, id_motorista, id_cliente_remetente, id_cliente_destinatario,
                              id_rota, id_tipo_carga, data_inicio, data_previsao_fim, data_fim_real,
                              status, valor_frete, peso_carga))
                    
                    conn.commit()
                    print(f"   📝 {batch_end} deliveries inserted...")
                
                # Get generated IDs
                cursor.execute(f"SELECT id_entrega FROM [{self.schema}].[Entregas]")
                self.generated_ids['entregas'] = [row[0] for row in cursor.fetchall()]
                
            print(f"✅ {self.table_sizes['Entregas']} deliveries created!")
            return True
            
        except Exception as e:
            print(f"❌ Error generating deliveries: {e}")
            return False

    def generate_coletas(self) -> bool:
        """Generate data for Coletas table"""
        try:
            print(f"📋 Generating {self.table_sizes['Coletas']} pickups...")
            
            # Select random deliveries for pickups
            entregas_selecionadas = random.sample(
                self.generated_ids['entregas'], 
                self.table_sizes['Coletas']
            )
            
            with pyodbc.connect(self.db_conn_str) as conn:
                cursor = conn.cursor()
                
                for i, id_entrega in enumerate(entregas_selecionadas):
                    data_hora_coleta = self.faker.date_between(
                        datetime(2023, 1, 1),
                        datetime.now()
                    )
                    
                    endereco_coleta = self.faker.address()
                    status_coleta = random.choices(
                        ['Agendada', 'Realizada', 'Cancelada', 'Problema'],
                        weights=[15, 70, 10, 5]
                    )[0]
                    
                    observacoes = random.choice([
                        None, 'Pickup completed without issues',
                        'Customer absent on first contact',
                        'Product checked and in perfect condition',
                        'Difficult access address',
                        'Pickup rescheduled at customer request'
                    ])
                    
                    cursor.execute(f"""
                        INSERT INTO [{self.schema}].[Coletas] 
                        (id_entrega, data_hora_coleta, endereco_coleta, status_coleta, observacoes)
                        VALUES (?, ?, ?, ?, ?)
                    """, (id_entrega, data_hora_coleta, endereco_coleta, status_coleta, observacoes))
                    
                    if (i + 1) % 5000 == 0:
                        conn.commit()
                        print(f"   📝 {i + 1} pickups inserted...")
                
                conn.commit()
                
            print(f"✅ {self.table_sizes['Coletas']} pickups created!")
            return True
            
        except Exception as e:
            print(f"❌ Error generating pickups: {e}")
            return False

    def generate_manutencoes_veiculo(self) -> bool:
        """Generate data for Manutencoes_Veiculo table"""
        try:
            print(f"🔧 Generating {self.table_sizes['Manutencoes_Veiculo']} maintenance records...")
            
            with pyodbc.connect(self.db_conn_str) as conn:
                cursor = conn.cursor()
                
                for i in range(self.table_sizes['Manutencoes_Veiculo']):
                    id_veiculo = random.choice(self.generated_ids['veiculos'])
                    data_manutencao = self.faker.date_between(
                        datetime(2023, 1, 1),
                        datetime.now()
                    )
                    
                    tipo_manutencao = random.choices(
                        ['Preventiva', 'Corretiva', 'Preditiva'],
                        weights=[60, 30, 10]
                    )[0]
                    
                    servicos = [
                        'Oil and filter change', 'Brake inspection', 'Alignment and balancing',
                        'Tire replacement', 'Engine maintenance', 'Suspension inspection',
                        'Battery replacement', 'Electrical system maintenance', 'Clutch inspection',
                        'Air conditioning maintenance', 'Belt replacement', 'Steering inspection'
                    ]
                    
                    descricao_servico = random.choice(servicos)
                    custo_manutencao = random.uniform(150, 5000)
                    tempo_parado = random.uniform(2, 48)
                    
                    cursor.execute(f"""
                        INSERT INTO [{self.schema}].[Manutencoes_Veiculo] 
                        (id_veiculo, data_manutencao, tipo_manutencao, descricao_servico, custo_manutencao, tempo_parado_horas)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (id_veiculo, data_manutencao, tipo_manutencao, descricao_servico, custo_manutencao, tempo_parado))
                    
                    if (i + 1) % 2000 == 0:
                        conn.commit()
                        print(f"   📝 {i + 1} maintenance records inserted...")
                
                conn.commit()
                
            print(f"✅ {self.table_sizes['Manutencoes_Veiculo']} maintenance records created!")
            return True
            
        except Exception as e:
            print(f"❌ Error generating maintenance records: {e}")
            return False

    def generate_abastecimentos(self) -> bool:
        """Generate data for Abastecimentos table"""
        try:
            print(f"⛽ Generating {self.table_sizes['Abastecimentos']} fuel records...")
            
            with pyodbc.connect(self.db_conn_str) as conn:
                cursor = conn.cursor()
                
                for i in range(self.table_sizes['Abastecimentos']):
                    id_veiculo = random.choice(self.generated_ids['veiculos'])
                    data_abastecimento = self.faker.date_between(
                        datetime(2023, 1, 1),
                        datetime.now()
                    )
                    
                    # Liters based on vehicle type (simulation)
                    litros = random.uniform(30, 200)
                    
                    # Realistic price per liter for Brazil
                    preco_por_litro = random.uniform(4.50, 6.50)
                    valor_total = litros * preco_por_litro
                    
                    tipo_combustivel = random.choices(
                        ['Diesel', 'Gasolina', 'Etanol', 'GNV'],
                        weights=[70, 20, 8, 2]
                    )[0]
                    
                    cursor.execute(f"""
                        INSERT INTO [{self.schema}].[Abastecimentos] 
                        (id_veiculo, data_abastecimento, litros, valor_total, tipo_combustivel)
                        VALUES (?, ?, ?, ?, ?)
                    """, (id_veiculo, data_abastecimento, litros, valor_total, tipo_combustivel))
                    
                    if (i + 1) % 1000 == 0:
                        conn.commit()
                        print(f"   📝 {i + 1} fuel records inserted...")
                
                conn.commit()
                
            print(f"✅ {self.table_sizes['Abastecimentos']} fuel records created!")
            return True
            
        except Exception as e:
            print(f"❌ Error generating fuel records: {e}")
            return False

    def generate_multas_transito(self) -> bool:
        """Generate data for Multas_Transito table"""
        try:
            print(f"🚨 Generating {self.table_sizes['Multas_Transito']} traffic tickets...")
            
            with pyodbc.connect(self.db_conn_str) as conn:
                cursor = conn.cursor()
                
                infracoes = [
                    'Speeding', 'Illegal parking', 'Running red light',
                    'Using cell phone while driving', 'Not wearing seat belt', 'Illegal overtaking',
                    'Stopping on pedestrian crossing', 'Prohibited turn', 'Driving without license',
                    'Irregular cargo transport', 'Vehicle with expired documentation'
                ]
                
                for i in range(self.table_sizes['Multas_Transito']):
                    id_veiculo = random.choice(self.generated_ids['veiculos'])
                    id_motorista = random.choice(self.generated_ids['motoristas']) if random.random() > 0.1 else None
                    
                    data_multa = self.faker.date_between(
                        datetime(2023, 1, 1),
                        datetime.now()
                    )
                    
                    local_multa = f"{self.faker.address()}, {self.faker.city()}"
                    descricao_infracao = random.choice(infracoes)
                    
                    # Realistic ticket values in Brazil
                    if descricao_infracao in ['Speeding', 'Running red light']:
                        valor_multa = random.uniform(130, 880)
                    elif descricao_infracao in ['Using cell phone while driving', 'Driving without license']:
                        valor_multa = random.uniform(260, 1500)
                    else:
                        valor_multa = random.uniform(88, 400)
                    
                    status_pagamento = random.choices(
                        ['Pendente', 'Paga', 'Recorrida'],
                        weights=[30, 60, 10]
                    )[0]
                    
                    cursor.execute(f"""
                        INSERT INTO [{self.schema}].[Multas_Transito] 
                        (id_veiculo, id_motorista, data_multa, local_multa, descricao_infracao, valor_multa, status_pagamento)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (id_veiculo, id_motorista, data_multa, local_multa, descricao_infracao, valor_multa, status_pagamento))
                
                conn.commit()
                
            print(f"✅ {self.table_sizes['Multas_Transito']} traffic tickets created!")
            return True
            
        except Exception as e:
            print(f"❌ Error generating traffic tickets: {e}")
            return False

    def show_statistics(self) -> bool:
        """Show statistics of generated data"""
        try:
            print("\n" + "=" * 60)
            print("📊 GENERATED DATA STATISTICS")
            print("=" * 60)
            
            with pyodbc.connect(self.db_conn_str) as conn:
                cursor = conn.cursor()
                
                total_records = 0
                
                for table_name, expected_count in self.table_sizes.items():
                    cursor.execute(f"SELECT COUNT(*) FROM [{self.schema}].[{table_name}]")
                    actual_count = cursor.fetchone()[0]
                    total_records += actual_count
                    
                    status = "✅" if actual_count == expected_count else "⚠️"
                    print(f"{status} {table_name:<20} {actual_count:>8,} records")
                
                print("-" * 60)
                print(f"📈 TOTAL RECORDS: {total_records:,}")
                print(f"🎯 TARGET (200,000):     200,000")
                print(f"📊 DIFFERENCE:          {total_records - 200000:+,}")
                
                # Additional interesting statistics
                print(f"\n📋 ADDITIONAL STATISTICS:")
                
                # Most active customers
                cursor.execute(f"""
                    SELECT TOP 5 c.nome_cliente, COUNT(e.id_entrega) as total_entregas
                    FROM [{self.schema}].[Clientes] c
                    LEFT JOIN [{self.schema}].[Entregas] e ON c.id_cliente = e.id_cliente_remetente
                    GROUP BY c.id_cliente, c.nome_cliente
                    ORDER BY total_entregas DESC
                """)
                
                print("🏆 Top 5 Sender Customers:")
                for nome, total in cursor.fetchall():
                    print(f"   • {nome[:30]:<30} {total:>4} deliveries")
                
                # Delivery status
                cursor.execute(f"""
                    SELECT status_entrega, COUNT(*) as quantidade
                    FROM [{self.schema}].[Entregas]
                    GROUP BY status_entrega
                    ORDER BY quantidade DESC
                """)
                
                print("\n📦 Delivery Status:")
                for status, qtd in cursor.fetchall():
                    percentual = (qtd / self.table_sizes['Entregas']) * 100
                    print(f"   • {status:<15} {qtd:>6,} ({percentual:>5.1f}%)")
                
                # Total revenue
                cursor.execute(f"SELECT SUM(valor_frete) FROM [{self.schema}].[Entregas]")
                receita_total = cursor.fetchone()[0] or 0
                print(f"\n💰 TOTAL REVENUE: R$ {receita_total:,.2f}")
                
                return True
                
        except Exception as e:
            print(f"❌ Error generating statistics: {e}")
            return False

    def generate_all_data(self, clear_existing: bool = True) -> bool:
        """Generate all system data"""
        start_time = time.time()
        
        print("🎲 FAKER DATA GENERATOR - DELIVERY SYSTEM")
        print("=" * 60)
        print(f"🎯 Target: 200,000 records distributed across 10 tables")
        print(f"📊 Planned distribution:")
        
        for table, count in self.table_sizes.items():
            print(f"   • {table:<20} {count:>8,} records")
        
        print("-" * 60)
        
        if clear_existing and not self.clear_tables():
            return False
        
        print()
        
        # Generation sequence (respecting FK dependencies)
        generators = [
            ('Customers', self.generate_clientes),
            ('Drivers', self.generate_motoristas),
            ('Vehicles', self.generate_veiculos),
            ('Cargo Types', self.generate_tipos_carga),
            ('Routes', self.generate_rotas),
            ('Deliveries', self.generate_entregas),
            ('Pickups', self.generate_coletas),
            ('Maintenance', self.generate_manutencoes_veiculo),
            ('Fuel Records', self.generate_abastecimentos),
            ('Traffic Tickets', self.generate_multas_transito),
        ]
        
        success_count = 0
        for description, generator_func in generators:
            print(f"\n🔄 Processing {description}...")
            if generator_func():
                success_count += 1
                print(f"✅ {description} completed!")
            else:
                print(f"❌ Failed in {description}!")
                return False
        
        # Show final statistics
        self.show_statistics()
        
        elapsed_time = time.time() - start_time
        print(f"\n⏱️  TOTAL TIME: {elapsed_time:.1f} seconds")
        print(f"🎉 GENERATION COMPLETED SUCCESSFULLY!")
        print(f"✅ {success_count}/{len(generators)} tables processed")
        
        return True

def main():
    """Main function"""
    print("🎲 Faker Data Generator - Delivery System")
    print("=" * 60)
    
    # Check arguments
    skip_clear = "--no-clear" in sys.argv
    
    if not skip_clear:
        print("⚠️  This script will CLEAR all existing data!")
        response = input("Type 'yes' to continue: ")
        if response.lower() != 'yes':
            print("❌ Operation cancelled")
            sys.exit(0)
    
    # Check dependencies
    try:
        import pyodbc
    except ImportError:
        print("❌ pyodbc is not installed!")
        print("💡 Run: pip install pyodbc")
        sys.exit(1)
    
    # Load environment variables
    if os.path.exists('.env'):
        print("📋 Loading environment variables...")
        with open('.env', 'r') as f:
            for line in f:
                if '=' in line and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value
    
    # Generate data
    generator = DataGenerator()
    
    if generator.generate_all_data(clear_existing=not skip_clear):
        print("\n🎊 PROCESS COMPLETED!")
        print("💡 Your data is ready for analysis and development!")
        sys.exit(0)
    else:
        print("\n💥 GENERATION FAILED!")
        sys.exit(1)

if __name__ == "__main__":
    main()