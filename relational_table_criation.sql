-- Tabela 1: Clientes (Remetentes/Destinatários)
CREATE TABLE Clientes (
    id_cliente INT PRIMARY KEY AUTO_INCREMENT,
    nome_cliente VARCHAR(255) NOT NULL,
    tipo_cliente VARCHAR(50) CHECK (tipo_cliente IN ('Pessoa Física', 'Pessoa Jurídica')),
    cpf_cnpj VARCHAR(18) UNIQUE, -- CPF (14 chars: XXX.XXX.XXX-XX) ou CNPJ (18 chars: XX.XXX.XXX/XXXX-XX)
    email VARCHAR(100),
    telefone VARCHAR(20),
    endereco VARCHAR(255),
    cidade VARCHAR(100),
    estado VARCHAR(50),
    cep VARCHAR(10),
    data_cadastro DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Tabela 2: Motoristas
CREATE TABLE Motoristas (
    id_motorista INT PRIMARY KEY AUTO_INCREMENT,
    nome_motorista VARCHAR(255) NOT NULL,
    cpf VARCHAR(14) UNIQUE NOT NULL,
    numero_cnh VARCHAR(20) UNIQUE NOT NULL,
    data_nascimento DATE,
    telefone VARCHAR(20),
    email VARCHAR(100),
    status_ativo BOOLEAN DEFAULT TRUE,
    data_contratacao DATE
);

-- Tabela 3: Veiculos
CREATE TABLE Veiculos (
    id_veiculo INT PRIMARY KEY AUTO_INCREMENT,
    placa VARCHAR(10) UNIQUE NOT NULL,
    modelo VARCHAR(100) NOT NULL,
    marca VARCHAR(100),
    ano_fabricacao INT,
    capacidade_carga_kg DECIMAL(10, 2),
    tipo_veiculo VARCHAR(50) CHECK (tipo_veiculo IN ('Caminhão', 'Van', 'Utilitário', 'Carro')),
    status_operacional VARCHAR(50) CHECK (status_operacional IN ('Disponível', 'Em Viagem', 'Em Manutenção', 'Inativo'))
);

-- Tabela 4: Tipos_Carga
CREATE TABLE Tipos_Carga (
    id_tipo_carga INT PRIMARY KEY AUTO_INCREMENT,
    nome_tipo VARCHAR(100) NOT NULL UNIQUE,
    descricao_tipo TEXT,
    requer_refrigeracao BOOLEAN DEFAULT FALSE,
    peso_medio_kg DECIMAL(10, 2)
);

-- Tabela 5: Rotas
CREATE TABLE Rotas (
    id_rota INT PRIMARY KEY AUTO_INCREMENT,
    nome_rota VARCHAR(255) NOT NULL,
    origem VARCHAR(255) NOT NULL,
    destino VARCHAR(255) NOT NULL,
    distancia_km DECIMAL(10, 2),
    tempo_estimado_horas DECIMAL(5, 2)
);

-- Tabela 6: Entregas (Tabela Principal)
CREATE TABLE Entregas (
    id_entrega INT PRIMARY KEY AUTO_INCREMENT,
    id_veiculo INT NOT NULL,
    id_motorista INT NOT NULL,
    id_cliente_remetente INT NOT NULL,
    id_cliente_destinatario INT NOT NULL,
    id_rota INT, -- Nulo se não houver rota pré-definida
    id_tipo_carga INT NOT NULL,
    data_inicio_entrega DATETIME NOT NULL,
    data_previsao_fim_entrega DATETIME,
    data_fim_real_entrega DATETIME,
    status_entrega VARCHAR(50) NOT NULL CHECK (status_entrega IN ('Agendada', 'Em Trânsito', 'Entregue', 'Atrasada', 'Cancelada', 'Problema')),
    valor_frete DECIMAL(10, 2) NOT NULL,
    peso_carga_kg DECIMAL(10, 2),
    FOREIGN KEY (id_veiculo) REFERENCES Veiculos(id_veiculo),
    FOREIGN KEY (id_motorista) REFERENCES Motoristas(id_motorista),
    FOREIGN KEY (id_cliente_remetente) REFERENCES Clientes(id_cliente),
    FOREIGN KEY (id_cliente_destinatario) REFERENCES Clientes(id_cliente),
    FOREIGN KEY (id_rota) REFERENCES Rotas(id_rota),
    FOREIGN KEY (id_tipo_carga) REFERENCES Tipos_Carga(id_tipo_carga)
);

-- Tabela 7: Coletas (Onde a carga é retirada)
CREATE TABLE Coletas (
    id_coleta INT PRIMARY KEY AUTO_INCREMENT,
    id_entrega INT NOT NULL UNIQUE, -- Uma coleta por entrega
    data_hora_coleta DATETIME NOT NULL,
    endereco_coleta VARCHAR(255),
    status_coleta VARCHAR(50) CHECK (status_coleta IN ('Agendada', 'Realizada', 'Cancelada', 'Problema')),
    observacoes TEXT,
    FOREIGN KEY (id_entrega) REFERENCES Entregas(id_entrega)
);

-- Tabela 8: Manutencoes_Veiculo
CREATE TABLE Manutencoes_Veiculo (
    id_manutencao INT PRIMARY KEY AUTO_INCREMENT,
    id_veiculo INT NOT NULL,
    data_manutencao DATETIME NOT NULL,
    tipo_manutencao VARCHAR(50) NOT NULL CHECK (tipo_manutencao IN ('Preventiva', 'Corretiva', 'Preditiva')),
    descricao_servico TEXT,
    custo_manutencao DECIMAL(10, 2) NOT NULL,
    tempo_parado_horas DECIMAL(5, 2),
    FOREIGN KEY (id_veiculo) REFERENCES Veiculos(id_veiculo)
);

-- Tabela 9: Abastecimentos
CREATE TABLE Abastecimentos (
    id_abastecimento INT PRIMARY KEY AUTO_INCREMENT,
    id_veiculo INT NOT NULL,
    data_abastecimento DATETIME NOT NULL,
    litros DECIMAL(10, 2) NOT NULL,
    valor_total DECIMAL(10, 2) NOT NULL,
    tipo_combustivel VARCHAR(50),
    FOREIGN KEY (id_veiculo) REFERENCES Veiculos(id_veiculo)
);

-- Tabela 10: Multas_Transito
CREATE TABLE Multas_Transito (
    id_multa INT PRIMARY KEY AUTO_INCREMENT,
    id_veiculo INT NOT NULL,
    id_motorista INT, -- Nulo se o motorista não foi identificado
    data_multa DATETIME NOT NULL,
    local_multa VARCHAR(255),
    descricao_infracao TEXT,
    valor_multa DECIMAL(10, 2) NOT NULL,
    status_pagamento VARCHAR(50) CHECK (status_pagamento IN ('Pendente', 'Paga', 'Recorrida')),
    FOREIGN KEY (id_veiculo) REFERENCES Veiculos(id_veiculo),
    FOREIGN KEY (id_motorista) REFERENCES Motoristas(id_motorista)
);