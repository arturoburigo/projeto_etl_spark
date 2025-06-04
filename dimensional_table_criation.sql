-- SQL para criar o Modelo Dimensional (Esquema Estrela)

-- 1. Dimensão de Data (Dim_Data)
-- Esta tabela será populada com datas e seus atributos para análises temporais.
CREATE TABLE Dim_Data (
    data_key INT PRIMARY KEY, -- Chave no formato YYYYMMDD
    data_completa DATE NOT NULL,
    ano INT NOT NULL,
    trimestre INT NOT NULL,
    nome_trimestre VARCHAR(20) NOT NULL,
    mes INT NOT NULL,
    nome_mes VARCHAR(20) NOT NULL,
    dia_do_mes INT NOT NULL,
    dia_da_semana INT NOT NULL,
    nome_dia_da_semana VARCHAR(20) NOT NULL,
    semana_do_ano INT NOT NULL,
    eh_fim_de_semana BOOLEAN NOT NULL,
    eh_feriado BOOLEAN NOT NULL DEFAULT FALSE,
    feriado_nome VARCHAR(100)
);

-- 2. Dimensão de Clientes (Dim_Cliente)
-- Descreve os clientes (remetentes e destinatários).
CREATE TABLE Dim_Cliente (
    id_cliente_key INT PRIMARY KEY AUTO_INCREMENT,
    id_cliente_origem INT UNIQUE, -- Chave do sistema OLTP para rastreamento
    nome_cliente VARCHAR(255) NOT NULL,
    tipo_cliente VARCHAR(50),
    cpf_cnpj VARCHAR(18) UNIQUE,
    email VARCHAR(100),
    telefone VARCHAR(20),
    endereco VARCHAR(255),
    cidade VARCHAR(100),
    estado VARCHAR(50),
    cep VARCHAR(10),
    data_cadastro DATETIME
);

-- 3. Dimensão de Motoristas (Dim_Motorista)
-- Descreve os motoristas.
CREATE TABLE Dim_Motorista (
    id_motorista_key INT PRIMARY KEY AUTO_INCREMENT,
    id_motorista_origem INT UNIQUE, -- Chave do sistema OLTP
    nome_motorista VARCHAR(255) NOT NULL,
    cpf VARCHAR(14) UNIQUE NOT NULL,
    numero_cnh VARCHAR(20) UNIQUE NOT NULL,
    data_nascimento DATE,
    telefone VARCHAR(20),
    email VARCHAR(100),
    status_ativo BOOLEAN,
    data_contratacao DATE
);

-- 4. Dimensão de Veículos (Dim_Veiculo)
-- Descreve os veículos.
CREATE TABLE Dim_Veiculo (
    id_veiculo_key INT PRIMARY KEY AUTO_INCREMENT,
    id_veiculo_origem INT UNIQUE, -- Chave do sistema OLTP
    placa VARCHAR(10) UNIQUE NOT NULL,
    modelo VARCHAR(100) NOT NULL,
    marca VARCHAR(100),
    ano_fabricacao INT,
    capacidade_carga_kg DECIMAL(10, 2),
    tipo_veiculo VARCHAR(50),
    status_operacional VARCHAR(50)
);

-- 5. Dimensão de Tipos de Carga (Dim_Tipo_Carga)
-- Descreve os tipos de carga.
CREATE TABLE Dim_Tipo_Carga (
    id_tipo_carga_key INT PRIMARY KEY AUTO_INCREMENT,
    id_tipo_carga_origem INT UNIQUE, -- Chave do sistema OLTP
    nome_tipo VARCHAR(100) NOT NULL UNIQUE,
    descricao_tipo TEXT,
    requer_refrigeracao BOOLEAN,
    peso_medio_kg DECIMAL(10, 2)
);

-- 6. Dimensão de Rotas (Dim_Rota)
-- Descreve as rotas.
CREATE TABLE Dim_Rota (
    id_rota_key INT PRIMARY KEY AUTO_INCREMENT,
    id_rota_origem INT UNIQUE, -- Chave do sistema OLTP
    nome_rota VARCHAR(255) NOT NULL,
    origem VARCHAR(255) NOT NULL,
    destino VARCHAR(255) NOT NULL,
    distancia_km DECIMAL(10, 2),
    tempo_estimado_horas DECIMAL(5, 2)
);

-- 7. Tabela de Fatos de Entregas (Fato_Entregas)
-- Tabela principal de fatos, representando cada entrega.
CREATE TABLE Fato_Entregas (
    -- Chaves Estrangeiras
    id_veiculo_key INT NOT NULL,
    id_motorista_key INT NOT NULL,
    id_cliente_remetente_key INT NOT NULL,
    id_cliente_destinatario_key INT NOT NULL,
    id_rota_key INT, -- Pode ser nulo se não houver rota pré-definida
    id_tipo_carga_key INT NOT NULL,
    data_inicio_entrega_key INT NOT NULL,
    data_previsao_fim_entrega_key INT,
    data_fim_real_entrega_key INT,

    -- Dimensões Degeneradas
    id_entrega_degenerada INT NOT NULL, -- ID da entrega do sistema OLTP
    status_entrega VARCHAR(50) NOT NULL,

    -- Medidas
    valor_frete DECIMAL(10, 2) NOT NULL,
    peso_carga_kg DECIMAL(10, 2),

    -- Chave Primária Composta
    PRIMARY KEY (id_entrega_degenerada), -- Usando a ID original como PK para unicidade de cada fato

    -- Definição das Chaves Estrangeiras
    FOREIGN KEY (id_veiculo_key) REFERENCES Dim_Veiculo(id_veiculo_key),
    FOREIGN KEY (id_motorista_key) REFERENCES Dim_Motorista(id_motorista_key),
    FOREIGN KEY (id_cliente_remetente_key) REFERENCES Dim_Cliente(id_cliente_key),
    FOREIGN KEY (id_cliente_destinatario_key) REFERENCES Dim_Cliente(id_cliente_key),
    FOREIGN KEY (id_rota_key) REFERENCES Dim_Rota(id_rota_key),
    FOREIGN KEY (id_tipo_carga_key) REFERENCES Dim_Tipo_Carga(id_tipo_carga_key),
    FOREIGN KEY (data_inicio_entrega_key) REFERENCES Dim_Data(data_key),
    FOREIGN KEY (data_previsao_fim_entrega_key) REFERENCES Dim_Data(data_key),
    FOREIGN KEY (data_fim_real_entrega_key) REFERENCES Dim_Data(data_key)
);

-- 8. Tabela de Fatos de Coletas (Fato_Coletas)
-- Representa os eventos de coleta.
CREATE TABLE Fato_Coletas (
    -- Chaves Estrangeiras
    id_entrega_key INT NOT NULL, -- Referencia a entrega associada (pode ser a degenerada da Fato_Entregas)
    data_hora_coleta_key INT NOT NULL,

    -- Dimensões Degeneradas
    id_coleta_degenerada INT NOT NULL, -- ID da coleta do sistema OLTP
    status_coleta VARCHAR(50),
    endereco_coleta VARCHAR(255),
    observacoes TEXT,

    -- Medidas (pode-se derivar uma medida como 'quantidade_coletas' = 1)
    quantidade_coletas INT DEFAULT 1,

    PRIMARY KEY (id_coleta_degenerada),
    FOREIGN KEY (id_entrega_key) REFERENCES Fato_Entregas(id_entrega_degenerada), -- Referencia a PK da Fato_Entregas
    FOREIGN KEY (data_hora_coleta_key) REFERENCES Dim_Data(data_key)
);

-- 9. Tabela de Fatos de Manutenções (Fato_Manutencoes)
-- Representa os eventos de manutenção de veículos.
CREATE TABLE Fato_Manutencoes (
    -- Chaves Estrangeiras
    id_veiculo_key INT NOT NULL,
    data_manutencao_key INT NOT NULL,

    -- Dimensões Degeneradas
    id_manutencao_degenerada INT NOT NULL, -- ID da manutenção do sistema OLTP
    tipo_manutencao VARCHAR(50) NOT NULL,
    descricao_servico TEXT,

    -- Medidas
    custo_manutencao DECIMAL(10, 2) NOT NULL,
    tempo_parado_horas DECIMAL(5, 2),

    PRIMARY KEY (id_manutencao_degenerada),
    FOREIGN KEY (id_veiculo_key) REFERENCES Dim_Veiculo(id_veiculo_key),
    FOREIGN KEY (data_manutencao_key) REFERENCES Dim_Data(data_key)
);

-- 10. Tabela de Fatos de Abastecimentos (Fato_Abastecimentos)
-- Representa os eventos de abastecimento de veículos.
CREATE TABLE Fato_Abastecimentos (
    -- Chaves Estrangeiras
    id_veiculo_key INT NOT NULL,
    data_abastecimento_key INT NOT NULL,

    -- Dimensões Degeneradas
    id_abastecimento_degenerada INT NOT NULL, -- ID do abastecimento do sistema OLTP
    tipo_combustivel VARCHAR(50),

    -- Medidas
    litros DECIMAL(10, 2) NOT NULL,
    valor_total DECIMAL(10, 2) NOT NULL,

    PRIMARY KEY (id_abastecimento_degenerada),
    FOREIGN KEY (id_veiculo_key) REFERENCES Dim_Veiculo(id_veiculo_key),
    FOREIGN KEY (data_abastecimento_key) REFERENCES Dim_Data(data_key)
);

-- 11. Tabela de Fatos de Multas (Fato_Multas)
-- Representa os eventos de multas de trânsito.
CREATE TABLE Fato_Multas (
    -- Chaves Estrangeiras
    id_veiculo_key INT NOT NULL,
    id_motorista_key INT, -- Pode ser nulo
    data_multa_key INT NOT NULL,

    -- Dimensões Degeneradas
    id_multa_degenerada INT NOT NULL, -- ID da multa do sistema OLTP
    local_multa VARCHAR(255),
    descricao_infracao TEXT,
    status_pagamento VARCHAR(50),

    -- Medidas
    valor_multa DECIMAL(10, 2) NOT NULL,

    PRIMARY KEY (id_multa_degenerada),
    FOREIGN KEY (id_veiculo_key) REFERENCES Dim_Veiculo(id_veiculo_key),
    FOREIGN KEY (id_motorista_key) REFERENCES Dim_Motorista(id_motorista_key),
    FOREIGN KEY (data_multa_key) REFERENCES Dim_Data(data_key)
);
