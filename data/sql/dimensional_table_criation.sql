CREATE TABLE Dim_Data (
    data_key INT PRIMARY KEY, -- formato YYYYMMDD
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
    eh_fim_de_semana BIT NOT NULL,
    eh_feriado BIT NOT NULL DEFAULT 0,
    feriado_nome VARCHAR(100)
);

CREATE TABLE Dim_Cliente (
    id_cliente_key INT PRIMARY KEY IDENTI   TY(1,1),
    id_cliente_origem INT UNIQUE,
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


CREATE TABLE Dim_Motorista (
    id_motorista_key INT PRIMARY KEY IDENTITY(1,1),
    id_motorista_origem INT UNIQUE,
    nome_motorista VARCHAR(255) NOT NULL,
    cpf VARCHAR(14) UNIQUE NOT NULL,
    numero_cnh VARCHAR(20) UNIQUE NOT NULL,
    data_nascimento DATE,
    telefone VARCHAR(20),
    email VARCHAR(100),
    status_ativo BIT,
    data_contratacao DATE
);


CREATE TABLE Dim_Veiculo (
    id_veiculo_key INT PRIMARY KEY IDENTITY(1,1),
    id_veiculo_origem INT UNIQUE,
    placa VARCHAR(10) UNIQUE NOT NULL,
    modelo VARCHAR(100) NOT NULL,
    marca VARCHAR(100),
    ano_fabricacao INT,
    capacidade_carga_kg DECIMAL(10, 2),
    tipo_veiculo VARCHAR(50),
    status_operacional VARCHAR(50)
);


CREATE TABLE Dim_Tipo_Carga (
    id_tipo_carga_key INT PRIMARY KEY IDENTITY(1,1),
    id_tipo_carga_origem INT UNIQUE,
    nome_tipo VARCHAR(100) NOT NULL UNIQUE,
    descricao_tipo VARCHAR(MAX),
    requer_refrigeracao BIT,
    peso_medio_kg DECIMAL(10, 2)
);


CREATE TABLE Dim_Rota (
    id_rota_key INT PRIMARY KEY IDENTITY(1,1),
    id_rota_origem INT UNIQUE,
    nome_rota VARCHAR(255) NOT NULL,
    origem VARCHAR(255) NOT NULL,
    destino VARCHAR(255) NOT NULL,
    distancia_km DECIMAL(10, 2),
    tempo_estimado_horas DECIMAL(5, 2)
);


CREATE TABLE Fato_Entregas (
    id_veiculo_key INT NOT NULL,
    id_motorista_key INT NOT NULL,
    id_cliente_remetente_key INT NOT NULL,
    id_cliente_destinatario_key INT NOT NULL,
    id_rota_key INT,
    id_tipo_carga_key INT NOT NULL,
    data_inicio_entrega_key INT NOT NULL,
    data_previsao_fim_entrega_key INT,
    data_fim_real_entrega_key INT,

    id_entrega_degenerada INT NOT NULL PRIMARY KEY,
    status_entrega VARCHAR(50) NOT NULL,

    valor_frete DECIMAL(10, 2) NOT NULL,
    peso_carga_kg DECIMAL(10, 2),

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


CREATE TABLE Fato_Coletas (
    id_entrega_key INT NOT NULL,
    data_hora_coleta_key INT NOT NULL,

    id_coleta_degenerada INT NOT NULL PRIMARY KEY,
    status_coleta VARCHAR(50),
    endereco_coleta VARCHAR(255),
    observacoes VARCHAR(MAX),

    quantidade_coletas INT DEFAULT 1,

    FOREIGN KEY (id_entrega_key) REFERENCES Fato_Entregas(id_entrega_degenerada),
    FOREIGN KEY (data_hora_coleta_key) REFERENCES Dim_Data(data_key)
);


CREATE TABLE Fato_Manutencoes (
    id_veiculo_key INT NOT NULL,
    data_manutencao_key INT NOT NULL,

    id_manutencao_degenerada INT NOT NULL PRIMARY KEY,
    tipo_manutencao VARCHAR(50) NOT NULL,
    descricao_servico VARCHAR(MAX),

    custo_manutencao DECIMAL(10, 2) NOT NULL,
    tempo_parado_horas DECIMAL(5, 2),

    FOREIGN KEY (id_veiculo_key) REFERENCES Dim_Veiculo(id_veiculo_key),
    FOREIGN KEY (data_manutencao_key) REFERENCES Dim_Data(data_key)
);


CREATE TABLE Fato_Abastecimentos (
    id_veiculo_key INT NOT NULL,
    data_abastecimento_key INT NOT NULL,

    id_abastecimento_degenerada INT NOT NULL PRIMARY KEY,
    tipo_combustivel VARCHAR(50),

    litros DECIMAL(10, 2) NOT NULL,
    valor_total DECIMAL(10, 2) NOT NULL,

    FOREIGN KEY (id_veiculo_key) REFERENCES Dim_Veiculo(id_veiculo_key),
    FOREIGN KEY (data_abastecimento_key) REFERENCES Dim_Data(data_key)
);


CREATE TABLE Fato_Multas (
    id_veiculo_key INT NOT NULL,
    id_motorista_key INT,
    data_multa_key INT NOT NULL,

    id_multa_degenerada INT NOT NULL PRIMARY KEY,
    local_multa VARCHAR(255),
    descricao_infracao VARCHAR(MAX),
    status_pagamento VARCHAR(50),

    valor_multa DECIMAL(10, 2) NOT NULL,

    FOREIGN KEY (id_veiculo_key) REFERENCES Dim_Veiculo(id_veiculo_key),
    FOREIGN KEY (id_motorista_key) REFERENCES Dim_Motorista(id_motorista_key),
    FOREIGN KEY (data_multa_key) REFERENCES Dim_Data(data_key)
);
