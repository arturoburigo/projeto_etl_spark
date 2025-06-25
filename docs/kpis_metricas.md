# 📈 KPIs e Métricas

## 📋 Visão Geral

O sistema calcula automaticamente um conjunto abrangente de **KPIs (Key Performance Indicators)** e **métricas operacionais** que fornecem insights valiosos sobre a performance do negócio de logística e transporte. Todos os indicadores são atualizados automaticamente através do pipeline ETL e armazenados na camada Gold para consumo por ferramentas de BI.

---

## 🎯 KPIs Principais

### 🚚 **1. On-Time Delivery (OTD)**

**Descrição**: Percentual de entregas realizadas dentro do prazo estabelecido.

**Fórmula**: 
```sql
OTD = (Entregas no Prazo / Total de Entregas) × 100
```

**Implementação**:
```python
kpi_otd = fato_entregas.withColumn("on_time",
    (col("data_fim_real_entrega_key") <= col("data_previsao_fim_entrega_key")) | 
    (col("data_previsao_fim_entrega_key").isNull())
).agg(
    (sum(when(col("on_time") == True, 1).otherwise(0)) / count("*") * 100)
    .alias("percentual_entregas_no_prazo")
)
```

**Metas de Negócio**:
- 🎯 **Excelente**: > 95%
- ✅ **Bom**: 90-95%
- ⚠️ **Atenção**: 80-90%
- 🚨 **Crítico**: < 80%

**Frequência de Atualização**: Diária  
**Granularidade**: Por dia, semana, mês, cliente, rota

---

### 💰 **2. Custo Médio de Frete por Rota**

**Descrição**: Custo médio de transporte por quilômetro em cada rota.

**Fórmula**: 
```sql
Custo por KM = Valor Total do Frete / Distância da Rota (KM)
```

**Implementação**:
```python
kpi_custo_rota = fato_entregas.join(dim_rota, 
    fato_entregas["id_rota_origem"] == dim_rota["id_rota_origem"], "left"
).groupBy(
    dim_rota["nome_rota"], 
    dim_rota["origem"], 
    dim_rota["destino"],
    dim_rota["distancia_km"]
).agg(
    avg("valor_frete").alias("custo_medio_frete"),
    (avg("valor_frete") / first("distancia_km")).alias("custo_por_km")
)
```

**Análises Disponíveis**:
- 📊 **Por Rota**: Identificação de rotas mais/menos rentáveis
- 📈 **Tendência Temporal**: Evolução dos custos ao longo do tempo
- 🔍 **Benchmarking**: Comparação entre rotas similares
- 💡 **Otimização**: Identificação de oportunidades de melhoria

**Frequência de Atualização**: Semanal  
**Granularidade**: Por rota, região, tipo de carga

---

### 🚛 **3. Utilização da Frota**

**Descrição**: Análise da utilização dos veículos por tipo e performance.

**Métricas Calculadas**:
- Total de entregas por tipo de veículo
- Taxa de ocupação da frota
- Quilometragem média por veículo
- Tempo médio em trânsito

**Implementação**:
```python
kpi_frota = fato_entregas.join(dim_veiculo, 
    fato_entregas["id_veiculo_origem"] == dim_veiculo["id_veiculo_origem"], "left"
).groupBy(
    dim_veiculo["tipo_veiculo"],
    dim_veiculo["marca"],
    dim_veiculo["capacidade_carga_kg"]
).agg(
    count("*").alias("total_entregas"),
    sum("peso_carga_kg").alias("peso_total_transportado"),
    avg("peso_carga_kg").alias("peso_medio_por_entrega"),
    (sum("peso_carga_kg") / first("capacidade_carga_kg") * 100).alias("taxa_ocupacao_media")
)
```

**Insights Gerados**:
- 🎯 **Eficiência por Tipo**: Caminhões vs Vans vs Utilitários
- 📊 **Capacidade**: Taxa de ocupação da carga
- 🔄 **Rotatividade**: Frequência de uso por veículo
- 💰 **ROI**: Retorno sobre investimento por veículo

**Frequência de Atualização**: Mensal  
**Granularidade**: Por veículo, tipo, marca, região

---

### 💼 **4. Revenue por Cliente**

**Descrição**: Valor total de frete gerado por cada cliente e análise de rentabilidade.

**Métricas Calculadas**:
- Valor total de frete por cliente
- Ticket médio por entrega
- Frequência de envios
- Rentabilidade por cliente

**Implementação**:
```python
kpi_cliente = fato_entregas.join(dim_cliente, 
    fato_entregas["id_cliente_remetente_origem"] == dim_cliente["id_cliente_origem"], "left"
).groupBy(
    dim_cliente["nome_cliente"],
    dim_cliente["tipo_cliente"],
    dim_cliente["cidade"],
    dim_cliente["estado"]
).agg(
    sum("valor_frete").alias("valor_total_frete"),
    count("*").alias("total_entregas"),
    avg("valor_frete").alias("ticket_medio"),
    sum("peso_carga_kg").alias("peso_total_enviado")
)
```

**Segmentação de Clientes**:
- 🥇 **Premium**: > R$ 50.000/mês
- 🥈 **Gold**: R$ 20.000 - R$ 50.000/mês  
- 🥉 **Silver**: R$ 5.000 - R$ 20.000/mês
- 📊 **Standard**: < R$ 5.000/mês

**Frequência de Atualização**: Mensal  
**Granularidade**: Por cliente, segmento, região, tipo

---

## 📊 Métricas Operacionais

### 📅 **Métricas Temporais**

#### **Total de Entregas Mensal**
```python
metrica_entregas_mes = fato_entregas.join(dim_data, 
    fato_entregas["data_inicio_entrega_key"] == dim_data["data_key"], "left"
).groupBy(
    dim_data["ano"], 
    dim_data["mes"],
    dim_data["nome_mes"]
).agg(
    count("*").alias("total_entregas"),
    sum("valor_frete").alias("receita_total"),
    avg("valor_frete").alias("ticket_medio")
).orderBy("ano", "mes")
```

#### **Peso Total Transportado por Mês**
```python
metrica_peso_mes = fato_entregas.join(dim_data, 
    fato_entregas["data_inicio_entrega_key"] == dim_data["data_key"], "left"
).groupBy(
    dim_data["ano"], 
    dim_data["mes"]
).agg(
    sum("peso_carga_kg").alias("peso_total_kg"),
    avg("peso_carga_kg").alias("peso_medio_kg"),
    count("*").alias("total_entregas")
).orderBy("ano", "mes")
```

### 🔧 **Métricas de Manutenção**

#### **Custo de Manutenção por Veículo**
```python
metrica_manutencao = fato_manutencoes.join(dim_veiculo,
    fato_manutencoes["id_veiculo_origem"] == dim_veiculo["id_veiculo_origem"], "left"
).groupBy(
    dim_veiculo["placa"],
    dim_veiculo["modelo"],
    dim_veiculo["ano_fabricacao"]
).agg(
    sum("custo_manutencao").alias("custo_total_manutencao"),
    count("*").alias("total_manutencoes"),
    avg("custo_manutencao").alias("custo_medio_manutencao"),
    sum("tempo_parado_horas").alias("tempo_total_parado")
)
```

#### **Eficiência de Combustível**
```python
metrica_combustivel = fato_abastecimentos.join(dim_veiculo,
    fato_abastecimentos["id_veiculo_origem"] == dim_veiculo["id_veiculo_origem"], "left"
).groupBy(
    dim_veiculo["placa"],
    dim_veiculo["tipo_veiculo"]
).agg(
    sum("litros").alias("total_litros"),
    sum("valor_total").alias("custo_total_combustivel"),
    avg("valor_total" / "litros").alias("preco_medio_por_litro")
)
```

---

## 📈 Dashboards e Visualizações

### 🎛️ **Dashboard Executivo**

**KPIs Principais**:
- 📊 On-Time Delivery (gauge chart)
- 💰 Revenue mensal (line chart)
- 🚛 Utilização da frota (bar chart)
- 📈 Tendências operacionais (combo chart)

**Filtros Disponíveis**:
- 📅 Período (dia, semana, mês, ano)
- 🌍 Região (estado, cidade)
- 👥 Cliente (individual, segmento)
- 🚚 Tipo de veículo

### 📊 **Dashboard Operacional**

**Métricas Detalhadas**:
- 🗺️ Mapa de entregas por região
- ⏱️ Tempo médio de entrega por rota
- 📦 Volume de carga por tipo
- 🔧 Status de manutenção da frota

### 💰 **Dashboard Financeiro**

**Análises Financeiras**:
- 💵 Receita por cliente/região/período
- 📊 Margem de lucro por rota
- 💸 Custos operacionais (combustível, manutenção, multas)
- 📈 Projeções e tendências

---

## 🎯 Alertas e Notificações

### 🚨 **Alertas Críticos**

**On-Time Delivery < 80%**:
```python
if otd_percentage < 80:
    send_alert("CRÍTICO: OTD abaixo de 80%", 
               recipients=["gerencia@empresa.com"])
```

**Custo por KM acima do benchmark**:
```python
if custo_km > benchmark_custo_km * 1.2:
    send_alert("ATENÇÃO: Custo por KM 20% acima do benchmark", 
               recipients=["operacoes@empresa.com"])
```

### ⚠️ **Alertas de Atenção**

- 📈 **Volume anômalo** de entregas (> 150% da média)
- 🔧 **Aumento nos custos** de manutenção (> 20% mês anterior)
- ⛽ **Consumo excessivo** de combustível por veículo
- 🚨 **Aumento nas multas** por motorista/veículo

---

## 🔍 Análises Avançadas

### 📊 **Análise de Tendências**

**Sazonalidade**:
```python
# Análise de padrões sazonais
sazonalidade = fato_entregas.join(dim_data,
    fato_entregas["data_inicio_entrega_key"] == dim_data["data_key"], "left"
).groupBy(
    dim_data["mes"],
    dim_data["nome_mes"]
).agg(
    count("*").alias("total_entregas"),
    avg("valor_frete").alias("ticket_medio")
).orderBy("mes")
```

**Crescimento YoY (Year over Year)**:
```python
# Comparação ano sobre ano
crescimento_yoy = fato_entregas.join(dim_data,
    fato_entregas["data_inicio_entrega_key"] == dim_data["data_key"], "left"
).groupBy(
    dim_data["ano"],
    dim_data["mes"]
).agg(
    count("*").alias("total_entregas"),
    sum("valor_frete").alias("receita_total")
).withColumn("crescimento_percentual", 
    (col("receita_total") - lag("receita_total").over(window)) / 
    lag("receita_total").over(window) * 100
)
```

### 🎯 **Análise de Performance**

**Eficiência por Motorista**:
```python
performance_motorista = fato_entregas.join(dim_motorista,
    fato_entregas["id_motorista_origem"] == dim_motorista["id_motorista_origem"], "left"
).groupBy(
    dim_motorista["nome_motorista"]
).agg(
    count("*").alias("total_entregas"),
    (sum(when(col("status_entrega") == "Entregue", 1).otherwise(0)) / count("*") * 100)
    .alias("taxa_sucesso"),
    avg("valor_frete").alias("receita_media_por_entrega")
)
```

---

## 📋 Catálogo de KPIs

| KPI | Descrição | Fórmula | Frequência | Meta |
|-----|-----------|---------|------------|------|
| **On-Time Delivery** | % entregas no prazo | (Entregas no Prazo / Total) × 100 | Diária | > 95% |
| **Custo por KM** | Custo médio por quilômetro | Valor Frete / Distância KM | Semanal | < R$ 2,50/km |
| **Taxa de Ocupação** | % capacidade utilizada | Peso Transportado / Capacidade × 100 | Mensal | > 80% |
| **Revenue por Cliente** | Receita gerada por cliente | Σ Valor Frete por Cliente | Mensal | Crescimento 10% |
| **Ticket Médio** | Valor médio por entrega | Σ Valor Frete / Nº Entregas | Mensal | > R$ 500 |
| **Tempo Médio Entrega** | Tempo médio para entrega | Média (Data Fim - Data Início) | Semanal | < 48h |
| **Taxa de Avarias** | % entregas com problemas | Entregas com Problema / Total × 100 | Mensal | < 2% |
| **Consumo Combustível** | Litros por 100km | (Litros / KM Rodados) × 100 | Mensal | < 35L/100km |

---

## 🚀 Próximos Desenvolvimentos

### 📊 **KPIs Planejados**

- 🤖 **Predição de Demanda**: ML para prever volume de entregas
- 🎯 **Score de Satisfação**: Baseado em feedback dos clientes  
- 🌱 **Pegada de Carbono**: Emissões de CO₂ por entrega
- 📱 **NPS Logístico**: Net Promoter Score específico para logística

### 🔮 **Análises Avançadas**

- 🧠 **Machine Learning**: Modelos preditivos para otimização de rotas
- 📈 **Forecasting**: Previsão de demanda e capacidade
- 🎯 **Otimização**: Algoritmos para alocação ótima de recursos
- 📊 **Real-time Analytics**: Dashboards em tempo real

---

Todos esses KPIs e métricas são calculados automaticamente pelo pipeline ETL e disponibilizados na camada Gold para consumo por ferramentas de Business Intelligence, garantindo insights precisos e atualizados para tomada de decisões estratégicas. 