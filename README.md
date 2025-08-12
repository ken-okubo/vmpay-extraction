# Vending Machine Analytics Pipeline

**[English](#english) | [Português](#português)**

---

## English

### Overview

A comprehensive data pipeline for vending machine analytics, designed to extract, process, and analyze transactional data from VM Pay API. This project demonstrates end-to-end ETL processes with Google Cloud integration for scalable data analytics.

### Key Features

- **Automated Data Extraction**: Fetches data from VM Pay API with configurable date ranges
- **Smart Data Processing**: Handles complex nested JSON structures and data deduplication
- **BigQuery Integration**: Sophisticated schema management with MERGE operations for data integrity
- **Cloud-Ready**: Google Cloud Function for automated daily processing
- **Robust Error Handling**: Comprehensive logging and error management throughout the pipeline

### Architecture

```
VM Pay API → Extract Scripts → Raw CSVs → Merge Scripts → BigQuery Tables
                ↓
         Reference Tables (products, categories, etc.)
                ↓
    Combined Analytics in BigQuery
```

### Data Sources

- **Cashless Transactions**: Real-time vending machine transaction data
- **Product Catalog**: Complete product information with categories and manufacturers
- **Client Management**: Store locations and client details
- **Machine Analytics**: Equipment performance and location data

### Technology Stack

- **Python 3.11+** with pandas, requests, google-cloud-bigquery
- **Google Cloud Platform**: BigQuery, Cloud Functions
- **Data Processing**: ETL pipelines with automatic schema detection
- **API Integration**: RESTful API consumption with authentication

### Use Cases

- Revenue analytics and trend analysis
- Product performance optimization
- Location-based sales insights
- Inventory management and forecasting
- Customer behavior analysis

### Quick Start

```bash
# Setup environment
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure environment variables
cp .env.example .env
# Edit .env with your VM_API_TOKEN and BIGQUERY_DATASET_ID

# Extract data
make extract-aux          # Reference tables
make extract-historical   # Historical transactions
make merge-cashless-only  # Combine data
make load-bigquery       # Upload to BigQuery
```

### 📁 Project Structure

- `extract/` - Data extraction scripts for each entity type
- `merge/` - Data processing and deduplication logic
- `load/` - BigQuery upload with schema management
- `cloud_function/` - Google Cloud Function for automation
- `data/` - Local data storage (excluded from git)

### 🔒 Security & Best Practices

- Environment variables for sensitive data
- Type-safe BigQuery schema management
- Idempotent operations for safe re-runs
- Comprehensive error handling and logging

---

## Português

### Visão Geral

Um pipeline de dados abrangente para análise de máquinas de venda automática, projetado para extrair, processar e analisar dados transacionais da API VM Pay. Este projeto demonstra processos ETL completos com integração Google Cloud para análise de dados escalável.

### Características Principais

- **Extração Automatizada de Dados**: Busca dados da API VM Pay com intervalos de datas configuráveis
- **Processamento Inteligente**: Manipula estruturas JSON complexas e deduplicação de dados
- **Integração BigQuery**: Gerenciamento sofisticado de schema com operações MERGE para integridade dos dados
- **Pronto para Nuvem**: Google Cloud Function para processamento diário automatizado
- **Tratamento Robusto de Erros**: Logging abrangente e gerenciamento de erros em todo o pipeline

### Arquitetura

```
VM Pay API → Scripts de Extração → CSVs → Scripts de Merge → Tabelas BigQuery
                ↓
         Tabelas de Referência (produtos, categorias, etc.)
                ↓
    Análises Combinadas no BigQuery
```

### Fontes de Dados

- **Transações Cashless**: Dados de transações de máquinas de venda em tempo real
- **Catálogo de Produtos**: Informações completas de produtos com categorias e fabricantes
- **Gestão de Clientes**: Localizações de lojas e detalhes de clientes
- **Análise de Máquinas**: Performance de equipamentos e dados de localização

### Stack Tecnológico

- **Python 3.11+** com pandas, requests, google-cloud-bigquery
- **Google Cloud Platform**: BigQuery, Cloud Functions
- **Processamento de Dados**: Pipelines ETL com detecção automática de schema
- **Integração de API**: Consumo de API RESTful com autenticação

### Casos de Uso

- Análise de receita e tendências
- Otimização de performance de produtos
- Insights de vendas por localização
- Gestão de inventário e previsões
- Análise de comportamento do cliente

### Início Rápido

```bash
# Configurar ambiente
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configurar variáveis de ambiente
cp .env.example .env
# Editar .env com seu VM_API_TOKEN e BIGQUERY_DATASET_ID

# Extrair dados
make extract-aux          # Tabelas de referência
make extract-historical   # Transações históricas
make merge-cashless-only  # Combinar dados
make load-bigquery       # Upload para BigQuery
```

### Estrutura do Projeto

- `extract/` - Scripts de extração de dados para cada tipo de entidade
- `merge/` - Lógica de processamento e deduplicação de dados
- `load/` - Upload para BigQuery com gerenciamento de schema
- `cloud_function/` - Google Cloud Function para automação
- `data/` - Armazenamento local de dados (excluído do git)

### Segurança & Boas Práticas

- Variáveis de ambiente para dados sensíveis
- Gerenciamento de schema BigQuery type-safe
- Operações idempotentes para re-execuções seguras
- Tratamento de erros e logging abrangentes

---

### Contact / Contato

**Ken Okubo**
[ken.okubo1@gmail.com]
[[LinkedIn](https://www.linkedin.com/in/ken-okubo-8b484978/)]
[[GitHub](https://github.com/ken-okubo)]
