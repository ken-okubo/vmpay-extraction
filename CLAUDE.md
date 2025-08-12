# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Development Commands

### Data Pipeline Commands (using Makefile)
```bash
# Extract auxiliary reference tables (products, categories, clients, manufacturers, locations)
make extract-aux

# Extract historical cashless transaction data
make extract-historical

# Merge cashless CSV files into single deduplicated table
make merge-cashless-only

# Upload all tables to BigQuery with merge logic
make load-bigquery

# Individual extraction commands (run with PYTHONPATH=.)
PYTHONPATH=. python extract/extract_products.py
PYTHONPATH=. python extract/extract_categories.py
PYTHONPATH=. python extract/extract_clients.py
PYTHONPATH=. python extract/extract_manufacturers.py
PYTHONPATH=. python extract/extract_locations.py
```

### Python Environment
```bash
# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Direct Python Execution
```bash
# Main pipeline (extracts last 7 days of cashless data + all reference tables)
python main.py

# Individual extraction scripts
python extract/extract_historical_cashless.py
python merge/merge_cashless_facts_only.py
python load/load_to_bigquery.py
```

## Architecture Overview

This is a **data pipeline for vending machine analytics**, extracting data from the VM Pay API and loading it into BigQuery for analysis.

### Core Components

**1. Data Extraction (`extract/`)**
- Fetches data from VM Pay API (https://vmpay.vertitecnologia.com.br/api/v1)
- Separate extractors for each entity type: cashless transactions, products, categories, clients, manufacturers, locations
- Uses shared `utils/fetch.py` for API authentication and requests
- Outputs raw CSV files to `data/raw_csv_outputs/`

**2. Data Processing (`merge/`)**
- `merge_cashless_facts_only.py`: Combines historical cashless CSV files into single deduplicated table
- Handles data cleaning and deduplication based on transaction_id

**3. Data Loading (`load/`)**
- `load_to_bigquery.py`: Sophisticated BigQuery loader with schema management
- Implements MERGE operations for upserts (insert new, update existing)
- Handles complex nested JSON fields from API responses
- Explicit type mapping for critical fields (IDs, codes, URLs as STRING)

**4. Cloud Deployment (`cloud_function/`)**
- Google Cloud Function for automated daily data fetching
- `daily_fetch.py`: Orchestrates daily data extraction
- `main.py`: HTTP trigger entry point

### Data Flow Architecture

```
VM Pay API → Extract Scripts → Raw CSVs → Merge Scripts → BigQuery Tables
                ↓
         Reference Tables (products, categories, etc.)
                ↓
    Combined Analytics in BigQuery
```

### Key Configuration Files

**Environment Variables (`.env`)**
- `VM_API_TOKEN`: Authentication token for VM Pay API
- `BIGQUERY_DATASET_ID`: Target BigQuery dataset

**Schema Configuration (`load/load_to_bigquery.py`)**
- `TABLE_STRING_COLUMNS`: Critical for maintaining data integrity (IDs, codes as strings)
- `TABLE_NUMERIC_COLUMNS`: Explicit numeric field definitions
- `TABLE_DATE_COLUMNS`: Timestamp field handling

### Important Data Handling Notes

- **Transaction IDs and codes must remain as strings** (not auto-converted to numbers)
- BigQuery schema is explicitly managed to prevent data type inference issues
- MERGE operations ensure idempotent loads (safe to re-run)
- Nested JSON fields from API are flattened using pandas.json_normalize()

### File Structure Patterns

- `extract/extract_*.py`: Individual entity extractors
- `data/raw_csv_outputs/`: Raw extracted data
- `data/historical_cashless/`: Weekly cashless data files
- `data/cashless_facts_full.csv`: Merged transaction data
- All scripts use `utils/fetch.py` for consistent API access