.PHONY: help extract-aux extract-historical merge-cashless-only load-bigquery all

help:
	@echo "Available commands:"
	@echo "  make extract-aux            → Extracts auxiliary tables (products, categories, etc.)"
	@echo "  make extract-historical     → Extracts full historical cashless data"
	@echo "  make merge-cashless-only    → Combines raw cashless CSVs into a single deduplicated table"
	@echo "  make load-bigquery          → Uploads all tables to BigQuery with merge logic"

extract-aux:
	PYTHONPATH=. python extract/extract_products.py
	PYTHONPATH=. python extract/extract_categories.py
	PYTHONPATH=. python extract/extract_clients.py
	PYTHONPATH=. python extract/extract_manufacturers.py
	PYTHONPATH=. python extract/extract_locations.py

extract-historical:
	python extract/extract_historical_cashless.py

merge-cashless-only:
	python merge/merge_cashless_facts_only.py

load-bigquery:
	python load/load_to_bigquery.py
