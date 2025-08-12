# main.py
from extract.extract_cashless import extract_cashless_range, save_to_csv
from extract.extract_products import main as extract_products
from extract.extract_categories import main as extract_categories
from extract.extract_clients import main as extract_clients
from extract.extract_manufacturers import main as extract_manufacturers
from extract.extract_locations import main as extract_locations

if __name__ == "__main__":
    data = extract_cashless_range(days_back=7)
    save_to_csv(data)

    extract_products()
    extract_categories()
    extract_clients()
    extract_manufacturers()
    extract_locations()
