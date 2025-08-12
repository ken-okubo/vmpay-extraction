import pandas as pd
from glob import glob

# Paths
HISTORICAL_DIR = "data/historical_cashless"
AUX_DIR = "data/raw_csv_outputs"
OUTPUT_PATH = "data/final_cashless_table.csv"

# Load all historical cashless CSVs
csv_files = sorted(glob(f"{HISTORICAL_DIR}/cashless_*.csv"))
print(f"🔍 Found {len(csv_files)} historical cashless files")

df_list = [pd.read_csv(f) for f in csv_files]
cashless = pd.concat(df_list, ignore_index=True)
cashless = cashless.rename(columns={"id": "transaction_id"})

# Load auxiliary tables
products = pd.read_csv(f"{AUX_DIR}/products.csv").rename(
    columns={"name": "product_description"}
)
categories = pd.read_csv(f"{AUX_DIR}/categories.csv").rename(
    columns={"name": "product_category"}
)
manufacturers = pd.read_csv(f"{AUX_DIR}/manufacturers.csv").rename(
    columns={"name": "manufacturer_name"}
)
locations = pd.read_csv(f"{AUX_DIR}/locations.csv")

# Normalize join keys
cashless["client.name_normalized"] = (
    cashless["client.name"].str.upper().str.strip()
)
locations["name_normalized"] = locations["name"].str.upper().str.strip()

# Merge all
df = (
    cashless.merge(
        products.rename(columns={"id": "product_id"}),
        left_on="good.id",
        right_on="product_id",
        how="left",
    )
    .merge(
        categories.rename(columns={"id": "category_id"}),
        on="category_id",
        how="left",
    )
    .merge(
        manufacturers.rename(columns={"id": "manufacturer_id"}),
        on="manufacturer_id",
        how="left",
    )
    .merge(
        locations.rename(columns={"name_normalized": "client_group"}),
        left_on="client.name_normalized",
        right_on="client_group",
        how="left",
    )
)

# Final output
final = pd.DataFrame(
    {
        "payment_method": df["eft_card_type.name"],
        "card_brand": df["eft_card_brand.name"],
        "masked_card_number": df["masked_card_number"],
        "sku_quantity": df["quantity"],
        "product_barcode": df["barcode"],
        "product_description": df["product_description"],
        "product_category": df["product_category"],
        "manufacturer_name": df["manufacturer_name"],
        "client_group": df["client_group"],
        "unique_order": df["transaction_id"].astype(str)
        + df["masked_card_number"],
    }
)

final.to_csv(OUTPUT_PATH, index=False)
print(f"✅ Final merged table saved to: {OUTPUT_PATH}")
