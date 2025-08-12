import pandas as pd

cashless = pd.read_csv("data/raw_csv_outputs/cashless_data.csv")
cashless = cashless.rename(columns={"id": "transaction_id"})

products = pd.read_csv("data/raw_csv_outputs/products.csv")
categories = pd.read_csv("data/raw_csv_outputs/categories.csv")
manufacturers = pd.read_csv("data/raw_csv_outputs/manufacturers.csv")
locations = pd.read_csv("data/raw_csv_outputs/locations.csv")

products = products.rename(columns={"name": "product_description"})
categories = categories.rename(columns={"name": "product_category"})
manufacturers = manufacturers.rename(columns={"name": "manufacturer_name"})

cashless["client.name_normalized"] = (
    cashless["client.name"].str.upper().str.strip()
)
locations = locations.dropna(subset=["name"])
locations["name_normalized"] = locations["name"].str.upper().str.strip()

df = cashless.merge(
    products.rename(columns={"id": "product_id"}),
    left_on="good.id",
    right_on="product_id",
    how="left",
)

df = df.merge(
    categories.rename(columns={"id": "category_id"}),
    on="category_id",
    how="left",
)

df = df.merge(
    manufacturers.rename(columns={"id": "manufacturer_id"}),
    on="manufacturer_id",
    how="left",
)

df = df.merge(
    locations.rename(columns={"name_normalized": "client_group"}),
    left_on="client.name_normalized",
    right_on="client_group",
    how="left",
)

df["transaction_id"] = df["transaction_id"].astype("Int64")

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

final.to_csv("data/final_cashless_table.csv", index=False)
print("✅ Final table saved in: data/final_cashless_table.csv")
