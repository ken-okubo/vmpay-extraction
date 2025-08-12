import pandas as pd
from utils.fetch import fetch_from_endpoint


def main():
    data = fetch_from_endpoint("products")
    df = pd.json_normalize(data)
    df.to_csv("data/raw_csv_outputs/products.csv", index=False)
    print(f"Saved {len(df)} products to CSV.")


if __name__ == "__main__":
    main()
