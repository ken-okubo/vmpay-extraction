import pandas as pd
from utils.fetch import fetch_from_endpoint


def main():
    data = fetch_from_endpoint("locations")
    df = pd.json_normalize(data)
    df.to_csv("data/raw_csv_outputs/locations.csv", index=False)
    print(f"Saved {len(df)} locations to CSV.")


if __name__ == "__main__":
    main()
