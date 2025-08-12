import pandas as pd
from datetime import datetime, timedelta, timezone
from utils.fetch import fetch_from_endpoint


def fetch_cashless_data(start_date, end_date, page=1, per_page=100):
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "page": page,
        "per_page": per_page,
    }
    return fetch_from_endpoint("cashless_facts", params=params)


def extract_cashless_range(days_back=7):
    start_date = (
        datetime.now(timezone.utc) - timedelta(days=days_back)
    ).isoformat() + "Z"
    end_date = datetime.now(timezone.utc).isoformat()

    print(f"Fetching data from {start_date} to {end_date}")
    page = 1
    all_data = []

    while True:
        data = fetch_cashless_data(start_date, end_date, page=page)
        if not data:
            break
        all_data.extend(data)
        if len(data) < 100:
            break
        page += 1

    print(f"Total records fetched: {len(all_data)}")
    return all_data


def save_to_csv(data, filename="data/raw_csv_outputs/cashless_data.csv"):
    df = pd.json_normalize(data)
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")


if __name__ == "__main__":
    data = extract_cashless_range(days_back=7)
    save_to_csv(data)
