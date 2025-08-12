import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from requests.exceptions import RequestException

load_dotenv()

ACCESS_TOKEN = os.getenv("VM_API_TOKEN")
BASE_URL = "https://vmpay.vertitecnologia.com.br/api/v1/cashless_facts"
OUTPUT_DIR = "data/historical_cashless"

os.makedirs(OUTPUT_DIR, exist_ok=True)


def fetch_cashless_data(start_date, end_date, page=1, per_page=100):
    params = {
        "access_token": ACCESS_TOKEN,
        "start_date": start_date,
        "end_date": end_date,
        "page": page,
        "per_page": per_page,
    }

    for attempt in range(3):
        try:
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
            return response.json()
        except RequestException as e:
            print(f"⚠️ Request failed (attempt {attempt + 1}/3): {e}")
            time.sleep(2**attempt)
    raise RuntimeError("❌ Failed to fetch after 3 attempts")


def extract_range(start_date: datetime, end_date: datetime):
    all_data = []
    page = 1

    print(f"Fetching from {start_date} to {end_date}...")
    while True:
        data = fetch_cashless_data(
            start_date=start_date.isoformat() + "Z",
            end_date=end_date.isoformat() + "Z",
            page=page,
        )
        if not data:
            break
        all_data.extend(data)
        if len(data) < 100:
            break
        page += 1
        time.sleep(1)

    print(f"Fetched {len(all_data)} records")
    return all_data


def save_data_as_csv(data, start_date, end_date):
    if not data:
        return

    filename = (
        f"{OUTPUT_DIR}/cashless_{start_date.date()}_to_{end_date.date()}.csv"
    )
    df = pd.json_normalize(data)
    df.to_csv(filename, index=False)
    print(f"Saved to {filename}")


def generate_date_ranges(start, end, step_days=7):
    current = start
    while current < end:
        yield current, min(current + timedelta(days=step_days), end)
        current += timedelta(days=step_days)


if __name__ == "__main__":
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime.now(timezone.utc)

    for start_date, end_date in generate_date_ranges(start, end, step_days=7):
        filename = f"{OUTPUT_DIR}/cashless_{start_date.date()}_to_{end_date.date()}.csv"

        if os.path.exists(filename):
            print(f"Skipped (already exists): {filename}")
            continue

        data = extract_range(start_date, end_date)
        save_data_as_csv(data, start_date, end_date)
        time.sleep(1)
