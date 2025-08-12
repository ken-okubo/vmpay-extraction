from utils.fetch import fetch_from_endpoint
from utils.load_bigquery import upload_dataframe_to_bigquery
import pandas as pd
from datetime import datetime, timedelta, timezone
import time
import traceback


def fetch_cashless_data(start_date, end_date):
    all_data = []
    page = 1
    while True:
        params = {
            "start_date": start_date.isoformat() + "Z",
            "end_date": end_date.isoformat() + "Z",
            "page": page,
            "per_page": 100,
        }
        data = fetch_from_endpoint("cashless_facts", params)
        if not data:
            break
        all_data.extend(data)
        if len(data) < 100:
            break
        page += 1
        time.sleep(1)
    return all_data


def update_aux_table(name):
    try:
        data = fetch_from_endpoint(name)
        df = pd.json_normalize(data)
        upload_dataframe_to_bigquery(df, table_name=name, id_column="id")
        print(f"Updated table '{name}' with {len(df)} rows.")
    except Exception as e:
        print(f"Failed to update table '{name}': {str(e)}")
        traceback.print_exc()


def main(date_arg=None):
    try:
        if date_arg:
            start = datetime.fromisoformat(date_arg).replace(
                tzinfo=timezone.utc
            )
        else:
            start = datetime.now(timezone.utc) - timedelta(days=1)
        end = start + timedelta(days=1)

        print(f"Fetching cashless data from {start.date()} to {end.date()}")
        cashless_data = fetch_cashless_data(start, end)

        if cashless_data:
            df = pd.json_normalize(cashless_data)
            df = df.rename(columns={"id": "transaction_id"})
            upload_dataframe_to_bigquery(
                df, table_name="cashless", id_column="transaction_id"
            )
            print(f"Uploaded {len(df)} cashless records.")
        else:
            print("No cashless data found.")

        print("Updating auxiliary tables...")
        for table in [
            "categories",
            "clients",
            "locations",
            "manufacturers",
            "products",
        ]:
            update_aux_table(table)

        print("Daily fetch completed successfully.")
    except Exception as e:
        print(f"An error occurred in the main function: {e}")
        traceback.print_exc()
        raise  # re-raise to propagate the exception to the Cloud Function
