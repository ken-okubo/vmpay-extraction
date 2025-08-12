import os
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
client = bigquery.Client()

# Get the dataset ID from environment
DATASET_ID = os.getenv("BIGQUERY_DATASET_ID")
if not DATASET_ID:
    # In a Cloud Function, environment variables are usually set in the deployment config
    # This check is more for local testing of this script.
    print(
        "Warning: BIGQUERY_DATASET_ID not found via .env. Ensure it's set in Cloud Function environment."
    )

# --- Configuration for Cloud Function Data Handling ---
# These column names should be the names AFTER sanitization
# (e.g., dots replaced with underscores)
TABLE_STRING_COLUMNS_CF = {
    "products": {
        "id": str,
        "type": str,
        "manufacturer_id": str,
        "category_id": str,
        "supply_category_id": str,
        "name": str,
        "upc_code": str,
        "barcode": str,
        "external_id": str,
        "image": str,
        "tags": str,
        "additional_barcodes": str,
        "ncm_code": str,
        "cest_code": str,
        "url": str,
        "inventories": str,
        "packing_id": str,
        "packing_name": str,
    },
    "categories": {"id": str, "name": str},
    "clients": {
        "id": str,
        "name": str,
        "corporate_name": str,
        "cpf": str,
        "cnpj": str,
        "nif": str,
        "contact_name": str,
        "contact_phone": str,
        "contact_email": str,
        "notes": str,
        "legal_type": str,
        "main_location_id": str,
    },
    "locations": {
        "id": str,
        "client_id": str,
        "name": str,
        "phone": str,
        "street": str,
        "number": str,
        "complement": str,
        "neighborhood": str,
        "city": str,
        "country": str,
        "state": str,
        "zip_code": str,
    },
    "manufacturers": {"id": str, "name": str},
    "cashless": {  # id_column for cashless is transaction_id
        "transaction_id": str,
        "point_of_sale": str,
        "kind": str,
        "status": str,
        "installation_id": str,
        "planogram_item_id": str,
        "equipment_id": str,
        "equipment_label_number": str,
        "equipment_serial_number": str,
        "masked_card_number": str,
        "issuer_authorization_code": str,
        "order_id": str,
        "cancel_reason_detailed": str,
        "physical_locator": str,
        "place": str,
        "planogram_item": str,
        "cashless_error_friendly": str,
        "client_id": str,
        "client_name": str,
        "location_id": str,
        "location_name": str,
        "machine_id": str,
        "machine_asset_number": str,
        "machine_model_id": str,
        "machine_model_name": str,
        "good_id": str,
        "good_type": str,
        "good_category_id": str,
        "good_manufacturer_id": str,
        "good_name": str,
        "good_upc_code": str,
        "good_barcode": str,
        "eft_provider_id": str,
        "eft_provider_name": str,
        "eft_authorizer_id": str,
        "eft_authorizer_name": str,
        "eft_card_brand_id": str,
        "eft_card_brand_name": str,
        "eft_card_type_id": str,
        "eft_card_type_name": str,
        "cashless_error_complete_description": str,
        "payment_authorizer_id": str,
        "payment_authorizer_name": str,
        "combo_items": str,
    },
}

TABLE_DATE_COLUMNS_CF = {
    "products": ["created_at", "updated_at"],
    "cashless": ["occurred_at"],
}

# Keep your existing NUMERIC_COLUMNS list, ensure names are sanitized if they came from dotted originals
# These names should be as they appear AFTER sanitize_columns() is called.
NUMERIC_COLUMNS_CF = [
    # "point_of_sale", # This was in your original, but for cashless it's also in TABLE_STRING_COLUMNS_CF. String takes precedence.
    "request_number",
    "cost_price",  # Present in both products and cashless, ensure it's handled correctly based on table if types differ
    "quantity",
    "value",
    "discount_value",
    # For products specific numeric columns (from your local script config):
    "weight",
    "default_price",
    "vendible_balance",
    "packing_quantity",
]
# --- End Configuration ---


def sanitize_columns(df: pd.DataFrame):
    """Replaces dots in DataFrame column names with underscores."""
    original_to_sanitized_map = {
        col: col.replace(".", "_") for col in df.columns
    }
    df.columns = [original_to_sanitized_map[col] for col in df.columns]
    return df


def create_table_if_not_exists(table_id: str, schema: list):
    """Creates a BigQuery table with the given schema if it does not already exist."""
    try:
        client.get_table(table_id)
        # print(f"Table {table_id} already exists.") # Less verbose for CF
    except NotFound:
        print(f"Table {table_id} not found. Creating with explicit schema...")
        table_ref = bigquery.Table(table_id, schema=schema)
        client.create_table(table_ref)
        print(f"Table {table_id} created successfully with explicit schema.")


def build_bq_schema_cf(df: pd.DataFrame, table_name: str):
    """Builds a BigQuery schema for the Cloud Function context."""
    bq_schema = []
    string_cols_for_table = TABLE_STRING_COLUMNS_CF.get(table_name, {})
    date_cols_for_table = TABLE_DATE_COLUMNS_CF.get(table_name, [])
    # Consider making NUMERIC_COLUMNS_CF table-specific if needed, or filter the global one
    # For now, we'll use the global NUMERIC_COLUMNS_CF and let string/date take precedence

    for column_name in df.columns:
        field_type = "STRING"  # Default to STRING

        if column_name in string_cols_for_table:
            field_type = "STRING"
        elif column_name in date_cols_for_table:
            # Ensure data is already datetime64 by this point
            if pd.api.types.is_datetime64_any_dtype(df[column_name].dtype):
                field_type = "TIMESTAMP"
            else:
                # Fallback or warning if not converted to datetime before schema build
                print(
                    f"Warning: Column 	'{column_name}	' in table 	'{table_name}	' was expected to be datetime but is {df[column_name].dtype}. Defaulting to STRING for schema."
                )
                field_type = "STRING"
        elif (
            column_name in NUMERIC_COLUMNS_CF
        ):  # Check after explicit strings/dates
            # This assumes that if a column is in NUMERIC_COLUMNS_CF, it's not also a primary string/date ID
            if pd.api.types.is_integer_dtype(df[column_name].dtype):
                # Check if it's genuinely numeric or an ID that happens to be int after pd.to_numeric
                field_type = "INTEGER"  # Or FLOAT if it could have decimals but was read as int
            elif pd.api.types.is_float_dtype(df[column_name].dtype):
                field_type = "FLOAT"
            else:
                # If to_numeric failed (errors='coerce'), it might be object with NaNs, or still string.
                # Defaulting to FLOAT for safety if it was in NUMERIC_COLUMNS_CF but dtype is ambiguous.
                # Or, could default to STRING if conversion was problematic.
                print(
                    f"Warning: Column 	'{column_name}	' in NUMERIC_COLUMNS_CF has dtype {df[column_name].dtype}. Defaulting to FLOAT for schema."
                )
                field_type = "FLOAT"  # Or STRING
        else:  # Infer for other columns not explicitly defined
            current_dtype = df[column_name].dtype
            if pd.api.types.is_integer_dtype(current_dtype):
                field_type = "INTEGER"
            elif pd.api.types.is_float_dtype(current_dtype):
                field_type = "FLOAT"
            elif pd.api.types.is_bool_dtype(current_dtype):
                field_type = "BOOLEAN"
            elif pd.api.types.is_datetime64_any_dtype(current_dtype):
                field_type = "TIMESTAMP"
            # else remains STRING (default)

        bq_schema.append(bigquery.SchemaField(column_name, field_type))
    # print(f"Generated schema for table 	'{table_name}	' (CF): {bq_schema}")
    return bq_schema


def upload_dataframe_to_bigquery(
    df: pd.DataFrame, table_name: str, id_column: str
):
    print(f"--- Processing table: {table_name} for Cloud Function --- ")
    # 1. Sanitize column names first
    df = sanitize_columns(df)
    # Ensure id_column is also sanitized if it came with dots
    id_column = id_column.replace(".", "_")

    # 2. Convert date columns to datetime objects
    date_cols_for_this_table = TABLE_DATE_COLUMNS_CF.get(table_name, [])
    for date_col in date_cols_for_this_table:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
            print(
                f"Converted column 	'{date_col}	' to datetime for table '{table_name}'."
            )

    # 3. Explicitly convert designated STRING columns (after sanitization and date conversion)
    string_cols_for_this_table = TABLE_STRING_COLUMNS_CF.get(table_name, {})
    for str_col_name in string_cols_for_this_table.keys():
        if str_col_name in df.columns:
            df[str_col_name] = df[str_col_name].fillna("").astype(str)

    # 4. Handle special structured string columns (tags, additional_barcodes)
    # Your existing logic for these seems fine, ensure it runs after general string conversion if they are also in TABLE_STRING_COLUMNS_CF
    if (
        "tags" in df.columns and "tags" in string_cols_for_this_table
    ):  # Check if it's meant to be a string
        try:
            df["tags"] = df["tags"].apply(
                lambda x: (
                    ", ".join([str(tag) for tag in x if tag is not None])
                    if isinstance(x, list)
                    # Ensure NaN becomes empty string
                    else (str(x) if pd.notna(x) else "")
                )
            )
            print("Processed 'tags' column.")
        except Exception as e:
            print(f"Failed to process 'tags' column: {e}")
            # Decide on error handling: raise or log

    if (
        "additional_barcodes" in df.columns
        and "additional_barcodes" in string_cols_for_this_table
    ):
        try:

            def convert_additional_barcodes_cf(x):
                if isinstance(x, list):
                    values = [
                        str(item["value"])
                        for item in x
                        if isinstance(item, dict)
                        and "value" in item
                        and item["value"] is not None
                    ]
                    return ", ".join(values)
                elif pd.notna(x):
                    return str(x)
                return ""  # Ensure NaN becomes empty string

            df["additional_barcodes"] = df["additional_barcodes"].apply(
                convert_additional_barcodes_cf
            )
            print("Processed 'additional_barcodes' column.")
        except Exception as e:
            print(f"Failed to process 'additional_barcodes' column: {e}")

    # 5. Convert NUMERIC columns (those not already forced to string/date)
    for num_col in NUMERIC_COLUMNS_CF:
        if (
            num_col in df.columns
            and num_col not in string_cols_for_this_table
            and num_col not in date_cols_for_this_table
        ):
            try:
                df[num_col] = pd.to_numeric(df[num_col], errors="coerce")
            except Exception as e:
                print(f"Failed to convert 	'{num_col}	' to numeric: {e}")

    # Validate that the ID column exists
    if id_column not in df.columns:
        raise ValueError(
            f"ID column 	'{id_column}	' not found in DataFrame for table 	'{table_name}	'. Columns: {df.columns.tolist()}"
        )

    # Define temp and final table names
    temp_table_id = f"{DATASET_ID}.temp_{table_name}"
    final_table_id = f"{DATASET_ID}.{table_name}"

    # Build the BigQuery schema based on the processed DataFrame
    bq_schema = build_bq_schema_cf(df, table_name)

    # Ensure the final table exists with the correct schema (important for first run after schema change)
    # This create_table_if_not_exists should ideally use the schema from the main load script if possible,
    # or ensure this build_bq_schema_cf is perfectly aligned.
    create_table_if_not_exists(final_table_id, schema=bq_schema)

    # Load data to a temporary table
    job_config_load = bigquery.LoadJobConfig(
        schema=bq_schema,
        write_disposition="WRITE_TRUNCATE",
    )
    print(
        f"Loading data into temporary table 	'{temp_table_id}	' for table 	'{table_name}	'..."
    )
    client.load_table_from_dataframe(
        df, temp_table_id, job_config=job_config_load
    ).result()
    print(
        f"Data loaded successfully into temporary table 	'{temp_table_id}	'."
    )

    # Construct MERGE SQL
    set_clause_parts = []
    for col_name_iter in df.columns:
        if col_name_iter != id_column:
            set_clause_parts.append(
                f"T.`{col_name_iter}` = S.`{col_name_iter}`"
            )
    set_clause = ",\n        ".join(set_clause_parts)

    insert_columns_list = [f"`{col}`" for col in df.columns]
    insert_columns = ", ".join(insert_columns_list)
    insert_values_list = [f"S.`{col}`" for col in df.columns]
    insert_values = ", ".join(insert_values_list)

    merge_sql = f"""
    MERGE `{final_table_id}` T
    USING `{temp_table_id}` S
    ON T.`{id_column}` = S.`{id_column}`
    WHEN MATCHED THEN
      UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_columns})
      VALUES ({insert_values})
    """
    print(f"Executing MERGE SQL for table 	'{table_name}	'...")
    try:
        query_job = client.query(merge_sql)
        query_job.result()
        print(
            f"Table 	'{table_name}	' merged successfully. Affected rows: {query_job.num_dml_affected_rows if query_job.num_dml_affected_rows is not None else 'N/A'}"
        )
    except Exception as e:
        print(f"Error during MERGE operation for table 	'{table_name}	': {e}")
        # Consider logging query_job.errors if available
        raise

    # Delete the temporary table
    client.delete_table(temp_table_id, not_found_ok=True)
    print(f"Temporary table 	'{temp_table_id}	' deleted.")
    print(
        f"--- Finished processing table: {table_name} for Cloud Function --- "
    )


# Example of how daily_fetch.py might call this (for testing this script directly):
if __name__ == "__main__":
    # This is for local testing of this util script.
    # Your daily_fetch.py will call upload_dataframe_to_bigquery directly.
    print("Local test run for utils_load_bigquery_cf_adjusted.py")
    # Create a sample DataFrame for a table, e.g., 'products'
    sample_data_products = {
        "id": ["prod123", "prod456"],
        "created_at": ["2023-01-01T10:00:00Z", "2023-01-02T12:00:00Z"],
        "updated_at": ["2023-01-01T11:00:00Z", "2023-01-02T13:00:00Z"],
        "name": ["Test Product 1", "Test Product 2"],
        "packing.id": ["packA", "packB"],  # Example of a column with a dot
        "cost_price": [10.50, 22.75],
        "tags": [["tag1", "tag2"], ["tag3"]],
    }
    sample_df_products = pd.DataFrame(sample_data_products)

    if DATASET_ID:  # Only run if DATASET_ID is available
        try:
            print(
                f"Attempting to upload sample 'products' data to {DATASET_ID}.products"
            )
            upload_dataframe_to_bigquery(
                sample_df_products, table_name="products", id_column="id"
            )
            print("Sample 'products' data upload test finished.")
        except Exception as e:
            print(f"Error during local test for 'products': {e}")
            import traceback

            traceback.print_exc()
    else:
        print("Skipping local test: BIGQUERY_DATASET_ID not set.")
