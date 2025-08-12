# Import necessary libraries
import pandas as pd
import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from dotenv import (
    load_dotenv,
)  # For loading environment variables from a .env file

# Load environment variables (e.g., GCP project ID, BigQuery Dataset ID)
load_dotenv()
# Initialize the BigQuery client
client = bigquery.Client()

# Get the BigQuery Dataset ID from environment variables
DATASET_ID = os.getenv("BIGQUERY_DATASET_ID")
if not DATASET_ID:
    raise ValueError("❌ BIGQUERY_DATASET_ID not set in .env file")

# --- User Configuration Section ---
# Define columns that MUST be treated as STRING for each table.
# This is crucial for IDs, codes (like CPF, CNPJ, zip codes), URLs, JSON strings, etc.
# The user needs to populate this based on their specific data knowledge.
# Keys are table names (as they will be in BigQuery), values are dictionaries
# where keys are column names and values are `str`.
TABLE_STRING_COLUMNS = {
    "categories": {
        "id": str,
        "name": str,
        # 'over18' will be inferred as BOOLEAN by the schema builder if Pandas reads it as bool
    },
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
        # 'latitude', 'longitude' will be inferred as FLOAT
    },
    "manufacturers": {"id": str, "name": str},
    "products": {
        "id": str,
        "type": str,  # e.g., Product, Service
        "manufacturer_id": str,
        "category_id": str,
        "supply_category_id": str,
        "name": str,
        "upc_code": str,
        "barcode": str,
        "external_id": str,
        "image": str,  # URL
        "tags": str,  # Expected to be a JSON string or comma-separated
        "additional_barcodes": str,  # Expected to be a JSON string or comma-separated
        "ncm_code": str,
        "cest_code": str,
        "url": str,  # Product URL
        "inventories": str,  # Expected to be a JSON string
        "packing_id": str,  # From packing.id after sanitization
        "packing_name": str,  # From packing.name after sanitization
        # 'created_at', 'updated_at' will be inferred as TIMESTAMP
        # 'ignore_distribution_center', 'ignore_automatic_picklist' as BOOLEAN
    },
    "cashless": {  # For the table derived from cashless_facts_full.csv
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
        "client_id": str,  # From client.id
        "client_name": str,  # From client.name
        "location_id": str,  # From location.id
        "location_name": str,  # From location.name
        "machine_id": str,  # From machine.id
        "machine_asset_number": str,  # From machine.asset_number
        "machine_model_id": str,  # From machine_model.id
        "machine_model_name": str,  # From machine_model.name
        "good_id": str,  # From good.id
        "good_type": str,  # From good.type
        "good_category_id": str,  # From good.category_id
        "good_manufacturer_id": str,  # From good.manufacturer_id
        "good_name": str,  # From good.name
        "good_upc_code": str,  # From good.upc_code
        "good_barcode": str,  # From good.barcode
        "eft_provider_id": str,  # From eft_provider.id
        "eft_provider_name": str,  # From eft_provider.name
        "eft_authorizer_id": str,  # From eft_authorizer.id
        "eft_authorizer_name": str,  # From eft_authorizer.name
        "eft_card_brand_id": str,  # From eft_card_brand.id
        "eft_card_brand_name": str,  # From eft_card_brand.name
        "eft_card_type_id": str,  # From eft_card_type.id
        "eft_card_type_name": str,  # From eft_card_type.name
        "cashless_error_complete_description": str,
        "payment_authorizer_id": str,  # From payment_authorizer.id
        "payment_authorizer_name": str,  # From payment_authorizer.name
        # 'occurred_at' will be inferred as TIMESTAMP
    },
}

# Define columns that are genuinely NUMERIC for each table.
# These will be attempted as FLOAT64 by default in build_bq_schema, adjust if INTEGER/BIGNUMERIC needed.
TABLE_NUMERIC_COLUMNS = {
    "products": [
        "weight",
        "cost_price",
        "default_price",
        "vendible_balance",
        "packing_quantity",  # From packing.quantity
    ],
    "locations": ["latitude", "longitude"],
    "cashless": [
        "number_of_payments",
        "quantity",
        "value",
        "discount_value",
        "cost_price",
        "request_number",  # If this can contain non-numeric chars or is very large, consider STRING
    ],
}

TABLE_DATE_COLUMNS = {
    "products": ["created_at", "updated_at"],
    "cashless": ["occurred_at"],
}
# --- End User Configuration Section ---

# Function to sanitize column names (e.g., replace '.' with '_')


def sanitize_columns(df):
    """Replaces dots in DataFrame column names with underscores."""
    df.columns = [col.replace(".", "_") for col in df.columns]
    # Also sanitize keys in the global config dicts if they contain dots (though not typical for table names)
    global TABLE_STRING_COLUMNS, TABLE_NUMERIC_COLUMNS
    # This part is more relevant if column names *within* the dicts had dots, which they shouldn't.
    # The primary sanitization is for df.columns.
    return df


# Function to build a BigQuery schema based on DataFrame dtypes and explicit definitions


def build_bq_schema(df, table_name):
    """Builds a list of bigquery.SchemaField objects for table creation."""
    bq_schema = []
    # Get the specific string and numeric column definitions for the current table
    string_cols_for_table = TABLE_STRING_COLUMNS.get(table_name, {})
    numeric_cols_for_table = TABLE_NUMERIC_COLUMNS.get(table_name, [])

    for column_name in df.columns:
        field_type = "STRING"  # Default to STRING for safety

        # Priority 1: Explicitly defined STRING columns
        if column_name in string_cols_for_table:
            field_type = "STRING"
        # Priority 2: Explicitly defined NUMERIC columns
        elif column_name in numeric_cols_for_table:
            # Defaulting to FLOAT64; adjust to INTEGER or BIGNUMERIC as needed here
            field_type = "FLOAT64"
        # Priority 3: Infer from Pandas dtype for other columns
        else:
            current_dtype = df[column_name].dtype
            if pd.api.types.is_integer_dtype(current_dtype):
                field_type = "INTEGER"
            elif pd.api.types.is_float_dtype(current_dtype):
                field_type = "FLOAT64"
            elif pd.api.types.is_bool_dtype(current_dtype):
                field_type = "BOOLEAN"
            elif pd.api.types.is_datetime64_any_dtype(current_dtype) or (
                df[column_name]
                .dropna()
                .astype(str)
                .str.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z?$")
                .all()
                and not df[column_name].dropna().empty
            ):
                # Check if it's datetime or looks like an ISO timestamp string
                field_type = "TIMESTAMP"
            # else it remains the default STRING

        bq_schema.append(bigquery.SchemaField(column_name, field_type))
    print(f"Generated schema for table '{table_name}': {bq_schema}")
    return bq_schema


# Function to create a table in BigQuery if it doesn't exist, using an explicit schema


def create_table_if_not_exists(table_id: str, schema: list):
    """Creates a BigQuery table with the given schema if it does not already exist."""
    try:
        client.get_table(table_id)  # Check if table exists
        print(f"Table {table_id} already exists.")
    except NotFound:
        print(f"Table {table_id} not found. Creating with explicit schema...")
        table_ref = bigquery.Table(table_id, schema=schema)
        client.create_table(table_ref)  # API request to create the table
        print(f"Table {table_id} created successfully with explicit schema.")


# Main function to upload data from a CSV and merge it into a BigQuery table


def upload_and_merge_table(
    table_name: str, csv_path: str, id_column: str = "id"
):
    """Loads data from CSV to a temp table, then merges into the final BigQuery table."""
    # Define BigQuery table IDs
    temp_table_id = f"{DATASET_ID}.temp_{table_name}"
    final_table_id = f"{DATASET_ID}.{table_name}"

    # Read CSV: Specify dtypes for critical string columns to ensure correct parsing by Pandas
    # This uses the TABLE_STRING_COLUMNS configuration for the current table
    specific_dtypes_for_pd_read = TABLE_STRING_COLUMNS.get(table_name, {})
    date_columns_for_table = TABLE_DATE_COLUMNS.get(table_name, [])

    print(
        f"Reading CSV '{csv_path}' with specific dtypes for strings: {specific_dtypes_for_pd_read}"
    )
    # It's good practice to also handle potential parsing issues for dates here if needed, using parse_dates in pd.read_csv
    df = pd.read_csv(
        csv_path,
        low_memory=False,
        dtype=specific_dtypes_for_pd_read,
        parse_dates=date_columns_for_table,
    )

    df = sanitize_columns(df)

    string_cols_to_convert_final_names = TABLE_STRING_COLUMNS.get(
        table_name, {}
    )
    print(
        f"Applying explicit string conversion for columns: {list(string_cols_to_convert_final_names.keys())}"
    )
    for col_name_final in string_cols_to_convert_final_names.keys():
        if col_name_final in df.columns:
            df[col_name_final] = df[col_name_final].fillna("").astype(str)
        else:
            print(
                f"Warning: Column '{col_name_final}' for table '{table_name}' not found in Df after satize_columns()."
            )

    # Sanitize column names (e.g. replace '.' with '_')

    # Validate that the ID column exists in the DataFrame after sanitization
    if id_column not in df.columns:
        # Get original column names for error message
        original_cols = pd.read_csv(csv_path, nrows=0).columns.tolist()
        raise ValueError(
            f"ID column '{id_column}' not found in DataFrame from '{csv_path}' after sanitizing. Original columns: {original_cols}. Sanitized columns: {df.columns.tolist()}"
        )

    # Build the BigQuery schema based on the DataFrame and explicit definitions
    bq_schema = build_bq_schema(df, table_name)

    # Ensure the final table exists with the correct schema.
    # Note: This function will only create it if it's missing.
    # If it exists with a *different* schema, this won't alter it. Schema changes to existing tables need manual ALTER TABLE or recreation.
    create_table_if_not_exists(final_table_id, schema=bq_schema)

    # Configure and run the job to load DataFrame into the temporary BigQuery table
    # This uses the explicit schema, not autodetect.
    job_config_df_load = bigquery.LoadJobConfig(
        schema=bq_schema,
        write_disposition="WRITE_TRUNCATE",  # Overwrite the temp table each time
    )
    print(f"Loading data into temporary table '{temp_table_id}'...")
    client.load_table_from_dataframe(
        df, temp_table_id, job_config=job_config_df_load
    ).result()  # Wait for the job to complete
    print(f"Data loaded successfully into temporary table '{temp_table_id}'.")

    # Construct the MERGE SQL statement
    # Ensure column names are quoted with backticks if they might conflict with SQL keywords or contain special characters
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
      UPDATE SET
        {set_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_columns})
      VALUES ({insert_values})
    """
    print(f"Executing MERGE SQL for table '{table_name}'...")
    # print(f"Merge SQL: {merge_sql}") # Uncomment to debug the MERGE statement
    try:
        query_job = client.query(merge_sql)
        query_job.result()  # Wait for the MERGE job to complete
        print(
            f"Table '{table_name}' merged successfully. {query_job.num_dml_affected_rows if query_job.num_dml_affected_rows is not None else 'Number of affected rows not available.'}"
        )
    except Exception as e:
        print(f"Error during MERGE operation for table {table_name}: {e}")
        # You might want to log query_job.errors if available
        raise  # Re-raise the exception to stop the process if a merge fails

    # Delete the temporary table after a successful merge
    client.delete_table(temp_table_id, not_found_ok=True)
    print(f"Temporary table '{temp_table_id}' deleted.")


# Function to orchestrate the upload for all defined tables


def upload_all_tables_v2():  # Renamed to differentiate from user's original
    """Orchestrates the CSV upload and merge process for all specified tables."""
    # User needs to adjust CSV paths and ID columns according to their project structure
    tables_to_upload = {
        "products": ("data/raw_csv_outputs/products.csv", "id"),
        "categories": ("data/raw_csv_outputs/categories.csv", "id"),
        "manufacturers": ("data/raw_csv_outputs/manufacturers.csv", "id"),
        "locations": ("data/raw_csv_outputs/locations.csv", "id"),
        "clients": ("data/raw_csv_outputs/clients.csv", "id"),
        "cashless": ("data/cashless_facts_full.csv", "transaction_id"),
    }

    for table_name, (path, id_column) in tables_to_upload.items():
        print(f"--- Processing table: {table_name} from CSV: {path} --- ")
        if not os.path.exists(path):
            print(
                f"❗ CSV file not found: {path}. Skipping table {table_name}."
            )
            continue
        try:
            upload_and_merge_table(table_name, path, id_column)
            print(f"✅ Successfully processed and merged table: {table_name}")
        except Exception as e:
            print(f"❌ Error processing table {table_name} from {path}: {e}")
            # Consider adding more detailed error logging or specific handling here
            # For a full run, you might want to decide if one error stops all, or if it continues.
        print(f"--- Finished processing table: {table_name} ---\n")


# Main execution block
if __name__ == "__main__":
    # To run this script:
    # 1. Ensure you have a .env file with BIGQUERY_DATASET_ID set.
    # 2. Ensure the CSV files are at the paths specified in `tables_to_upload`.
    # 3. Adjust `TABLE_STRING_COLUMNS` and `TABLE_NUMERIC_COLUMNS` based on your data.
    # 4. Uncomment the line below to execute the upload process.

    print("Starting CSV upload process with explicit schema handling...")
    upload_all_tables_v2()
    print("CSV upload process script finished.")
