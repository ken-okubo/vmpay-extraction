import pandas as pd
from glob import glob

# Paths
INPUT_DIR = "data/historical_cashless"
OUTPUT_PATH = "data/cashless_facts_full.csv"

# Step 1: Find all CSV files
csv_files = sorted(glob(f"{INPUT_DIR}/cashless_*.csv"))
print(f"Found {len(csv_files)} files")

# Step 2: Load and combine all
df_list = [pd.read_csv(f) for f in csv_files]
combined = pd.concat(df_list, ignore_index=True)

# Rename 'id' to 'transaction_id'
combined = combined.rename(columns={"id": "transaction_id"})

# Step 3: Ensure 'transaction_date' exists for sorting
if "occurred_at" not in combined.columns:
    raise ValueError("'occurred_at' column is required for sorting")

# Step 4: Sort and deduplicate by id, keeping the most recent transaction
combined = combined.sort_values(by="occurred_at")
before = len(combined)
combined = combined.drop_duplicates(subset=["transaction_id"], keep="last")
after = len(combined)

print(f"Dropped {before - after} duplicates (kept latest by id)")

# Step 5: Save to output
combined.to_csv(OUTPUT_PATH, index=False)
print(f"Final cashless table saved to: {OUTPUT_PATH}")
