import pandas as pd
from supabase import create_client, Client
import math

# -------------------------------
# CONFIGURATION
# -------------------------------
INPUT_CSV = "/home/santhosh/Projects/Test_Projects/Test1/PremierProject/PremierData/docTR/doctr_example/output.csv"

# Supabase credentials
SUPABASE_URL = "http://localhost:54321"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyAgCiAgICAicm9sZSI6ICJzZXJ2aWNlX3JvbGUiLAogICAgImlzcyI6ICJzdXBhYmFzZS1kZW1vIiwKICAgICJpYXQiOiAxNjQxNzY5MjAwLAogICAgImV4cCI6IDE3OTk1MzU2MDAKfQ.DaYlNEoUrrEn2Ig7tqibS-PHK5vgusbcbo7X36XVt4Q"

TABLE_NAME = "bin_part_weight_db"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# -------------------------------
# LOAD CSV
# -------------------------------
df = pd.read_csv(INPUT_CSV)

# Optional: convert numeric columns
numeric_cols = [
    "SL NO",
    "PART NAME",
    "PART NUMBER",
    "MODEL",
    "PART WEIGHT",
    "BIN & COVER WEIGHT",
    "COVER QTY",
    "BIN QTY",
    "BIN WEIGHT",
    "COVR QTY VARIATION"]

for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")  # converts empty strings to NaN

def sanitize_numeric(value):
    try:
        f = float(value)
        if math.isfinite(f):
            return f
        else:
            return None  # replaces inf, -inf
    except:
        return None  # non-numeric → None

for col in numeric_cols:
    df[col] = df[col].apply(sanitize_numeric)

df = df.where(pd.notnull(df), None)

# -------------------------------
# UPLOAD TO SUPABASE
# -------------------------------
for idx, row in df.iterrows():
    data = row.to_dict()
    try:
        supabase.table(TABLE_NAME).insert(data).execute()
        print(f"Inserted row {idx+1}")
    except Exception as e:
        print(f"Failed to insert row {idx+1}: {e}")

print("\n✅ All rows processed")
