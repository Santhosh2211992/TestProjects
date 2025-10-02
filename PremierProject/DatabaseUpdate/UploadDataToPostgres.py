import psycopg2
import pandas as pd
import math
import numpy as np

# -----------------------------
# CONFIG
# -----------------------------
POSTGRES_HOST = "172.18.0.4"   # your Postgres container or host IP
POSTGRES_PORT = 5432
POSTGRES_DB = "postgres"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "your-super-secret-and-long-postgres-password"
TABLE_NAME = "bin_part_weight_db"
INPUT_CSV = "/home/santhosh/Projects/Test_Projects/Test1/PremierProject/PremierData/docTR/doctr_example/output.csv"

# -----------------------------
# LOAD CSV
# -----------------------------
df = pd.read_csv(INPUT_CSV)

# Sanitize numeric columns
numeric_cols = [
    "PART WEIGHT",
    "BIN & COVER WEIGHT",
    "BIN WEIGHT"
]

# Columns expected to be numeric
float_cols = ["PART WEIGHT"]
int_cols = ["SL NO", "COVER QTY", "BIN QTY", "COVR QTY VARIATION"]

# Sanitize floats
def sanitize_numeric(value):
    try:
        f = float(value)
        if math.isfinite(f):
            return f
        else:
            return None
    except:
        return None

for col in float_cols:
    df[col] = df[col].apply(sanitize_numeric)

# Sanitize integers
def sanitize_integer(value):
    try:
        # If value is float nan, return None
        if isinstance(value, float) and math.isnan(value):
            return 0
        val = int(float(value))  # handle float strings like "40.0"
        if val < -9223372036854775808 or val > 9223372036854775807:
            return 0
        return val
    except:
        return 0

for col in int_cols:
    df[col] = df[col].apply(sanitize_integer)

# Replace remaining NaN with None
df = df.where(pd.notnull(df), None)
# Replace numeric NaNs with None
df = df.applymap(lambda x: None if (isinstance(x, float) and np.isnan(x)) else x)

# Optional: also handle empty strings
df = df.applymap(lambda x: None if isinstance(x, str) and x.strip() == "" else x)

# For printing only: convert all np.nan to None explicitly
df_print = df.applymap(lambda x: None if (x is None or (isinstance(x, float) and np.isnan(x))) else x)

def to_none(x):
    if isinstance(x, float) and math.isnan(x):  # np.nan
        return None
    if x is pd.NA:  # pandas NA
        return None
    if isinstance(x, str) and x.strip() == "":  # empty string
        return None
    return x

df = df.applymap(to_none)

# Show all rows and columns when printing
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)
pd.set_option('display.max_colwidth', None)

# Example: df is your DataFrame
print(df)
# Example: df is your DataFrame
print(df_print)

# -----------------------------
# INSERT INTO POSTGRES
# -----------------------------
conn = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    dbname=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
)
cur = conn.cursor()

# Insert rows
# for idx, row in df.iterrows():
#     values = tuple(row[col] for col in df.columns)
#     placeholders = ", ".join(["%s"] * len(df.columns))
    
#     # Quote column names to handle spaces/special characters
#     quoted_columns = ", ".join([f'"{col}"' for col in df.columns])
#     sql = f'INSERT INTO {TABLE_NAME} ({quoted_columns}) VALUES ({placeholders})'
    
#     try:
#         cur.execute(sql, values)
#         print(f"Inserted row {idx+1}")
#     except Exception as e:
#         print(f"Failed to insert row {idx+1}: {e}")
for idx, row in df.iterrows():
    values = tuple(row[col] for col in df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    quoted_columns = ", ".join([f'"{col}"' for col in df.columns])
    sql = f'INSERT INTO {TABLE_NAME} ({quoted_columns}) VALUES ({placeholders})'
    
    try:
        cur.execute(sql, values)
        conn.commit()
        print(f"Inserted row {idx+1}")
    except Exception as e:
        conn.rollback()
        print(f"Failed to insert row {idx+1}: {e}")

conn.commit()
cur.close()
conn.close()
print("✅ All rows processed")

