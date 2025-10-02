import pandas as pd

# -------------------------------
# CONFIGURATION
# -------------------------------
INPUT_TXT = "/home/santhosh/Projects/Test_Projects/Test1/PremierProject/PremierData/docTR/doctr_example/extracted_data.txt"
OUTPUT_CSV = "/home/santhosh/Projects/Test_Projects/Test1/PremierProject/PremierData/docTR/doctr_example/output.csv"

# Define the 10 column headers
COLUMNS = [
    "SL NO",
    "PART NAME",
    "PART NUMBER",
    "MODEL",
    "PART WEIGHT",
    "BIN & COVER WEIGHT",
    "COVER QTY",
    "BIN QTY",
    "BIN WEIGHT",
    "COVR QTY VARIATION"
]

# -------------------------------
# TXT → TABLE
# -------------------------------
def txt_to_table(input_txt):
    with open(input_txt, "r", encoding="utf-8") as f:
        # Keep empty cells as blank strings
        lines = [line.rstrip() for line in f]

    rows = []
    for i in range(0, len(lines), len(COLUMNS)):
        chunk = lines[i:i+len(COLUMNS)]
        if len(chunk) == len(COLUMNS):
            rows.append(chunk)

    df = pd.DataFrame(rows, columns=COLUMNS)
    return df


if __name__ == "__main__":
    df = txt_to_table(INPUT_TXT)

    # Print in nice table format
    print(df.to_markdown(index=False))

    # Save to CSV
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\n✅ Saved {len(df)} rows to {OUTPUT_CSV}")
