import pdfplumber
import re

# -------------------------------
# CONFIGURATION
# -------------------------------
PDF_FILE = "/home/santhosh/Projects/Test_Projects/Test1/PremierProject/PremierData/BIN PART WEIGHT DETAILS (1).pdf"

# -------------------------------
# PDF PARSER
# -------------------------------
def parse_pdf_table(pdf_path):
    data_rows = []
    sl_counter = 1

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue

            lines = text.splitlines()
            print(lines)

            for line in lines:
                # Example line:
                # "FR NUMBER PLATE 603-K0L-00 KOL 1.22 2.24 57 51 50 2.47"
                if re.search(r"\d{3}-[A-Z0-9-]+", line):
                    tokens = line.split()
                    # print(f"Tokens: {tokens}")

                    try:
                        part_name = " ".join(tokens[0:-7])  # everything before part number
                        part_number = tokens[-7]
                        model = tokens[-6]
                        part_weight = float(tokens[-5])
                        bin_cover_weight = float(tokens[-4])
                        bin_qty = int(tokens[-3])
                        bin_weight = float(tokens[-2])
                        cover_qty_variation = tokens[-1]

                        row = {
                            "sl_no": sl_counter,
                            "part_name": part_name,
                            "part_pic": None,  # placeholder for image
                            "part_number": part_number,
                            "model": model,
                            "part_weight": part_weight,
                            "bin_cover_weight": bin_cover_weight,
                            "bin_qty": bin_qty,
                            "bin_weight": bin_weight,
                            "cover_qty": None,     # adjust if parsed separately
                            "variation": cover_qty_variation,  # placeholder
                        }
                        data_rows.append(row)
                        sl_counter += 1
                    except Exception as e:
                        print(f"Skipping line (parse error): {line}")
                        print(e)

    return data_rows


if __name__ == "__main__":
    rows = parse_pdf_table(PDF_FILE)
    if rows:
        print(f"Extracted {len(rows)} rows:")
        for r in rows:
            print(r)
    else:
        print("No valid rows found in PDF ❌")
