import os
from doctr.io import DocumentFile
from doctr.models import ocr_predictor

# -------------------------------
# CONFIGURATION
# -------------------------------
PDF_FILE = "/home/santhosh/Projects/Test_Projects/Test1/PremierProject/PremierData/BIN PART WEIGHT DETAILS (1).pdf"

# Load doctr model (detector + recognizer)
model = ocr_predictor(pretrained=True)

# Load PDF (doctr handles PDFs by converting pages into images internally)
doc = DocumentFile.from_pdf(PDF_FILE)

# Run OCR
result = model(doc)

# -------------------------------
# PROCESS OUTPUT
# -------------------------------
# result.pages -> list of Page objects, each with blocks/lines/words
for page_idx, page in enumerate(result.pages, start=1):
    print(f"\n--- Page {page_idx} ---")
    for block in page.blocks:
        for line in block.lines:
            # Each line has multiple words
            line_text = " ".join([word.value for word in line.words])
            print(line_text)
