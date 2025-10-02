from doctr.io import DocumentFile
from doctr.models import ocr_predictor

model = ocr_predictor(pretrained=True)
# PDF
doc = DocumentFile.from_pdf("/home/santhosh/Projects/Test_Projects/Test1/PremierProject/PremierData/BIN PART WEIGHT DETAILS (1).pdf")
# Analyze
result = model(doc)
print(result)