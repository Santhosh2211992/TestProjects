import qrcode
from PIL import Image, ImageDraw, ImageFont

# ----------------------
# Input data
# ----------------------
data = "81136 K0Y DR00"
title = "LID USB CHARGER"
part_number = "Part No: 81136 K0Y DR00"

# ----------------------
# Generate QR code
# ----------------------
qr = qrcode.QRCode(
    version=1,
    error_correction=qrcode.constants.ERROR_CORRECT_H,
    box_size=10,
    border=4,
)
qr.add_data(data)
qr.make(fit=True)

qr_img = qr.make_image(fill_color="black", back_color="white").convert("RGB")

# ----------------------
# Add Title and Part Number
# ----------------------
extra_height = 100  # space for title + part number
new_img = Image.new("RGB", (qr_img.width, qr_img.height + extra_height), "white")
new_img.paste(qr_img, (0, extra_height))

draw = ImageDraw.Draw(new_img)

# Load font
try:
    font = ImageFont.truetype("arial.ttf", 24)
except:
    font = ImageFont.load_default()

# Helper to measure text size (handles new/old Pillow)
def get_text_size(draw, text, font):
    try:
        # Newer Pillow (textbbox returns (x0, y0, x1, y1))
        bbox = draw.textbbox((0, 0), text, font=font)
        return bbox[2] - bbox[0], bbox[3] - bbox[1]
    except AttributeError:
        # Older Pillow
        return draw.textsize(text, font=font)

# Title
title_w, title_h = get_text_size(draw, title, font)
draw.text(((qr_img.width - title_w) // 2, 5), title, font=font, fill="black")

# Part number
part_w, part_h = get_text_size(draw, part_number, font)
draw.text(((qr_img.width - part_w) // 2, extra_height - part_h - 5), part_number, font=font, fill="black")

# ----------------------
# Save final image
# ----------------------
new_img.save("LID USB CHARGER.png")
print("QR code saved as qrcode_with_text.png")
