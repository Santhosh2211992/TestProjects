import qrcode
import random
import string

def generate_sku(prefix="SKU", length=8):
    """
    Generate a SKU string.
    Example: SKU-4G7H9K2L
    """
    random_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
    return f"{prefix}-{random_part}"

# Generate a new SKU
sku = generate_sku()
print("Generated SKU:", sku)

# Generate QR Code for SKU
qr = qrcode.QRCode(
    version=1,
    error_correction=qrcode.constants.ERROR_CORRECT_M,
    box_size=10,
    border=4,
)
qr.add_data(sku)
qr.make(fit=True)

img = qr.make_image(fill_color="black", back_color="white")

# Save as file
filename = f"{sku}.png"
img.save(filename)

print(f"QR code saved as {filename}")
