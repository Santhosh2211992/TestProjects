import qrcode

# Data you want to encode
data = "64303-K0L-D000"

# Generate QR code
qr = qrcode.QRCode(
    version=1,  # controls the size of the QR Code (1 = smallest, 40 = largest)
    error_correction=qrcode.constants.ERROR_CORRECT_H,  # higher error correction = more robust
    box_size=10,  # size of each box in pixels
    border=4,  # thickness of border (minimum is 4)
)
qr.add_data(data)
qr.make(fit=True)

# Create an image from the QR Code instance
img = qr.make_image(fill_color="black", back_color="white")

# Save the QR code image
img.save("BRACKET FR NUMBER PLATE.png")

print("QR code saved as qrcode.png")
