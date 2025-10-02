# qrcode_generator.py

import qrcode
from PIL import Image, ImageDraw, ImageFont


class QRCodeGenerator:
    def __init__(self, font_path: str = "arial.ttf", font_size: int = 24):
        self.font_path = font_path
        self.font_size = font_size

    def _load_font(self):
        try:
            return ImageFont.truetype(self.font_path, self.font_size)
        except Exception:
            return ImageFont.load_default()

    def _get_text_size(self, draw, text, font):
        """Helper to measure text size (works across Pillow versions)."""
        try:
            bbox = draw.textbbox((0, 0), text, font=font)
            return bbox[2] - bbox[0], bbox[3] - bbox[1]
        except AttributeError:
            return draw.textsize(text, font=font)

    def generate(
        self,
        data: str,
        title: str = "",
        part_number: str = "",
        output_file: str = "qrcode_with_text.png",
    ):
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
        extra_height = 100
        new_img = Image.new("RGB", (qr_img.width, qr_img.height + extra_height), "white")
        new_img.paste(qr_img, (0, extra_height))

        draw = ImageDraw.Draw(new_img)
        font = self._load_font()

        if title:
            title_w, title_h = self._get_text_size(draw, title, font)
            draw.text(((qr_img.width - title_w) // 2, 5), title, font=font, fill="black")

        if part_number:
            part_w, part_h = self._get_text_size(draw, part_number, font)
            draw.text(
                ((qr_img.width - part_w) // 2, extra_height - part_h - 5),
                part_number,
                font=font,
                fill="black",
            )

        # ----------------------
        # Save final image
        # ----------------------
        new_img.save(output_file)
        return output_file
    
# ✅ Usage in another script
# from qrcode_generator import QRCodeGenerator

# generator = QRCodeGenerator(font_path="arial.ttf", font_size=24)
# file_path = generator.generate(
#     data="81136 K0Y DR00",
#     title="LID USB CHARGER",
#     part_number="Part No: 81136 K0Y DR00",
#     output_file="lid_usb_charger.png"
# )

# print(f"QR code saved at {file_path}")
