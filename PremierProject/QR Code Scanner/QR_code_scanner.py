# qr_scanner.py

def scan_qr():
    """
    Simple QR code scanner function that uses keyboard input.
    The scanner emulates a keyboard and sends the QR code followed by Enter.
    """
    print("Scan a QR code (Ctrl+C to exit):")
    while True:
        try:
            code = input().strip()
            if code:
                print("QR Code:", code)
        except KeyboardInterrupt:
            print("\nExiting scanner...")
            break


def main():
    """Entry point for standalone use."""
    scan_qr()


if __name__ == "__main__":
    main()
