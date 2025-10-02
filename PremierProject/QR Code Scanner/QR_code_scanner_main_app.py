from QR_code_scanner_background_app import QRScanner
import time

TARGET_CODE = "64303-K0L-D000"  # the QR code that should stop the scanner

def handle_scan(code):
    print("Got:", code)
    if code == TARGET_CODE:
        print(f"Target code '{TARGET_CODE}' scanned. Stopping scanner...")
        scanner_obj.stop()  # stop the scanner thread

scanner_obj = QRScanner(handle_scan)
scanner_obj.start()

# Main loop can continue doing other work
try:
    while scanner_obj._running:
        # print("Main program working...")
        time.sleep(0.5)
finally:
    scanner_obj.stop()
    print("Exited cleanly.")

scanner_obj._thread.join()