# main_app.py
from QR_code_scanner_serial_module import QRcodeScanner
import time

class InventoryManager:
    def __init__(self, port='/dev/ttyACM0', expected_codes=None):
        """
        Args:
            port (str): Serial port for scanner
            expected_codes (list[str]): List of valid QRcodes
        """
        self.expected_codes = expected_codes or ["12345", "ABCDE", "XYZ789"]
        self.scanner = QRcodeScanner(port=port, baudrate=9600)
        self.current_code = None

    def start_scan(self):
        """Start scanning and pass callback."""
        print("Please scan a QRcode...")
        self.scanner.start(callback=self.handle_scan)

    def handle_scan(self, QRcode):
        """Callback function called by the scanner."""
        print(f"Scanned QRcode: {QRcode}")
        self.current_code = QRcode

    def stop_scanner(self):
        """Stop the QRcode scanner cleanly."""
        self.scanner.stop()


# Example usage
if __name__ == "__main__":
    # Initialize manager with expected codes
    manager = InventoryManager(port='/dev/ttyACM0', expected_codes=["ABCDE", "XYZ789"])
    
    # Start scanning
    manager.start_scan()  # assumes start_scan() in InventoryManager starts the QRcodeScanner

    try:
        while True:
            # Main thread just keeps alive
            time.sleep(1)

            # # Optionally, you can check if a valid code is scanned
            # if manager.current_code in manager.expected_codes:
            #     print(f"Valid QRcode scanned: {manager.current_code}")
            #     # Stop scanner from main thread
            #     manager.stop_scanner()
            #     break  # exit loop if you want to proceed


            if manager.current_code:
                print(f"Valid QRcode scanned: {manager.current_code}")
                # Stop scanner from main thread
                manager.stop_scanner()
                break  # exit loop if you want to proceed
    except KeyboardInterrupt:
        print("Exiting...")
        manager.stop_scanner()