# barcode_scanner.py
import serial
import threading

class QRcodeScanner:
    def __init__(self, port='/dev/ttyACM0', baudrate=9600, timeout=1):
        """
        Initialize the barcode scanner.

        Args:
            port (str): Serial port (e.g., '/dev/ttyACM0' or 'COM3').
            baudrate (int): Serial baud rate.
            timeout (float): Serial read timeout in seconds.
        """
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.scanned = ""
        self._running = False
        self._thread = None
        self._callback = None
        self.ser = None

    def start(self, callback=None):
        """
        Start reading from the scanner in a background thread.

        Args:
            callback (callable): Optional function to call with the scanned barcode.
        """
        self._callback = callback
        self._running = True
        self.ser = serial.Serial(port=self.port, baudrate=self.baudrate, timeout=self.timeout)
        self._thread = threading.Thread(target=self._read_loop, daemon=True)
        self._thread.start()

    def _read_loop(self):
        while self._running:
            if not self.ser or not self.ser.is_open:
                break  # Stop if port is closed
            try:
                line = self.ser.readline().decode('utf-8', errors='ignore').strip()
                if line:
                    self.scanned = line
                    if self._callback:
                        self._callback(line)
            except serial.SerialException as e:
                print("Serial error:", e)
                break
            except Exception as e:
                print("Unexpected error:", e)
                break

    def stop(self):
        """Stop reading from the scanner and close the serial port."""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1)
        if self.ser and self.ser.is_open:
            self.ser.close()

    def get_last_scanned(self):
        """Return the last scanned barcode."""
        return self.scanned


# Example usage
if __name__ == "__main__":
    def print_barcode(barcode):
        print("Scanned barcode:", barcode)

    scanner = QRcodeScanner(port='/dev/ttyACM0', baudrate=9600)
    scanner.start(callback=print_barcode)

    try:
        while True:
            pass  # Keep main thread alive
    except KeyboardInterrupt:
        print("Exiting...")
        scanner.stop()
