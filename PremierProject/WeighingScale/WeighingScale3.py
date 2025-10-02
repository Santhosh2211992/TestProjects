# scale_reader.py
import serial
import time
import re

class SerialScale:
    def __init__(self, port="/dev/ttyUSB0", baud=9600, stable_seconds=2, tolerance=0.001, timeout=1, stable_callback=None):
        """
        Initialize the serial scale.
        :param port: Serial port (e.g., "/dev/ttyUSB0" or "COM3")
        :param baud: Baud rate (default 9600)
        :param stable_seconds: Duration to consider weight stable
        :param tolerance: Minimum change to consider weight changed
        :param timeout: Serial read timeout in seconds
        :param stable_callback: Function to call when weight stabilizes
        """
        self.port = port
        self.baud = baud
        self.stable_seconds = stable_seconds
        self.tolerance = tolerance
        self.timeout = timeout
        self.stable_callback = stable_callback

        self.ser = serial.Serial(
            port=self.port,
            baudrate=self.baud,
            bytesize=serial.EIGHTBITS,
            parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
            timeout=self.timeout
        )

        self.last_weight = None
        self.stable_start_time = None
        self.stable_reported = False

        print(f"Listening on {self.ser.port} ...")

    @staticmethod
    def parse_weight(raw: str):
        """
        Extract numeric weight from raw string.
        """
        cleaned = ''.join(ch for ch in raw if ch.isprintable())
        match = re.search(r"[-+]?\d+\.\d+", cleaned)
        if match:
            return float(match.group())
        else:
            print(f"Parse error: {repr(cleaned)}")
            return None

    def read_weight(self):
        """
        Read a line from serial and parse weight.
        """
        line = self.ser.readline().decode(errors="ignore").strip()
        if not line:
            return None
        return self.parse_weight(line)

    def monitor(self):
        """
        Continuously monitor the scale and call stable_callback when weight stabilizes.
        """
        try:
            while True:
                weight = self.read_weight()
                if weight is None:
                    continue

                # Weight changed
                if self.last_weight is None or abs(weight - self.last_weight) > self.tolerance:
                    self.last_weight = weight
                    self.stable_start_time = time.time()
                    self.stable_reported = False
                else:
                    # Candidate for stability
                    if self.stable_start_time is None:
                        self.stable_start_time = time.time()

                    elapsed = time.time() - self.stable_start_time
                    if elapsed >= self.stable_seconds and not self.stable_reported:
                        self.stable_reported = True
                        if self.stable_callback:
                            self.stable_callback(weight)

        except KeyboardInterrupt:
            print("Stopped by user")
        finally:
            self.ser.close()


if __name__ == "__main__":
    # Example callback
    def on_stable(weight):
        print(f"Stable weight detected: {weight} kg")

    scale = SerialScale(port="/dev/ttyUSB0", stable_seconds=2, stable_callback=on_stable)
    scale.monitor()
