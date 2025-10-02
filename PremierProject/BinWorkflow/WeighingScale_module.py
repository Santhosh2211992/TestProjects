# scale_reader.py
import serial
import time
import re
import threading

class WeighingScale:
    def __init__(self, port="/dev/ttyUSB0", baud=9600, stable_seconds=2, tolerance=0.001, timeout=1, stable_callback=None):
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
        self._running = False  # Control flag

        print(f"Listening on {self.ser.port} ...")

    @staticmethod
    def parse_weight(raw: str):
        cleaned = ''.join(ch for ch in raw if ch.isprintable())
        match = re.search(r"[-+]?\d+\.\d+", cleaned)
        if match:
            return float(match.group())
        else:
            return None

    def read_weight(self):
        line = self.ser.readline().decode(errors="ignore").strip()
        if not line:
            return None
        return self.parse_weight(line)

    def monitor(self):
        """ Continuously monitor the scale until self._running is False """
        self._running = True
        try:
            while self._running:
                weight = self.read_weight()
                if weight is None:
                    continue

                # Weight changed
                if self.last_weight is None or abs(weight - self.last_weight) > self.tolerance:
                    self.last_weight = weight
                    self.stable_start_time = time.time()
                    self.stable_reported = False
                else:
                    if self.stable_start_time is None:
                        self.stable_start_time = time.time()

                    elapsed = time.time() - self.stable_start_time
                    if elapsed >= self.stable_seconds and not self.stable_reported:
                        self.stable_reported = True
                        if self.stable_callback:
                            self.stable_callback(weight)
                            # Stop monitoring from callback by main thread

        finally:
            pass
            # self.ser.close() # Do not close serial as you're simply restarting monitor function whenever you need a stable weight

    def stop(self):
        """Stop monitoring from the main thread"""
        self._running = False


if __name__ == "__main__":
    import time

    def on_stable(weight):
        print(f"Stable weight detected: {weight} kg")
        # We could set an event or just let main call stop()

    scale = WeighingScale(port="/dev/ttyUSB0", stable_seconds=2, stable_callback=on_stable)

    # Run monitor in a separate thread
    t = threading.Thread(target=scale.monitor)
    t.start()

    try:
        # Main thread waits until stable weight is detected
        while True:
            if scale.stable_reported:
                print("Stopping monitoring from main thread...")
                scale.stop()
                break
            time.sleep(0.1)
    except KeyboardInterrupt:
        scale.stop()

    t.join()
    print("Monitoring stopped.")
