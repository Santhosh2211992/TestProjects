# qr_scanner.py
import threading

class QRScanner:
    def __init__(self, callback):
        self.callback = callback
        self._running = False
        self._thread = None

    def _scan_qr(self):
        print("Scan a QR code (Ctrl+C to exit):")
        while self._running:
            try:
                code = input().strip()
                if code:
                    print(f"scanned code: {code}")
                    self.callback(code)
            except EOFError:
                # input stream closed
                break
            except KeyboardInterrupt:
                print("\nScanner stopped by user.")
                break

    def start(self):
        if not self._running:
            self._running = True
            self._thread = threading.Thread(target=self._scan_qr, daemon=True)
            self._thread.start()

    def stop(self):
        """Stop the scanner thread gracefully."""
        if self._running:
            self._running = False
            print("Stopping QR scanner...")
            # Only join if we are NOT in the scanner thread
            if threading.current_thread() != self._thread and self._thread:
                self._thread.join(timeout=1)
