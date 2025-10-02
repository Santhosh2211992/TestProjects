# qrscanner.py

from evdev import InputDevice, list_devices, categorize, ecodes


class QRScanner:
    def __init__(self, keyword: str = "USBScn"):
        self.keyword = keyword
        self.device = self._find_scanner()

    def _find_scanner(self):
        """Automatically find the scanner device by keyword."""
        for path in list_devices():
            dev = InputDevice(path)
            if self.keyword.lower() in dev.name.lower():
                return dev
        return None

    def listen(self, callback=None):
        """
        Listen for QR code scans.
        - If callback is provided, call it with each scanned QR code.
        - Otherwise, print the scanned QR codes.
        """
        if not self.device:
            print("Scanner not found!")
            return

        print(f"Listening for QR codes on: {self.device.name} ({self.device.path})")
        scanned = ""

        try:
            for event in self.device.read_loop():
                if event.type == ecodes.EV_KEY:
                    data = categorize(event)
                    if data.keystate == 1:  # key down
                        key = data.keycode.replace("KEY_", "")
                        if key == "ENTER":
                            if scanned:
                                if callback:
                                    callback(scanned)
                                else:
                                    print("QR Code:", scanned)
                                scanned = ""
                        else:
                            scanned += key
        except KeyboardInterrupt:
            print("\nExiting QR scanner...")


def main():
    """CLI entrypoint for running the scanner directly."""
    scanner = QRScanner()
    scanner.listen()


if __name__ == "__main__":
    main()
