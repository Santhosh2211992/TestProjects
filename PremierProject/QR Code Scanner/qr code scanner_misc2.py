from evdev import InputDevice, list_devices, categorize, ecodes

def find_scanner(keyword="USBScn"):
    """Automatically find the Hangzhou/USBScn scanner device."""
    for path in list_devices():
        dev = InputDevice(path)
        if keyword.lower() in dev.name.lower():
            return dev
    return None

scanner = find_scanner()
if not scanner:
    print("Scanner not found!")
    exit(1)

print(f"Listening for QR codes on: {scanner.name} ({scanner.path})")

scanned = ""
try:
    for event in scanner.read_loop():
        if event.type == ecodes.EV_KEY:
            data = categorize(event)
            if data.keystate == 1:  # key down
                key = data.keycode.replace("KEY_", "")
                if key == "ENTER":
                    if scanned:
                        print("QR Code:", scanned)
                        scanned = ""
                else:
                    scanned += key
except KeyboardInterrupt:
    print("\nExiting...")
