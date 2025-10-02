from evdev import InputDevice, list_devices, categorize, ecodes

def find_scanner(keyword="barcode"):
    """Find input device containing the keyword in its name."""
    devices = [InputDevice(path) for path in list_devices()]
    for dev in devices:
        if keyword.lower() in dev.name.lower():
            return dev
    return None

scanner = find_scanner("barcode")  # change "barcode" → part of your scanner name
if not scanner:
    print("Scanner not found. Try changing keyword or run device list:")
    for d in [InputDevice(path) for path in list_devices()]:
        print(f"{d.path}: {d.name}")
    exit(1)

print(f"Using scanner: {scanner.name} at {scanner.path}")
print("Waiting for QR codes...")

scanned = ""
for event in scanner.read_loop():
    if event.type == ecodes.EV_KEY:
        data = categorize(event)
        if data.keystate == 1:  # Key down event
            key = data.keycode.replace("KEY_", "")
            if key == "ENTER":
                if scanned:
                    print(f"QR Code: {scanned}")
                    scanned = ""
            else:
                scanned += key
