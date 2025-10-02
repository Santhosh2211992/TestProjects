from evdev import InputDevice, categorize, ecodes, list_devices

# List all input devices
devices = [InputDevice(path) for path in list_devices()]
for dev in devices:
    print(dev.path, dev.name, dev.phys)

# Choose your scanner's device path (e.g. '/dev/input/event3')
scanner = InputDevice('/dev/input/event3')

scanned = ""
print("Waiting for QR code...")

for event in scanner.read_loop():
    if event.type == ecodes.EV_KEY:
        data = categorize(event)
        if data.keystate == 1:  # Key down
            keycode = data.keycode.replace("KEY_", "")
            if keycode == "ENTER":
                print(f"QR Code: {scanned}")
                scanned = ""
            else:
                scanned += keycode
