import serial
import time
import re

def parse_weight(raw: str):
    # Strip control chars like STX/ETX
    cleaned = ''.join(ch for ch in raw if ch.isprintable())

    # Debug
    # print(f"raw: {repr(raw)} | cleaned: {repr(cleaned)}")

    # Extract number with regex
    match = re.search(r"[-+]?\d+\.\d+", cleaned)
    if match:
        return float(match.group())
    else:
        print(f"Parse error: {repr(cleaned)}")
        return None

# ---- Config ----
PORT = "/dev/ttyUSB0"  # or "COM3" on Windows
BAUD = 9600
STABLE_SECONDS = 5      # how long to wait before confirming stability
TOLERANCE = 0.001        # small fluctuations ignored (kg)

ser = serial.Serial(
    port=PORT,
    baudrate=BAUD,
    bytesize=serial.EIGHTBITS,
    parity=serial.PARITY_NONE,
    stopbits=serial.STOPBITS_ONE,
    timeout=1
)

print(f"Listening on {ser.port} ...")

last_weight = None
stable_start_time = None
stable_reported = False

try:
    while True:
        line = ser.readline().decode(errors="ignore").strip()
        if line:
            # print(line)
            weight = parse_weight(line)
            if weight is not None:
                # print(f"Weight: {weight}")
                if last_weight is None or abs(weight - last_weight) > TOLERANCE:
                    # weight changed significantly → reset stability tracking
                    print(f"Weight changed → {weight} kg")
                    last_weight = weight
                    stable_start_time = time.time()
                    stable_reported = False
                else:
                    # weight is within tolerance → candidate for stability
                    if stable_start_time is None:
                        stable_start_time = time.time()

                    elapsed = time.time() - stable_start_time
                    if elapsed >= STABLE_SECONDS and not stable_reported:
                        print(f"Weight stable for {STABLE_SECONDS}s → {weight} kg")
                        stable_reported = True

except KeyboardInterrupt:
    print("Stopped by user")
finally:
    ser.close()
