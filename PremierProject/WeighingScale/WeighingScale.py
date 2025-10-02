import serial

# Configure according to your scale’s manual
ser = serial.Serial(
    port="/dev/ttyUSB0",  # or "COM3" on Windows
    baudrate=9600,
    bytesize=serial.EIGHTBITS,
    parity=serial.PARITY_NONE,
    stopbits=serial.STOPBITS_ONE,
    timeout=1  # seconds
)

print("Listening on", ser.port)

try:
    while True:
        line = ser.readline().decode(errors="ignore").strip()
        if line:
            print("Raw data:", line)
except KeyboardInterrupt:
    print("Stopped")
finally:
    ser.close()
