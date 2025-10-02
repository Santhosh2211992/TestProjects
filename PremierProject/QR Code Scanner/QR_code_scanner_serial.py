import serial

# Configure your serial port
ser = serial.Serial(
    port='/dev/ttyACM0',  # or 'COM3' on Windows
    baudrate=9600,        # check your scanner settings
    timeout=1             # seconds
)

scanned = ""

try:
    while True:
        line = ser.readline().decode('utf-8', errors='ignore').strip()
        if line:  # if any data received
            scanned = line
            print("Scanned barcode:", scanned)

except KeyboardInterrupt:
    print("Exiting...")

finally:
    ser.close()
