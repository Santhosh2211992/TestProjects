"""
Professional QR Scanner MQTT Service
Event-driven architecture with proper MQTT communication
"""

import serial
import threading
import json
import time
import paho.mqtt.client as mqtt
from datetime import datetime
from typing import Optional, Callable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QRScannerMQTTService:
    """
    QR/Barcode scanner service that communicates via MQTT.
    Publishes scan events instead of using callbacks.
    """
    
    def __init__(
        self,
        device_id: str,
        port: str = '/dev/ttyACM0',
        baudrate: int = 9600,
        broker: str = "localhost",
        mqtt_port: int = 1883,
        timeout: float = 1.0
    ):
        self.device_id = device_id
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        
        # Serial connection
        self.ser: Optional[serial.Serial] = None
        
        # State
        self.last_scanned: Optional[str] = None
        self._running = False
        self._scan_thread: Optional[threading.Thread] = None
        
        # MQTT
        self.broker = broker
        self.mqtt_port = mqtt_port
        self.client = mqtt.Client(client_id=f"qr_{device_id}")
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.connected = False
        
        # Topics
        self.topic_data = f"factory/qr/{device_id}/data"
        self.topic_status = f"factory/qr/{device_id}/status"
        self.topic_cmd = f"factory/qr/{device_id}/cmd/#"
        self.topic_error = f"factory/qr/{device_id}/error"
        
    def _on_connect(self, client, userdata, flags, rc):
        """MQTT connection callback"""
        if rc == 0:
            logger.info(f"QR Scanner {self.device_id} connected to MQTT")
            self.connected = True
            self.client.subscribe(self.topic_cmd)
            self._publish_status("ready")
        else:
            logger.error(f"Failed to connect: {rc}")
            self.connected = False
            
    def _on_message(self, client, userdata, msg):
        """Handle incoming commands"""
        try:
            command = msg.topic.split("/")[-1]
            payload = json.loads(msg.payload.decode()) if msg.payload else {}
            
            logger.info(f"Received command: {command}")
            
            if command == "start_scan":
                self.start_scanning()
            elif command == "stop_scan":
                self.stop_scanning()
            elif command == "get_status":
                self._publish_status("scanning" if self._running else "idle")
            elif command == "get_last":
                if self.last_scanned:
                    self._publish_data(self.last_scanned, cached=True)
            else:
                logger.warning(f"Unknown command: {command}")
                
        except Exception as e:
            logger.error(f"Error processing command: {e}")
            self._publish_error(f"Command error: {e}")
    
    def connect(self):
        """Connect to MQTT broker and serial port"""
        try:
            # MQTT connection
            self.client.connect(self.broker, self.mqtt_port, 60)
            self.client.loop_start()
            
            # Wait for MQTT
            timeout_wait = 5
            start = time.time()
            while not self.connected and (time.time() - start) < timeout_wait:
                time.sleep(0.1)
                
            if not self.connected:
                raise Exception("MQTT connection timeout")
            
            # Serial connection
            self.ser = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                timeout=self.timeout
            )
            
            logger.info(f"QR Scanner {self.device_id} ready on {self.port}")
            self._publish_status("ready")
            return True
            
        except Exception as e:
            logger.error(f"Connection error: {e}")
            self._publish_error(f"Connection failed: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from MQTT and serial"""
        self.stop_scanning()
        
        if self.ser and self.ser.is_open:
            self.ser.close()
            
        self.client.loop_stop()
        self.client.disconnect()
        logger.info(f"QR Scanner {self.device_id} disconnected")
    
    def start_scanning(self):
        """Start continuous scanning in background thread"""
        if self._running:
            logger.warning("Scanner already running")
            return
            
        self._running = True
        self._scan_thread = threading.Thread(target=self._scan_loop, daemon=True)
        self._scan_thread.start()
        logger.info("Started QR scanning")
        self._publish_status("scanning")
    
    def stop_scanning(self):
        """Stop scanning"""
        if not self._running:
            return
            
        self._running = False
        if self._scan_thread:
            self._scan_thread.join(timeout=2)
            self._scan_thread = None
            
        logger.info("Stopped QR scanning")
        self._publish_status("idle")
    
    def _scan_loop(self):
        """Continuous scanning loop"""
        while self._running:
            if not self.ser or not self.ser.is_open:
                logger.error("Serial port not available")
                break
                
            try:
                line = self.ser.readline().decode('utf-8', errors='ignore').strip()
                
                if line:
                    # Valid scan
                    self.last_scanned = line
                    self._publish_data(line)
                    logger.info(f"Scanned: {line}")
                    
            except serial.SerialException as e:
                logger.error(f"Serial error: {e}")
                self._publish_error(f"Serial error: {e}")
                break
            except Exception as e:
                logger.error(f"Scan error: {e}")
                self._publish_error(f"Scan error: {e}")
                time.sleep(0.5)
    
    def _publish_data(self, qr_code: str, cached: bool = False):
        """Publish scanned QR code"""
        if not self.connected:
            return
            
        payload = {
            "msg_type": "data",
            "timestamp": datetime.now().isoformat(),
            "device_id": self.device_id,
            "qr_code": qr_code,
            "cached": cached
        }
        
        self.client.publish(self.topic_data, json.dumps(payload), qos=1)
    
    def _publish_status(self, status: str, details: dict = None):
        """Publish status update"""
        if not self.connected:
            return
            
        payload = {
            "msg_type": "status",
            "timestamp": datetime.now().isoformat(),
            "device_id": self.device_id,
            "status": status,
            "details": details or {},
            "last_scanned": self.last_scanned
        }
        
        self.client.publish(self.topic_status, json.dumps(payload), qos=1)
    
    def _publish_error(self, error_msg: str):
        """Publish error"""
        payload = {
            "msg_type": "error",
            "timestamp": datetime.now().isoformat(),
            "device_id": self.device_id,
            "error_msg": error_msg
        }
        
        self.client.publish(self.topic_error, json.dumps(payload), qos=1)


# ==========================================
# MAIN
# ==========================================

if __name__ == "__main__":
    import sys
    
    DEVICE_ID = "qr_scanner_01"
    PORT = "/dev/qr_code_reader"
    BROKER = "localhost"
    
    scanner_service = QRScannerMQTTService(
        device_id=DEVICE_ID,
        port=PORT,
        broker=BROKER
    )
    
    if scanner_service.connect():
        logger.info("QR Scanner service running. Press Ctrl+C to exit.")
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            scanner_service.disconnect()
            sys.exit(0)
    else:
        logger.error("Failed to start QR scanner service")
        sys.exit(1)


# ==========================================
# TESTING
# ==========================================
"""
# Start service:
python qr_mqtt_service.py

# Test commands:

# Start scanning
mosquitto_pub -t factory/qr/qr_scanner_01/cmd/start_scan -m '{}'

# Stop scanning
mosquitto_pub -t factory/qr/qr_scanner_01/cmd/stop_scan -m '{}'

# Get last scanned code
mosquitto_pub -t factory/qr/qr_scanner_01/cmd/get_last -m '{}'

# Subscribe to all QR data
mosquitto_sub -t 'factory/qr/#' -v
"""