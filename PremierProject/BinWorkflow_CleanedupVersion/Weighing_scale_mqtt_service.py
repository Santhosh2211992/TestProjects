"""
Professional Weighing Scale MQTT Service
Publishes weight data via MQTT instead of callbacks
"""

import serial
import time
import re
import threading
import json
import paho.mqtt.client as mqtt
from datetime import datetime
from typing import Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WeighingScaleMQTTService:
    """
    Weighing scale service that communicates via MQTT.
    Publishes weight readings and stability events.
    """
    
    def __init__(
        self,
        device_id: str,
        port: str = "/dev/ttyUSB0",
        baud: int = 9600,
        stable_seconds: float = 2.0,
        tolerance: float = 0.001,
        broker: str = "localhost",
        mqtt_port: int = 1883
    ):
        self.device_id = device_id
        self.port = port
        self.baud = baud
        self.stable_seconds = stable_seconds
        self.tolerance = tolerance
        
        # Serial connection
        self.ser: Optional[serial.Serial] = None
        
        # State tracking
        self.last_weight: Optional[float] = None
        self.stable_start_time: Optional[float] = None
        self.stable_reported = False
        self.tare_weight: float = 0.0
        self._running = False
        self._monitor_thread: Optional[threading.Thread] = None
        
        # MQTT client
        self.broker = broker
        self.mqtt_port = mqtt_port
        self.client = mqtt.Client(client_id=f"scale_{device_id}")
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.connected = False
        
        # Topics
        self.topic_data = f"factory/scale/{device_id}/data"
        self.topic_status = f"factory/scale/{device_id}/status"
        self.topic_cmd = f"factory/scale/{device_id}/cmd/#"
        self.topic_error = f"factory/scale/{device_id}/error"

        # Job Details
        self.current_jobs_correlation_id = None

    def _on_connect(self, client, userdata, flags, rc):
        """MQTT connection callback"""
        if rc == 0:
            logger.info(f"Scale {self.device_id} connected to MQTT broker")
            self.connected = True
            self.client.subscribe(self.topic_cmd)
            self._publish_status("ready")
        else:
            logger.error(f"Failed to connect to MQTT broker: {rc}")
            self.connected = False
            
    def _on_message(self, client, userdata, msg):
        """Handle incoming MQTT commands"""
        try:
            command = msg.topic.split("/")[-1]
            payload = json.loads(msg.payload.decode()) if msg.payload else {}
            
            logger.info(f"Received command: {command}")
            
            if command == "start_monitoring":
                self.current_jobs_correlation_id = payload.get("correlation_id")
                self.start_monitoring()
            elif command == "stop_monitoring":
                self.stop_monitoring()
            elif command == "tare":
                self.tare()
            elif command == "read_once":
                self.read_once()
            elif command == "get_status":
                self._publish_status("ready" if self._running else "idle")
            else:
                logger.warning(f"Unknown command: {command}")
                
        except Exception as e:
            logger.error(f"Error processing command: {e}")
            self._publish_error(f"Command error: {e}")
    
    def connect(self):
        """Connect to MQTT broker and serial port"""
        try:
            # Connect to MQTT
            self.client.connect(self.broker, self.mqtt_port, 60)
            self.client.loop_start()
            
            # Wait for MQTT connection
            timeout = 5
            start = time.time()
            while not self.connected and (time.time() - start) < timeout:
                time.sleep(0.1)
                
            if not self.connected:
                raise Exception("MQTT connection timeout")
            
            # Connect to serial port
            self.ser = serial.Serial(
                port=self.port,
                baudrate=self.baud,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                timeout=1
            )
            
            logger.info(f"Scale {self.device_id} ready on {self.port}")
            self._publish_status("ready")
            return True
            
        except Exception as e:
            logger.error(f"Connection error: {e}")
            self._publish_error(f"Connection failed: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from MQTT and serial"""
        self.stop_monitoring()
        
        if self.ser and self.ser.is_open:
            self.ser.close()
            
        self.client.loop_stop()
        self.client.disconnect()
        logger.info(f"Scale {self.device_id} disconnected")
    
    @staticmethod
    def parse_weight(raw: str) -> Optional[float]:
        """Extract weight value from serial data"""
        cleaned = ''.join(ch for ch in raw if ch.isprintable())
        match = re.search(r"[-+]?\d+\.\d+", cleaned)
        if match:
            return float(match.group())
        return None
    
    def read_weight(self) -> Optional[float]:
        """Read single weight value from scale"""
        if not self.ser or not self.ser.is_open:
            return None
            
        try:
            line = self.ser.readline().decode(errors="ignore").strip()
            if not line:
                return None
            return self.parse_weight(line)
        except Exception as e:
            logger.error(f"Error reading weight: {e}")
            return None
    
    def read_once(self):
        """Read and publish single weight reading"""
        weight = self.read_weight()
        if weight is not None:
            self._publish_data(weight, stable=False)
    
    def tare(self):
        """Set current weight as tare"""
        weight = self.read_weight()
        if weight is not None:
            self.tare_weight = weight
            logger.info(f"Tare set to {self.tare_weight:.3f} kg")
            self._publish_status("tared", {"tare_weight": self.tare_weight})
    
    def start_monitoring(self):
        """Start continuous weight monitoring in background thread"""
        if self._running:
            logger.warning("Monitoring already running")
            return
            
        self._running = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        logger.info("Started weight monitoring")
        self._publish_status("monitoring")
    
    def stop_monitoring(self):
        """Stop weight monitoring"""
        if not self._running:
            return
            
        self._running = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=2)
            self._monitor_thread = None
            
        logger.info("Stopped weight monitoring")
        self._publish_status("idle")
    
    def _monitor_loop(self):
        """Continuous monitoring loop - publishes all readings"""
        self.last_weight = None
        self.stable_start_time = None
        self.stable_reported = False
        
        while self._running:
            try:
                weight = self.read_weight()
                # print(f"[Debug: Weight: {weight}]")
                if weight is None:
                    continue
                
                # Publish every reading for transparency
                net_weight = weight - self.tare_weight
                
                # Check for stability
                if self.last_weight is None or abs(weight - self.last_weight) > self.tolerance:
                    # Weight changed - reset stability timer
                    # print("[Debug]: Weight Changed")
                    self.last_weight = weight
                    self.stable_start_time = time.time()
                    self.stable_reported = False
                else:
                    # Weight unchanged
                    # print("[Debug]: Weight unchanged")
                    if self.stable_start_time is None:
                        self.stable_start_time = time.time()
                    
                    elapsed = time.time() - self.stable_start_time
                    # print(f"[Debug]: Elapsed Time: {elapsed}")
                    # print(f"[Debug]: self.last_weight: {self.last_weight}")
                    
                    if elapsed >= self.stable_seconds and not self.stable_reported:
                        # Weight is now stable
                        self.stable_reported = True
                        self._publish_data(weight, stable=True, net_weight=net_weight)
                        logger.info(f"Stable weight: {weight:.3f} kg (net: {net_weight:.3f} kg)")
                
            except Exception as e:
                logger.error(f"Monitor loop error: {e}")
                self._publish_error(f"Monitoring error: {e}")
                time.sleep(1)
    
    def _publish_data(self, weight: float, stable: bool, net_weight: Optional[float] = None):
        """Publish weight data to MQTT"""
        if not self.connected:
            return
            
        payload = {
            "msg_type": "data",
            "timestamp": datetime.now().isoformat(),
            "device_id": self.device_id,
            "weight": round(weight, 3),
            "unit": "kg",
            "stable": stable,
            "tare_weight": round(self.tare_weight, 3),
            "net_weight": round(net_weight, 3) if net_weight is not None else None,
            "correlation_id": self.current_jobs_correlation_id
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
            "details": details or {}
        }
        
        self.client.publish(self.topic_status, json.dumps(payload), qos=1)
    
    def _publish_error(self, error_msg: str):
        """Publish error message"""
        payload = {
            "msg_type": "error",
            "timestamp": datetime.now().isoformat(),
            "device_id": self.device_id,
            "error_msg": error_msg
        }
        
        self.client.publish(self.topic_error, json.dumps(payload), qos=1)


# ==========================================
# MAIN - Service Entry Point
# ==========================================

if __name__ == "__main__":
    import sys
    
    # Configuration
    DEVICE_ID = "scale_01"
    PORT = "/dev/weighing_scale"
    BROKER = "localhost"
    
    # Create and start service
    scale_service = WeighingScaleMQTTService(
        device_id=DEVICE_ID,
        port=PORT,
        broker=BROKER
    )
    
    if scale_service.connect():
        logger.info("Scale service running. Press Ctrl+C to exit.")
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            scale_service.disconnect()
            sys.exit(0)
    else:
        logger.error("Failed to start scale service")
        sys.exit(1)


# ==========================================
# TESTING COMMANDS
# ==========================================
"""
# Start the service:
python scale_mqtt_service.py

# In another terminal, test with mosquitto:

# Start monitoring
mosquitto_pub -t factory/scale/scale_01/cmd/start_monitoring -m '{}'

# Stop monitoring
mosquitto_pub -t factory/scale/scale_01/cmd/stop_monitoring -m '{}'

# Tare the scale
mosquitto_pub -t factory/scale/scale_01/cmd/tare -m '{}'

# Read once
mosquitto_pub -t factory/scale/scale_01/cmd/read_once -m '{}'

# Subscribe to all scale data
mosquitto_sub -t 'factory/scale/#' -v
"""