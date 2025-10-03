"""
Professional RFID Reader MQTT Service
Refactored to match standardized architecture with clean MQTT communication
"""

import struct
import serial
import socket
import json
import time
import threading
from datetime import datetime
from typing import Optional, Dict, Any
import paho.mqtt.client as mqtt
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ==========================================
# COMMUNICATION INTERFACES
# ==========================================

class RFIDComm:
    """Base interface for RFID communication"""
    def send(self, data: bytes):
        raise NotImplementedError
    
    def receive(self, size: int) -> bytes:
        raise NotImplementedError
    
    def close(self):
        raise NotImplementedError


class RFIDSerial(RFIDComm):
    """Serial communication for RFID reader"""
    def __init__(self, port="/dev/ttyUSB0", baudrate=57600, timeout=1):
        self.ser = serial.Serial(port, baudrate, timeout=timeout)
        logger.info(f"Serial connection established: {port}")
    
    def send(self, data: bytes):
        self.ser.write(data)
    
    def receive(self, size: int) -> bytes:
        return self.ser.read(size)
    
    def close(self):
        if self.ser and self.ser.is_open:
            self.ser.close()


class RFIDTCP(RFIDComm):
    """TCP/IP communication for RFID reader"""
    def __init__(self, ip="192.168.1.101", port=49152, timeout=10):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(timeout)
        self.sock.connect((ip, port))
        logger.info(f"TCP connection established: {ip}:{port}")
    
    def send(self, data: bytes):
        self.sock.sendall(data)
    
    def receive(self, size: int) -> bytes:
        return self.sock.recv(size)
    
    def close(self):
        if self.sock:
            self.sock.close()


# ==========================================
# RFID PROTOCOL HANDLER
# ==========================================

def calculate_checksum(data: bytes) -> int:
    """Calculate checksum for RFID protocol"""
    return (~sum(data) + 1) & 0xFF


class RFIDProtocol:
    """RFID reader protocol handler"""
    
    def __init__(self, comm: RFIDComm):
        self.comm = comm
    
    def send_command(self, cid1: int, cid2: int, info: bytes = b"", addr: int = 0xFFFF):
        """Send command to RFID reader"""
        start = 0x7C
        header = struct.pack("<BHB", start, addr, cid1) + struct.pack("B", cid2)
        length = len(info)
        packet = header + struct.pack("B", length) + info
        cs = calculate_checksum(packet)
        full_packet = packet + struct.pack("B", cs)
        self.comm.send(full_packet)
    
    def read_response(self) -> Dict[str, Any]:
        """Read response from RFID reader"""
        try:
            header = self.comm.receive(6)
            if len(header) < 6:
                return {"error": "Incomplete header", "success": False}
            
            if header[0] != 0xCC:
                return {"error": f"Unexpected start byte: {header[0]:02X}", "success": False}
            
            length = header[5]
            remaining = self.comm.receive(length + 1)
            
            if len(remaining) < length + 1:
                return {"error": "Incomplete response", "success": False}
            
            addr = struct.unpack("<H", header[1:3])[0]
            cid1, rtn = header[3], header[4]
            data = remaining[:length]
            chksum = remaining[length:length+1]
            
            return {
                "success": True,
                "addr": addr,
                "cid1": cid1,
                "rtn": rtn,
                "data": data.hex(),
                "data_bytes": data,
                "chksum": chksum.hex()
            }
        except Exception as e:
            return {"error": str(e), "success": False}
    
    def read_type_c_uii(self) -> Dict[str, Any]:
        """Read Type C tag (EPC)"""
        self.send_command(0x20, 0x00)
        return self.read_response()
    
    def get_basic_parameters(self) -> Dict[str, Any]:
        """Get reader parameters"""
        self.send_command(0x81, 0x32)
        return self.read_response()
    
    def set_basic_parameters(self, params: bytes) -> Dict[str, Any]:
        """Set reader parameters"""
        self.send_command(0x81, 0x31, params)
        return self.read_response()
    
    def software_reset(self) -> Dict[str, Any]:
        """Software reset"""
        self.send_command(0xD0, 0x00)
        return self.read_response()


# ==========================================
# RFID MQTT SERVICE
# ==========================================

class RFIDReaderMQTTService:
    """
    RFID Reader service with standardized MQTT communication.
    Matches architecture of QR and Scale services.
    """
    
    def __init__(
        self,
        device_id: str,
        zone: str = "Unknown",
        broker: str = "localhost",
        mqtt_port: int = 1883
    ):
        self.device_id = device_id
        self.zone = zone
        
        # Communication
        self.comm: Optional[RFIDComm] = None
        self.protocol: Optional[RFIDProtocol] = None
        
        # State
        self._running = False
        self._polling_thread: Optional[threading.Thread] = None
        self.epc_map: Dict[str, Dict] = {}  # Track seen tags
        self.buzzer_enabled = True
        
        # MQTT
        self.broker = broker
        self.mqtt_port = mqtt_port
        self.client = mqtt.Client(client_id=f"rfid_{device_id}")
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.connected = False
        
        # Topics - standardized structure
        self.topic_data = f"factory/rfid/{device_id}/data"
        self.topic_status = f"factory/rfid/{device_id}/status"
        self.topic_cmd = f"factory/rfid/{device_id}/cmd/#"
        self.topic_error = f"factory/rfid/{device_id}/error"
    
    def _on_connect(self, client, userdata, flags, rc):
        """MQTT connection callback"""
        if rc == 0:
            logger.info(f"RFID Reader {self.device_id} connected to MQTT")
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
            
            if command == "connect":
                self._handle_connect(payload)
            elif command == "disconnect":
                self._handle_disconnect()
            elif command == "start_polling":
                self.start_polling()
            elif command == "stop_polling":
                self.stop_polling()
            elif command == "read_once":
                self._handle_read_once()
            elif command == "get_params":
                self._handle_get_params()
            elif command == "set_buzzer":
                self._handle_set_buzzer(payload)
            elif command == "reset":
                self._handle_reset()
            elif command == "get_status":
                self._publish_status("polling" if self._running else "idle")
            else:
                logger.warning(f"Unknown command: {command}")
        
        except Exception as e:
            logger.error(f"Error processing command: {e}")
            self._publish_error(f"Command error: {e}")
    
    # ==========================================
    # COMMAND HANDLERS
    # ==========================================
    
    def _handle_connect(self, payload: Dict):
        """Connect to RFID reader"""
        try:
            conn_type = payload.get("type", "tcp")
            
            if conn_type == "serial":
                port = payload.get("port", "/dev/ttyUSB0")
                baudrate = payload.get("baudrate", 57600)
                self.comm = RFIDSerial(port, baudrate)
            else:
                ip = payload.get("ip", "192.168.1.101")
                tcp_port = payload.get("tcp_port", 49152)
                self.comm = RFIDTCP(ip, tcp_port)
            
            self.protocol = RFIDProtocol(self.comm)
            
            self._publish_status("connected", {
                "type": conn_type,
                "zone": self.zone
            })
            
        except Exception as e:
            logger.error(f"Connection error: {e}")
            self._publish_error(f"Connection failed: {e}")
    
    def _handle_disconnect(self):
        """Disconnect from RFID reader"""
        self.stop_polling()
        
        if self.comm:
            self.comm.close()
            self.comm = None
            self.protocol = None
        
        self._publish_status("disconnected")
    
    def _handle_read_once(self):
        """Read single tag"""
        if not self.protocol:
            self._publish_error("Not connected to reader")
            return
        
        try:
            response = self.protocol.read_type_c_uii()
            
            if response.get("success") and response.get("rtn") == 0x02:
                tag_data = self._parse_tag_data(response)
                if tag_data:
                    self._publish_data(tag_data, single_read=True)
            else:
                self._publish_status("no_tag_detected")
        
        except Exception as e:
            logger.error(f"Read error: {e}")
            self._publish_error(f"Read failed: {e}")
    
    def _handle_get_params(self):
        """Get reader parameters"""
        if not self.protocol:
            self._publish_error("Not connected to reader")
            return
        
        try:
            response = self.protocol.get_basic_parameters()
            
            payload = {
                "msg_type": "response",
                "timestamp": datetime.now().isoformat(),
                "device_id": self.device_id,
                "command": "get_params",
                "response": response
            }
            
            self.client.publish(
                f"factory/rfid/{self.device_id}/response",
                json.dumps(payload),
                qos=1
            )
        
        except Exception as e:
            logger.error(f"Get params error: {e}")
            self._publish_error(f"Get params failed: {e}")
    
    def _handle_set_buzzer(self, payload: Dict):
        """Enable/disable buzzer"""
        if not self.protocol:
            self._publish_error("Not connected to reader")
            return
        
        try:
            enable = payload.get("enable", True)
            
            # Get current params
            response = self.protocol.get_basic_parameters()
            if not response.get("success"):
                self._publish_error("Failed to get current parameters")
                return
            
            data = bytearray(bytes.fromhex(response["data"]))
            if len(data) < 27:
                self._publish_error("Unexpected parameter length")
                return
            
            # Modify buzzer flag (byte 11)
            data[11] = 1 if enable else 0
            
            # Set new params
            set_response = self.protocol.set_basic_parameters(bytes(data))
            
            if set_response.get("success"):
                self.buzzer_enabled = enable
                logger.info(f"Buzzer {'enabled' if enable else 'disabled'}")
                self._publish_status("buzzer_updated", {"enabled": enable})
            else:
                self._publish_error("Failed to set buzzer")
        
        except Exception as e:
            logger.error(f"Set buzzer error: {e}")
            self._publish_error(f"Set buzzer failed: {e}")
    
    def _handle_reset(self):
        """Software reset"""
        if not self.protocol:
            self._publish_error("Not connected to reader")
            return
        
        try:
            response = self.protocol.software_reset()
            
            if response.get("success"):
                logger.info("Reader reset successful")
                self._publish_status("reset_complete")
            else:
                self._publish_error("Reset failed")
        
        except Exception as e:
            logger.error(f"Reset error: {e}")
            self._publish_error(f"Reset failed: {e}")
    
    # ==========================================
    # TAG POLLING
    # ==========================================
    
    def start_polling(self):
        """Start continuous tag polling"""
        if not self.protocol:
            self._publish_error("Not connected to reader")
            return
        
        if self._running:
            logger.warning("Polling already running")
            return
        
        self._running = True
        self._polling_thread = threading.Thread(target=self._polling_loop, daemon=True)
        self._polling_thread.start()
        logger.info("Started RFID polling")
        self._publish_status("polling")
    
    def stop_polling(self):
        """Stop tag polling"""
        if not self._running:
            return
        
        self._running = False
        if self._polling_thread:
            self._polling_thread.join(timeout=2)
            self._polling_thread = None
        
        logger.info("Stopped RFID polling")
        self._publish_status("idle")
    
    def _polling_loop(self):
        """Continuous polling loop"""
        while self._running:
            try:
                response = self.protocol.read_type_c_uii()
                
                # Check for valid tag read (rtn = 0x02)
                if response.get("success") and response.get("rtn") == 0x02:
                    tag_data = self._parse_tag_data(response)
                    
                    if tag_data:
                        epc = tag_data["epc"]
                        
                        # Update or create entry in epc_map
                        if epc in self.epc_map:
                            self.epc_map[epc]["count"] += 1
                            self.epc_map[epc]["rssi"] = tag_data["rssi"]
                            self.epc_map[epc]["last_seen"] = tag_data["timestamp"]
                        else:
                            self.epc_map[epc] = {
                                "epc": epc,
                                "count": 1,
                                "rssi": tag_data["rssi"],
                                "antenna": tag_data["antenna"],
                                "last_seen": tag_data["timestamp"],
                                "location": self.zone
                            }
                        
                        # Publish tag data
                        self._publish_data(self.epc_map[epc])
                
                time.sleep(1)  # Poll interval
            
            except Exception as e:
                logger.error(f"Polling error: {e}")
                self._publish_error(f"Polling error: {e}")
                time.sleep(1)
    
    def _parse_tag_data(self, response: Dict) -> Optional[Dict]:
        """Parse tag data from response"""
        try:
            data_bytes = response.get("data_bytes")
            if not data_bytes or len(data_bytes) < 4:
                return None
            
            antenna = data_bytes[0]
            pc = data_bytes[1:3]
            epc = data_bytes[3:-1]
            rssi = data_bytes[-1]
            
            # Convert EPC to hex string
            epc_str = " ".join(f"{b:02X}" for b in epc)
            
            # Convert RSSI to dBm
            rssi_dbm = -(256 - rssi)
            
            return {
                "epc": epc_str,
                "antenna": antenna,
                "rssi": rssi_dbm,
                "timestamp": datetime.now().isoformat()
            }
        
        except Exception as e:
            logger.error(f"Parse error: {e}")
            return None
    
    # ==========================================
    # MQTT PUBLISHING
    # ==========================================
    
    def _publish_data(self, tag_data: Dict, single_read: bool = False):
        """Publish tag data"""
        if not self.connected:
            return
        
        payload = {
            "msg_type": "data",
            "timestamp": datetime.now().isoformat(),
            "device_id": self.device_id,
            "single_read": single_read,
            **tag_data
        }
        
        self.client.publish(self.topic_data, json.dumps(payload), qos=1)
    
    def _publish_status(self, status: str, details: Dict = None):
        """Publish status update"""
        if not self.connected:
            return
        
        payload = {
            "msg_type": "status",
            "timestamp": datetime.now().isoformat(),
            "device_id": self.device_id,
            "status": status,
            "zone": self.zone,
            "details": details or {},
            "tags_seen": len(self.epc_map)
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
    # LIFECYCLE
    # ==========================================
    
    def connect_mqtt(self):
        """Connect to MQTT broker"""
        try:
            self.client.connect(self.broker, self.mqtt_port, 60)
            self.client.loop_start()
            
            # Wait for connection
            timeout = 5
            start = time.time()
            while not self.connected and (time.time() - start) < timeout:
                time.sleep(0.1)
            
            if not self.connected:
                raise Exception("MQTT connection timeout")
            
            logger.info(f"RFID Reader {self.device_id} MQTT ready")
            return True
        
        except Exception as e:
            logger.error(f"MQTT connection error: {e}")
            return False
    
    def disconnect_all(self):
        """Disconnect from everything"""
        self.stop_polling()
        
        if self.comm:
            self.comm.close()
            self.comm = None
            self.protocol = None
        
        self.client.loop_stop()
        self.client.disconnect()
        logger.info(f"RFID Reader {self.device_id} disconnected")


# ==========================================
# MULTI-READER MANAGER
# ==========================================

class RFIDServiceManager:
    """
    Manages multiple RFID readers.
    Each reader gets its own service instance.
    """
    
    def __init__(self, broker: str = "localhost", port: int = 1883):
        self.broker = broker
        self.port = port
        self.services: Dict[str, RFIDReaderMQTTService] = {}
        self.lock = threading.Lock()
        
        # Control MQTT client
        self.client = mqtt.Client(client_id="rfid_manager")
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.connected = False
    
    def _on_connect(self, client, userdata, flags, rc):
        """Manager MQTT connection"""
        if rc == 0:
            logger.info("RFID Manager connected to MQTT")
            self.connected = True
            # Subscribe to all RFID device commands
            self.client.subscribe("factory/rfid/+/cmd/#")
        else:
            logger.error(f"Manager connection failed: {rc}")
    
    def _on_message(self, client, userdata, msg):
        """Route messages to appropriate service"""
        try:
            parts = msg.topic.split("/")
            if len(parts) < 4:
                return
            
            # factory/rfid/{device_id}/cmd/{command}
            device_id = parts[2]
            
            # Create service if doesn't exist and command is 'add_device'
            command = parts[-1]
            if device_id not in self.services and command == "add_device":
                payload = json.loads(msg.payload.decode())
                self.create_service(device_id, payload.get("zone", "Unknown"))
            
            # Forward message to service's MQTT client
            # (Service will handle it through its own on_message)
        
        except Exception as e:
            logger.error(f"Manager routing error: {e}")
    
    def create_service(self, device_id: str, zone: str):
        """Create new RFID service instance"""
        with self.lock:
            if device_id in self.services:
                logger.warning(f"Service {device_id} already exists")
                return
            
            service = RFIDReaderMQTTService(
                device_id=device_id,
                zone=zone,
                broker=self.broker,
                mqtt_port=self.port
            )
            
            if service.connect_mqtt():
                self.services[device_id] = service
                logger.info(f"Created service for {device_id}")
            else:
                logger.error(f"Failed to create service for {device_id}")
    
    def start(self):
        """Start manager"""
        self.client.connect(self.broker, self.port, 60)
        self.client.loop_start()
        
        # Wait for connection
        timeout = 5
        start = time.time()
        while not self.connected and (time.time() - start) < timeout:
            time.sleep(0.1)
        
        if not self.connected:
            raise Exception("Manager MQTT connection failed")
        
        logger.info("RFID Service Manager started")
    
    def stop(self):
        """Stop manager and all services"""
        for service in self.services.values():
            service.disconnect_all()
        
        self.client.loop_stop()
        self.client.disconnect()
        logger.info("RFID Service Manager stopped")


# ==========================================
# MAIN
# ==========================================

if __name__ == "__main__":
    import sys
    
    # For single reader deployment
    if len(sys.argv) > 1 and sys.argv[1] == "single":
        device_id = sys.argv[2] if len(sys.argv) > 2 else "192.168.1.102"
        zone = sys.argv[3] if len(sys.argv) > 3 else "Zone E"
        
        service = RFIDReaderMQTTService(
            device_id=device_id,
            zone=zone,
            broker="localhost"
        )
        
        if service.connect_mqtt():
            logger.info(f"RFID service running for {device_id}. Press Ctrl+C to exit.")
            
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Shutting down...")
                service.disconnect_all()
                sys.exit(0)
        else:
            logger.error("Failed to start RFID service")
            sys.exit(1)
    
    # For multi-reader deployment (default)
    else:
        manager = RFIDServiceManager(broker="localhost")
        manager.start()
        
        logger.info("Multi-reader RFID Manager running. Press Ctrl+C to exit.")
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            manager.stop()
            sys.exit(0)


# ==========================================
# TESTING COMMANDS
# ==========================================
"""
# Start service (single reader mode):
python refactored_rfid_service.py single 192.168.1.102 "Zone E"

# Start service (multi-reader manager mode):
python refactored_rfid_service.py

# Test commands:

# Add a new reader
mosquitto_pub -t factory/rfid/192.168.1.102/cmd/add_device -m '{"zone":"Zone E"}'

# Connect to reader
mosquitto_pub -t factory/rfid/192.168.1.102/cmd/connect \
  -m '{"type":"tcp","ip":"192.168.1.102","tcp_port":49152}'

# Start polling
mosquitto_pub -t factory/rfid/192.168.1.102/cmd/start_polling -m '{}'

# Stop polling
mosquitto_pub -t factory/rfid/192.168.1.102/cmd/stop_polling -m '{}'

# Read single tag
mosquitto_pub -t factory/rfid/192.168.1.102/cmd/read_once -m '{}'

# Get parameters
mosquitto_pub -t factory/rfid/192.168.1.102/cmd/get_params -m '{}'

# Enable buzzer
mosquitto_pub -t factory/rfid/192.168.1.102/cmd/set_buzzer -m '{"enable":true}'

# Disable buzzer
mosquitto_pub -t factory/rfid/192.168.1.102/cmd/set_buzzer -m '{"enable":false}'

# Reset reader
mosquitto_pub -t factory/rfid/192.168.1.102/cmd/reset -m '{}'

# Disconnect
mosquitto_pub -t factory/rfid/192.168.1.102/cmd/disconnect -m '{}'

# Subscribe to all RFID data
mosquitto_sub -t 'factory/rfid/#' -v

# Subscribe to specific reader
mosquitto_sub -t 'factory/rfid/192.168.1.102/#' -v
"""