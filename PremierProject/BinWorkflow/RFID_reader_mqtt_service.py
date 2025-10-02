#!/usr/bin/env python3
"""
RFID Reader Service (MQTT)
Headless service with tag polling, manual commands, and response log.
"""

import struct
import serial
import socket
import asyncio
import json
from datetime import datetime
from typing import List, Optional, Dict
from concurrent.futures import Future
import time
import json
import random
import threading
import paho.mqtt.client as mqtt


class MQTTClientApp:
    def __init__(self, broker, port, username=None, password=None,
                 client_id=None, keepalive=60, protocol=mqtt.MQTTv5,
                 transport="tcp"):
        self.broker = broker
        self.port = port
        self.username = username
        self.password = password
        self.client_id = client_id or f"mqtt-client-{random.randint(0, 1000)}"
        self.keepalive = keepalive
        self.connected_flag = False

        # Create MQTT Client
        self.client = mqtt.Client(
            client_id=self.client_id,
            protocol=protocol,
            transport=transport
        )

        if username and password:
            self.client.username_pw_set(username, password)

        # Assign default callbacks (can be overridden later)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect

    # --- Default Callbacks ---
    def on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            print(f"✅ {self.client_id} connected to MQTT Broker")
            self.connected_flag = True
        else:
            print(f"❌ Connection failed, return code {rc}")
            self.connected_flag = False

    def on_disconnect(self, client, userdata, rc, properties=None):
        print(f"🔌 {self.client_id} disconnected from MQTT Broker")
        self.connected_flag = False

    # --- Connection Management ---
    def connect(self, timeout=5) -> bool:
        self.client.connect(self.broker, self.port, keepalive=self.keepalive)
        self.client.loop_start()

        # Wait for connection or timeout
        start_time = time.time()
        while not self.connected_flag and time.time() - start_time < timeout:
            time.sleep(0.1)

        return self.connected_flag

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()

    # --- Publish (only if data is not None) ---
    def publish(self, topic, data, qos=0):
        # Validate topic
        if not topic or not isinstance(topic, str):
            print("⚠️ Invalid topic")
            return False

        # Validate data
        if data is None:
            print("⚠️ No data to send")
            return False

        # Ensure string payload
        if not isinstance(data, str):
            try:
                data = json.dumps(data)
            except Exception as e:
                print(f"❌ Failed to serialize data: {e}")
                return False

        # Publish
        result = self.client.publish(topic, data, qos=qos)
        if result[0] == 0:
            print(f"📡 Sent to {topic}: {data}")
            return True
        else:
            print(f"⚠️ Failed to send message to {topic}")
            return False


    # --- Subscribe ---
    def subscribe(self, topic, qos=0):
        self.client.subscribe(topic, qos=qos)

# --------------------
# Helpers
# --------------------
def calculate_checksum(data: bytes) -> int:
    return (~sum(data) + 1) & 0xFF


class RFIDSerial:
    def __init__(self, port="/dev/ttyUSB0", baudrate=57600):
        self.ser = serial.Serial(port, baudrate, timeout=1)

    def send(self, data: bytes):
        self.ser.write(data)

    def receive(self, size: int) -> bytes:
        return self.ser.read(size)

    def close(self):
        self.ser.close()


class RFIDTCP:
    def __init__(self, ip="192.168.1.101", port=49152):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(10)
        self.sock.connect((ip, port))

    def send(self, data: bytes):
        self.sock.sendall(data)

    def receive(self, size: int) -> bytes:
        return self.sock.recv(size)

    def close(self):
        self.sock.close()


class RFIDReader:
    def __init__(self, comm):
        self.comm = comm

    def send_command(self, cid1, cid2, info=b"", addr=0xFFFF):
        start = 0x7C
        header = struct.pack("<BHB", start, addr, cid1) + struct.pack("B", cid2)
        length = len(info)
        packet = header + struct.pack("B", length) + info
        cs = calculate_checksum(packet)
        full_packet = packet + struct.pack("B", cs)
        self.comm.send(full_packet)

    # def read_response(self):
    #     header = self.comm.receive(6)
    #     if len(header) < 6:
    #         return {"error": "Incomplete header"}
    #     if header[0] != 0xCC:
    #         return {"error": f"Unexpected start byte: {header[0]:02X}"}

    #     addr = struct.unpack("<H", header[1:3])[0]
    #     cid1, rtn, length = header[3:6]
    #     data = self.comm.receive(length)
    #     chksum = self.comm.receive(1)

    #     return {
    #         "addr": addr,
    #         "cid1": cid1,
    #         "rtn": rtn,
    #         "data": data.hex(" "),
    #         "chksum": chksum.hex(" ")
    #     }

    def read_response(self):
        # Read complete response based on protocol
        header = self.comm.receive(6)
        if len(header) < 6:
            return {"error": "Incomplete header"}
        if header[0] != 0xCC:
            return {"error": f"Unexpected start byte: {header[0]:02X}"}
        
        length = header[5]  # LENGTH field
        remaining = self.comm.receive(length + 1)  # data + checksum
        
        if len(remaining) < length + 1:
            return {"error": "Incomplete response"}
        
        # Parse complete response
        addr = struct.unpack("<H", header[1:3])[0]
        cid1, rtn = header[3], header[4]
        data = remaining[:length]
        chksum = remaining[length:length+1]

        return {
            "addr": addr,
            "cid1": cid1,
            "rtn": rtn,
            "data": data.hex(),
            "chksum": chksum.hex()
        }

    def read_type_c_uii(self):
        self.send_command(0x20, 0x00)
        return self.read_response()

    def get_basic_parameters(self):
        self.send_command(0x81, 0x32)
        return self.read_response()

    def software_reset(self):
        self.send_command(0xD0, 0x00)
        return self.read_response()


# --------------------
# RFID Service
# --------------------
class RFIDService:
    def __init__(self, payload, broker="mqtt.localhost", port=1883, base_topic="rfid"):
        self.reader: Optional[RFIDReader] = None
        self.comm = None
        self.running = False
        self.response_log: List[dict] = []
        self.event_loop: asyncio.AbstractEventLoop | None = None
        self.polling_future: Future | None = None

        self.base_topic = base_topic
        self.epc_map = {}  # {epc_str: {"count": int, "rssi": int, "last_seen": str, "antenna": int}}
        self.zone = payload.get("zone")

        # ✅ Use MQTTClientApp
        self.client = MQTTClientApp(
            broker=broker,
            port=port,
            username=None,
            password=None
        )
        self.client.client.on_message = self.on_message  # keep callback
        self.client.connect()

        # Subscribe to all command topics
        for cmd in [
            "connect", "disconnect", "read_tag", "get_params",
            "reset", "manual_command", "start_polling", "stop_polling", "buzzer_toggle"
        ]:
            self.client.subscribe(f"{self.base_topic}/{cmd}")

    # ---- MQTT Publish ----
    def publish(self, topic: str, payload: dict):
        self.client.publish(f"{self.base_topic}/{topic}", payload)

    # ---- Command Handlers ----
    def handle_connect(self, msg):
        try:
            if msg.get("type") == "serial":
                self.comm = RFIDSerial(msg.get("port", "/dev/ttyUSB0"), msg.get("baudrate", 57600))
            else:
                self.comm = RFIDTCP(msg.get("ip", "192.168.1.101"), msg.get("tcp_port", 49152))
            self.reader = RFIDReader(self.comm)
            self.publish("status", {"status": "connected", "type": msg.get("type")})
        except Exception as e:
            self.publish("status", {"error": str(e)})

    def handle_disconnect(self, _msg):
        self.running = False
        if self.polling_future:
            self.polling_future.cancel()
            self.polling_future = None
        if self.comm:
            self.comm.close()
            self.reader, self.comm = None, None
            self.publish("status", {"status": "disconnected"})
        else:
            self.publish("status", {"error": "not connected"})

    def handle_read_tag(self, _msg):
        if not self.reader:
            self.publish("response", {"error": "Not connected"})
            return
        res = self.reader.read_type_c_uii()
        self.response_log.append({"time": str(datetime.now()), "response": res})
        self.publish("response", res)

    def handle_get_params(self, _msg):
        if not self.reader:
            self.publish("response", {"error": "Not connected"})
            return
        res = self.reader.get_basic_parameters()
        self.response_log.append({"time": str(datetime.now()), "response": res})
        self.publish("response", res)

    def handle_reset(self, _msg):
        if not self.reader:
            self.publish("response", {"error": "Not connected"})
            return
        res = self.reader.software_reset()
        self.response_log.append({"time": str(datetime.now()), "response": res})
        self.publish("response", res)

    def handle_manual_command(self, msg):
        if not self.reader:
            self.publish("response", {"error": "Not connected"})
            return
        info_bytes = bytes.fromhex(msg.get("info", "")) if msg.get("info") else b""
        self.reader.send_command(msg["cid1"], msg["cid2"], info_bytes)
        res = self.reader.read_response()
        self.response_log.append({"time": str(datetime.now()), "response": res})
        self.publish("response", res)

    def handle_buzzer_toggle(self, _msg):
        enable = _msg.get("enable_buzzer", True)
        if not self.reader:
            self.publish("response", {"error": "Not connected"})
            return
        res = self.set_buzzer(enable)
        self.response_log.append({"time": str(datetime.now()), "response": res})
        self.publish("response", res)

    def set_buzzer(self, enable: bool):
        # Step 1: get current params
        res = self.reader.get_basic_parameters()
        print(f"set_buzzer - get_basic_parameters: {res}")
        if not res or "data" not in res:
            print("[ERROR] Failed to get parameters")
            return None

        data = bytearray(bytes.fromhex(res["data"]))
        if len(data) < 27:
            print("[ERROR] Unexpected parameter length")
            return None

        # Step 2: modify buzzer flag
        data[11] = 1 if enable else 0  # BZ is the 12th byte (index 11)

        # Step 3: send Set Basic Parameters (correct command codes)
        # CID1=0x81, CID2=0x31 for Set Base Parameters
        self.reader.send_command(0x81, 0x31, bytes(data))
        return self.reader.read_response()


    # async def tag_polling(self):
    #     self.publish("status", {"status": "tag_polling"})
    #     loop = asyncio.get_running_loop()
    #     while self.running:
    #         try:
    #             res = await loop.run_in_executor(None, self.reader.read_type_c_uii)
    #             self.response_log.append({"time": str(datetime.now()), "response": res})
    #             self.publish("tags", res)
    #         except Exception as e:
    #             self.publish("tags", {"error": str(e)})
    #         await asyncio.sleep(1)

    async def tag_polling(self):
        self.publish("status", {"status": "tag_polling"})
        loop = asyncio.get_running_loop()

        while self.running:
            try:
                res = await loop.run_in_executor(None, self.reader.read_type_c_uii)

                # Validate response
                if not res or res.get("rtn") != 0x02:
                    await asyncio.sleep(1)
                    continue

                try:
                    # Decode raw data
                    data = bytes.fromhex(res["data"])
                    antenna = data[0]
                    pc = data[1:3]          # Not used now, but kept for completeness
                    epc = data[3:-1]
                    rssi = data[-1]

                    # Convert to usable formats
                    epc_str = " ".join(f"{b:02X}" for b in epc)
                    rssi_dbm = -(256 - rssi)   # Same as your reference
                    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    # Update epc_map
                    if epc_str in self.epc_map:
                        self.epc_map[epc_str]["count"] += 1
                        self.epc_map[epc_str]["rssi"] = rssi_dbm
                        self.epc_map[epc_str]["last_seen"] = now_str
                        self.epc_map[epc_str]["antenna"] = antenna
                    else:
                        self.epc_map[epc_str] = {
                            "count": 1,
                            "rssi": rssi_dbm,
                            "last_seen": now_str,
                            "antenna": antenna,
                            "epc": epc_str,
                            "location": self.zone  # Static for now
                        }

                    # Prepare payload
                    payload = self.epc_map[epc_str]

                    # Publish to MQTT
                    self.publish("factory/tag/data", payload)

                    # Also log internally (optional)
                    self.response_log.append({"time": now_str, "response": payload})

                except Exception as inner_e:
                    self.publish("factory/tag/data", {"error": str(inner_e)})

            except Exception as e:
                self.publish("factory/tag/data", {"error": str(e)})

            await asyncio.sleep(1)


    def handle_start_polling(self, _msg):
        if not self.reader:
            self.publish("status", {"error": "Not connected"})
            return
        if not self.running:
            self.running = True
            if self.event_loop is None:
                self.publish("status", {"error": "event loop not ready"})
                self.running = False
                return
            self.polling_future = asyncio.run_coroutine_threadsafe(
                self.tag_polling(), self.event_loop
            )
            self.publish("status", {"status": "create_task - tag_polling"})
        self.publish("status", {"status": "started"})

    def handle_stop_polling(self, _msg):
        self.running = False
        if self.polling_future:
            self.polling_future.cancel()
            self.polling_future = None
        self.publish("status", {"status": "stopped"})

    # ---- Dispatcher ----
    def on_message(self, _client, _userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
            command = msg.topic.split("/")[-1]
            handler_map = {
                "connect": self.handle_connect,
                "disconnect": self.handle_disconnect,
                "read_tag": self.handle_read_tag,
                "get_params": self.handle_get_params,
                "reset": self.handle_reset,
                "manual_command": self.handle_manual_command,
                "start_polling": self.handle_start_polling,
                "stop_polling": self.handle_stop_polling,
                "buzzer_toggle": self.handle_buzzer_toggle,
            }
            if command in handler_map:
                handler_map[command](payload)
            else:
                self.publish("error", {"error": f"Unknown command {command}"})
        except Exception as e:
            self.publish("error", {"error": str(e)})

class RFIDServiceManager:
    def __init__(self, use_websockets=False):
        # self.broker = "mqtt.rflex.ai"
        self.broker = "localhost"
        self.port = 1883
        self.base_topic = "rfid"
        self.services: Dict[str, RFIDService] = {}
        self.lock = threading.Lock()
        self.transport = "tcp"
        if use_websockets:
            self.port = 9001
            self.transport = "websockets"

        # Shared MQTT client for commands only
        self.mqtt = MQTTClientApp(broker=self.broker, port=self.port, username=None, password=None,
                 client_id=None, keepalive=60, protocol=mqtt.MQTTv5,
                 transport=self.transport)
        if use_websockets:
            # # Use WebSockets through Caddy (TLS terminated at 443)
            # self.mqtt.client.tls_set()  # trust system CAs (Let's Encrypt)
            self.mqtt.client.ws_set_options(path="/") 
        self.mqtt.client.on_message = self.on_message
        self.mqtt.connect()

        # Subscribe to all command topics
        for cmd in [
            "connect", "disconnect", "read_tag", "get_params",
            "reset", "manual_command", "start_polling", "stop_polling"
        ]:
            self.mqtt.subscribe(f"{self.base_topic}/+/{cmd}")

    def on_message(self, _client, _userdata, msg):
        print(f"msg: {msg}")
        try:
            payload = json.loads(msg.payload.decode())
        except Exception as e:
            print(f"[MANAGER] Invalid JSON: {msg.payload}, error: {e}")
            return
        print(f"payload: {payload}")

        parts = msg.topic.split("/")
        if len(parts) < 3:
            print(f"[MANAGER] Invalid topic: {msg.topic}")
            return
        print(f"parts: {parts}")

        _, ip, command = parts
        print(f"[MANAGER] Command {command} for {ip}: {payload}")

        print(f"self.services: {self.services}")
        if ip not in self.services and command == "connect":
            self.start_service(ip, payload)

        print(f"self.services: {self.services}")
        if ip not in self.services:
            print(f"[MANAGER] Device {ip} not found for command {command}")
            return

        service = self.services[ip]
        print(f"service: {service}")
        handler_map = {
            "connect": service.handle_connect,
            "disconnect": service.handle_disconnect,
            "read_tag": service.handle_read_tag,
            "get_params": service.handle_get_params,
            "reset": service.handle_reset,
            "manual_command": service.handle_manual_command,
            "start_polling": service.handle_start_polling,
            "stop_polling": service.handle_stop_polling,
            "buzzer_toggle": service.handle_buzzer_toggle,
        }

        if command in handler_map:
            handler_map[command](payload)
        else:
            print(f"[MANAGER] Unknown command {command} for {ip}")

    def start_service(self, ip: str, payload: dict):
        print(f"[MANAGER] Starting service for {ip}")
        ready_event = threading.Event()
        error_container = {}

        def run():
            try:
                asyncio.run(self._run_service(ip, payload, ready_event))
            except Exception as e:
                error_container["error"] = e
                ready_event.set()

        t = threading.Thread(target=run, daemon=True)
        t.start()

        # Block here until service is ready (or error)
        ready_event.wait()

        if "error" in error_container:
            raise RuntimeError(f"Failed to start service for {ip}: {error_container['error']}")

        print(f"[MANAGER] Service for {ip} started successfully")

    async def _run_service(self, ip: str, payload: dict, ready_event: threading.Event):
        try:
            service = RFIDService(payload, broker=self.broker, port=self.port,
                                base_topic=f"{self.base_topic}/{ip}")
            service.event_loop = asyncio.get_running_loop()
            service.publish("status", {"status": "service up", "ip": ip})
            with self.lock:
                self.services[ip] = service
            ready_event.set()
            await asyncio.Event().wait()  # keep running
        except Exception as e:
            print(f"[MANAGER] Error starting service for {ip}: {e}")
            ready_event.set()

# --------------------
# Main entry point
# --------------------
async def main():
    manager = RFIDServiceManager()
    print("[SYSTEM] Multi-device RFID manager is up.")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())


# Example Usage
# mosquitto_pub -h mqtt.rflex.ai -p 1883 -t rfid/192.168.1.102/connect -m '{"type":"tcp","ip":"192.168.1.102","tcp_port":49152,"zone":"Zone F"}'
# mosquitto_pub -h mqtt.rflex.ai -p 1883 -t rfid/192.168.1.101/connect -m '{"type":"tcp","ip":"192.168.1.101","tcp_port":49152,"zone":"Zone E"}'
# mosquitto_pub -h mqtt.rflex.ai -p 1883 -t rfid/192.168.1.102/start_polling -m '{}'
# mosquitto_pub -h mqtt.rflex.ai -p 1883 -t rfid/192.168.1.101/start_polling -m '{}'
# mosquitto_pub -h mqtt.rflex.ai -p 1883 -t rfid/192.168.1.102/stop_polling -m '{}'
# mosquitto_pub -h mqtt.rflex.ai -p 1883 -t rfid/192.168.1.101/stop_polling -m '{}'
# mosquitto_pub -h mqtt.rflex.ai -p 1883 -t rfid/192.168.1.102/buzzer_toggle -m '{"buzzer_mode": true}'
# mosquitto_pub -h mqtt.rflex.ai -p 1883 -t rfid/192.168.1.102/buzzer_toggle -m '{"buzzer_mode": false}'
# mosquitto_sub -h mqtt.rflex.ai -p 1883 -t "rfid/#" -v


#  1984  sudo ip addr add 192.168.1.15/24 dev enp111s0
#  1985  sudo ip route add default via 192.168.1.1
#  1986  ip addr show
#  1993  sudo nmap -sn 192.168.1.0/24
