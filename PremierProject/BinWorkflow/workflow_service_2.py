import json
import random
import time
import paho.mqtt.client as mqtt
from InventoryManager_module import InventoryManager
from PartDB import PartDatabase
from datetime import datetime
from WeighingScale_module import WeighingScale
import threading
# ----------------------------
# DB CONFIG
# ----------------------------
POSTGRES_HOST = "172.18.0.16"   # your Postgres container or host IP
POSTGRES_PORT = 5432
POSTGRES_DB = "postgres"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "your-super-secret-and-long-postgres-password"
TABLE_NAME = "public.bin_part_weight_db"
EMPTY_WEIGHT_TABLE_NAME = "public.rfid_bin_db"

# ----------------------------
# MQTT CONFIG
# ----------------------------
BROKER = "localhost"
PORT = 1883
TOPIC_ACK = "factory/bin_flow/ack"

# ----------------------------
# MOCK MODULES
# ----------------------------
class MockRFID:
    def read_uid(self):
        return f"RFID{random.randint(1000, 9999)}"

class MockScale:
    def __init__(self):
        self.tare = 2.5  # kg
    def get_weight(self):
        return round(random.uniform(5.0, 15.0), 2)

class QRScanner:
    def __init__(self):
            # Initialize self.inventory_manager with expected codes
            self.inventory_manager = InventoryManager(port='/dev/ttyACM0', expected_codes=["ABCDE", "XYZ789"])
            self.current_code = None
            self.previous_code = None

    def scan_job(self):
        # Start scanning
        self.inventory_manager.start_scan()  # assumes start_scan() in InventoryManager starts the QRcodeScanner

        scanned_code = None
        try:
            while True:
                time.sleep(1)  # keep main thread alive

                if self.inventory_manager.current_code:
                    scanned_code = self.inventory_manager.current_code
                    print(f"Valid QRcode scanned: {scanned_code}")

                    # Stop scanner from main thread
                    self.inventory_manager.stop_scanner()

                    # Reset so a future scan_job call can wait again
                    self.inventory_manager.current_code = None
                    break
        except KeyboardInterrupt:
            print("Exiting...")
            self.inventory_manager.stop_scanner()

        return scanned_code

class MockPrinter:
    def print_label(self, job_data):
        print(f"[Printer] Printing release doc for {job_data['job_id']} - Bin {job_data['bin_uid']}")

# ----------------------------
# WORKFLOW DAEMON
# ----------------------------
class WorkflowDaemon:
    def __init__(self):
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect(BROKER, PORT, 60)
        self.client.loop_start()
        db = PartDatabase(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            tablename=TABLE_NAME
            )
        
        empty_weight_db = PartDatabase(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            tablename=EMPTY_WEIGHT_TABLE_NAME
            )

        # mock devices
        self.rfid = MockRFID()
        self.scale = MockScale()
        self.qr = QRScanner()
        self.db = db
        self.empty_weight_db = empty_weight_db
        self.printer = MockPrinter()
        self.scale_port="/dev/ttyUSB0"
        self.weighing_scale = WeighingScale(
            port=self.scale_port,
            stable_seconds=2,
            stable_callback=self.on_stable_weight
        )
        self.stable_weight = None

        self.job_count = 0

        self.task_order = ["job_allocation", "verification", "job_closeout", "dispatch"]
        self.client.publish("rfid/192.168.1.102/connect", '{"type":"tcp","ip":"192.168.1.102","tcp_port":49152,"zone":"Zone E"}')

    def on_stable_weight(self, weight):
        print(f"Stable weight detected: {weight} kg")
        self.stable_weight = weight

    def get_stable_weight(self):
        """
        Starts the scale monitoring and waits until the weight stabilizes.
        Returns the stable weight.
        """
        # Run monitor in a separate thread
        self.monitor_thread = threading.Thread(target=self.weighing_scale.monitor)
        self.monitor_thread.start()

        # Main thread waits until stable weight is reported
        while not self.weighing_scale.stable_reported:
            time.sleep(0.1)

        # Stop the monitor from main thread
        self.weighing_scale.stop()
        self.monitor_thread.join()
        print("[InventoryManager] Scale monitoring stopped.")

        return self.stable_weight

    # --- Default Callbacks ---
    def on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            print("✅ Connected to MQTT Broker")

            # Subscribe using single-level wildcard for device IP
            topic = "rfid/+/factory/tag/data"
            self.client.subscribe(topic)
            print(f"Subscribed to topic: {topic}")
        else:
            print(f"❌ Connection failed, return code {rc}")

    def on_message(self, client, userdata, msg):
        try:
            # Decode the payload from bytes to string
            payload_str = msg.payload.decode()
            
            # Parse JSON string into Python dictionary
            data = json.loads(payload_str)

            # Extract the fields
            epc = data.get("epc")
            count = data.get("count")
            rssi = data.get("rssi")
            last_seen_str = data.get("last_seen")
            antenna = data.get("antenna")
            location = data.get("location")

            # Convert last_seen to datetime object
            last_seen = datetime.fromisoformat(last_seen_str) if last_seen_str else None

            print(f"EPC: {epc}, Count: {count}, RSSI: {rssi}, Last Seen: {last_seen}, Antenna: {antenna}, Location: {location}")

            empty_bin_weight_data = self.empty_weight_db.get_row(key_value="E7 76 09 89 49 00 37 33 90 00 00 01", key_column="epc")
            # print(f"empty bin weight data: {empty_bin_weight_data}")

            # Suppose this comes from scale reading
            loaded_bin_weight = self.get_stable_weight()

            # Extract empty bin weight
            empty_weight = empty_bin_weight_data.get("empty_bin_weight", 0)

            # Compute net load
            net_weight = loaded_bin_weight - empty_weight

            print(f"Empty bin weight: {empty_weight:.3f} kg")
            print(f"Loaded bin weight: {loaded_bin_weight:.3f} kg")
            print(f"Net load weight: {net_weight:.3f} kg")

            self.client.publish("rfid/192.168.1.102/stop_polling", '{}')

        except json.JSONDecodeError as e:
            print("Error decoding JSON:", e)
        except Exception as e:
            print("Unexpected error:", e)

    def run_task(self, task):
        data = {}

        if task == "job_allocation":
            job = self.qr.scan_job()
            data = self.db.get_part_details(job)
            # self.db.log_event(task, data)

        elif task == "verification":
            self.client.publish("rfid/192.168.1.102/start_polling", '{}')
            gross = self.scale.get_weight()
            tare = self.scale.tare
            net = gross - tare
            count = int(net / 0.2)  # assume each part = 200g
            data = {"gross_weight": gross, "tare": tare, "net": net, "count": count, "count_ok": count >= 10}
            # self.db.log_event(task, data)

        elif task == "job_closeout":
            data = {"job_id": f"JOB{self.job_count}", "status": "closed"}
            # Ensure a green/red light gets turned on for "go/no go" result.
            # self.db.log_event(task, data)

        elif task == "dispatch":
            job_data = {"job_id": f"JOB{self.job_count}", "bin_uid": f"BIN{1000+self.job_count}"}
            self.printer.print_label(job_data)
            data = {"dispatch_id": f"DSP{self.job_count}", "status": "dispatched"}
            # self.db.log_event(task, data)
            self.job_count += 1

        payload = {"task": task, "data": data}
        self.client.publish(TOPIC_ACK, json.dumps(payload))
        print(f"[Workflow Service] Published {task}: {data}")

    def start_service(self):
        print("Workflow Service ready. Tasks:", self.task_order)
        print("Type 'exit' to quit.")
        while True:
            task = input("Enter completed task: ").strip()
            if task.lower() == "exit":
                break
            if task not in self.task_order:
                print("Invalid task name. Try again.")
                continue
            self.run_task(task)

        self.client.loop_stop()
        self.client.disconnect()

# ----------------------------
# MAIN
# ----------------------------
if __name__ == "__main__":
    daemon = WorkflowDaemon()
    daemon.start_service()
