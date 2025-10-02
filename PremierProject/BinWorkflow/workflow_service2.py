import json
import random
import time
import paho.mqtt.client as mqtt
from InventoryManager_module import InventoryManager
from PartDB import PartDatabase
# ----------------------------
# DB CONFIG
# ----------------------------
POSTGRES_HOST = "172.18.0.4"   # your Postgres container or host IP
POSTGRES_PORT = 5432
POSTGRES_DB = "postgres"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "your-super-secret-and-long-postgres-password"
TABLE_NAME = "public.bin_part_weight_db"

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

        # mock devices
        self.rfid = MockRFID()
        self.scale = MockScale()
        self.qr = QRScanner()
        self.db = db
        self.printer = MockPrinter()

        self.job_count = 0

        self.task_order = ["job_allocation", "verification", "job_closeout", "dispatch"]

    def run_task(self, task):
        data = {}

        if task == "job_allocation":
            job = self.qr.scan_job()
            data = self.db.get_part_details(job)
            # self.db.log_event(task, data)

        elif task == "verification":
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
