import json
import random
import time
import paho.mqtt.client as mqtt

# ----------------------------
# CONFIG
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

class MockQRScanner:
    def scan_job(self):
        return {"job_id": f"JOB{random.randint(100,999)}", "sku": "SKU123", "target_count": 50}

class MockPrinter:
    def print_label(self, job_data):
        print(f"[Printer] Printing release doc for {job_data['job_id']} - Bin {job_data['bin_uid']}")

class MockSupabase:
    def log_event(self, event, data):
        print(f"[Supabase] Event: {event} | Data: {data}")

# ----------------------------
# WORKFLOW DAEMON
# ----------------------------
class WorkflowDaemon:
    def __init__(self):
        self.client = mqtt.Client()
        self.client.connect(BROKER, PORT, 60)
        self.client.loop_start()

        # mock devices
        self.rfid = MockRFID()
        self.scale = MockScale()
        self.qr = MockQRScanner()
        self.printer = MockPrinter()
        self.db = MockSupabase()

        self.job_count = 0

        self.task_order = ["bin_registration", "job_allocation", "verification", "job_closeout", "dispatch"]

    def run_task(self, task):
        data = {}

        if task == "bin_registration":
            uid = self.rfid.read_uid()
            tare = round(random.uniform(2.0, 3.0), 2)
            data = {"bin_uid": uid, "tare_weight": tare}
            self.db.log_event(task, data)

        elif task == "job_allocation":
            job = self.qr.scan_job()
            data = {**job}
            self.db.log_event(task, data)

        elif task == "verification":
            gross = self.scale.get_weight()
            tare = self.scale.tare
            net = gross - tare
            count = int(net / 0.2)  # assume each part = 200g
            data = {"gross_weight": gross, "tare": tare, "net": net, "count": count, "count_ok": count >= 10}
            self.db.log_event(task, data)

        elif task == "job_closeout":
            data = {"job_id": f"JOB{self.job_count}", "status": "closed"}
            self.db.log_event(task, data)

        elif task == "dispatch":
            job_data = {"job_id": f"JOB{self.job_count}", "bin_uid": f"BIN{1000+self.job_count}"}
            self.printer.print_label(job_data)
            data = {"dispatch_id": f"DSP{self.job_count}", "status": "dispatched"}
            self.db.log_event(task, data)
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
