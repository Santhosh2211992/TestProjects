import sys
import json
import random
from PySide6.QtWidgets import QApplication, QWidget, QVBoxLayout, QLabel, QCheckBox, QPushButton
from PySide6.QtCore import QTimer
import paho.mqtt.client as mqtt

# ---------------------------
# MQTT Setup
# ---------------------------
BROKER = "localhost"
PORT = 1883
TOPIC = "factory/bin_flow"

client = mqtt.Client()

# Store acknowledgements
acknowledgements = {
    "bin_registration": False,
    "job_allocation": False,
    "verification": False,
    "job_closeout": False,
    "dispatch": False
}

def on_connect(client, userdata, flags, rc):
    print("Connected with result code", rc)
    client.subscribe(TOPIC + "/ack")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        task = payload.get("task")
        print(f"Acknowledged: {task}")
        if task in acknowledgements:
            acknowledgements[task] = True
    except Exception as e:
        print("Error parsing message:", e)

client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER, PORT, 60)
client.loop_start()

# ---------------------------
# PySide6 GUI
# ---------------------------
class BinWorkflowApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Bin Workflow Monitor")
        self.layout = QVBoxLayout()

        self.checkboxes = {}
        for task in acknowledgements:
            cb = QCheckBox(task.replace("_", " ").title())
            cb.setEnabled(False)
            self.checkboxes[task] = cb
            self.layout.addWidget(cb)

        self.send_button = QPushButton("Start Bin Registration")
        self.send_button.clicked.connect(self.start_bin_registration)
        self.layout.addWidget(self.send_button)

        self.setLayout(self.layout)

        # Timer to poll acknowledgements and update GUI
        self.timer = QTimer()
        self.timer.setInterval(500)  # 500ms
        self.timer.timeout.connect(self.update_checklist)
        self.timer.start()

    def send_mqtt(self, task, data=None):
        payload = {"task": task, "data": data or {}}
        client.publish(TOPIC, json.dumps(payload))
        print("Sent MQTT:", payload)

    def update_checklist(self):
        for task, cb in self.checkboxes.items():
            if acknowledgements[task] and not cb.isChecked():
                cb.setChecked(True)
                # Automatically trigger next step
                if task == "bin_registration":
                    self.send_job_allocation()
                elif task == "job_allocation":
                    self.send_verification()
                elif task == "verification":
                    self.send_job_closeout()
                elif task == "job_closeout":
                    self.send_dispatch()

    # ---------------------------
    # Workflow methods
    # ---------------------------
    def start_bin_registration(self):
        # simulate reading RFID UID and tare weight
        uid = f"BIN{random.randint(1000,9999)}"
        tare_weight = round(random.uniform(1.0, 5.0), 2)
        self.send_mqtt("bin_registration", {"uid": uid, "tare_weight": tare_weight})

    def send_job_allocation(self):
        job_qr = f"JOB{random.randint(100,999)}"
        sku = f"SKU{random.randint(1,20)}"
        target_count = random.randint(10,50)
        self.send_mqtt("job_allocation", {"job": job_qr, "sku": sku, "target_count": target_count})

    def send_verification(self):
        gross_weight = round(random.uniform(5.0, 50.0), 2)
        uid = f"BIN{random.randint(1000,9999)}"
        tare_weight = round(random.uniform(1.0, 5.0), 2)
        net_weight = gross_weight - tare_weight
        part_mean = 2.0
        estimated_count = round(net_weight / part_mean)
        self.send_mqtt("verification", {
            "gross_weight": gross_weight,
            "uid": uid,
            "tare_weight": tare_weight,
            "net_weight": net_weight,
            "estimated_count": estimated_count
        })

    def send_job_closeout(self):
        count_ok = random.choice([True, False])
        self.send_mqtt("job_closeout", {"count_ok": count_ok})

    def send_dispatch(self):
        self.send_mqtt("dispatch", {"status": "dispatched"})

# ---------------------------
# Run App
# ---------------------------
app = QApplication(sys.argv)
window = BinWorkflowApp()
window.show()
sys.exit(app.exec())
