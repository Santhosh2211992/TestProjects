import sys
import json
import time
from PySide6.QtWidgets import QApplication, QWidget, QVBoxLayout, QLabel, QCheckBox, QPushButton
from PySide6.QtCore import QTimer, Qt
import paho.mqtt.client as mqtt
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

# ---------------------------
# MQTT Setup
# ---------------------------
BROKER = "localhost"
PORT = 1883
TOPIC_CMD = "factory/bin_flow"
TOPIC_ACK = "factory/bin_flow/ack"

client = mqtt.Client()

# Store acknowledgements
acknowledgements = {
    "bin_registration": False,
    "job_allocation": False,
    "verification": False,
    "job_closeout": False,
    "dispatch": False
}

workflow_log = {}  # Store simulated data for PDF

def on_connect(client, userdata, flags, rc):
    print("Connected with result code", rc)
    client.subscribe(TOPIC_ACK)

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        task = payload.get("task")
        print(f"Acknowledged: {task}")
        if task in acknowledgements:
            acknowledgements[task] = True
            workflow_log[task] = payload.get("data", {})
    except Exception as e:
        print("Error parsing message:", e)

client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER, PORT, 60)
client.loop_start()

# ---------------------------
# Workflow tasks
# ---------------------------
tasks = ["bin_registration", "job_allocation", "verification", "job_closeout", "dispatch"]

# ---------------------------
# PySide6 GUI
# ---------------------------
class JobWorkflowApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Job Workflow Monitor")
        self.setFixedSize(400, 400)
        self.layout = QVBoxLayout()
        self.setLayout(self.layout)

        self.status_label = QLabel("Status: Idle")
        self.status_label.setAlignment(Qt.AlignCenter)
        self.layout.addWidget(self.status_label)

        # Checklist
        self.checkboxes = {}
        for task in acknowledgements:
            cb = QCheckBox(task.replace("_"," ").title())
            cb.setEnabled(False)
            self.checkboxes[task] = cb
            self.layout.addWidget(cb)

        # Buttons
        self.start_button = QPushButton("Start New Job")
        self.start_button.clicked.connect(self.start_job)
        self.layout.addWidget(self.start_button)

        self.stop_button = QPushButton("Stop Job")
        self.stop_button.clicked.connect(self.stop_job)
        self.layout.addWidget(self.stop_button)

        self.current_task_index = 0
        self.job_running = False

        # Timer to check for MQTT ack
        self.timer = QTimer()
        self.timer.setInterval(500)  # check every 0.5s
        self.timer.timeout.connect(self.check_ack)

    # ---------------------------
    # MQTT send
    # ---------------------------
    def send_mqtt(self, task, data=None):
        payload = {"task": task, "data": data or {}}
        client.publish(TOPIC_CMD, json.dumps(payload))
        print("Sent MQTT:", payload)

    # ---------------------------
    # Job control
    # ---------------------------
    def start_job(self):
        if self.job_running:
            self.status_label.setText("Job already running!")
            return
        self.status_label.setText("Job Started")
        self.current_task_index = 0
        self.job_running = True
        workflow_log.clear()
        # Reset checkboxes and acknowledgements
        for task, cb in self.checkboxes.items():
            cb.setChecked(False)
            acknowledgements[task] = False
        self.send_next_task()
        self.timer.start()

    def send_next_task(self):
        if self.current_task_index >= len(tasks):
            self.status_label.setText("Job Completed")
            self.job_running = False
            self.timer.stop()
            return
        task = tasks[self.current_task_index]
        self.status_label.setText(f"Waiting for: {task.replace('_',' ').title()}")
        # Simulated data
        data = {
            "uid": f"BIN{1000 + self.current_task_index}",
            "tare_weight": round(1 + self.current_task_index, 2),
            "gross_weight": round(10 + self.current_task_index,2),
            "target_count": 10 + self.current_task_index,
            "count_ok": True
        }
        workflow_log[task] = data
        self.send_mqtt(task, data)

    def check_ack(self):
        if not self.job_running or self.current_task_index >= len(tasks):
            return
        task = tasks[self.current_task_index]
        if acknowledgements[task]:
            self.checkboxes[task].setChecked(True)
            self.current_task_index += 1
            self.send_next_task()

    def stop_job(self):
        if not self.job_running:
            self.status_label.setText("No job running!")
            return
        self.timer.stop()
        self.job_running = False
        self.status_label.setText("Job Stopped. PDF generated.")
        self.generate_pdf()

    # ---------------------------
    # PDF generation
    # ---------------------------
    def generate_pdf(self):
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas
        import time

        filename = f"job_report_{int(time.time())}.pdf"
        c = canvas.Canvas(filename, pagesize=letter)
        width, height = letter
        y = height - 50
        c.setFont("Helvetica-Bold", 16)
        c.drawString(50, y, "Job Workflow Report")
        y -= 30
        c.setFont("Helvetica", 12)
        for task, data in workflow_log.items():
            c.drawString(50, y, f"Task: {task.replace('_',' ').title()}")
            y -= 15
            for k, v in data.items():
                c.drawString(70, y, f"{k}: {v}")
                y -= 15
            y -= 10
        c.save()
        print(f"PDF generated: {filename}")

# ---------------------------
# Run App
# ---------------------------
app = QApplication(sys.argv)
window = JobWorkflowApp()
window.show()
sys.exit(app.exec())
