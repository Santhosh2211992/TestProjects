import sys
import json
import random
import time
from PySide6.QtWidgets import QApplication, QWidget, QVBoxLayout, QPushButton, QLabel
from PySide6.QtCore import QTimer, Qt
import paho.mqtt.client as mqtt
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

# ---------------------------
# MQTT Setup
# ---------------------------
BROKER = "localhost"
PORT = 1883
TOPIC = "factory/bin_flow"

client = mqtt.Client()
client.connect(BROKER, PORT, 60)
client.loop_start()

# ---------------------------
# Workflow Tasks
# ---------------------------
tasks = ["bin_registration", "job_allocation", "verification", "job_closeout", "dispatch"]

# To store simulated task results
workflow_log = {}

# ---------------------------
# GUI App
# ---------------------------
class JobSimulator(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Job Simulator")
        self.setFixedSize(300, 200)
        self.layout = QVBoxLayout()
        self.setLayout(self.layout)

        self.status_label = QLabel("Status: Idle")
        self.status_label.setAlignment(Qt.AlignCenter)
        self.layout.addWidget(self.status_label)

        self.start_button = QPushButton("Start New Job")
        self.start_button.clicked.connect(self.start_job)
        self.layout.addWidget(self.start_button)

        self.stop_button = QPushButton("Stop Job")
        self.stop_button.clicked.connect(self.stop_job)
        self.layout.addWidget(self.stop_button)

        self.current_task_index = 0
        self.job_running = False
        self.timer = QTimer()
        self.timer.setInterval(1000)  # 1 second per task
        self.timer.timeout.connect(self.run_next_task)

    def send_mqtt(self, task, data=None):
        payload = {"task": task, "data": data or {}}
        client.publish(TOPIC, json.dumps(payload))
        print("Sent MQTT:", payload)

    def start_job(self):
        if self.job_running:
            self.status_label.setText("Job already running!")
            return
        self.status_label.setText("Job Started")
        self.current_task_index = 0
        self.job_running = True
        workflow_log.clear()
        self.timer.start()

    def run_next_task(self):
        if self.current_task_index >= len(tasks):
            self.timer.stop()
            self.status_label.setText("Job Completed")
            self.job_running = False
            return

        task = tasks[self.current_task_index]
        # Simulate data
        data = {
            "uid": f"BIN{random.randint(1000,9999)}",
            "tare_weight": round(random.uniform(1,5), 2),
            "gross_weight": round(random.uniform(5,50),2),
            "target_count": random.randint(10,50),
            "count_ok": random.choice([True, False])
        }
        workflow_log[task] = data
        self.send_mqtt(task, data)
        self.status_label.setText(f"Task: {task.replace('_',' ').title()}")
        self.current_task_index += 1

    def stop_job(self):
        if not self.job_running:
            self.status_label.setText("No job running!")
            return
        self.timer.stop()
        self.job_running = False
        self.status_label.setText("Job Stopped. PDF generated.")
        self.generate_pdf()

    def generate_pdf(self):
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
window = JobSimulator()
window.show()
sys.exit(app.exec())
