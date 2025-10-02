import sys
import json
import time
from PySide6.QtWidgets import QApplication, QWidget, QVBoxLayout, QLabel, QCheckBox, QPushButton
from PySide6.QtCore import QTimer, Qt
import paho.mqtt.client as mqtt
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.platypus import Table, TableStyle
from reportlab.lib import colors
import copy

# ---------------------------
# MQTT Setup
# ---------------------------
BROKER = "localhost"
PORT = 1883
TOPIC_CMD = "factory/bin_flow"
TOPIC_ACK = "factory/bin_flow/ack"

client = mqtt.Client()

# Track acknowledgements for current job
acknowledgements = {
    "bin_registration": False,
    "job_allocation": False,
    "verification": False,
    "job_closeout": False,
    "dispatch": False
}

# Store workflow data for current job and all jobs
current_job_log = {}
all_jobs_log = []

def on_connect(client, userdata, flags, rc):
    print("Connected with result code", rc)
    client.subscribe(TOPIC_ACK)

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        task = payload.get("task")
        if task in acknowledgements:
            acknowledgements[task] = True
            current_job_log[task] = payload.get("data", {})
            print(f"Acknowledged: {task}")
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
# GUI App
# ---------------------------
class JobWorkflowApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Continuous Job Workflow")
        self.setFixedSize(450, 450)
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

        self.stop_button = QPushButton("Stop System & Generate PDF")
        self.stop_button.clicked.connect(self.stop_system)
        self.layout.addWidget(self.stop_button)

        self.current_task_index = 0
        self.job_running = False

        # Timer to check MQTT acknowledgements
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
        current_job_log.clear()
        # Reset checkboxes and acknowledgements
        for task, cb in self.checkboxes.items():
            cb.setChecked(False)
            acknowledgements[task] = False
        self.send_next_task()
        self.timer.start()

    def send_next_task(self):
        print(f"Current job log0: {current_job_log}")
        print(f"All job log0: {all_jobs_log}")
        if self.current_task_index >= len(tasks):
            # Job completed
            self.timer.stop()
            self.status_label.setText("Job Completed. Starting next job...")
            all_jobs_log.append(copy.deepcopy(current_job_log))
            self.job_running = False
            QTimer.singleShot(1000, self.start_job)  # start next job after 1 sec
            return

        task = tasks[self.current_task_index]
        self.status_label.setText(f"Waiting for: {task.replace('_',' ').title()}")
        # Simulated data
        data = {
            "uid": f"BIN{1000 + len(all_jobs_log)}",
            "tare_weight": round(1 + self.current_task_index, 2),
            "gross_weight": round(10 + self.current_task_index,2),
            "target_count": 10 + self.current_task_index,
            "count_ok": True
        }
        # current_job_log[task] = data
        # print(f"Current job log1: {current_job_log}")
        self.send_mqtt(task, data)

    def check_ack(self):
        if not self.job_running or self.current_task_index >= len(tasks):
            return
        task = tasks[self.current_task_index]
        if acknowledgements[task]:
            self.checkboxes[task].setChecked(True)
            self.current_task_index += 1
            self.send_next_task()

    def stop_system(self):
        if self.job_running and self.current_task_index != 0:
            self.status_label.setText("Cannot stop during an ongoing job!")
            return
        self.generate_consolidated_pdf()
        self.status_label.setText("System Stopped. Consolidated PDF generated.")

    # ---------------------------
    # PDF generation
    # ---------------------------
    def generate_consolidated_pdf(self):
        filename = f"consolidated_job_report_{int(time.time())}.pdf"
        c = canvas.Canvas(filename, pagesize=letter)
        width, height = letter
        c.setFont("Helvetica-Bold", 16)
        c.drawString(50, height - 50, "Consolidated Job Workflow Report")

        y = height - 80
        for i, job in enumerate(all_jobs_log):
            c.setFont("Helvetica-Bold", 14)
            c.drawString(50, y, f"Job {i+1}")
            y -= 20

            # Build table data
            table_data = [["Task", "UID", "Tare", "Gross", "Target Count", "Count OK"]]
            for task_name, data in job.items():
                table_data.append([
                    task_name.replace("_"," ").title(),
                    str(data.get("uid","")),
                    str(data.get("tare_weight","")),
                    str(data.get("gross_weight","")),
                    str(data.get("target_count","")),
                    str(data.get("count_ok",""))
                ])
            table = Table(table_data, colWidths=[80]*6)
            table.setStyle(TableStyle([
                ('BACKGROUND', (0,0), (-1,0), colors.gray),
                ('TEXTCOLOR',(0,0),(-1,0),colors.whitesmoke),
                ('GRID', (0,0), (-1,-1), 0.5, colors.black),
                ('FONT', (0,0), (-1,-1), 'Helvetica', 10)
            ]))
            table.wrapOn(c, width, y)
            table.drawOn(c, 50, y - len(table_data)*15)
            y -= len(table_data)*15 + 30
            if y < 100:
                c.showPage()
                y = height - 50
        c.save()
        print(f"Consolidated PDF generated: {filename}")

# ---------------------------
# Run App
# ---------------------------
app = QApplication(sys.argv)
window = JobWorkflowApp()
window.show()
sys.exit(app.exec())
