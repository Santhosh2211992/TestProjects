import sys
import json
import time
from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QCheckBox,
    QPushButton, QTextEdit, QSizePolicy, QFrame
)
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

acknowledgements = {
    "bin_registration": False,
    "job_allocation": False,
    "verification": False,
    "job_closeout": False,
    "dispatch": False
}

current_job_log = {}
all_jobs_log = []

task_instructions = {
    "bin_registration": "Place empty bin on weighing station and scan RFID tag.",
    "job_allocation": "Scan Job QR code to allocate the job.",
    "verification": "Place bin on scale and verify net weight vs target count.",
    "job_closeout": "Check part count, supervisor override if mismatch.",
    "dispatch": "Dispatch bin to next station."
}

tasks = ["bin_registration", "job_allocation", "verification", "job_closeout", "dispatch"]

# ---------------------------
# MQTT callbacks
# ---------------------------
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
# Professional GUI App
# ---------------------------
class JobWorkflowApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Professional Continuous Job Workflow")
        self.showFullScreen()  # Fullscreen

        # Main vertical layout
        self.main_layout = QVBoxLayout(self)
        self.setLayout(self.main_layout)
        self.main_layout.setContentsMargins(20, 20, 20, 20)
        self.main_layout.setSpacing(15)

        # ---------------------------
        # Top status panel
        # ---------------------------
        self.status_frame = QFrame()
        self.status_frame.setFrameShape(QFrame.StyledPanel)
        self.status_layout = QHBoxLayout()
        self.status_frame.setLayout(self.status_layout)
        self.main_layout.addWidget(self.status_frame)

        self.status_label = QLabel("Status: Idle | Job: None | Task: None")
        self.status_label.setAlignment(Qt.AlignCenter)
        self.status_label.setStyleSheet("font-size: 20pt; font-weight: bold; color: green;")
        self.status_layout.addWidget(self.status_label)

        # ---------------------------
        # Middle panel: Instructions (left) + Job/Details + Checklist (right)
        # ---------------------------
        self.middle_frame = QFrame()
        self.middle_layout = QHBoxLayout()
        self.middle_frame.setLayout(self.middle_layout)
        self.middle_frame.setFrameShape(QFrame.StyledPanel)
        self.main_layout.addWidget(self.middle_frame, stretch=3)

        # Instructions panel (left)
        self.instructions = QTextEdit()
        self.instructions.setReadOnly(True)
        self.instructions.setStyleSheet("font-size: 16pt; padding: 10px;")
        self.instructions.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.middle_layout.addWidget(self.instructions, stretch=2)

        # Right panel (Job Details + Checklist stacked vertically)
        self.right_panel = QFrame()
        self.right_layout = QVBoxLayout()
        self.right_panel.setLayout(self.right_layout)
        self.middle_layout.addWidget(self.right_panel, stretch=2)

        # Job/Part details panel
        self.job_details = QTextEdit()
        self.job_details.setReadOnly(True)
        self.job_details.setStyleSheet("font-size: 14pt; padding: 10px;")
        self.job_details.setPlaceholderText("Job/Part details will appear here after QR scan...")
        self.right_layout.addWidget(self.job_details, stretch=2)

        # Checklist panel
        self.checklist_frame = QFrame()
        self.checklist_frame.setFrameShape(QFrame.StyledPanel)
        self.checklist_layout = QVBoxLayout()
        self.checklist_frame.setLayout(self.checklist_layout)
        self.right_layout.addWidget(self.checklist_frame, stretch=1)

        self.checkboxes = {}
        for task in acknowledgements:
            cb = QCheckBox(task.replace("_"," ").title())
            cb.setEnabled(False)
            cb.setStyleSheet("font-size: 16pt;")
            self.checkboxes[task] = cb
            self.checklist_layout.addWidget(cb)

        # ---------------------------
        # Bottom panel: Buttons
        # ---------------------------
        self.button_frame = QFrame()
        self.button_layout = QHBoxLayout()
        self.button_frame.setLayout(self.button_layout)
        self.button_frame.setFrameShape(QFrame.StyledPanel)
        self.main_layout.addWidget(self.button_frame)

        # Buttons
        self.start_button = QPushButton("START JOB")
        self.start_button.setStyleSheet("font-size: 16pt; background-color: green; color: white; padding: 10px;")
        self.start_button.clicked.connect(self.start_job)
        self.button_layout.addWidget(self.start_button)

        self.stop_button = QPushButton("STOP SYSTEM")
        self.stop_button.setStyleSheet("font-size: 16pt; background-color: orange; color: white; padding: 10px;")
        self.stop_button.clicked.connect(self.stop_system)
        self.button_layout.addWidget(self.stop_button)

        self.emergency_button = QPushButton("EMERGENCY STOP")
        self.emergency_button.setStyleSheet("font-size: 16pt; background-color: red; color: white; padding: 10px;")
        self.emergency_button.clicked.connect(self.emergency_stop)
        self.button_layout.addWidget(self.emergency_button)

        self.quit_button = QPushButton("QUIT")
        self.quit_button.setStyleSheet("font-size: 16pt; background-color: gray; color: white; padding: 10px;")
        self.quit_button.clicked.connect(self.close)
        self.button_layout.addWidget(self.quit_button)

        # ---------------------------
        # Job control
        # ---------------------------
        self.current_task_index = 0
        self.job_running = False
        self.timer = QTimer()
        self.timer.setInterval(500)
        self.timer.timeout.connect(self.check_ack)

    # ---------------------------
    # MQTT send
    # ---------------------------
    def send_mqtt(self, task, data=None):
        payload = {"task": task, "data": data or {}}
        client.publish(TOPIC_CMD, json.dumps(payload))
        print("Sent MQTT:", payload)

    # ---------------------------
    # Job logic
    # ---------------------------
    def start_job(self):
        if self.job_running:
            self.status_label.setText("Job already running!")
            return
        self.status_label.setText("Job Started")
        self.current_task_index = 0
        self.job_running = True
        current_job_log.clear()
        self.job_details.clear()
        for task, cb in self.checkboxes.items():
            cb.setChecked(False)
            acknowledgements[task] = False
        self.send_next_task()
        self.timer.start()

    def send_next_task(self):
        if self.current_task_index >= len(tasks):
            self.timer.stop()
            self.status_label.setText("Job Completed. Starting next job...")
            all_jobs_log.append(copy.deepcopy(current_job_log))
            self.job_running = False
            QTimer.singleShot(1000, self.start_job)
            return

        task = tasks[self.current_task_index]
        self.status_label.setText(f"Status: Running | Job: {len(all_jobs_log)+1} | Task: {task.replace('_',' ').title()}")
        self.instructions.setHtml(f"<b>Instructions:</b><br>{task_instructions[task]}")

        # Simulated data
        data = {
            "uid": f"BIN{1000 + len(all_jobs_log)}",
            "tare_weight": round(1 + self.current_task_index, 2),
            "gross_weight": round(10 + self.current_task_index,2),
            "target_count": 10 + self.current_task_index,
            "count_ok": True
        }
        self.send_mqtt(task, data)

    def check_ack(self):
        if not self.job_running or self.current_task_index >= len(tasks):
            return
        task = tasks[self.current_task_index]
        if acknowledgements[task]:
            self.checkboxes[task].setChecked(True)

            # If job_allocation, update job/part details panel
            if task == "job_allocation":
                data = current_job_log.get(task, {})
                details_text = (
                    f"<b>Job Details:</b><br>"
                    f"UID: {data.get('uid', '')}<br>"
                    f"Tare Weight: {data.get('tare_weight', '')}<br>"
                    f"Gross Weight: {data.get('gross_weight', '')}<br>"
                    f"Target Count: {data.get('target_count', '')}<br>"
                    f"Count OK: {data.get('count_ok', '')}"
                )
                self.job_details.setHtml(details_text)

            self.current_task_index += 1
            self.send_next_task()

    def stop_system(self):
        if self.job_running and self.current_task_index != 0:
            self.status_label.setText("Cannot stop during an ongoing job!")
            return
        self.generate_consolidated_pdf()
        self.status_label.setText("System Stopped. Consolidated PDF generated.")

    def emergency_stop(self):
        self.timer.stop()
        self.job_running = False
        self.status_label.setText("!!! EMERGENCY STOP ACTIVATED !!!")
        self.instructions.setText("System halted immediately. Resolve issues and restart.")

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
                    task_name.replace("_", " ").title(),
                    str(data.get("uid", "")),
                    str(data.get("tare_weight", "")),
                    str(data.get("gross_weight", "")),
                    str(data.get("target_count", "")),
                    str(data.get("count_ok", ""))
                ])

            # Create the table
            table = Table(table_data, colWidths=[80]*6)
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.gray),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
                ('FONT', (0, 0), (-1, -1), 'Helvetica', 10),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER')
            ]))

            # Calculate table height
            table_height = len(table_data) * 18  # approximate row height
            if y - table_height < 50:  # check if new page needed
                c.showPage()
                y = height - 50

            table.wrapOn(c, width, y)
            table.drawOn(c, 50, y - table_height)
            y -= table_height + 30

        c.save()
        print(f"Consolidated PDF generated: {filename}")

# ---------------------------
# Run App
# ---------------------------
app = QApplication(sys.argv)
window = JobWorkflowApp()
window.show()
sys.exit(app.exec())
