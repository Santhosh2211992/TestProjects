"""
Refactored Workflow GUI - MQTT Based
Professional UI that communicates via MQTT instead of direct calls
"""

import sys
import json
import time
from datetime import datetime
from typing import Optional, Dict
import paho.mqtt.client as mqtt
from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QTextEdit, QFrame, QProgressBar
)
from PySide6.QtCore import QTimer, Qt, Signal, QObject
from PySide6.QtGui import QFont
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.platypus import Table, TableStyle
from reportlab.lib import colors
import copy
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ==========================================
# MQTT SIGNAL BRIDGE
# ==========================================

class MQTTSignals(QObject):
    """Qt signals for MQTT callbacks (thread-safe)"""
    workflow_status = Signal(dict)
    task_ack = Signal(dict)
    job_complete = Signal(dict)
    error = Signal(dict)


# ==========================================
# MQTT CLIENT WRAPPER
# ==========================================

class WorkflowMQTTClient:
    """MQTT client for workflow GUI communication"""
    
    def __init__(self, broker: str = "localhost", port: int = 1883):
        self.broker = broker
        self.port = port
        self.signals = MQTTSignals()
        
        self.client = mqtt.Client(client_id="workflow_gui")
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.connected = False
    
    def _on_connect(self, client, userdata, flags, rc):
        """MQTT connection callback"""
        if rc == 0:
            logger.info("GUI connected to MQTT broker")
            self.connected = True
            
            # Subscribe to workflow topics
            self.client.subscribe("factory/workflow/status")
            self.client.subscribe("factory/workflow/ack")
            self.client.subscribe("factory/workflow/job_complete")
            self.client.subscribe("factory/workflow/error")
        else:
            logger.error(f"Connection failed: {rc}")
            self.connected = False
    
    def _on_message(self, client, userdata, msg):
        """Route MQTT messages to Qt signals"""
        try:
            payload = json.loads(msg.payload.decode())
            topic = msg.topic
            
            if topic == "factory/workflow/status":
                self.signals.workflow_status.emit(payload)
            elif topic == "factory/workflow/ack":
                self.signals.task_ack.emit(payload)
            elif topic == "factory/workflow/job_complete":
                self.signals.job_complete.emit(payload)
            elif topic == "factory/workflow/error":
                self.signals.error.emit(payload)
        
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def connect(self):
        """Connect to MQTT broker"""
        try:
            self.client.connect(self.broker, self.port, 60)
            self.client.loop_start()
            
            # Wait for connection
            timeout = 5
            start = time.time()
            while not self.connected and (time.time() - start) < timeout:
                time.sleep(0.1)
            
            return self.connected
        except Exception as e:
            logger.error(f"MQTT connection error: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from MQTT broker"""
        self.client.loop_stop()
        self.client.disconnect()
    
    def start_job(self):
        """Send command to start new job"""
        self.client.publish("factory/workflow/cmd/start_job", "{}", qos=1)
    
    def abort_job(self):
        """Send command to abort current job"""
        self.client.publish("factory/workflow/cmd/abort_job", "{}", qos=1)
    
    def get_status(self):
        """Request current status"""
        self.client.publish("factory/workflow/cmd/get_status", "{}", qos=1)


# ==========================================
# MAIN GUI APPLICATION
# ==========================================

class WorkflowGUI(QWidget):
    """Professional workflow GUI with MQTT communication"""
    
    def __init__(self):
        super().__init__()
        
        # MQTT client
        self.mqtt = WorkflowMQTTClient()
        
        # Connect signals
        self.mqtt.signals.workflow_status.connect(self.handle_workflow_status)
        self.mqtt.signals.task_ack.connect(self.handle_task_ack)
        self.mqtt.signals.job_complete.connect(self.handle_job_complete)
        self.mqtt.signals.error.connect(self.handle_error)
        
        # State
        self.current_job_id: Optional[str] = None
        self.current_state: str = "idle"
        self.job_history = []
        self.current_job_data = {}
        self.tasks_completed = set()
        
        # Task names for display
        self.task_names = {
            "job_allocation": "Job Allocation",
            "verification": "Verification",
            "job_closeout": "Job Closeout",
            "dispatch": "Dispatch"
        }
        
        # Setup UI first
        self.setup_ui()
        
        # Status update timer (start after UI is fully ready)
        self.status_timer = QTimer()
        self.status_timer.timeout.connect(self.update_connection_status)
        # Use single-shot timer to delay first check
        QTimer.singleShot(1000, lambda: self.status_timer.start(5000))
    
    def setup_ui(self):
        """Initialize UI components"""
        self.setWindowTitle("Factory Workflow System - MQTT Edition")
        self.showFullScreen()
        
        # Main layout
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(20, 20, 20, 20)
        main_layout.setSpacing(15)
        
        # ==========================================
        # TOP: Status Bar
        # ==========================================
        status_frame = QFrame()
        status_frame.setFrameShape(QFrame.StyledPanel)
        status_frame.setStyleSheet("background-color: #2c3e50; border-radius: 5px;")
        status_layout = QHBoxLayout(status_frame)
        
        # Connection status indicator
        self.connection_label = QLabel("● Connecting...")
        self.connection_label.setStyleSheet("color: orange; font-size: 14pt; font-weight: bold;")
        status_layout.addWidget(self.connection_label)
        
        status_layout.addStretch()
        
        # Workflow status
        self.status_label = QLabel("Status: Idle | Job: None | State: None")
        self.status_label.setAlignment(Qt.AlignCenter)
        self.status_label.setStyleSheet("font-size: 20pt; font-weight: bold; color: white;")
        status_layout.addWidget(self.status_label, stretch=2)
        
        status_layout.addStretch()
        
        # Job counter
        self.job_counter_label = QLabel("Jobs: 0")
        self.job_counter_label.setStyleSheet("color: white; font-size: 14pt; font-weight: bold;")
        status_layout.addWidget(self.job_counter_label)
        
        main_layout.addWidget(status_frame)
        
        # ==========================================
        # MIDDLE: Content Area
        # ==========================================
        content_frame = QFrame()
        content_frame.setFrameShape(QFrame.StyledPanel)
        content_layout = QHBoxLayout(content_frame)
        
        # Left panel: Instructions & Progress
        left_panel = QFrame()
        left_layout = QVBoxLayout(left_panel)
        
        # Instructions
        inst_label = QLabel("Instructions")
        inst_label.setStyleSheet("font-size: 16pt; font-weight: bold; color: #2c3e50;")
        left_layout.addWidget(inst_label)
        
        self.instructions_text = QTextEdit()
        self.instructions_text.setReadOnly(True)
        self.instructions_text.setStyleSheet("""
            font-size: 16pt; 
            padding: 10px;
            background-color: #ecf0f1;
            border: 2px solid #bdc3c7;
            border-radius: 5px;
        """)
        self.instructions_text.setHtml("<i>Waiting to start job...</i>")
        left_layout.addWidget(self.instructions_text, stretch=2)
        
        # Progress bar
        progress_label = QLabel("Workflow Progress")
        progress_label.setStyleSheet("font-size: 14pt; font-weight: bold; color: #2c3e50; margin-top: 10px;")
        left_layout.addWidget(progress_label)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                border: 2px solid #bdc3c7;
                border-radius: 5px;
                text-align: center;
                font-size: 14pt;
                font-weight: bold;
                height: 40px;
            }
            QProgressBar::chunk {
                background-color: #27ae60;
            }
        """)
        self.progress_bar.setRange(0, 4)
        self.progress_bar.setValue(0)
        left_layout.addWidget(self.progress_bar)
        
        # Result indicator
        self.result_indicator = QLabel("Result: Pending")
        self.result_indicator.setAlignment(Qt.AlignCenter)
        self.result_indicator.setStyleSheet("""
            font-size: 24pt; 
            font-weight: bold; 
            background-color: #95a5a6; 
            color: white;
            padding: 20px; 
            border-radius: 8px;
            margin-top: 10px;
        """)
        left_layout.addWidget(self.result_indicator)
        
        content_layout.addWidget(left_panel, stretch=3)
        
        # Right panel: Job Details & Tasks
        right_panel = QFrame()
        right_layout = QVBoxLayout(right_panel)
        
        # Job details
        details_label = QLabel("Job Details")
        details_label.setStyleSheet("font-size: 16pt; font-weight: bold; color: #2c3e50;")
        right_layout.addWidget(details_label)
        
        self.job_details = QTextEdit()
        self.job_details.setReadOnly(True)
        self.job_details.setStyleSheet("""
            font-size: 14pt;
            padding: 10px;
            background-color: #ecf0f1;
            border: 2px solid #bdc3c7;
            border-radius: 5px;
        """)
        self.job_details.setPlaceholderText("Job details will appear here...")
        right_layout.addWidget(self.job_details, stretch=2)
        
        # Task checklist
        checklist_label = QLabel("Task Checklist")
        checklist_label.setStyleSheet("font-size: 16pt; font-weight: bold; color: #2c3e50; margin-top: 10px;")
        right_layout.addWidget(checklist_label)
        
        checklist_frame = QFrame()
        checklist_frame.setFrameShape(QFrame.StyledPanel)
        checklist_frame.setStyleSheet("""
            background-color: #ecf0f1;
            border: 2px solid #bdc3c7;
            border-radius: 5px;
            padding: 10px;
        """)
        checklist_layout = QVBoxLayout(checklist_frame)
        
        self.task_labels = {}
        for task_key, task_name in self.task_names.items():
            label = QLabel(f"○ {task_name}")
            label.setStyleSheet("font-size: 16pt; color: #7f8c8d; padding: 5px;")
            self.task_labels[task_key] = label
            checklist_layout.addWidget(label)
        
        checklist_layout.addStretch()
        right_layout.addWidget(checklist_frame, stretch=1)
        
        content_layout.addWidget(right_panel, stretch=2)
        
        main_layout.addWidget(content_frame, stretch=3)
        
        # ==========================================
        # BOTTOM: Control Buttons
        # ==========================================
        button_frame = QFrame()
        button_frame.setFrameShape(QFrame.StyledPanel)
        button_layout = QHBoxLayout(button_frame)
        
        # Start Job button
        self.start_button = QPushButton("START JOB")
        self.start_button.setStyleSheet("""
            QPushButton {
                font-size: 18pt; 
                background-color: #27ae60; 
                color: white; 
                padding: 15px;
                border-radius: 5px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #229954;
            }
            QPushButton:pressed {
                background-color: #1e8449;
            }
            QPushButton:disabled {
                background-color: #95a5a6;
            }
        """)
        self.start_button.clicked.connect(self.start_job)
        button_layout.addWidget(self.start_button)
        
        # Abort Job button
        self.abort_button = QPushButton("ABORT JOB")
        self.abort_button.setStyleSheet("""
            QPushButton {
                font-size: 18pt; 
                background-color: #e67e22; 
                color: white; 
                padding: 15px;
                border-radius: 5px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #d68910;
            }
            QPushButton:pressed {
                background-color: #ca6f1e;
            }
            QPushButton:disabled {
                background-color: #95a5a6;
            }
        """)
        self.abort_button.clicked.connect(self.abort_job)
        self.abort_button.setEnabled(False)
        button_layout.addWidget(self.abort_button)
        
        # Generate Report button
        self.report_button = QPushButton("GENERATE REPORT")
        self.report_button.setStyleSheet("""
            QPushButton {
                font-size: 18pt; 
                background-color: #3498db; 
                color: white; 
                padding: 15px;
                border-radius: 5px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #2e86c1;
            }
            QPushButton:pressed {
                background-color: #2874a6;
            }
        """)
        self.report_button.clicked.connect(self.generate_report)
        button_layout.addWidget(self.report_button)
        
        # Quit button
        self.quit_button = QPushButton("QUIT")
        self.quit_button.setStyleSheet("""
            QPushButton {
                font-size: 18pt; 
                background-color: #7f8c8d; 
                color: white; 
                padding: 15px;
                border-radius: 5px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #707b7c;
            }
            QPushButton:pressed {
                background-color: #616a6b;
            }
        """)
        self.quit_button.clicked.connect(self.close)
        button_layout.addWidget(self.quit_button)
        
        main_layout.addWidget(button_frame)
    
    # ==========================================
    # MQTT MESSAGE HANDLERS
    # ==========================================
    
    def handle_workflow_status(self, payload: Dict):
        """Handle workflow status updates"""
        state = payload.get("state", "unknown")
        job_id = payload.get("job_id")
        
        self.current_state = state
        if job_id:
            self.current_job_id = job_id
        
        # Update status label
        status_text = f"Status: {state.replace('_', ' ').title()}"
        if job_id:
            status_text += f" | Job: {job_id}"
        
        self.status_label.setText(status_text)
        
        # Update instructions based on state
        state_instructions = {
            "idle": "<i>System idle. Click START JOB to begin.</i>",
            "job_allocation": "<b>Scan the Job QR code</b><br>Place the QR code under the scanner to allocate the job.",
            "waiting_rfid": "<b>Place bin on station</b><br>Position the RFID-tagged bin for identification.",
            "waiting_weight": "<b>Place parts in bin</b><br>Loading bin with parts. Wait for weight to stabilize...",
            "verification": "<b>Verifying count...</b><br>System is verifying part count against target.",
            "job_closeout": "<b>Closing job...</b><br>Finalizing job data and preparing for dispatch.",
            "dispatch": "<b>Dispatching...</b><br>Printing release document and completing job.",
            "error": "<b style='color: red;'>ERROR</b><br>An error occurred. Job has been aborted."
        }
        
        instruction = state_instructions.get(state, f"<i>State: {state}</i>")
        self.instructions_text.setHtml(instruction)
        
        # Update button states
        if state in ["idle", "error"]:
            self.start_button.setEnabled(True)
            self.abort_button.setEnabled(False)
        else:
            self.start_button.setEnabled(False)
            self.abort_button.setEnabled(True)
    
    def handle_task_ack(self, payload: Dict):
        """Handle task acknowledgment"""
        task = payload.get("task")
        success = payload.get("success", False)
        data = payload.get("data", {})
        
        if not success:
            logger.error(f"Task {task} failed: {data}")
            return
        
        # Mark task as completed
        self.tasks_completed.add(task)
        
        # Update task checklist
        if task in self.task_labels:
            label = self.task_labels[task]
            label.setText(f"✓ {self.task_names[task]}")
            label.setStyleSheet("font-size: 16pt; color: #27ae60; padding: 5px; font-weight: bold;")
        
        # Update progress bar
        self.progress_bar.setValue(len(self.tasks_completed))
        
        # Store task data
        self.current_job_data[task] = data
        
        # Handle specific tasks
        if task == "job_allocation":
            self.display_job_details(data)
        
        elif task == "verification":
            count_ok = data.get("count_ok", False)
            target = data.get("target_count", 0)
            actual = data.get("actual_count", 0)
            
            if count_ok:
                self.result_indicator.setText(f"PASS ✓ ({actual}/{target})")
                self.result_indicator.setStyleSheet("""
                    font-size: 24pt; 
                    font-weight: bold; 
                    background-color: #27ae60; 
                    color: white;
                    padding: 20px; 
                    border-radius: 8px;
                """)
            else:
                self.result_indicator.setText(f"FAIL ✗ ({actual}/{target})")
                self.result_indicator.setStyleSheet("""
                    font-size: 24pt; 
                    font-weight: bold; 
                    background-color: #e74c3c; 
                    color: white;
                    padding: 20px; 
                    border-radius: 8px;
                """)
    
    def handle_job_complete(self, payload: Dict):
        """Handle job completion"""
        logger.info(f"Job completed: {payload}")
        
        # Add to history
        self.job_history.append(copy.deepcopy(self.current_job_data))
        
        # Update job counter
        self.job_counter_label.setText(f"Jobs: {len(self.job_history)}")
        
        # Show completion message briefly
        self.instructions_text.setHtml(
            "<b style='color: green;'>Job Completed Successfully!</b><br>"
            "Starting next job in 3 seconds..."
        )
        
        # Reset for next job after delay
        QTimer.singleShot(3000, self.reset_for_next_job)
    
    def handle_error(self, payload: Dict):
        """Handle error messages"""
        error_msg = payload.get("error_msg", "Unknown error")
        logger.error(f"Workflow error: {error_msg}")
        
        self.instructions_text.setHtml(
            f"<b style='color: red;'>ERROR</b><br>{error_msg}"
        )
        
        self.result_indicator.setText("ERROR")
        self.result_indicator.setStyleSheet("""
            font-size: 24pt; 
            font-weight: bold; 
            background-color: #e74c3c; 
            color: white;
            padding: 20px; 
            border-radius: 8px;
        """)
    
    # ==========================================
    # UI METHODS
    # ==========================================
    
    def display_job_details(self, data: Dict):
        """Display job/part details"""
        details_html = "<b>Part Details:</b><br><br>"
        
        field_map = {
            "SL NO": "Serial Number",
            "PART NAME": "Part Name",
            "PART NUMBER": "Part Number",
            "MODEL": "Model",
            "PART WEIGHT": "Part Weight (kg)",
            "BIN & COVER WEIGHT": "Bin & Cover Weight (kg)",
            "COVER QTY": "Cover Quantity",
            "BIN QTY": "Target Bin Quantity",
            "BIN WEIGHT": "Bin Weight (kg)",
            "COVR QTY VARIATION": "Tolerance"
        }
        
        for key, label in field_map.items():
            value = data.get(key, "N/A")
            details_html += f"<b>{label}:</b> {value}<br>"
        
        self.job_details.setHtml(details_html)
    
    def reset_for_next_job(self):
        """Reset UI for next job"""
        self.current_job_id = None
        self.current_job_data = {}
        self.tasks_completed.clear()
        
        # Reset task checklist
        for task_key, task_name in self.task_names.items():
            label = self.task_labels[task_key]
            label.setText(f"○ {task_name}")
            label.setStyleSheet("font-size: 16pt; color: #7f8c8d; padding: 5px;")
        
        # Reset progress
        self.progress_bar.setValue(0)
        
        # Reset result indicator
        self.result_indicator.setText("Result: Pending")
        self.result_indicator.setStyleSheet("""
            font-size: 24pt; 
            font-weight: bold; 
            background-color: #95a5a6; 
            color: white;
            padding: 20px; 
            border-radius: 8px;
        """)
        
        # Clear job details
        self.job_details.clear()
    
    def update_connection_status(self):
        """Update connection status indicator"""
        # Guard against early calls before UI is ready
        if not hasattr(self, 'connection_label') or self.connection_label is None:
            return
        
        if self.mqtt.connected:
            self.connection_label.setText("● Connected")
            self.connection_label.setStyleSheet("color: #27ae60; font-size: 14pt; font-weight: bold;")
        else:
            self.connection_label.setText("● Disconnected")
            self.connection_label.setStyleSheet("color: #e74c3c; font-size: 14pt; font-weight: bold;")
    
    # ==========================================
    # BUTTON HANDLERS
    # ==========================================
    
    def start_job(self):
        """Start new job via MQTT"""
        if not self.mqtt.connected:
            self.instructions_text.setHtml("<b style='color: red;'>ERROR: Not connected to MQTT broker</b>")
            return
        
        self.reset_for_next_job()
        self.mqtt.start_job()
        logger.info("Start job command sent")
    
    def abort_job(self):
        """Abort current job via MQTT"""
        self.mqtt.abort_job()
        logger.info("Abort job command sent")
    
    def generate_report(self):
        """Generate PDF report of job history"""
        if not self.job_history:
            self.instructions_text.setHtml("<b>No jobs to report</b>")
            return
        
        filename = f"workflow_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        
        try:
            c = canvas.Canvas(filename, pagesize=letter)
            width, height = letter
            
            # Title
            c.setFont("Helvetica-Bold", 20)
            c.drawString(50, height - 50, "Factory Workflow Report")
            
            c.setFont("Helvetica", 12)
            c.drawString(50, height - 75, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            c.drawString(50, height - 95, f"Total Jobs: {len(self.job_history)}")
            
            y = height - 130
            
            for idx, job in enumerate(self.job_history, 1):
                if y < 100:
                    c.showPage()
                    y = height - 50
                
                # Job header
                c.setFont("Helvetica-Bold", 14)
                c.drawString(50, y, f"Job {idx}")
                y -= 25
                
                # Job details table
                table_data = [["Task", "Detail", "Value"]]
                
                for task, data in job.items():
                    if isinstance(data, dict):
                        for key, value in data.items():
                            table_data.append([
                                task.replace("_", " ").title(),
                                str(key),
                                str(value)
                            ])
                
                table = Table(table_data, colWidths=[120, 200, 150])
                table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 10),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.black)
                ]))
                
                table_height = len(table_data) * 20
                table.wrapOn(c, width, height)
                table.drawOn(c, 50, y - table_height)
                
                y -= table_height + 40
            
            c.save()
            
            self.instructions_text.setHtml(
                f"<b style='color: green;'>Report Generated</b><br>{filename}"
            )
            logger.info(f"Report generated: {filename}")
        
        except Exception as e:
            logger.error(f"Report generation error: {e}")
            self.instructions_text.setHtml(
                f"<b style='color: red;'>Report Error</b><br>{str(e)}"
            )
    
    # ==========================================
    # APPLICATION LIFECYCLE
    # ==========================================
    
    def showEvent(self, event):
        """Connect to MQTT when window is shown"""
        super().showEvent(event)
        
        if not self.mqtt.connected:
            if self.mqtt.connect():
                logger.info("GUI connected to MQTT")
                self.update_connection_status()
            else:
                self.instructions_text.setHtml(
                    "<b style='color: red;'>Failed to connect to MQTT broker</b><br>"
                    "Check if broker is running on localhost:1883"
                )
    
    def closeEvent(self, event):
        """Clean disconnect on close"""
        self.mqtt.disconnect()
        super().closeEvent(event)


# ==========================================
# MAIN
# ==========================================

def main():
    app = QApplication(sys.argv)
    
    # Set application style
    app.setStyle("Fusion")
    
    # Create and show GUI
    window = WorkflowGUI()
    window.show()
    
    sys.exit(app.exec())


if __name__ == "__main__":
    main()


# ==========================================
# USAGE
# ==========================================
"""
1. Start MQTT broker:
   docker-compose up -d mosquitto

2. Start workflow orchestrator:
   python workflow_orchestrator.py

3. Start device services:
   sudo systemctl start qr-scanner scale
   python refactored_rfid_service.py

4. Start GUI:
   python refactored_workflow_gui.py

The GUI will:
- Connect to MQTT broker automatically
- Show real-time workflow status
- Display task progress
- Show job details
- Generate PDF reports
"""