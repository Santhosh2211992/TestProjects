"""
Professional Workflow Orchestrator
Coordinates all devices via MQTT without direct instantiation
Implements state machine pattern with proper error handling
"""

import json
import time
import uuid
from enum import Enum
from typing import Dict, Optional, Any
from datetime import datetime
from dataclasses import dataclass, asdict
import paho.mqtt.client as mqtt
import logging

from PartDB import PartDatabase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ==========================================
# STATE MACHINE
# ==========================================

class WorkflowState(str, Enum):
    """Workflow states"""
    IDLE = "idle"
    JOB_ALLOCATION = "job_allocation"
    WAITING_RFID = "waiting_rfid"
    WAITING_WEIGHT = "waiting_weight"
    VERIFICATION = "verification"
    JOB_CLOSEOUT = "job_closeout"
    DISPATCH = "dispatch"
    ERROR = "error"


class TaskStatus(str, Enum):
    """Task execution status"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"


@dataclass
class JobContext:
    """Context data for current job"""
    job_id: str
    correlation_id: str
    part_number: Optional[str] = None
    part_details: Optional[Dict[str, Any]] = None
    rfid_epc: Optional[str] = None
    empty_bin_weight: Optional[float] = None
    gross_weight: Optional[float] = None
    net_weight: Optional[float] = None
    target_count: Optional[int] = None
    actual_count: Optional[int] = None
    count_ok: Optional[bool] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    errors: list = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []


# ==========================================
# WORKFLOW ORCHESTRATOR
# ==========================================

class WorkflowOrchestrator:
    """
    Orchestrates workflow by coordinating devices via MQTT.
    No direct hardware instantiation - pure message-driven.
    """
    
    def __init__(
        self,
        broker: str = "localhost",
        port: int = 1883,
        db_config: Dict[str, Any] = None
    ):
        self.broker = broker
        self.port = port
        
        # Database connections
        self.part_db = PartDatabase(**db_config["part_db"]) if db_config else None
        self.bin_db = PartDatabase(**db_config["bin_db"]) if db_config else None
        
        # State management
        self.state = WorkflowState.IDLE
        self.current_job: Optional[JobContext] = None
        self.job_history: list = []
        
        # Device tracking
        self.devices = {
            "qr_scanner": None,
            "rfid_reader": None,
            "scale": None,
            "printer": None
        }
        
        # Timeout management
        self.timeouts = {
            "qr_scan": 30.0,
            "rfid_read": 10.0,
            "weight_stable": 15.0
        }
        self.timeout_timer: Optional[float] = None
        
        # MQTT
        self.client = mqtt.Client(client_id="workflow_orchestrator")
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.connected = False
        
    def _on_connect(self, client, userdata, flags, rc):
        """MQTT connection callback"""
        if rc == 0:
            logger.info("Workflow Orchestrator connected to MQTT")
            self.connected = True
            
            # Subscribe to all device data topics
            self.client.subscribe("factory/+/+/data")
            self.client.subscribe("factory/+/+/status")
            self.client.subscribe("factory/+/+/error")
            self.client.subscribe("factory/workflow/cmd/#")
            
            self._publish_status(WorkflowState.IDLE)
        else:
            logger.error(f"MQTT connection failed: {rc}")
            
    def _on_message(self, client, userdata, msg):
        """Route incoming messages to appropriate handlers"""
        try:
            topic_parts = msg.topic.split("/")
            payload = json.loads(msg.payload.decode())
            
            if len(topic_parts) < 3:
                return
            
            # Parse topic: factory/{device_type}/{device_id}/{msg_type}
            device_type = topic_parts[1]
            device_id = topic_parts[2]
            msg_type = topic_parts[3] if len(topic_parts) > 3 else None
            
            # Handle workflow commands
            if device_type == "workflow" and device_id == "cmd":
                self._handle_workflow_command(msg_type, payload)
                return
            
            # Handle device messages
            if msg_type == "data":
                self._handle_device_data(device_type, device_id, payload)
            elif msg_type == "status":
                self._handle_device_status(device_type, device_id, payload)
            elif msg_type == "error":
                self._handle_device_error(device_type, device_id, payload)
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            self._publish_error(f"Message handler error: {e}")
    
    def _handle_workflow_command(self, command: str, payload: Dict):
        """Handle workflow control commands"""
        logger.info(f"Workflow command: {command}")
        
        if command == "start_job":
            self.start_new_job()
        elif command == "abort_job":
            self.abort_job()
        elif command == "get_status":
            self._publish_status(self.state)
        elif command == "set_devices":
            # Allow dynamic device ID configuration
            self.devices.update(payload.get("devices", {}))
            logger.info(f"Updated devices: {self.devices}")
    
    def _handle_device_data(self, device_type: str, device_id: str, payload: Dict):
        """Handle data from devices based on current state"""
        
        # Only process if we're expecting this data
        if self.state == WorkflowState.IDLE:
            return
        
        correlation_id = payload.get("correlation_id")
        if self.current_job and correlation_id != self.current_job.correlation_id:
            logger.warning(f"Ignoring message with wrong correlation_id: {correlation_id}")
            return
        
        # Route to state-specific handlers
        if device_type == "qr" and self.state == WorkflowState.JOB_ALLOCATION:
            self._handle_qr_scan(payload)
        elif device_type == "rfid" and self.state == WorkflowState.WAITING_RFID:
            self._handle_rfid_read(payload)
        elif device_type == "scale" and self.state == WorkflowState.WAITING_WEIGHT:
            self._handle_weight_reading(payload)
    
    def _handle_device_status(self, device_type: str, device_id: str, payload: Dict):
        """Handle device status updates"""
        logger.info(f"{device_type}/{device_id} status: {payload.get('status')}")
    
    def _handle_device_error(self, device_type: str, device_id: str, payload: Dict):
        """Handle device errors"""
        error_msg = f"{device_type}/{device_id}: {payload.get('error_msg')}"
        logger.error(error_msg)
        
        if self.current_job:
            self.current_job.errors.append(error_msg)
            self._transition_to_error(error_msg)
    
    # ==========================================
    # WORKFLOW STATE MACHINE
    # ==========================================
    
    def start_new_job(self):
        """Start a new job workflow"""
        if self.state != WorkflowState.IDLE:
            logger.warning("Cannot start job - workflow already running")
            return
        
        # Create new job context
        job_id = f"JOB_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        correlation_id = str(uuid.uuid4())
        
        self.current_job = JobContext(
            job_id=job_id,
            correlation_id=correlation_id,
            started_at=datetime.now()
        )
        
        logger.info(f"Starting new job: {job_id}")
        
        # Transition to job allocation
        self._transition_to(WorkflowState.JOB_ALLOCATION)
    
    def _transition_to(self, new_state: WorkflowState):
        """Transition to new state and trigger actions"""
        old_state = self.state
        self.state = new_state
        logger.info(f"State transition: {old_state} -> {new_state}")
        
        # Start timeout timer
        self._start_timeout()
        
        # Publish state change
        self._publish_status(new_state)
        
        # Trigger state entry actions
        if new_state == WorkflowState.JOB_ALLOCATION:
            self._enter_job_allocation()
        elif new_state == WorkflowState.WAITING_RFID:
            self._enter_waiting_rfid()
        elif new_state == WorkflowState.WAITING_WEIGHT:
            self._enter_waiting_weight()
        elif new_state == WorkflowState.VERIFICATION:
            self._enter_verification()
        elif new_state == WorkflowState.JOB_CLOSEOUT:
            self._enter_job_closeout()
        elif new_state == WorkflowState.DISPATCH:
            self._enter_dispatch()
        elif new_state == WorkflowState.IDLE:
            self._enter_idle()
    
    def _enter_job_allocation(self):
        """Start QR code scanning for job allocation"""
        qr_device = self.devices.get("qr_scanner", "qr_scanner_01")
        
        # Command QR scanner to start
        self._send_device_command("qr", qr_device, "start_scan")
        
        logger.info("Waiting for QR code scan...")
    
    def _handle_qr_scan(self, payload: Dict):
        """Process QR code scan result"""
        qr_code = payload.get("qr_code")
        logger.info(f"QR code scanned: {qr_code}")
        
        # Stop QR scanner
        qr_device = self.devices.get("qr_scanner", "qr_scanner_01")
        self._send_device_command("qr", qr_device, "stop_scan")
        
        # Lookup part details in database
        if self.part_db:
            part_details = self.part_db.get_part_details(qr_code)
            if part_details:
                self.current_job.part_number = qr_code
                self.current_job.part_details = part_details
                
                # Calculate target count if we have part weight
                part_weight = part_details.get("PART WEIGHT")
                if part_weight:
                    # Store for later verification
                    self.current_job.target_count = part_details.get("BIN QTY")
                
                logger.info(f"Part details loaded: {part_details.get('PART NAME')}")
                
                # Publish job allocation success
                self._publish_task_ack("job_allocation", True, part_details)
                
                # Move to next state
                self._transition_to(WorkflowState.WAITING_RFID)
            else:
                error = f"Part number not found in database: {qr_code}"
                logger.error(error)
                self._publish_task_ack("job_allocation", False, {"error": error})
                self._transition_to_error(error)
        else:
            logger.warning("Part database not configured")
            self._transition_to(WorkflowState.WAITING_RFID)
    
    def _enter_waiting_rfid(self):
        """Start RFID polling for bin identification"""
        rfid_device = self.devices.get("rfid_reader", "192.168.1.102")
        
        # Command RFID reader to start polling
        self._send_device_command("rfid", rfid_device, "start_polling")
        
        logger.info("Waiting for RFID tag...")
    
    def _handle_rfid_read(self, payload: Dict):
        """Process RFID tag read"""
        epc = payload.get("epc")
        logger.info(f"RFID tag detected: {epc}")
        
        # Only process first stable read
        if self.current_job.rfid_epc:
            return
        
        self.current_job.rfid_epc = epc
        
        # Lookup empty bin weight
        if self.bin_db:
            bin_data = self.bin_db.get_row(key_value=epc, key_column="epc")
            if bin_data:
                self.current_job.empty_bin_weight = bin_data.get("empty_bin_weight", 0)
                logger.info(f"Empty bin weight: {self.current_job.empty_bin_weight:.3f} kg")
        
        # Stop RFID polling
        rfid_device = self.devices.get("rfid_reader", "192.168.1.102")
        self._send_device_command("rfid", rfid_device, "stop_polling")
        
        # Move to weight measurement
        self._transition_to(WorkflowState.WAITING_WEIGHT)
    
    def _enter_waiting_weight(self):
        """Start scale monitoring for stable weight"""
        scale_device = self.devices.get("scale", "scale_01")
        
        # Set tare if we have empty bin weight
        if self.current_job.empty_bin_weight:
            # Note: This assumes scale can accept tare value via MQTT
            # Otherwise, physical tare must be done manually
            pass
        
        # Command scale to start monitoring
        self._send_device_command("scale", scale_device, "start_monitoring")
        
        logger.info("Waiting for stable weight...")
    
    def _handle_weight_reading(self, payload: Dict):
        """Process weight reading from scale"""
        stable = payload.get("stable", False)
        
        if not stable:
            return  # Wait for stable weight
        
        weight = payload.get("weight")
        net_weight = payload.get("net_weight")
        
        logger.info(f"Stable weight: {weight:.3f} kg (net: {net_weight:.3f} kg)")
        
        self.current_job.gross_weight = weight
        self.current_job.net_weight = net_weight
        
        # Stop scale monitoring
        scale_device = self.devices.get("scale", "scale_01")
        self._send_device_command("scale", scale_device, "stop_monitoring")
        
        # Move to verification
        self._transition_to(WorkflowState.VERIFICATION)
    
    def _enter_verification(self):
        """Verify part count against target"""
        if not self.current_job.part_details:
            logger.warning("No part details for verification")
            self._transition_to(WorkflowState.JOB_CLOSEOUT)
            return
        
        part_weight = self.current_job.part_details.get("PART WEIGHT")
        target_count = self.current_job.target_count
        net_weight = self.current_job.net_weight
        
        if part_weight and net_weight:
            # Calculate actual count
            actual_count = int(net_weight / part_weight)
            self.current_job.actual_count = actual_count
            
            # Verify against target
            tolerance = self.current_job.part_details.get("COVR QTY VARIATION", 0)
            count_ok = abs(actual_count - target_count) <= tolerance
            self.current_job.count_ok = count_ok
            
            logger.info(f"Count verification: {actual_count}/{target_count} - {'PASS' if count_ok else 'FAIL'}")
            
            verification_data = {
                "target_count": target_count,
                "actual_count": actual_count,
                "count_ok": count_ok,
                "gross_weight": self.current_job.gross_weight,
                "net_weight": net_weight,
                "rfid_epc": self.current_job.rfid_epc
            }
            
            self._publish_task_ack("verification", True, verification_data)
        
        self._transition_to(WorkflowState.JOB_CLOSEOUT)
    
    def _enter_job_closeout(self):
        """Close out the job"""
        closeout_data = {
            "job_id": self.current_job.job_id,
            "part_number": self.current_job.part_number,
            "count_ok": self.current_job.count_ok,
            "actual_count": self.current_job.actual_count,
            "target_count": self.current_job.target_count
        }
        
        self._publish_task_ack("job_closeout", True, closeout_data)
        
        # Move to dispatch
        self._transition_to(WorkflowState.DISPATCH)
    
    def _enter_dispatch(self):
        """Dispatch bin and print label"""
        # Command printer (if available)
        printer_device = self.devices.get("printer")
        if printer_device:
            print_data = {
                "job_id": self.current_job.job_id,
                "part_number": self.current_job.part_number,
                "part_name": self.current_job.part_details.get("PART NAME") if self.current_job.part_details else "",
                "count": self.current_job.actual_count,
                "timestamp": datetime.now().isoformat()
            }
            self._send_device_command("printer", printer_device, "print_label", print_data)
        
        dispatch_data = {
            "job_id": self.current_job.job_id,
            "status": "dispatched"
        }
        
        self._publish_task_ack("dispatch", True, dispatch_data)
        
        # Complete job
        self._complete_job()
    
    def _enter_idle(self):
        """Return to idle state"""
        logger.info("Workflow idle")
        self._cancel_timeout()
    
    def _complete_job(self):
        """Complete current job and return to idle"""
        if self.current_job:
            self.current_job.completed_at = datetime.now()
            self.job_history.append(self.current_job)
            logger.info(f"Job completed: {self.current_job.job_id}")
            
            # Publish job summary
            self._publish_job_summary(self.current_job)
        
        self.current_job = None
        self._transition_to(WorkflowState.IDLE)
    
    def _transition_to_error(self, error_msg: str):
        """Transition to error state"""
        self.state = WorkflowState.ERROR
        logger.error(f"Workflow error: {error_msg}")
        
        if self.current_job:
            self.current_job.errors.append(error_msg)
        
        self._publish_error(error_msg)
        
        # Stop all devices
        self._stop_all_devices()
        
        # Return to idle after logging
        self.current_job = None
        self._transition_to(WorkflowState.IDLE)
    
    def abort_job(self):
        """Abort current job"""
        if self.state == WorkflowState.IDLE:
            return
        
        logger.warning("Job aborted by user")
        self._stop_all_devices()
        self.current_job = None
        self._transition_to(WorkflowState.IDLE)
    
    # ==========================================
    # TIMEOUT MANAGEMENT
    # ==========================================
    
    def _start_timeout(self):
        """Start timeout timer for current state"""
        timeout_map = {
            WorkflowState.JOB_ALLOCATION: self.timeouts["qr_scan"],
            WorkflowState.WAITING_RFID: self.timeouts["rfid_read"],
            WorkflowState.WAITING_WEIGHT: self.timeouts["weight_stable"]
        }
        
        timeout = timeout_map.get(self.state)
        if timeout:
            self.timeout_timer = time.time() + timeout
    
    def _cancel_timeout(self):
        """Cancel timeout timer"""
        self.timeout_timer = None
    
    def _check_timeout(self):
        """Check if current operation has timed out"""
        if self.timeout_timer and time.time() > self.timeout_timer:
            self._transition_to_error(f"Timeout in state: {self.state}")
    
    # ==========================================
    # MQTT HELPERS
    # ==========================================
    
    def _send_device_command(self, device_type: str, device_id: str, command: str, params: Dict = None):
        """Send command to device via MQTT"""
        topic = f"factory/{device_type}/{device_id}/cmd/{command}"
        payload = params or {}
        
        # Add correlation ID
        if self.current_job:
            payload["correlation_id"] = self.current_job.correlation_id
        
        self.client.publish(topic, json.dumps(payload), qos=1)
        logger.info(f"Sent command: {topic}")
    
    def _stop_all_devices(self):
        """Stop all active devices"""
        for device_type, device_id in self.devices.items():
            if device_id:
                stop_cmd = "stop_scan" if device_type == "qr_scanner" else "stop_monitoring" if device_type == "scale" else "stop_polling"
                self._send_device_command(device_type.split("_")[0], device_id, stop_cmd)
    
    def _publish_status(self, state: WorkflowState):
        """Publish workflow status"""
        payload = {
            "msg_type": "status",
            "timestamp": datetime.now().isoformat(),
            "state": state.value,
            "job_id": self.current_job.job_id if self.current_job else None
        }
        self.client.publish("factory/workflow/status", json.dumps(payload), qos=1)
    
    def _publish_task_ack(self, task: str, success: bool, data: Dict):
        """Publish task acknowledgment"""
        payload = {
            "task": task,
            "success": success,
            "data": data,
            "timestamp": datetime.now().isoformat(),
            "job_id": self.current_job.job_id if self.current_job else None
        }
        self.client.publish("factory/workflow/ack", json.dumps(payload), qos=1)
    
    def _publish_job_summary(self, job: JobContext):
        """Publish completed job summary"""
        payload = asdict(job)
        payload["started_at"] = job.started_at.isoformat() if job.started_at else None
        payload["completed_at"] = job.completed_at.isoformat() if job.completed_at else None
        
        self.client.publish("factory/workflow/job_complete", json.dumps(payload), qos=1)
    
    def _publish_error(self, error_msg: str):
        """Publish error"""
        payload = {
            "msg_type": "error",
            "timestamp": datetime.now().isoformat(),
            "error_msg": error_msg,
            "job_id": self.current_job.job_id if self.current_job else None
        }
        self.client.publish("factory/workflow/error", json.dumps(payload), qos=1)
    
    # ==========================================
    # LIFECYCLE
    # ==========================================
    
    def start(self):
        """Start orchestrator"""
        self.client.connect(self.broker, self.port, 60)
        self.client.loop_start()
        
        # Wait for connection
        timeout = 5
        start = time.time()
        while not self.connected and (time.time() - start) < timeout:
            time.sleep(0.1)
        
        if not self.connected:
            raise Exception("Failed to connect to MQTT broker")
        
        logger.info("Workflow Orchestrator started")
    
    def stop(self):
        """Stop orchestrator"""
        self.abort_job()
        self.client.loop_stop()
        self.client.disconnect()
        logger.info("Workflow Orchestrator stopped")
    
    def run(self):
        """Run orchestrator main loop"""
        try:
            while True:
                self._check_timeout()
                time.sleep(0.1)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            self.stop()


# ==========================================
# MAIN
# ==========================================

if __name__ == "__main__":
    # Configuration
    db_config = {
        "part_db": {
            "host": "172.18.0.16",
            "port": 5432,
            "dbname": "postgres",
            "user": "postgres",
            "password": "password",
            "tablename": "part_weight_db"
        },
        "bin_db": {
            "host": "172.18.0.16",
            "port": 5432,
            "dbname": "postgres",
            "user": "postgres",
            "password": "password",
            "tablename": "rfid_bin_db"
        }
    }
    
    orchestrator = WorkflowOrchestrator(
        broker="localhost",
        port=1883,
        db_config=db_config
    )
    
    # Configure device IDs
    orchestrator.devices = {
        "qr_scanner": "qr_scanner_01",
        "rfid_reader": "192.168.1.102",
        "scale": "scale_01",
        "printer": None  # Optional
    }
    
    orchestrator.start()
    orchestrator.run()


# ==========================================
# TESTING
# ==========================================
"""
# Start orchestrator:
python workflow_orchestrator.py

# Start a new job:
mosquitto_pub -t factory/workflow/cmd/start_job -m '{}'

# Abort job:
mosquitto_pub -t factory/workflow/cmd/abort_job -m '{}'

# Monitor workflow:
mosquitto_sub -t 'factory/workflow/#' -v
"""