"""
Professional MQTT Communication Schema
Defines standardized topics and message formats for all modules
"""

from enum import Enum
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field
from datetime import datetime

# ==========================================
# TOPIC STRUCTURE
# ==========================================
class TopicStructure:
    """Standardized MQTT topic hierarchy"""
    
    # Base topics
    BASE = "factory"
    
    # Device categories
    RFID = f"{BASE}/rfid"
    SCALE = f"{BASE}/scale"
    QR = f"{BASE}/qr"
    PRINTER = f"{BASE}/printer"
    WORKFLOW = f"{BASE}/workflow"
    
    # Command/Response pattern
    @staticmethod
    def cmd(device_type: str, device_id: str, command: str) -> str:
        """factory/{device_type}/{device_id}/cmd/{command}"""
        return f"{TopicStructure.BASE}/{device_type}/{device_id}/cmd/{command}"
    
    @staticmethod
    def status(device_type: str, device_id: str) -> str:
        """factory/{device_type}/{device_id}/status"""
        return f"{TopicStructure.BASE}/{device_type}/{device_id}/status"
    
    @staticmethod
    def data(device_type: str, device_id: str) -> str:
        """factory/{device_type}/{device_id}/data"""
        return f"{TopicStructure.BASE}/{device_type}/{device_id}/data"
    
    @staticmethod
    def error(device_type: str, device_id: str) -> str:
        """factory/{device_type}/{device_id}/error"""
        return f"{TopicStructure.BASE}/{device_type}/{device_id}/error"

# ==========================================
# MESSAGE SCHEMAS
# ==========================================

class MessageType(str, Enum):
    """Standard message types"""
    COMMAND = "command"
    STATUS = "status"
    DATA = "data"
    ERROR = "error"
    ACK = "ack"

class DeviceStatus(str, Enum):
    """Device operational states"""
    IDLE = "idle"
    BUSY = "busy"
    ERROR = "error"
    OFFLINE = "offline"
    READY = "ready"

class BaseMessage(BaseModel):
    """Base message structure for all MQTT messages"""
    msg_type: MessageType
    timestamp: datetime = Field(default_factory=datetime.now)
    device_id: str
    correlation_id: Optional[str] = None  # For request-response tracking
    
    class Config:
        use_enum_values = True

# --- RFID Messages ---
class RFIDDataMessage(BaseMessage):
    """RFID tag read data"""
    msg_type: MessageType = MessageType.DATA
    epc: str
    rssi: int
    antenna: int
    location: str
    count: int = 1

class RFIDCommandMessage(BaseMessage):
    """RFID reader commands"""
    msg_type: MessageType = MessageType.COMMAND
    command: str  # "start_polling", "stop_polling", "read_once"
    params: Dict[str, Any] = {}

# --- Scale Messages ---
class ScaleDataMessage(BaseMessage):
    """Weight measurement data"""
    msg_type: MessageType = MessageType.DATA
    weight: float
    unit: str = "kg"
    stable: bool
    tare_weight: Optional[float] = None
    net_weight: Optional[float] = None

class ScaleCommandMessage(BaseMessage):
    """Scale commands"""
    msg_type: MessageType = MessageType.COMMAND
    command: str  # "tare", "read", "monitor_stability"
    params: Dict[str, Any] = {}

# --- QR Scanner Messages ---
class QRDataMessage(BaseMessage):
    """QR code scan data"""
    msg_type: MessageType = MessageType.DATA
    qr_code: str
    scan_time: datetime = Field(default_factory=datetime.now)

class QRCommandMessage(BaseMessage):
    """QR scanner commands"""
    msg_type: MessageType = MessageType.COMMAND
    command: str  # "start_scan", "stop_scan"

# --- Workflow Messages ---
class WorkflowTaskMessage(BaseMessage):
    """Workflow task execution"""
    msg_type: MessageType = MessageType.COMMAND
    task: str  # "job_allocation", "verification", etc.
    job_id: Optional[str] = None
    params: Dict[str, Any] = {}

class WorkflowAckMessage(BaseMessage):
    """Workflow task acknowledgment"""
    msg_type: MessageType = MessageType.ACK
    task: str
    success: bool
    data: Dict[str, Any] = {}
    error_msg: Optional[str] = None

# --- Status Messages ---
class StatusMessage(BaseMessage):
    """Device status update"""
    msg_type: MessageType = MessageType.STATUS
    status: DeviceStatus
    details: Dict[str, Any] = {}

# --- Error Messages ---
class ErrorMessage(BaseMessage):
    """Error reporting"""
    msg_type: MessageType = MessageType.ERROR
    error_code: str
    error_msg: str
    details: Dict[str, Any] = {}


# ==========================================
# EXAMPLE USAGE
# ==========================================

if __name__ == "__main__":
    # Example: RFID publishing tag data
    rfid_data = RFIDDataMessage(
        device_id="rfid_reader_01",
        epc="E7 76 09 89 49 00 37 33 90 00 00 01",
        rssi=-45,
        antenna=1,
        location="Zone E",
        correlation_id="job_123"
    )
    
    topic = TopicStructure.data("rfid", "rfid_reader_01")
    print(f"Publish to: {topic}")
    print(f"Payload: {rfid_data.model_dump_json(indent=2)}")
    
    # Example: Scale publishing stable weight
    scale_data = ScaleDataMessage(
        device_id="scale_01",
        weight=12.456,
        stable=True,
        tare_weight=2.5,
        net_weight=9.956,
        correlation_id="job_123"
    )
    
    topic = TopicStructure.data("scale", "scale_01")
    print(f"\nPublish to: {topic}")
    print(f"Payload: {scale_data.model_dump_json(indent=2)}")