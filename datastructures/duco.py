"""
Comprehensive dataclass definitions for home automation modules
Provides type safety, validation, and structured data publishing
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum, IntEnum
from typing import Optional, Literal, Annotated, Dict, Any


# ============================================================================
# Base Classes and Common Types
# ============================================================================

class ConnectionStatus(Enum):
    """Connection status for devices"""
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    ERROR = "error"
    UNKNOWN = "unknown"


@dataclass
class BaseDevice:
    """Base class for all device types"""
    device_id: str
    name: Optional[str] = None
    timestamp: Optional[str] = None
    connection_status: ConnectionStatus = ConnectionStatus.UNKNOWN

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, handling enums"""
        data = asdict(self)
        # Convert enums to their values
        for key, value in data.items():
            if isinstance(value, Enum):
                data[key] = value.value
        return data

    def add_timestamp(self):
        """Add current timestamp if not set"""
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()


# ============================================================================
# DUCO Ventilation System (already in duco.py - enhanced version)
# ============================================================================

class NodeType(IntEnum):
    """Node/component types in the DUCO system"""
    UNKNOWN = 0
    DUCOTRONIC_GRILLE = 7
    CONTROL_SWITCH_RF_BAT = 8
    CONTROL_SWITCH_RF_WIRED = 9
    HUMIDITY_ROOM_SENSOR = 10
    CO2_ROOM_SENSOR = 12
    SENSORLESS_VALVE = 13
    HUMIDITY_VALVE = 14
    CO2_VALVE = 16
    DUCOBOX = 17
    SWITCH_CONTACT = 18
    IAV_VALVE = 22
    IAV_HUMIDITY = 23
    IAV_CO2 = 25
    CONTROL_UNIT = 27
    CO2_RH_VALVE = 28
    SUN_CONTROL_SWITCH = 29
    VENTILATIVE_COOLING_SWITCH = 30
    EXTERNAL_MULTIZONE_VALVE = 31
    HUMIDITY_BOX_SENSOR = 35
    CO2_BOX_SENSOR = 37
    MOTOR_RELAY = 38
    WEATHER_STATION = 39
    MODBUS_MOTOR = 40
    DIGITAL_INPUT = 41
    DIGITAL_OUTPUT = 42
    MODBUS_RELAY = 44
    PERILEX = 45
    RELAY_OUTPUT = 46


class VentilationMode(IntEnum):
    """Ventilation mode settings"""
    AUTO = 0
    MANUAL_1 = 4
    MANUAL_2 = 5
    MANUAL_3 = 6
    NOT_HOME = 7
    PERMANENT_1 = 8
    PERMANENT_2 = 9
    PERMANENT_3 = 10
    MANUAL_1_X2 = 11
    MANUAL_2_X2 = 12
    MANUAL_3_X2 = 13
    MANUAL_1_X3 = 14
    MANUAL_2_X3 = 15
    MANUAL_3_X3 = 16


@dataclass
class BaseComponent:
    """Base class for all DUCO components"""
    device_id: str
    timestamp: Optional[str] = None

    # Read parameters
    remaining_time_current_mode: Optional[int] = None
    flow_rate: Optional[Annotated[int, "0-100"]] = None
    air_quality_rh: Optional[Annotated[int, "0-100"]] = None
    air_quality_co2: Optional[Annotated[int, "0-100"]] = None
    humidity_level: Optional[Annotated[int, "0-100"]] = None
    co2_level: Optional[int] = None

    # Hold parameters
    ventilation_mode: Optional[VentilationMode] = None
    identification: Optional[Literal[0, 1]] = None

    # System field
    node_type: Optional[NodeType] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with enum handling"""
        data = asdict(self)
        if self.node_type:
            data['node_type'] = self.node_type.value
            data['node_type_name'] = self.node_type.name
        if self.ventilation_mode:
            data['ventilation_mode'] = self.ventilation_mode.value
            data['ventilation_mode_name'] = self.ventilation_mode.name
        return data


@dataclass
class DucoBoxSystem(BaseComponent):
    """DucoBox system-level component"""
    status: Optional[Literal[0, 1, 2]] = None  # OK=0, ERROR=1, INACTIVE=2
    remaining_filter_time: Optional[int] = None
    filter_status: Optional[Literal[0, 1, 2]] = None  # OK=0, DIRTY=1, INACTIVE=2
    remaining_write_actions: Optional[int] = None
    api_version: Optional[str] = None

    # Temperature zones (DucoBox Energy)
    supply_temp_zone1: Optional[float] = None
    supply_temp_zone2: Optional[float] = None
    supply_temp_zone3: Optional[float] = None
    supply_temp_zone4: Optional[float] = None

    # System temperatures (DucoBox Energy)
    temperature_oda: Optional[float] = None  # Outdoor air
    temperature_sup: Optional[float] = None  # Supply air
    temperature_eta: Optional[float] = None  # Extract air
    temperature_eha: Optional[float] = None  # Exhaust air


@dataclass
class DucoNode(BaseComponent):
    """Individual node/valve/sensor in DUCO network"""
    node_id: int = 0
    node_type_name: Optional[str] = None


def serialize_device(device: BaseDevice) -> Dict[str, Any]:
    """
    Serialize any device dataclass to dictionary.
    Handles enums and adds timestamp if missing.
    """
    if not hasattr(device, 'timestamp') or device.timestamp is None:
        device.timestamp = datetime.now().isoformat()

    return device.to_dict()


def deserialize_device(data: Dict[str, Any], device_class: type) -> BaseDevice:
    """
    Deserialize dictionary back to device dataclass.
    Handles enum conversion.
    """
    # Convert string enums back to enum objects
    for key, value in data.items():
        if isinstance(value, str):
            # Try to find matching enum in device_class
            field_type = device_class.__annotations__.get(key)
            if field_type and hasattr(field_type, '__bases__'):
                if Enum in field_type.__bases__:
                    try:
                        data[key] = field_type(value)
                    except (ValueError, KeyError):
                        pass

    return device_class(**data)


# ============================================================================
# Usage Examples
# ============================================================================

if __name__ == "__main__":
    print("=== DUCO System ===")
    ducobox = DucoBoxSystem(
        device_id="ducobox_main",
        node_type=NodeType.DUCOBOX,
        ventilation_mode=VentilationMode.AUTO,
        humidity_level=45,
        co2_level=650,
        temperature_oda=15.5,
        api_version="2.5"
    )
    print(ducobox.to_dict())

