"""
Niko Home Control - Structured Dataclasses with Inheritance Hierarchy
Designed for Redis publishing and UI consumption
"""

import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, StrEnum
from typing import Optional, List, Dict, Any
from uuid import uuid4


# ============================================================================
# Core Enums
# ============================================================================

class DeviceType(StrEnum):
    """Device type enumeration from Niko API"""
    ACTION = "action"
    PANEL = "panel"
    HVAC = "hvac"
    THERMOSTAT = "thermostat"
    MULTISENSOR = "multisensor"
    CENTRALMETER = "centralmeter"
    SMART_PLUG = "smartplug"
    ENERGYHOME = "energyhome"
    VIDEODOORSTATION = "videodoorstation"
    AUDIO_CONTROL = "audiocontrol"
    CHARGING_STATION = "chargingstation"
    # Add other types from Niko documentation
    LIGHT = "light"
    SOCKET = "socket"
    FAN = "fan"
    RELAY = "relay"
    GENERIC = "generic"


class TechnologyType(StrEnum):
    """Manufacturer/technology type"""
    NIKOHOMECONTROL = "nikohomecontrol"
    ZIGBEE = "zigbee"
    TOUCHSWITCH = "touchswitch"
    SONOS = "sonos"
    BOSE = "bose"
    GENERIC = "generic"


class ConnectionStatus(Enum):
    """Connection status"""
    ONLINE = "online"
    OFFLINE = "offline"
    UNKNOWN = "unknown"


# ============================================================================
# Core Base Classes
# ============================================================================

@dataclass
class BaseEntity:
    """Base entity with common fields for all objects"""
    id: str = field(default_factory=lambda: str(uuid4()))
    name: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    source: str = "niko_home_control"

    def to_dict(self) -> Dict[str, Any]:
        """Convert dataclass to dictionary, handling enums and nested objects"""
        result = {}
        for field_name in self.__dataclass_fields__:
            value = getattr(self, field_name)

            if isinstance(value, Enum):
                result[field_name] = value.value
            elif isinstance(value, (str, int, float, bool, type(None))):
                result[field_name] = value
            elif isinstance(value, list):
                result[field_name] = [
                    item.to_dict() if hasattr(item, 'to_dict') else item
                    for item in value
                ]
            elif isinstance(value, dict):
                result[field_name] = {
                    k: v.to_dict() if hasattr(v, 'to_dict') else v
                    for k, v in value.items()
                }
            elif hasattr(value, 'to_dict'):
                result[field_name] = value.to_dict()
            else:
                result[field_name] = str(value)

        return result


@dataclass
class BaseDevice(BaseEntity):
    """Base device with shared device properties"""
    uuid: str = ""  # Niko's UUID
    device_type: str = "action"  # Using string instead of enum for flexibility
    technology: str = "nikohomecontrol"  # Using string instead of enum
    model: str = ""
    identifier: str = ""  # Niko config GUID

    # Status fields
    online: bool = False
    connection_status: str = "unknown"

    # Location
    location_id: Optional[str] = None
    location_name: Optional[str] = None
    location_icon: Optional[str] = None
    icon_code: Optional[str] = None

    # Traits
    mac_address: Optional[str] = None
    channel: Optional[int] = None

    # Runtime properties (dictionary of property_name: value)
    properties: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Post-initialization to ensure proper types"""
        # Ensure connection_status is a string
        if isinstance(self.connection_status, Enum):
            self.connection_status = self.connection_status.value

        # Ensure device_type and technology are strings
        if isinstance(self.device_type, Enum):
            self.device_type = self.device_type.value
        if isinstance(self.technology, Enum):
            self.technology = self.technology.value


# ============================================================================
# Action Device Base Classes
# ============================================================================

@dataclass
class BaseActionDevice(BaseDevice):
    """Base class for all action devices"""
    device_type: str = "action"

    # Common action properties
    basic_state: Optional[str] = None  # On, Off, Triggered, Intermediate
    all_started: Optional[bool] = None


@dataclass
class BaseMotorAction(BaseActionDevice):
    """Base class for motor actions"""
    position: Optional[int] = None  # 0-100
    aligned: Optional[bool] = None
    moving: Optional[bool] = None
    last_direction: Optional[str] = None  # Open, Close


@dataclass
class BaseThermostatDevice(BaseDevice):
    """Base class for thermostat devices"""
    device_type: str = "thermostat"

    # Common thermostat properties
    program: Optional[str] = None  # Day, Night, Eco, Off, etc.
    ambient_temperature: Optional[float] = None
    setpoint_temperature: Optional[float] = None
    overrule_active: Optional[bool] = None
    overrule_setpoint: Optional[float] = None
    overrule_time: Optional[int] = None
    eco_save: Optional[bool] = None
    demand: Optional[str] = None  # Heating, Cooling, None


@dataclass
class BaseMeteringDevice(BaseDevice):
    """Base class for metering devices"""
    device_type: str = "centralmeter"

    # Common metering properties
    electrical_power: Optional[float] = None  # W
    electrical_power1: Optional[float] = None  # Phase 1
    electrical_power2: Optional[float] = None  # Phase 2
    electrical_power3: Optional[float] = None  # Phase 3
    electrical_energy: Optional[float] = None  # Wh
    report_instant_usage: Optional[bool] = None

    # Meter specific
    meter_type: Optional[str] = None  # 1-Phase, 3-Phase
    clamp_type: Optional[str] = None  # 63A, 120A, 80A
    inverted: Optional[bool] = None
    flow: Optional[str] = None  # Producer, Consumer
    segment: Optional[str] = None  # Central, Subsegment


# ============================================================================
# Specific Action Device Classes
# ============================================================================

@dataclass
class DimmerAction(BaseActionDevice):
    """Dimmer action device"""
    model: str = "dimmer"

    # Dimmer specific
    brightness: Optional[int] = None  # 0-100
    status: Optional[str] = None  # On, Off


@dataclass
class FanAction(BaseActionDevice):
    """Fan action device"""
    model: str = "fan"

    # Fan specific
    fan_speed: Optional[str] = None  # Low, Medium, High, Boost


@dataclass
class RelayAction(BaseActionDevice):
    """Relay action device"""
    model: str = "relay"

    # Relay specific
    status: Optional[str] = None  # On, Off


@dataclass
class MotorAction(BaseMotorAction):
    """Generic motor action (shutters, blinds, gates)"""
    motor_type: str = "generic"  # rolldownshutter, sunblind, gate, venetianblind


@dataclass
class RollerShutterAction(MotorAction):
    """Roller shutter action"""
    model: str = "rolldownshutter"


@dataclass
class VenetianBlindAction(MotorAction):
    """Venetian blind action"""
    model: str = "venetianblind"


@dataclass
class GateAction(MotorAction):
    """Gate action"""
    model: str = "gate"


@dataclass
class AccessControlAction(BaseActionDevice):
    """Access control action"""
    model: str = "accesscontrol"

    # Access control specific
    doorlock: Optional[str] = None  # Open, Closed
    call_pending: Optional[bool] = None
    call_answered: Optional[bool] = None
    button_id: Optional[str] = None
    ringtone: Optional[str] = None


@dataclass
class BellButtonAction(AccessControlAction):
    """Bell button action"""
    model: str = "bellbutton"


@dataclass
class GarageDoorAction(BaseActionDevice):
    """Garage door action"""
    model: str = "garagedoor"


@dataclass
class AlarmAction(BaseActionDevice):
    """Alarm action"""
    model: str = "alarms"


@dataclass
class PanicModeAction(BaseActionDevice):
    """Panic mode action"""
    model: str = "alarms"


@dataclass
class MoodAction(BaseActionDevice):
    """Mood (scene) action"""
    model: str = "comfort"

    # Mood specific
    mood_active: Optional[bool] = None
    mood_icon: Optional[int] = None


@dataclass
class AllOffAction(MoodAction):
    """All-off action"""
    model: str = "alloff"

    all_off_active: Optional[bool] = None


@dataclass
class FreeStartStopAction(BaseActionDevice):
    """Free start/stop action"""
    model: str = "generic"

    start_active: Optional[bool] = None
    start_text: Optional[str] = None
    stop_text: Optional[str] = None


@dataclass
class HouseModeAction(FreeStartStopAction):
    """House mode action"""
    model: str = "overallcomfort"


@dataclass
class PIRAction(BaseActionDevice):
    """PIR (motion detection) action"""
    model: str = "pir"


@dataclass
class PresenceSimulationAction(PIRAction):
    """Presence simulation action"""
    model: str = "simulation"


@dataclass
class PlayerStatusAction(BaseActionDevice):
    """Player status action"""
    model: str = "playerstatus"

    feedback_message: Optional[str] = None


@dataclass
class ConditionalAction(BaseActionDevice):
    """Conditional action"""
    model: str = "condition"


@dataclass
class PeakModeAction(BaseActionDevice):
    """Peak mode action"""
    model: str = "peakmode"


@dataclass
class SolarModeAction(BaseActionDevice):
    """Solar mode action"""
    model: str = "solarmode"


@dataclass
class TimeScheduleAction(BaseActionDevice):
    """Time schedule action"""
    model: str = "timeschedule"

    active: Optional[bool] = None


# ============================================================================
# Thermostat and HVAC Classes
# ============================================================================

@dataclass
class Thermostat(BaseThermostatDevice):
    """Standard thermostat"""
    model: str = "thermostat"


@dataclass
class HVACThermostat(BaseThermostatDevice):
    """HVAC thermostat"""
    model: str = "hvacthermostat"

    # HVAC specific
    protect_mode: Optional[bool] = None
    operation_mode: Optional[str] = None  # Heating, Cooling
    hvac_on: Optional[str] = None  # On, Off
    thermostat_on: Optional[str] = None  # On, Off
    hvac_fan_speed: Optional[str] = None  # Low, Medium, High


@dataclass
class TouchSwitchThermostat(BaseThermostatDevice):
    """Touch switch thermostat (Digital Black)"""
    model: str = "touchswitch"
    technology: str = "touchswitch"


@dataclass
class VirtualThermostat(BaseThermostatDevice):
    """Virtual thermostat"""
    model: str = "virtual"


@dataclass
class ThermoSwitch(BaseDevice):
    """Thermo switch (temperature/humidity sensor)"""
    device_type: str = "multisensor"
    model: str = "thermoswitch"

    # Sensor specific
    ambient_temperature: Optional[float] = None
    humidity: Optional[float] = None
    heat_index: Optional[float] = None

    # Configuration
    temperature_calibration_offset: Optional[float] = None
    ambient_temperature_reporting: Optional[bool] = None
    humidity_reporting: Optional[bool] = None


@dataclass
class VirtualFlag(BaseActionDevice):
    """Virtual flag"""
    model: str = "flag"

    status: Optional[bool] = None  # True = On, False = Off


# ============================================================================
# Metering and Energy Classes
# ============================================================================

@dataclass
class BatteryMeteringClamp(BaseMeteringDevice):
    """Battery metering clamp"""
    model: str = "battery-clamp"

    # Battery specific
    electrical_energy_charged: Optional[float] = None  # Wh
    electrical_energy_discharged: Optional[float] = None  # Wh


@dataclass
class ZigBeeBatteryMeteringClamp(BatteryMeteringClamp):
    """ZigBee battery metering clamp"""
    technology: str = "zigbee"


@dataclass
class ElectricityMeteringModule(BaseMeteringDevice):
    """Electricity metering module with clamp"""
    model: str = "electricity-clamp"


@dataclass
class ZigBeeElectricityMeteringModule(ElectricityMeteringModule):
    """ZigBee electricity metering module"""
    technology: str = "zigbee"


@dataclass
class PulseMeteringModule(BaseDevice):
    """Pulse metering module (electricity, gas, water)"""
    device_type: str = "centralmeter"
    model: str = "pulse-meter"

    # Pulse meter specific
    electrical_energy: Optional[float] = None  # Wh
    gas_volume: Optional[float] = None  # m³
    water_volume: Optional[float] = None  # m³

    # Configuration
    pulses_per_unit: Optional[int] = None
    flow: Optional[str] = None  # Producer, Consumer
    segment: Optional[str] = None  # Central, Subsegment


@dataclass
class SmartPlug(BaseDevice):
    """Smart plug device"""
    device_type: str = "smartplug"
    model: str = "smartplug"

    # Smart plug specific
    status: Optional[str] = None  # On, Off
    electrical_power: Optional[float] = None  # W
    electrical_energy: Optional[float] = None  # Wh
    report_instant_usage: Optional[bool] = None

    # Configuration
    feedback_enabled: Optional[bool] = None
    measuring_only: Optional[bool] = None
    switching_only: Optional[bool] = None
    group_id: Optional[int] = None


@dataclass
class ZigBeeSmartPlug(SmartPlug):
    """ZigBee smart plug"""
    technology: str = "zigbee"
    model: str = "naso"  # Niko specific model


@dataclass
class GenericZigBeeSmartPlug(SmartPlug):
    """Generic ZigBee smart plug"""
    technology: str = "zigbee"
    model: str = "generic"


@dataclass
class EnergyHome(BaseDevice):
    """Energy home functionality"""
    device_type: str = "energyhome"
    model: str = "energyhome"

    # Energy production/consumption
    electrical_power_to_grid: Optional[float] = None  # W
    electrical_power_from_grid: Optional[float] = None  # W
    electrical_energy_to_grid: Optional[float] = None  # Wh
    electrical_energy_from_grid: Optional[float] = None  # Wh
    electrical_energy_production: Optional[float] = None  # Wh
    electrical_energy_self_consumption: Optional[float] = None  # Wh
    electrical_energy_consumption: Optional[float] = None  # Wh

    # Peak power
    electrical_peak_power_to_grid: Optional[float] = None  # W
    electrical_peak_power_from_grid: Optional[float] = None  # W
    electrical_monthly_peak_power_from_grid: Optional[float] = None  # W

    # Other utilities
    water_volume: Optional[float] = None  # m³
    gas_volume: Optional[float] = None  # m³

    # Costs
    electrical_energy_to_grid_cost: Optional[float] = None
    electrical_energy_from_grid_cost: Optional[float] = None
    water_cost: Optional[float] = None
    gas_cost: Optional[float] = None

    # Threshold
    electrical_power_production_threshold_exceeded: Optional[bool] = None
    report_instant_usage: Optional[bool] = None

    # Configuration
    peak_mode: Optional[str] = None  # Direct, Predict
    peak_aggregation: Optional[str] = None  # Maximum, Mean
    peak_percentile: Optional[int] = None
    peak_estimations: Optional[int] = None
    peak_mode_optimized_prediction: Optional[bool] = None


# ============================================================================
# Audio/Video Classes
# ============================================================================

@dataclass
class OutdoorVideoDoorStation(BaseDevice):
    """Outdoor video door station"""
    device_type: str = "videodoorstation"
    model: str = "robinsip"

    # Video door station specific
    number_of_buttons: Optional[int] = None
    call_status_01: Optional[str] = None  # Idle, Ringing, Active
    call_status_02: Optional[str] = None
    call_status_03: Optional[str] = None
    call_status_04: Optional[str] = None
    ip_address: Optional[int] = None

    # Button names
    button_name_01: Optional[str] = None
    button_name_02: Optional[str] = None
    button_name_03: Optional[str] = None
    button_name_04: Optional[str] = None


@dataclass
class AudioControlAction(BaseActionDevice):
    """Audio control action"""
    model: str = "audiocontrol"

    # Audio specific
    playback: Optional[str] = None  # Playing, Paused, Buffering
    volume: Optional[int] = None  # 0-100
    muted: Optional[bool] = None
    favourite: Optional[str] = None
    title: Optional[str] = None
    volume_aligned: Optional[bool] = None
    title_aligned: Optional[bool] = None
    connected: Optional[bool] = None

    # Configuration
    manufacturer: Optional[str] = None  # Bose, Sonos
    speaker_uuid: Optional[str] = None  # UUID of speaker


@dataclass
class Speaker(BaseDevice):
    """Base speaker device"""
    device_type: str = "audiocontrol"

    # Speaker specific
    household_id: Optional[str] = None
    player_id: Optional[str] = None
    group_id: Optional[str] = None
    group_name: Optional[str] = None
    group_coordinator: Optional[bool] = None

    # Favourites (dynamic fields would be added during creation)
    favourites: Dict[str, str] = field(default_factory=dict)


@dataclass
class SonosSpeaker(Speaker):
    """Sonos speaker"""
    technology: str = "sonos"
    model: str = "sonos"

    api_version: Optional[str] = None
    software_version: Optional[str] = None


@dataclass
class BoseSpeaker(Speaker):
    """Bose speaker"""
    technology: str = "bose"
    model: str = "bose"


# ============================================================================
# Generic Implementation Classes
# ============================================================================

@dataclass
class GenericVentilation(BaseDevice):
    """Generic ventilation implementation"""
    device_type: str = "hvac"
    model: str = "generic_ventilation"
    technology: str = "generic"

    # Ventilation specific
    program: Optional[str] = None  # Home, Away, Vacation, etc.
    fan_speed: Optional[str] = None  # Off, Low, Medium, High, Automatic
    fan_speed_percent: Optional[int] = None  # 0-100
    boost: Optional[bool] = None
    status: Optional[str] = None  # On, Off
    co2_level: Optional[int] = None  # ppm
    humidity: Optional[int] = None  # %
    coupling_status: Optional[str] = None  # Ok, NoInternet, etc.

    # Configuration
    player_name: Optional[str] = None


@dataclass
class GenericHeatingCooling(BaseDevice):
    """Generic heating/cooling implementation"""
    device_type: str = "hvac"
    model: str = "generic_heating_cooling"
    technology: str = "generic"

    # HVAC specific
    setpoint_temperature: Optional[float] = None
    ambient_temperature: Optional[float] = None
    outdoor_temperature: Optional[float] = None
    program: Optional[str] = None
    operation_mode: Optional[str] = None  # Cool, Heat, Auto
    fan_speed: Optional[str] = None
    fan_speed_percent: Optional[int] = None
    status: Optional[str] = None  # On, Off
    overrule_active: Optional[bool] = None
    coupling_status: Optional[str] = None

    # Configuration
    player_name: Optional[str] = None


@dataclass
class GenericWarmWater(BaseDevice):
    """Generic warm water implementation"""
    device_type: str = "hvac"
    model: str = "generic_warm_water"
    technology: str = "generic"

    # Warm water specific
    domestic_hot_water_temperature: Optional[float] = None
    program: Optional[str] = None
    boost: Optional[bool] = None
    coupling_status: Optional[str] = None

    # Configuration
    player_name: Optional[str] = None


@dataclass
class GenericZigBeeHeatingCooling(GenericHeatingCooling):
    """Generic ZigBee heating/cooling implementation"""
    technology: str = "zigbee"
    model: str = "zigbee_thermostat"

    # ZigBee specific
    partner: Optional[str] = None
    supports_weekly_program: Optional[bool] = None


@dataclass
class GenericChargingStation(BaseDevice):
    """Generic charging station"""
    device_type: str = "chargingstation"
    model: str = "generic_charging_station"
    technology: str = "generic"

    # Charging station specific
    charging_mode: Optional[str] = None  # Solar, Normal, Smart
    charging_status: Optional[str] = None  # Active, Inactive, BatteryFull, Error
    ev_status: Optional[str] = None  # Idle, Connected, Charging, Error
    electrical_power: Optional[float] = None  # W
    electrical_energy: Optional[float] = None  # Wh
    boost: Optional[bool] = None
    coupling_status: Optional[str] = None

    # Smart charging
    target_distance: Optional[int] = None  # km
    target_time: Optional[str] = None  # ISO time
    reachable_distance: Optional[int] = None  # km
    target_reached: Optional[bool] = None
    next_charging_time: Optional[str] = None  # ISO time

    # Configuration
    player_name: Optional[str] = None


# ============================================================================
# System and Location Classes
# ============================================================================

@dataclass
class Location(BaseEntity):
    """Location/room in the system"""
    uuid: str = ""  # Niko's UUID
    index: int = 0
    icon: str = "general"
    device_count: int = 0


@dataclass
class Notification(BaseEntity):
    """System notification"""
    uuid: str = ""  # Niko's UUID
    time_occurred: str = ""  # YYYYMMDDHHMMSS
    type: str = ""  # alarm, notification
    status: str = ""  # new, read
    text: Optional[str] = None


@dataclass
class SystemInfo(BaseEntity):
    """System information"""
    last_config: str = ""  # YYYYMMDDHHMMSS
    water_tariff: float = 0.0
    currency: str = "EUR"
    units: int = 0  # 0 = metric
    language: str = "EN"
    electricity_tariff: float = 0.0
    gas_tariff: float = 0.0
    sw_versions: Dict[str, str] = field(default_factory=dict)


@dataclass
class TimeInfo(BaseEntity):
    """Time information"""
    gmt_offset: int = 0  # seconds
    timezone: str = ""
    is_dst: bool = False
    utc_time: str = ""  # YYYYMMDDHHMMSS


@dataclass
class MQTTMessage(BaseEntity):
    """MQTT message wrapper"""
    topic: str = ""
    method: str = ""
    params: Optional[Dict[str, Any]] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None


# ============================================================================
# Data Conversion and Factory
# ============================================================================

class NikoDataConverter:
    """Convert Niko API data to typed dataclasses"""

    # Model to class mapping
    DEVICE_MODEL_MAP = {
        # Action devices
        "dimmer": DimmerAction,
        "fan": FanAction,
        "light": RelayAction,
        "socket": RelayAction,
        "switched-fan": RelayAction,
        "switched-generic": RelayAction,
        "rolldownshutter": RollerShutterAction,
        "sunblind": MotorAction,
        "gate": GateAction,
        "venetianblind": VenetianBlindAction,
        "reymers": MotorAction,
        "velux": MotorAction,
        "accesscontrol": AccessControlAction,
        "bellbutton": BellButtonAction,
        "garagedoor": GarageDoorAction,
        "alarms": AlarmAction,
        "comfort": MoodAction,
        "alloff": AllOffAction,
        "generic": FreeStartStopAction,
        "overallcomfort": HouseModeAction,
        "pir": PIRAction,
        "simulation": PresenceSimulationAction,
        "playerstatus": PlayerStatusAction,
        "condition": ConditionalAction,
        "peakmode": PeakModeAction,
        "solarmode": SolarModeAction,
        "timeschedule": TimeScheduleAction,

        # Thermostats
        "thermostat": Thermostat,
        "hvacthermostat": HVACThermostat,
        "touchswitch": TouchSwitchThermostat,
        "virtual": VirtualThermostat,

        # Sensors
        "thermoswitch": ThermoSwitch,
        "thermoswitchx1": ThermoSwitch,
        "thermoswitchx1feedback": ThermoSwitch,
        "thermoswitchx2feedback": ThermoSwitch,
        "thermoswitchx3feedback": ThermoSwitch,
        "thermoswitchx4feedback": ThermoSwitch,
        "thermoventilationcontrollerfeedback": ThermoSwitch,
        "flag": VirtualFlag,

        # Metering
        "battery-clamp": BatteryMeteringClamp,
        "electricity-clamp": ElectricityMeteringModule,
        "electricity-pulse": PulseMeteringModule,
        "gas": PulseMeteringModule,
        "water": PulseMeteringModule,

        # Smart plugs
        "naso": ZigBeeSmartPlug,
        "generic": GenericZigBeeSmartPlug,

        # Energy
        "energyhome": EnergyHome,

        # Audio/Video
        "robinsip": OutdoorVideoDoorStation,
        "audiocontrol": AudioControlAction,
        "sonos": SonosSpeaker,
        "bose": BoseSpeaker,
    }

    @classmethod
    def create_device(cls, api_data: Dict[str, Any]) -> BaseDevice:
        """
        Create appropriate device dataclass from Niko API data.

        Args:
            api_data: Raw device data from Niko API

        Returns:
            Typed device dataclass
        """
        model = str(api_data.get("Model", "")).lower()
        device_type = str(api_data.get("Type", ""))
        technology = str(api_data.get("Technology", ""))

        # Debug output
        print(f"Creating device: model={model}, type={device_type}, tech={technology}")

        # Find the appropriate class
        device_class = cls.DEVICE_MODEL_MAP.get(model)
        if not device_class:
            # Fallback based on device type
            if "thermostat" in model or device_type in ["hvac", "thermostat"]:
                device_class = Thermostat
            elif "meter" in model or device_type == "centralmeter":
                device_class = ElectricityMeteringModule
            elif "plug" in model or device_type == "smartplug":
                device_class = SmartPlug
            elif "sensor" in model or device_type == "multisensor":
                device_class = ThermoSwitch
            else:
                device_class = BaseActionDevice
                print(f"Warning: Using BaseActionDevice as fallback for model '{model}'")

        # Extract common fields
        online_str = api_data.get("Online", "False")
        online = online_str == "True" if isinstance(online_str, str) else bool(online_str)

        device = device_class(
            id=str(uuid4()),  # Our internal ID
            uuid=str(api_data.get("Uuid", "")),
            name=str(api_data.get("Name", "")),
            device_type=device_type,
            technology=technology,
            model=model,
            identifier=str(api_data.get("Identifier", "")),
            online=online,
            connection_status="online" if online else "offline"
        )

        # Extract traits
        traits = api_data.get("Traits", {})
        if isinstance(traits, dict):
            device.mac_address = traits.get("MacAddress")
            if "Channel" in traits:
                try:
                    device.channel = int(traits["Channel"])
                except (ValueError, TypeError):
                    device.channel = None
            device.meter_type = traits.get("MeterType")

        # Extract parameters
        params = api_data.get("Parameters", [])
        if isinstance(params, list):
            for param in params:
                if isinstance(param, dict):
                    for key, value in param.items():
                        if key == "LocationId":
                            device.location_id = str(value)
                        elif key == "LocationName":
                            device.location_name = str(value)
                        elif key == "LocationIcon":
                            device.location_icon = str(value)
                        elif key == "IconCode":
                            device.icon_code = str(value)
                        elif key == "ClampType":
                            device.clamp_type = str(value)
                        elif key == "Flow":
                            device.flow = str(value)
                        elif key == "Segment":
                            device.segment = str(value)

        # Extract properties
        properties = api_data.get("Properties", [])
        props_dict = {}

        if isinstance(properties, list):
            # Convert list of property dicts to single dict
            for prop in properties:
                if isinstance(prop, dict):
                    for key, value in prop.items():
                        props_dict[key] = value
        elif isinstance(properties, dict):
            # Already a dict
            props_dict = properties

        # Map properties to device attributes
        cls._map_properties_to_device(device, props_dict)

        # Store all properties in the properties dict for completeness
        device.properties = props_dict

        return device

    @classmethod
    def _map_properties_to_device(cls, device: BaseDevice, properties: Dict[str, Any]):
        """Map property dictionary to device attributes."""

        # Helper function to safely convert values
        def safe_convert(value, target_type):
            if value is None:
                return None
            try:
                if target_type == int:
                    return int(float(value)) if isinstance(value, str) else int(value)
                elif target_type == float:
                    return float(value)
                elif target_type == bool:
                    if isinstance(value, str):
                        return value.lower() in ["true", "yes", "1", "on"]
                    return bool(value)
                else:
                    return str(value)
            except (ValueError, TypeError):
                return None

        # Common properties
        if "Status" in properties:
            device.status = safe_convert(properties["Status"], str)

        if "BasicState" in properties:
            device.basic_state = safe_convert(properties["BasicState"], str)

        # Map based on device type
        if isinstance(device, DimmerAction):
            if "Brightness" in properties:
                device.brightness = safe_convert(properties["Brightness"], int)
            if "Aligned" in properties:
                device.aligned = safe_convert(properties["Aligned"], bool)

        elif isinstance(device, MotorAction):
            if "Position" in properties:
                device.position = safe_convert(properties["Position"], int)
            if "Aligned" in properties:
                device.aligned = safe_convert(properties["Aligned"], bool)
            if "Moving" in properties:
                device.moving = safe_convert(properties["Moving"], bool)
            if "LastDirection" in properties:
                device.last_direction = safe_convert(properties["LastDirection"], str)

        elif isinstance(device, BaseThermostatDevice):
            if "Program" in properties:
                device.program = safe_convert(properties["Program"], str)
            if "AmbientTemperature" in properties:
                device.ambient_temperature = safe_convert(properties["AmbientTemperature"], float)
            if "SetpointTemperature" in properties:
                device.setpoint_temperature = safe_convert(properties["SetpointTemperature"], float)
            if "OverruleActive" in properties:
                device.overrule_active = safe_convert(properties["OverruleActive"], bool)
            if "EcoSave" in properties:
                device.eco_save = safe_convert(properties["EcoSave"], bool)
            if "Demand" in properties:
                device.demand = safe_convert(properties["Demand"], str)

        elif isinstance(device, BaseMeteringDevice):
            if "ElectricalPower" in properties:
                device.electrical_power = safe_convert(properties["ElectricalPower"], float)
            if "ElectricalEnergy" in properties:
                device.electrical_energy = safe_convert(properties["ElectricalEnergy"], float)
            if "ReportInstantUsage" in properties:
                device.report_instant_usage = safe_convert(properties["ReportInstantUsage"], bool)

        elif isinstance(device, SmartPlug):
            if "ElectricalPower" in properties:
                device.electrical_power = safe_convert(properties["ElectricalPower"], float)
            if "ElectricalEnergy" in properties:
                device.electrical_energy = safe_convert(properties["ElectricalEnergy"], float)
            if "ReportInstantUsage" in properties:
                device.report_instant_usage = safe_convert(properties["ReportInstantUsage"], bool)

        elif isinstance(device, ThermoSwitch):
            if "AmbientTemperature" in properties:
                device.ambient_temperature = safe_convert(properties["AmbientTemperature"], float)
            if "Humidity" in properties:
                device.humidity = safe_convert(properties["Humidity"], float)
            if "HeatIndex" in properties:
                device.heat_index = safe_convert(properties["HeatIndex"], float)


# ============================================================================
# Redis Publisher
# ============================================================================

class RedisPublisher:
    """Publish dataclasses to Redis with consistent structure"""

    @staticmethod
    def create_key(entity: BaseEntity, prefix: str = "niko") -> str:
        """Create Redis key for an entity."""
        if isinstance(entity, BaseDevice):
            return f"{prefix}:device:{entity.uuid or entity.id}"
        elif isinstance(entity, Location):
            return f"{prefix}:location:{entity.uuid or entity.id}"
        elif isinstance(entity, Notification):
            return f"{prefix}:notification:{entity.uuid or entity.id}"
        elif isinstance(entity, SystemInfo):
            return f"{prefix}:system:info"
        elif isinstance(entity, TimeInfo):
            return f"{prefix}:system:time"
        else:
            return f"{prefix}:{entity.__class__.__name__.lower()}:{entity.id}"

    @staticmethod
    def prepare_for_redis(entity: BaseEntity) -> Dict[str, Any]:
        """Prepare entity for Redis storage."""
        # Use the entity's to_dict method if available
        if hasattr(entity, 'to_dict'):
            data = entity.to_dict()
        else:
            # Get all dataclass fields
            data = {
                field.name: getattr(entity, field.name)
                for field in entity.__dataclass_fields__.values()
            }

        # Add metadata
        data["_class"] = entity.__class__.__name__
        data["_timestamp"] = datetime.now().isoformat()

        return data

    @classmethod
    def publish_device(cls, device: BaseDevice, redis_client) -> str:
        """Publish device to Redis."""
        key = cls.create_key(device)
        data = cls.prepare_for_redis(device)

        # Store in Redis (assuming redis_client is a Redis client instance)
        redis_client.set(key, json.dumps(data, default=str))

        # Also publish to a channel for real-time updates
        channel = f"niko:device:updates:{device.uuid or device.id}"
        redis_client.publish(channel, json.dumps(data, default=str))

        return key

    @classmethod
    def publish_location(cls, location: Location, redis_client) -> str:
        """Publish location to Redis."""
        key = cls.create_key(location)
        data = cls.prepare_for_redis(location)
        redis_client.set(key, json.dumps(data, default=str))
        return key


# ============================================================================
# UI Data Provider
# ============================================================================

class UIDataProvider:
    """Provide structured data for UI consumption"""

    @staticmethod
    def get_device_summary(device: BaseDevice) -> Dict[str, Any]:
        """Get summary of device for UI display."""
        summary = {
            "id": device.id,
            "uuid": device.uuid,
            "name": device.name,
            "type": device.device_type,
            "model": device.model,
            "online": device.online,
            "location": device.location_name or "Unknown",
            "icon": device.icon_code or "default",
            "last_update": device.timestamp
        }

        # Add type-specific summary
        if isinstance(device, DimmerAction):
            summary.update({
                "status": device.status,
                "brightness": device.brightness,
                "icon": "lightbulb"
            })
        elif isinstance(device, (MotorAction, RollerShutterAction, VenetianBlindAction, GateAction)):
            summary.update({
                "position": device.position,
                "moving": device.moving,
                "icon": "blinds" if "shutter" in device.model else "gate"
            })
        elif isinstance(device, (Thermostat, HVACThermostat, TouchSwitchThermostat, VirtualThermostat)):
            summary.update({
                "temperature": device.ambient_temperature,
                "setpoint": device.setpoint_temperature,
                "icon": "thermostat"
            })
        elif isinstance(device, SmartPlug):
            summary.update({
                "status": device.status,
                "power": device.electrical_power,
                "icon": "power"
            })
        elif isinstance(device, ThermoSwitch):
            summary.update({
                "temperature": device.ambient_temperature,
                "humidity": device.humidity,
                "icon": "thermometer"
            })

        return summary

    @staticmethod
    def get_location_overview(locations: List[Location], devices: List[BaseDevice]) -> Dict[str, Any]:
        """Get overview of locations with device counts for UI."""
        overview = {}

        for location in locations:
            # Count devices in this location
            device_count = sum(1 for d in devices if d.location_id == location.uuid)

            overview[location.uuid] = {
                "name": location.name,
                "icon": location.icon,
                "device_count": device_count,
                "devices": []
            }

        # Add device summaries
        for device in devices:
            if device.location_id in overview:
                overview[device.location_id]["devices"].append(
                    UIDataProvider.get_device_summary(device)
                )

        return overview


# ============================================================================
# Usage Example
# ============================================================================

if __name__ == "__main__":
    # Example: Convert Niko API data to dataclass
    niko_device_data = {
        "Uuid": "21a967a1-676d-487b-b8d4-9736ef16d450",
        "Type": "action",
        "Technology": "nikohomecontrol",
        "Model": "dimmer",
        "Identifier": "a4fafca1-bde4-4ad7-94f9-292c60c26bf7",
        "Name": "Living Room Light",
        "Online": "True",
        "Traits": {},
        "Parameters": [
            {"LocationId": "7762f934-83d3-4c66-b4bd-df7065cb1c6a"},
            {"LocationName": "Living Room"},
            {"LocationIcon": "general"},
            {"IconCode": "light"}
        ],
        "Properties": [
            {"Status": "On"},
            {"Brightness": "75"},
            {"Aligned": "True"}
        ]
    }

    # Also test with a relay device
    relay_device_data = {
        "Uuid": "device-uuid-123",
        "Type": "relay",
        "Technology": "nikohomecontrol",
        "Model": "light",
        "Identifier": "identifier-123",
        "Name": "Kitchen Light",
        "Online": "True",
        "Traits": {},
        "Parameters": [
            {"LocationId": "location-uuid-123"},
            {"LocationName": "Kitchen"},
            {"LocationIcon": "general"}
        ],
        "Properties": [
            {"Status": "Off"}
        ]
    }

    # Convert to typed dataclasses
    dimmer = NikoDataConverter.create_device(niko_device_data)
    relay = NikoDataConverter.create_device(relay_device_data)

    print(f"Created dimmer: {dimmer.name}, Type: {dimmer.__class__.__name__}")
    print(f"Created relay: {relay.name}, Type: {relay.__class__.__name__}")

    # Prepare for Redis
    redis_data_dimmer = RedisPublisher.prepare_for_redis(dimmer)
    print(f"\nDimmer Redis data keys: {list(redis_data_dimmer.keys())}")

    # Get UI summary
    ui_summary = UIDataProvider.get_device_summary(dimmer)
    print(f"\nUI Summary:\n{json.dumps(ui_summary, indent=2)}")