"""
DUCO Modbus TCP Python Wrapper
For DucoBox Silent Connect, Focus, and Energy ventilation systems
API Version: v2.5+

Features:
- Correct decimal register addressing with offset support
- Device-specific parameter validation
- Complete coverage of all system and node parameters
"""

import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, List, Union

from pymodbus.client import ModbusTcpClient


class VentilationMode(Enum):
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


class VentilationStatus(Enum):
    """Ventilation status codes"""
    OK = 0
    ERROR = 1
    INACTIVE = 2


class FilterStatus(Enum):
    """Filter status codes"""
    OK = 0
    DIRTY = 1
    INACTIVE = 2


class NodeType(Enum):
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


@dataclass
class NodeCapabilities:
    """Defines which parameters are available for each node type"""
    has_remaining_time: bool = False
    has_flow_level: bool = False
    has_air_quality_rh: bool = False
    has_air_quality_co2: bool = False
    has_humidity: bool = False
    has_co2: bool = False
    can_set_mode: bool = False
    can_identify: bool = False
    has_oda: bool = False
    has_sup: bool = False
    has_eta: bool = False
    has_eha: bool = False


# Device capability mapping based on "Toepassing" column from PDF
NODE_CAPABILITIES = {
    NodeType.CONTROL_SWITCH_RF_BAT: NodeCapabilities(
        has_remaining_time=True,
        can_set_mode=True,
        can_identify=True
    ),
    NodeType.CONTROL_SWITCH_RF_WIRED: NodeCapabilities(
        has_remaining_time=True,
        can_set_mode=True,
        can_identify=True
    ),
    NodeType.HUMIDITY_ROOM_SENSOR: NodeCapabilities(
        has_air_quality_rh=True,
        has_humidity=True,
        can_set_mode=True,
        can_identify=True
    ),
    NodeType.CO2_ROOM_SENSOR: NodeCapabilities(
        has_air_quality_co2=True,
        has_co2=True,
        can_set_mode=True,
        can_identify=True
    ),
    NodeType.SENSORLESS_VALVE: NodeCapabilities(
        has_remaining_time=True,
        has_flow_level=True,
        can_set_mode=True,
        can_identify=True
    ),
    NodeType.HUMIDITY_VALVE: NodeCapabilities(
        has_remaining_time=True,
        has_flow_level=True,
        has_air_quality_rh=True,
        has_humidity=True,
        can_set_mode=True,
        can_identify=True
    ),
    NodeType.CO2_VALVE: NodeCapabilities(
        has_remaining_time=True,
        has_flow_level=True,
        has_air_quality_co2=True,
        has_co2=True,
        can_set_mode=True,
        can_identify=True
    ),
    NodeType.SWITCH_CONTACT: NodeCapabilities(
        has_remaining_time=True,
        can_identify=True
    ),
    NodeType.IAV_VALVE: NodeCapabilities(
        has_remaining_time=True,
        has_flow_level=True,
        can_set_mode=True,
        can_identify=True
    ),
    NodeType.IAV_HUMIDITY: NodeCapabilities(
        has_remaining_time=True,
        has_flow_level=True,
        has_air_quality_rh=True,
        has_humidity=True,
        can_set_mode=True,
        can_identify=True
    ),
    NodeType.IAV_CO2: NodeCapabilities(
        has_remaining_time=True,
        has_flow_level=True,
        has_air_quality_co2=True,
        has_co2=True,
        can_set_mode=True,
        can_identify=True
    ),
    NodeType.CO2_RH_VALVE: NodeCapabilities(
        has_remaining_time=True,
        has_flow_level=True,
        has_air_quality_rh=True,
        has_air_quality_co2=True,
        has_humidity=True,
        has_co2=True,
        can_set_mode=True,
        can_identify=True
    ),
    NodeType.HUMIDITY_BOX_SENSOR: NodeCapabilities(
        has_humidity=True,
        can_set_mode=True
    ),
    NodeType.CO2_BOX_SENSOR: NodeCapabilities(
        has_co2=True,
        can_set_mode=True
    ),
    NodeType.DUCOTRONIC_GRILLE: NodeCapabilities(
        has_flow_level=True,
        has_humidity=True,
        can_set_mode=True,
        can_identify=True
    ),
}


class DucoModbusClient:
    """
    Python wrapper for DUCO Modbus TCP interface

    Supports DucoBox Silent Connect, Focus, and Energy with Connectivity Board
    """
    # Default Modbus settings
    DEFAULT_PORT = 502
    DEFAULT_TIMEOUT = 3

    # Write action limits
    WRITE_INTERVAL = 2  # seconds between writes

    def __init__(self, host: str, port: int = DEFAULT_PORT,
                 unit_id: int = 1, register_offset: int = 0):
        """
        Initialize DUCO Modbus TCP client

        Args:
            host: IP address of the DucoBox Connectivity Board
            port: Modbus TCP port (default: 502)
            unit_id: Modbus unit ID (configurable via Display menu)
            register_offset: Register offset for compatibility
        """
        self.host = host
        self.port = port
        self.unit_id = unit_id
        self.register_offset = register_offset

        self.client = ModbusTcpClient(host, port=port, timeout=self.DEFAULT_TIMEOUT)
        self._last_write_time = 0

        # Detect pymodbus API version
        self._use_unit_param = self._detect_api_version()

        # Cache for node types to avoid repeated queries
        self._node_type_cache: Dict[int, NodeType] = {}

    def connect(self) -> bool:
        """Establish connection to the DucoBox"""
        return self.client.connect()

    def disconnect(self):
        """Close connection to the DucoBox"""
        self.client.close()

    def _detect_api_version(self) -> str | None:
        """Detect which pymodbus API parameter to use"""
        import inspect
        sig = inspect.signature(self.client.read_input_registers)
        params = sig.parameters.keys()

        if 'slave' in params:
            return 'slave'
        elif 'unit' in params:
            return 'unit'
        else:
            return None

    def _enforce_write_limit(self):
        """Enforce 2-second delay between write operations"""
        elapsed = time.time() - self._last_write_time
        if elapsed < self.WRITE_INTERVAL:
            time.sleep(self.WRITE_INTERVAL - elapsed)
        self._last_write_time = time.time()

    def _adjust_register(self, register: int, no_shift: bool = False) -> int:
        """Apply register offset if configured"""
        return register + self.register_offset if no_shift else register

    def _convert_temperature(self, value: int) -> Optional[float]:
        """
        Convert temperature value from Modbus register
        Handles negative temperatures (values > 32767)
        """
        if value is None:
            return None
        if value >= 32768:
            return (value - 65536) / 10.0
        return value / 10.0

    def read_input_register(self, register: int, no_shift: bool = False, debug: bool = False) -> Optional[int]:
        """Read a single INPUT register"""
        try:
            kwargs = {'address': self._adjust_register(register, no_shift=no_shift), 'count': 1}

            if self._use_unit_param == 'unit':
                kwargs['unit'] = self.unit_id
            elif self._use_unit_param == 'slave':
                kwargs['slave'] = self.unit_id

            if debug:
                print(
                    f"Reading INPUT register {register} (adjusted: {self._adjust_register(register, no_shift=no_shift)})")

            result = self.client.read_input_registers(**kwargs)

            if hasattr(result, 'isError') and not result.isError():
                return result.registers[0]
            elif hasattr(result, 'registers') and result.registers:
                return result.registers[0]

            return None
        except Exception as e:
            if debug:
                print(f"Exception reading INPUT register {register}: {e}")
            return None

    def read_holding_register(self, register: int, debug: bool = False) -> Optional[int]:
        """Read a single HOLDING register"""
        try:
            kwargs = {'address': self._adjust_register(register), 'count': 1}

            if self._use_unit_param == 'unit':
                kwargs['unit'] = self.unit_id
            elif self._use_unit_param == 'slave':
                kwargs['slave'] = self.unit_id

            if debug:
                print(f"Reading HOLDING register {register} (adjusted: {self._adjust_register(register)})")

            result = self.client.read_holding_registers(**kwargs)

            if hasattr(result, 'isError') and not result.isError():
                return result.registers[0]
            elif hasattr(result, 'registers') and result.registers:
                return result.registers[0]

            return None
        except Exception as e:
            if debug:
                print(f"Exception reading HOLDING register {register}: {e}")
            return None

    def write_register(self, register: int, value: int, debug: bool = False) -> bool:
        """
        Write a single HOLDING register
        Enforces write rate limiting
        """
        self._enforce_write_limit()
        try:
            kwargs = {'address': self._adjust_register(register), 'value': value}

            if self._use_unit_param == 'unit':
                kwargs['unit'] = self.unit_id
            elif self._use_unit_param == 'slave':
                kwargs['slave'] = self.unit_id

            if debug:
                print(f"Writing HOLDING register {register} (adjusted: {self._adjust_register(register)}) = {value}")

            result = self.client.write_register(**kwargs)

            return not result.isError()
        except Exception as e:
            if debug:
                print(f"Exception writing HOLDING register {register}: {e}")
            return False

    # ===== SYSTEM-LEVEL PARAMETERS (registers 20-31 decimal) =====

    def get_temperature_oda(self) -> Optional[float]:
        """Get outdoor air temperature (°C) - DucoBox Energy only - Register 20"""
        value = self.read_input_register(20)
        return self._convert_temperature(value)

    def get_temperature_sup(self) -> Optional[float]:
        """Get supply air temperature (°C) - DucoBox Energy only - Register 21"""
        value = self.read_input_register(21)
        return self._convert_temperature(value)

    def get_temperature_eta(self) -> Optional[float]:
        """Get extract air temperature (°C) - DucoBox Energy only - Register 22"""
        value = self.read_input_register(22)
        return self._convert_temperature(value)

    def get_temperature_eha(self) -> Optional[float]:
        """Get exhaust air temperature (°C) - DucoBox Energy only - Register 23"""
        value = self.read_input_register(23)
        return self._convert_temperature(value)

    def get_outdoor_temperature(self) -> Optional[float]:
        """Get outdoor temperature from weather station (°C) - Register 24"""
        value = self.read_input_register(24)
        return self._convert_temperature(value)

    def get_wind_speed(self) -> Optional[float]:
        """Get wind speed from weather station (dm/s) - Register 25"""
        value = self.read_input_register(25)
        return value / 10.0 if value is not None else None

    def get_rain(self) -> Optional[bool]:
        """Get rain status from weather station - Register 26"""
        value = self.read_input_register(26)
        return bool(value) if value is not None else None

    def get_light_south(self) -> Optional[float]:
        """Get light intensity south from weather station (kilolux) - Register 27"""
        value = self.read_input_register(27)
        return value / 1000.0 if value is not None else None

    def get_light_east(self) -> Optional[float]:
        """Get light intensity east from weather station (kilolux) - Register 28"""
        value = self.read_input_register(28)
        return value / 1000.0 if value is not None else None

    def get_light_west(self) -> Optional[float]:
        """Get light intensity west from weather station (kilolux) - Register 29"""
        value = self.read_input_register(29)
        return value / 1000.0 if value is not None else None

    def get_api_version(self) -> Optional[str]:
        """Get local API version (e.g., 2.5 returns as 205 -> "2.5") - Register 30"""
        value = self.read_input_register(30)
        if value is not None:
            major = value // 100
            minor = value % 100
            return f"{major}.{minor}"
        return None

    def get_remaining_write_actions(self) -> Optional[int]:
        """Get remaining write actions until midnight - Register 31"""
        return self.read_input_register(31)

    # ===== DUCOBOX PARAMETERS (registers 100-110 decimal) =====

    def get_system_type(self) -> Optional[int]:
        """Get system type (17 = DucoBox) - Register 100"""
        return self.read_input_register(100)

    def get_remaining_time_current_mode(self) -> Optional[int]:
        """Get remaining time in current ventilation mode (seconds) - Register 102"""
        return self.read_input_register(102)

    def get_flow_level_vs_target(self) -> Optional[int]:
        """Get actual flow level vs target (percentage) - Register 103"""
        return self.read_input_register(103)

    def get_indoor_air_quality_rh(self) -> Optional[int]:
        """Get indoor air quality based on relative humidity (%) - Register 104"""
        return self.read_input_register(104)

    def get_indoor_air_quality_co2(self) -> Optional[int]:
        """Get indoor air quality based on CO2 (%) - Register 105"""
        return self.read_input_register(105)

    def get_ventilation_status(self) -> Optional[VentilationStatus]:
        """Get ventilation status - Register 106"""
        value = self.read_input_register(106)
        if value is not None:
            try:
                return VentilationStatus(value)
            except ValueError:
                return None
        return None

    def get_filter_remaining_time(self) -> Optional[int]:
        """Get filter remaining lifetime (days) - DucoBox Energy only - Register 107"""
        return self.read_input_register(107)

    def get_filter_status(self) -> Optional[FilterStatus]:
        """Get filter status - DucoBox Energy only - Register 108"""
        value = self.read_input_register(108)
        if value is not None:
            try:
                return FilterStatus(value)
            except ValueError:
                return None
        return None

    def get_humidity(self) -> Optional[int]:
        """Get relative humidity (%) - Register 109"""
        return self.read_input_register(109)

    def get_co2(self) -> Optional[int]:
        """Get CO2 level (ppm) - Register 110"""
        return self.read_input_register(110)

    # ===== DUCOBOX HOLDING REGISTERS (write/read) =====

    def get_ventilation_mode(self) -> Optional[VentilationMode]:
        """Get current ventilation mode - HOLDING Register 100"""
        value = self.read_holding_register(100)
        if value is not None:
            try:
                return VentilationMode(value)
            except ValueError:
                return None
        return None

    def set_ventilation_mode(self, mode: Union[VentilationMode, int]) -> bool:
        """Set ventilation mode - HOLDING Register 100"""
        if isinstance(mode, VentilationMode):
            mode = mode.value
        return self.write_register(100, mode)

    def identify_ducobox(self, enable: bool = True) -> bool:
        """Enable/disable DucoBox identification (blue light) - HOLDING Register 101"""
        return self.write_register(101, 1 if enable else 0)

    def set_supply_temperature_zone(self, zone: int, temperature: float) -> bool:
        """
        Set comfort temperature for supply zone (DucoBox Energy only)
        HOLDING Registers 102-105

        Args:
            zone: Zone number (1-4)
            temperature: Temperature in °C
        """
        if not 1 <= zone <= 4:
            raise ValueError("Zone must be between 1 and 4")

        register = 101 + zone  # 102 to 105
        value = int(temperature * 10)
        return self.write_register(register, value)

    def get_supply_temperature_zone(self, zone: int) -> Optional[float]:
        """Get comfort temperature for supply zone (DucoBox Energy only)"""
        if not 1 <= zone <= 4:
            raise ValueError("Zone must be between 1 and 4")

        register = 101 + zone
        value = self.read_holding_register(register)
        return self._convert_temperature(value)

    # ===== NODE-LEVEL PARAMETERS =====

    def _node_register(self, node: int, param: int) -> int:
        """
        Calculate node register address in XXyy format (decimal)
        Example: node=52, param=1 -> 5201
        """
        return (node * 100) + param

    def _get_node_capabilities(self, node: int) -> Optional[NodeCapabilities]:
        """Get capabilities for a node type"""
        node_type = self.get_node_type(node)
        if node_type:
            # Return capabilities if known, otherwise return None
            return NODE_CAPABILITIES.get(node_type)
        return None

    def get_node_type(self, node: int) -> Optional[NodeType]:
        """Get node type - Parameter xx00"""
        # Check cache first
        if node in self._node_type_cache:
            return self._node_type_cache[node]

        register = self._node_register(node, 0)
        value = self.read_input_register(register)
        if value is not None:
            try:
                node_type = NodeType(value)
                self._node_type_cache[node] = node_type
                return node_type
            except ValueError:
                self._node_type_cache[node] = NodeType.UNKNOWN
                return NodeType.UNKNOWN
        return None

    def get_node_remaining_time(self, node: int) -> Optional[int]:
        """Get node remaining time in current mode (seconds) - Parameter xx02"""
        caps = self._get_node_capabilities(node)
        if caps and not caps.has_remaining_time:
            return None
        register = self._node_register(node, 2)
        return self.read_input_register(register)

    def get_node_flow_level(self, node: int) -> Optional[int]:
        """Get node flow level vs target (percentage) - Parameter xx03"""
        caps = self._get_node_capabilities(node)
        if caps and not caps.has_flow_level:
            return None
        register = self._node_register(node, 3)
        return self.read_input_register(register)

    def get_node_air_quality_rh(self, node: int) -> Optional[int]:
        """Get node indoor air quality based on RH (%) - Parameter xx04"""
        caps = self._get_node_capabilities(node)
        if caps and not caps.has_air_quality_rh:
            return None
        register = self._node_register(node, 4)
        return self.read_input_register(register)

    def get_node_air_quality_co2(self, node: int) -> Optional[int]:
        """Get node indoor air quality based on CO2 (%) - Parameter xx05"""
        caps = self._get_node_capabilities(node)
        if caps and not caps.has_air_quality_co2:
            return None
        register = self._node_register(node, 5)
        return self.read_input_register(register)

    def get_node_humidity(self, node: int) -> Optional[int]:
        """Get node humidity (%) - Parameter xx09"""
        caps = self._get_node_capabilities(node)
        if caps and not caps.has_humidity:
            return None
        register = self._node_register(node, 9)
        return self.read_input_register(register)

    def get_node_co2(self, node: int) -> Optional[int]:
        """Get node CO2 (ppm) - Parameter xx10"""
        caps = self._get_node_capabilities(node)
        if caps and not caps.has_co2:
            return None
        register = self._node_register(node, 10)
        return self.read_input_register(register)

    def set_node_ventilation_mode(self, node: int, mode: Union[VentilationMode, int]) -> bool:
        """Set node ventilation mode - HOLDING Parameter xx00"""
        caps = self._get_node_capabilities(node)
        if caps and not caps.can_set_mode:
            raise ValueError(f"Node {node} does not support ventilation mode setting")

        if isinstance(mode, VentilationMode):
            mode = mode.value
        register = self._node_register(node, 0)
        return self.write_register(register, mode)

    def identify_node(self, node: int, enable: bool = True, force: bool = False) -> bool:
        """
        Enable/disable node identification (blue light) - HOLDING Parameter xx01

        Args:
            node: Node ID
            enable: True to turn on identification, False to turn off
            force: If True, try to identify even if capabilities say it's not supported

        Returns:
            True if successful, False if failed
        """
        # Check capabilities first (unless forcing)
        if not force:
            caps = self._get_node_capabilities(node)
            if caps and not caps.can_identify:
                node_type = self.get_node_type(node)
                type_name = node_type.name if node_type else "Unknown"
                print(f"Warning: Node {node} (type: {type_name}) may not support identification.")
                print("Use force=True to try anyway.")
                return False

        # Try to write to the identification register
        register = self._node_register(node, 1)
        success = self.write_register(register, 1 if enable else 0)

        if success:
            action = "enabled" if enable else "disabled"
            print(f"Node {node} identification {action}.")
        else:
            print(f"Failed to identify node {node}.")

        return success

    def get_node_info(self, node: int) -> Dict:
        """Get all available information for a node"""
        node_type = self.get_node_type(node)
        if not node_type:
            return {}

        info = {
            'node_id': node,
            'type': node_type.name,
        }

        caps = self._get_node_capabilities(node)
        if not caps:
            return info

        if caps.has_remaining_time:
            val = self.get_node_remaining_time(node)
            if val is not None:
                info['remaining_time_seconds'] = val

        if caps.has_flow_level:
            val = self.get_node_flow_level(node)
            if val is not None:
                info['flow_level_percent'] = val

        if caps.has_air_quality_rh:
            val = self.get_node_air_quality_rh(node)
            if val is not None:
                info['air_quality_rh_percent'] = val

        if caps.has_air_quality_co2:
            val = self.get_node_air_quality_co2(node)
            if val is not None:
                info['air_quality_co2_percent'] = val

        if caps.has_humidity:
            val = self.get_node_humidity(node)
            if val is not None:
                info['humidity_percent'] = val

        if caps.has_co2:
            val = self.get_node_co2(node)
            if val is not None:
                info['co2_ppm'] = val

        return info

    # ===== NETWORK DISCOVERY =====

    def get_active_nodes(self) -> List[int]:
        """
        Get list of active node numbers in the network
        Scans INPUT registers 0-8 for node presence bit fields

        Each register covers 16 nodes:
        - Register 0: nodes 0-15
        - Register 1: nodes 16-31
        - Register 3: nodes 48-63 (your node 52 example)
        """
        active_nodes = []

        for reg in range(0, 9):
            value = self.read_input_register(reg + self.register_offset)
            if value is not None:
                base_node = reg * 16

                for bit in range(16):
                    if value & (1 << bit):
                        node_num = base_node + bit
                        if 1 <= node_num <= 143:
                            active_nodes.append(node_num)

        return sorted(active_nodes)

    def scan_network(self) -> Dict[int, NodeType]:
        """Scan network and return dictionary of active nodes with their types"""
        nodes = {}
        active_nodes = self.get_active_nodes()

        for node in active_nodes:
            node_type = self.get_node_type(node)
            if node_type:
                nodes[node] = node_type

        return nodes

    def scan_network_detailed(self, identify_during_scan: bool = False, scan_delay: float = 0.5) -> Dict[int, Dict]:
        """Scan network and return detailed info for all nodes

        Args:
            identify_during_scan: If True, briefly identify each node with blue light
            scan_delay: Delay between scanning nodes (seconds)
        """
        detailed = {}
        active_nodes = self.get_active_nodes()

        print(f"Found {len(active_nodes)} active nodes: {active_nodes}")

        for i, node in enumerate(active_nodes):
            try:
                if identify_during_scan:
                    # Briefly identify this node
                    self.identify_node(node, enable=True, force=True)
                    print(f"\nScanning node {node} (identified with blue light)...")
                else:
                    print(f"\nScanning node {node}...")

                # Get node info
                node_info = self.get_node_info(node)
                detailed[node] = node_info

                # Display what we found
                node_type = node_info.get('type', 'UNKNOWN')
                print(f"  Type: {node_type}")

                # Show key parameters based on node type
                if node_type in ['HUMIDITY_ROOM_SENSOR', 'HUMIDITY_VALVE', 'HUMIDITY_BOX_SENSOR']:
                    if 'humidity_percent' in node_info:
                        print(f"  Humidity: {node_info['humidity_percent']}%")
                    if 'air_quality_rh_percent' in node_info:
                        print(f"  Air Quality (RH): {node_info['air_quality_rh_percent']}%")

                elif node_type in ['CO2_ROOM_SENSOR', 'CO2_VALVE', 'CO2_BOX_SENSOR']:
                    if 'co2_ppm' in node_info:
                        print(f"  CO2: {node_info['co2_ppm']} ppm")
                    if 'air_quality_co2_percent' in node_info:
                        print(f"  Air Quality (CO2): {node_info['air_quality_co2_percent']}%")

                elif node_type in ['SENSORLESS_VALVE', 'IAV_VALVE']:
                    if 'flow_level_percent' in node_info:
                        print(f"  Flow Level: {node_info['flow_level_percent']}%")

                elif node_type in ['CONTROL_SWITCH_RF_BAT', 'CONTROL_SWITCH_RF_WIRED']:
                    print(f"  Control Switch")

                # Show remaining time if available
                if 'remaining_time_seconds' in node_info:
                    mins = node_info['remaining_time_seconds'] // 60
                    secs = node_info['remaining_time_seconds'] % 60
                    print(f"  Remaining time: {mins}m {secs}s")

                # Turn off identification after scanning if it was enabled
                if identify_during_scan:
                    self.identify_node(node, enable=False, force=True)
                    time.sleep(0.1)  # Brief pause

                # Add delay between nodes if requested
                if i < len(active_nodes) - 1 and scan_delay > 0:
                    time.sleep(scan_delay)

            except Exception as e:
                print(f"  Error scanning node {node}: {e}")
                detailed[node] = {'error': str(e)}

                # Make sure to turn off identification even on error
                if identify_during_scan:
                    try:
                        self.identify_node(node, enable=False, force=True)
                    except:
                        pass

        return detailed

    def identify_all_nodes(self, duration: float = 3.0) -> bool:
        """
        Identify all active nodes sequentially

        Args:
            duration: How long to keep each node identified (seconds)

        Returns:
            True if successful
        """
        active_nodes = self.get_active_nodes()

        if not active_nodes:
            print("No active nodes found.")
            return False

        print(f"Identifying {len(active_nodes)} nodes for {duration} seconds each...")

        for node in active_nodes:
            try:
                node_type = self.get_node_type(node)
                type_name = node_type.name if node_type else "Unknown"
                print(f"Identifying node {node} ({type_name})...")

                # Turn on identification
                if self.identify_node(node, enable=True, force=True):
                    # Keep it on for the duration
                    time.sleep(duration)
                    # Turn off
                    self.identify_node(node, enable=False, force=True)
                else:
                    print(f"  Failed to identify node {node}")

            except Exception as e:
                print(f"  Error with node {node}: {e}")

        print("Identification complete.")
        return True

    def get_node_type_name(self, node: int) -> str:
        """Get human-readable node type name"""
        node_type = self.get_node_type(node)
        if not node_type:
            return "Unknown"

        # Map enum names to more readable versions
        type_map = {
            'CONTROL_SWITCH_RF_BAT': 'Control Switch (RF/Battery)',
            'CONTROL_SWITCH_RF_WIRED': 'Control Switch (RF/Wired)',
            'HUMIDITY_ROOM_SENSOR': 'Humidity Room Sensor',
            'CO2_ROOM_SENSOR': 'CO2 Room Sensor',
            'SENSORLESS_VALVE': 'Sensorless Valve',
            'HUMIDITY_VALVE': 'Humidity Valve',
            'CO2_VALVE': 'CO2 Valve',
            'SWITCH_CONTACT': 'Switch Contact',
            'IAV_VALVE': 'iAV Valve',
            'IAV_HUMIDITY': 'iAV Humidity',
            'IAV_CO2': 'iAV CO2',
            'CO2_RH_VALVE': 'CO2/RH Valve',
            'HUMIDITY_BOX_SENSOR': 'Humidity Box Sensor',
            'CO2_BOX_SENSOR': 'CO2 Box Sensor',
            'DUCOTRONIC_GRILLE': 'Ducotronic Grille',
            'CONTROL_UNIT': 'Control Unit',
            'SUN_CONTROL_SWITCH': 'Sun Control Switch',
            'VENTILATIVE_COOLING_SWITCH': 'Ventilative Cooling Switch',
            'EXTERNAL_MULTIZONE_VALVE': 'External Multizone Valve',
            'WEATHER_STATION': 'Weather Station',
        }

        return type_map.get(node_type.name, node_type.name.replace('_', ' ').title())

    # ===== CONVENIENCE METHODS =====

    def get_system_info(self) -> Dict:
        """Get comprehensive system information"""
        info = {
            'api_version': self.get_api_version(),
            'system_type': self.get_system_type(),
            'ventilation_status': self.get_ventilation_status(),
            'ventilation_mode': self.get_ventilation_mode(),
            'remaining_write_actions': self.get_remaining_write_actions(),
            'humidity': self.get_humidity(),
            'co2': self.get_co2(),
            'air_quality_rh': self.get_indoor_air_quality_rh(),
            'air_quality_co2': self.get_indoor_air_quality_co2(),
        }
        return {k: v for k, v in info.items() if v is not None}

    def get_temperatures(self) -> Dict:
        """Get all available temperatures"""
        temps = {
            'outdoor_air': self.get_temperature_oda(),
            'supply_air': self.get_temperature_sup(),
            'extract_air': self.get_temperature_eta(),
            'exhaust_air': self.get_temperature_eha(),
            'outdoor': self.get_outdoor_temperature(),
        }
        return {k: v for k, v in temps.items() if v is not None}

    def get_weather_data(self) -> Dict:
        """Get weather station data"""
        weather = {
            'temperature': self.get_outdoor_temperature(),
            'wind_speed': self.get_wind_speed(),
            'rain': self.get_rain(),
            'light_south': self.get_light_south(),
            'light_east': self.get_light_east(),
            'light_west': self.get_light_west(),
        }
        return {k: v for k, v in weather.items() if v is not None}


# Example usage
if __name__ == "__main__":
    client = DucoModbusClient(
        host="10.141.1.64",
        register_offset=0
    )

    if client.connect():
        print("✓ Connected to DucoBox")

        # System information
        print("\n=== System Information ===")
        info = client.get_system_info()
        for key, value in info.items():
            print(f"  {key}: {value}")

        # Temperatures (if DucoBox Energy)
        print("\n=== Temperatures ===")
        temps = client.get_temperatures()
        for key, value in temps.items():
            print(f"  {key}: {value:.1f}°C")

        # Network scan with detailed info
        print("\n=== Network Scan (Detailed) ===")
        detailed_network_info = client.scan_network_detailed()
        for node_id, node_info in detailed_network_info.items():
            print(f"\nNode {node_id}:")
            for key, value in node_info.items():
                print(f"  {key}: {value}")

    else:
        print("Failed to connect to DucoBox")
        exit(1)
