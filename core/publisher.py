"""
Integration layer between dataclasses and Redis publisher
Provides type-safe publishing and retrieval
"""

import json
import logging
from datetime import datetime
from typing import Optional, Type, TypeVar, List

import redis

from datastructures.duco import (
    BaseDevice, DucoBoxSystem, DucoNode,
    serialize_device, deserialize_device
)
from datastructures.niko import NikoDataConverter, RedisPublisher, UIDataProvider, Location

T = TypeVar('T', bound=BaseDevice)


class TypedRedisPublisher:
    """
    Type-safe Redis publisher that works with dataclasses.
    Provides automatic serialization, validation, and retrieval.
    """

    # Key pattern templates for different device types
    KEY_PATTERNS = {
        # DUCO
        'ducobox_system': 'duco:system:{device_id}',
        'duco_node': 'duco:node:{node_id}',
        'duco_network': 'duco:network:nodes',

        # Niko
        'niko_action': 'niko:action:{device_id}',
        'niko_light': 'niko:light:{device_id}',
        'niko_shutter': 'niko:shutter:{device_id}',
        'niko_thermostat': 'niko:thermostat:{device_id}',

        # Weather
        'weather_current': 'weather:current',
        'weather_forecast': 'weather:forecast:{date}',
        'weather_station': 'weather:station:{device_id}',

        # Sunblind
        'sunblind': 'sunblind:{screen_name}',
        'sunblind_last_update': 'sunblind:last_updated',

        # Climate
        'climate_sensor': 'climate:sensor:{device_id}',
        'climate_room': 'climate:room:{room}',
        'climate_zone': 'climate:zone:{zone}',

        # Energy
        'energy_meter': 'energy:meter:{device_id}',
    }

    # Default TTLs for different data types (seconds)
    DEFAULT_TTLS = {
        'ducobox_system': 300,  # 5 minutes
        'duco_node': 300,  # 5 minutes
        'niko_action': None,  # No expiration
        'weather_current': 3600,  # 1 hour
        'weather_forecast': 10800,  # 3 hours
        'sunblind': None,  # No expiration
        'climate_sensor': 3600,  # 1 hour
        'energy_meter': 300,  # 5 minutes
    }

    def __init__(
            self,
            redis_host: str = 'localhost',
            redis_port: int = 6379,
            redis_db: int = 0,
            key_prefix: Optional[str] = None,
            enable_pubsub: bool = True,
            logger: Optional[logging.Logger] = None
    ):
        """
        Initialize typed Redis publisher.

        Args:
            redis_host: Redis server hostname
            redis_port: Redis server port
            redis_db: Redis database number
            key_prefix: Optional prefix for all keys
            enable_pubsub: Enable pub/sub notifications
            logger: Optional logger instance
        """
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=True
        )
        self.key_prefix = key_prefix
        self.enable_pubsub = enable_pubsub
        self.logger = logger or logging.getLogger(__name__)

    def _build_key(self, pattern_name: str, **kwargs) -> str:
        """Build Redis key from pattern template"""
        pattern = self.KEY_PATTERNS.get(pattern_name, pattern_name)
        key = pattern.format(**kwargs)

        if self.key_prefix:
            key = f"{self.key_prefix}:{key}"

        return key

    def _add_timestamp(self, device: BaseDevice) -> BaseDevice:
        """Add timestamp to device if not present"""
        if not hasattr(device, 'timestamp') or device.timestamp is None:
            device.timestamp = datetime.now().isoformat()
        return device

    def publish_device(
            self,
            device: BaseDevice,
            pattern_name: str,
            ttl: Optional[int] = None,
            **key_params
    ) -> bool:
        """
        Publish a device dataclass to Redis.

        Args:
            device: Device dataclass instance
            pattern_name: Key pattern name from KEY_PATTERNS
            ttl: Time to live (None = use default, 0 = no expiration)
            **key_params: Parameters for key pattern (e.g., device_id="xyz")

        Returns:
            True if successful

        Example:
            light = NikoLight(device_id="light_001", state=True, brightness=75)
            publisher.publish_device(light, 'niko_light', device_id=light.device_id)
        """
        try:
            # Add timestamp
            device = self._add_timestamp(device)

            # Serialize device
            data = serialize_device(device)
            serialized = json.dumps(data)

            # Build key
            key = self._build_key(pattern_name, **key_params)

            # Determine TTL
            if ttl is None:
                ttl = self.DEFAULT_TTLS.get(pattern_name, 0)

            # Store in Redis
            if ttl and ttl > 0:
                self.redis_client.setex(key, ttl, serialized)
            else:
                self.redis_client.set(key, serialized)

            # Publish to pub/sub if enabled
            if self.enable_pubsub:
                channel = f"updates:{key}"
                self.redis_client.publish(channel, serialized)

            self.logger.debug(f"Published {device.__class__.__name__} to {key}")
            return True

        except Exception as e:
            self.logger.error(f"Error publishing device: {e}", exc_info=True)
            return False

    def get_device(
            self,
            device_class: Type[T],
            pattern_name: str,
            **key_params
    ) -> Optional[T]:
        """
        Retrieve and deserialize a device from Redis.

        Args:
            device_class: Device dataclass type
            pattern_name: Key pattern name
            **key_params: Parameters for key pattern

        Returns:
            Deserialized device instance or None

        Example:
            light = publisher.get_device(NikoLight, 'niko_light', device_id="light_001")
        """
        try:
            key = self._build_key(pattern_name, **key_params)
            data = self.redis_client.get(key)

            if data:
                parsed = json.loads(data)
                return deserialize_device(parsed, device_class)

            return None

        except Exception as e:
            self.logger.error(f"Error retrieving device: {e}", exc_info=True)
            return None

    def delete_device(self, pattern_name: str, **key_params) -> bool:
        """Delete a device from Redis"""
        try:
            key = self._build_key(pattern_name, **key_params)
            self.redis_client.delete(key)
            return True
        except Exception as e:
            self.logger.error(f"Error deleting device: {e}")
            return False

    def list_devices(self, pattern_name: str, **wildcards) -> List[str]:
        """
        List all device keys matching a pattern.

        Args:
            pattern_name: Key pattern name
            **wildcards: Wildcard values (use '*' for wildcards)

        Returns:
            List of matching keys

        Example:
            # Get all Niko lights
            keys = publisher.list_devices('niko_light', device_id='*')
        """
        try:
            pattern = self._build_key(pattern_name, **wildcards)
            keys = self.redis_client.keys(pattern)
            return [k for k in keys]
        except Exception as e:
            self.logger.error(f"Error listing devices: {e}")
            return []

    # ========================================================================
    # Convenience methods for specific device types
    # ========================================================================

    # DUCO Methods
    def publish_ducobox(self, ducobox: DucoBoxSystem) -> bool:
        """Publish DucoBox system data"""
        return self.publish_device(
            ducobox,
            'ducobox_system',
            device_id=ducobox.device_id
        )

    def publish_duco_node(self, node: DucoNode) -> bool:
        """Publish DUCO node data"""
        return self.publish_device(
            node,
            'duco_node',
            node_id=node.node_id
        )

    def get_ducobox(self, device_id: str = "ducobox_main") -> Optional[DucoBoxSystem]:
        """Get DucoBox system data"""
        return self.get_device(DucoBoxSystem, 'ducobox_system', device_id=device_id)

    def get_duco_node(self, node_id: int) -> Optional[DucoNode]:
        """Get DUCO node data"""
        return self.get_device(DucoNode, 'duco_node', node_id=node_id)

    def publish_duco_network(self, nodes: List[DucoNode]) -> int:
        """Publish multiple DUCO nodes efficiently"""
        success_count = 0
        pipe = self.redis_client.pipeline()

        for node in nodes:
            try:
                node = self._add_timestamp(node)
                data = serialize_device(node)
                serialized = json.dumps(data)
                key = self._build_key('duco_node', node_id=node.node_id)

                ttl = self.DEFAULT_TTLS.get('duco_node', 0)
                if ttl:
                    pipe.setex(key, ttl, serialized)
                else:
                    pipe.set(key, serialized)

                success_count += 1
            except Exception as e:
                self.logger.error(f"Error preparing node {node.node_id}: {e}")

        try:
            pipe.execute()
        except Exception as e:
            self.logger.error(f"Error executing pipeline: {e}")
            return 0

        return success_count

    # Niko Methods
    def publish_all_devices(niko_api):
        devices_data = niko_api.list_devices()

        for device_data in devices_data:
            # Convert to typed dataclass
            device = NikoDataConverter.create_device(device_data)

            # Publish to Redis
            key = RedisPublisher.publish_device(device, niko_api.redis_client)
            print(f"Published {device.name} to {key}")

            # Get UI summary
            ui_summary = UIDataProvider.get_device_summary(device)
            # Use ui_summary for your UI interface

    # Get locations
    def publish_locations(niko_api):
        locations_data = niko_api.list_locations()

        for location_data in locations_data:
            location = Location(
                uuid=location_data.get("Uuid", ""),
                name=location_data.get("Name", ""),
                index=int(location_data.get("Index", 0)),
                icon=location_data.get("Icon", "general")
            )

            key = RedisPublisher.publish_location(location, niko_api.redis_client)
            print(f"Published location {location.name} to {key}")

# ============================================================================
# Usage Examples
# ============================================================================

if __name__ == "__main__":
    # Initialize publisher
    publisher = TypedRedisPublisher(
        key_prefix="smarthome",
        enable_pubsub=True
    )

    print("=== Publishing DUCO Data ===")
    # Create and publish DucoBox
    from datastructures.duco import VentilationMode, NodeType

    ducobox = DucoBoxSystem(
        device_id="ducobox_main",
        node_type=NodeType.DUCOBOX,
        ventilation_mode=VentilationMode.AUTO,
        humidity_level=45,
        co2_level=650,
        temperature_oda=15.5
    )
    publisher.publish_ducobox(ducobox)

    # Publish multiple nodes
    nodes = [
        DucoNode(
            device_id=f"node_{i}",
            node_id=i,
            node_type=NodeType.HUMIDITY_VALVE,
            humidity_level=45 + i
        )
        for i in range(1, 4)
    ]
    count = publisher.publish_duco_network(nodes)
    print(f"Published {count} nodes")

    print("\n=== Publishing Niko Data ===")
