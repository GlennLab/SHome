"""
Complete Redis Publisher Implementation
Handles both Niko and Duco devices with proper serialization
"""

import json
import logging
from datetime import datetime
from typing import Optional, Type, TypeVar, List, Dict, Any

import redis

from datastructures.duco import (
    BaseDevice, DucoBoxSystem, DucoNode,
    serialize_device, deserialize_device
)
from datastructures.niko import (
    NikoDataConverter, BaseDevice as NikoBaseDevice,
    Location, RedisPublisher as NikoRedisPublisher
)

T = TypeVar('T', bound=BaseDevice)


class UnifiedRedisPublisher:
    """
    Unified Redis publisher for both Niko and Duco devices.
    Provides automatic serialization, validation, and retrieval.
    """

    # Key pattern templates
    KEY_PATTERNS = {
        # DUCO
        'ducobox_system': 'duco:system:{device_id}',
        'duco_node': 'duco:node:{node_id}',
        'duco_network': 'duco:network:nodes',

        # Niko
        'niko_device': 'niko:device:{device_uuid}',
        'niko_location': 'niko:location:{location_uuid}',
        'niko_all_devices': 'niko:devices:all',
        'niko_all_locations': 'niko:locations:all',
    }

    # Default TTLs (seconds)
    DEFAULT_TTLS = {
        'ducobox_system': 300,  # 5 minutes
        'duco_node': 300,  # 5 minutes
        'niko_device': None,  # No expiration (event-driven updates)
        'niko_location': None,  # No expiration
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
        Initialize unified Redis publisher.

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

    def _add_timestamp(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Add timestamp to data if not present"""
        if 'timestamp' not in data or data['timestamp'] is None:
            data['timestamp'] = datetime.now().isoformat()
        return data

    def publish_device(
            self,
            device: Any,
            pattern_name: str,
            ttl: Optional[int] = None,
            **key_params
    ) -> bool:
        """
        Publish a device dataclass to Redis.

        Args:
            device: Device dataclass instance (Niko or Duco)
            pattern_name: Key pattern name from KEY_PATTERNS
            ttl: Time to live (None = use default, 0 = no expiration)
            **key_params: Parameters for key pattern

        Returns:
            True if successful
        """
        try:
            # Serialize device
            if hasattr(device, 'to_dict'):
                data = device.to_dict()
            else:
                data = serialize_device(device)

            data = self._add_timestamp(data)
            serialized = json.dumps(data, default=str)

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
            pattern_name: str,
            **key_params
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve a device from Redis.

        Args:
            pattern_name: Key pattern name
            **key_params: Parameters for key pattern

        Returns:
            Deserialized device data or None
        """
        try:
            key = self._build_key(pattern_name, **key_params)
            data = self.redis_client.get(key)

            if data:
                return json.loads(data)

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

    def list_keys(self, pattern_name: str, **wildcards) -> List[str]:
        """
        List all keys matching a pattern.

        Args:
            pattern_name: Key pattern name
            **wildcards: Wildcard values (use '*' for wildcards)

        Returns:
            List of matching keys
        """
        try:
            pattern = self._build_key(pattern_name, **wildcards)
            keys = self.redis_client.keys(pattern)
            return [k for k in keys]
        except Exception as e:
            self.logger.error(f"Error listing keys: {e}")
            return []

    # ========================================================================
    # DUCO Methods
    # ========================================================================

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

    def get_ducobox(self, device_id: str = "ducobox_main") -> Optional[Dict]:
        """Get DucoBox system data"""
        return self.get_device('ducobox_system', device_id=device_id)

    def get_duco_node(self, node_id: int) -> Optional[Dict]:
        """Get DUCO node data"""
        return self.get_device('duco_node', node_id=node_id)

    def publish_duco_network(self, nodes: List[DucoNode]) -> int:
        """Publish multiple DUCO nodes efficiently"""
        success_count = 0
        pipe = self.redis_client.pipeline()

        for node in nodes:
            try:
                data = serialize_device(node)
                data = self._add_timestamp(data)
                serialized = json.dumps(data, default=str)
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

    def get_all_duco_nodes(self) -> List[Dict]:
        """Get all DUCO nodes"""
        keys = self.list_keys('duco_node', node_id='*')
        nodes = []
        for key in keys:
            try:
                data = self.redis_client.get(key)
                if data:
                    nodes.append(json.loads(data))
            except Exception as e:
                self.logger.error(f"Error loading node from {key}: {e}")
        return nodes

    # ========================================================================
    # Niko Methods
    # ========================================================================

    def publish_niko_device(self, device: NikoBaseDevice) -> bool:
        """Publish Niko device data"""
        return self.publish_device(
            device,
            'niko_device',
            device_uuid=device.uuid
        )

    def publish_niko_location(self, location: Location) -> bool:
        """Publish Niko location data"""
        return self.publish_device(
            location,
            'niko_location',
            location_uuid=location.uuid
        )

    def get_niko_device(self, device_uuid: str) -> Optional[Dict]:
        """Get Niko device data"""
        return self.get_device('niko_device', device_uuid=device_uuid)

    def get_niko_location(self, location_uuid: str) -> Optional[Dict]:
        """Get Niko location data"""
        return self.get_device('niko_location', location_uuid=location_uuid)

    def publish_all_niko_devices(self, devices: List[NikoBaseDevice]) -> int:
        """Publish multiple Niko devices efficiently"""
        success_count = 0
        pipe = self.redis_client.pipeline()

        for device in devices:
            try:
                data = device.to_dict()
                data = self._add_timestamp(data)
                serialized = json.dumps(data, default=str)
                key = self._build_key('niko_device', device_uuid=device.uuid)

                pipe.set(key, serialized)
                success_count += 1
            except Exception as e:
                self.logger.error(f"Error preparing device {device.uuid}: {e}")

        try:
            pipe.execute()
        except Exception as e:
            self.logger.error(f"Error executing pipeline: {e}")
            return 0

        return success_count

    def get_all_niko_devices(self) -> List[Dict]:
        """Get all Niko devices"""
        keys = self.list_keys('niko_device', device_uuid='*')
        devices = []
        for key in keys:
            try:
                data = self.redis_client.get(key)
                if data:
                    devices.append(json.loads(data))
            except Exception as e:
                self.logger.error(f"Error loading device from {key}: {e}")
        return devices

    def get_all_niko_locations(self) -> List[Dict]:
        """Get all Niko locations"""
        keys = self.list_keys('niko_location', location_uuid='*')
        locations = []
        for key in keys:
            try:
                data = self.redis_client.get(key)
                if data:
                    locations.append(json.loads(data))
            except Exception as e:
                self.logger.error(f"Error loading location from {key}: {e}")
        return locations

    # ========================================================================
    # Batch Operations
    # ========================================================================

    def publish_batch(self, items: List[tuple]) -> Dict[str, int]:
        """
        Publish multiple items in batch.

        Args:
            items: List of tuples (device, pattern_name, key_params_dict)

        Returns:
            Dict with success and failure counts
        """
        results = {'success': 0, 'failed': 0}
        pipe = self.redis_client.pipeline()

        for device, pattern_name, key_params in items:
            try:
                if hasattr(device, 'to_dict'):
                    data = device.to_dict()
                else:
                    data = serialize_device(device)

                data = self._add_timestamp(data)
                serialized = json.dumps(data, default=str)
                key = self._build_key(pattern_name, **key_params)

                ttl = self.DEFAULT_TTLS.get(pattern_name, 0)
                if ttl:
                    pipe.setex(key, ttl, serialized)
                else:
                    pipe.set(key, serialized)

            except Exception as e:
                self.logger.error(f"Error preparing item: {e}")
                results['failed'] += 1

        try:
            pipe.execute()
            results['success'] = len(items) - results['failed']
        except Exception as e:
            self.logger.error(f"Error executing batch: {e}")
            results['failed'] = len(items)

        return results


# ============================================================================
# Usage Example
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    # Initialize publisher
    publisher = UnifiedRedisPublisher(
        key_prefix="smarthome",
        enable_pubsub=True
    )

    print("=== Testing DUCO Publishing ===")
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
    print("Published DucoBox")

    # Retrieve it
    retrieved = publisher.get_ducobox()
    print(f"Retrieved: {retrieved}")

    print("\n=== Testing Niko Publishing ===")
    # Would need actual Niko devices here
    print("Complete!")