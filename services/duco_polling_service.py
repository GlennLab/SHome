"""
Duco Ventilation Polling Service
Periodically polls Duco system and publishes to Redis
"""

import logging
import time
import threading
from typing import Optional, Dict, List, Any
from datetime import datetime

from datastructures.duco import (
    DucoBoxSystem, DucoNode, NodeType, VentilationMode,
)
from modules.duco import DucoModbusClient
from core.publisher import UnifiedRedisPublisher


class DucoPollingService:
    """
    Polling service for Duco ventilation system.
    Reads system and node data via Modbus TCP and publishes to Redis.
    """

    def __init__(
            self,
            duco_client: DucoModbusClient,
            redis_publisher: UnifiedRedisPublisher,
            poll_interval: int = 30,
            logger: Optional[logging.Logger] = None
    ):
        """
        Initialize Duco polling service.

        Args:
            duco_client: Connected DucoModbusClient instance
            redis_publisher: UnifiedRedisPublisher instance
            poll_interval: Polling interval in seconds (default: 30)
            logger: Optional logger
        """
        self.duco_client = duco_client
        self.redis_publisher = redis_publisher
        self.poll_interval = poll_interval
        self.logger = logger or logging.getLogger(__name__)

        self.running = False
        self.thread: Optional[threading.Thread] = None

        # Cache for node IDs
        self.active_nodes: List[int] = []
        self.last_network_scan: Optional[datetime] = None
        self.network_scan_interval = 300  # Re-scan network every 5 minutes

        # Statistics
        self.stats = {
            'polls': 0,
            'system_updates': 0,
            'node_updates': 0,
            'errors': 0,
            'last_poll': None,
            'last_error': None
        }

    def start(self):
        """Start the polling service"""
        if self.running:
            self.logger.warning("Service already running")
            return

        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        self.logger.info(f"Duco polling service started (interval: {self.poll_interval}s)")

    def stop(self):
        """Stop the polling service"""
        if not self.running:
            return

        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        self.logger.info("Duco polling service stopped")

    def _run_loop(self):
        """Main polling loop"""
        while self.running:
            try:
                # Check if we need to rescan the network
                should_rescan = (
                    self.last_network_scan is None or
                    (datetime.now() - self.last_network_scan).total_seconds() > self.network_scan_interval
                )

                if should_rescan:
                    self._scan_network()

                # Poll system and nodes
                self._poll_system()
                self._poll_nodes()

                # Update statistics
                self.stats['polls'] += 1
                self.stats['last_poll'] = datetime.now().isoformat()

            except Exception as e:
                self.logger.error(f"Error in polling loop: {e}", exc_info=True)
                self.stats['errors'] += 1
                self.stats['last_error'] = {
                    'time': datetime.now().isoformat(),
                    'error': str(e)
                }

            # Wait for next poll (check running flag every second for quick shutdown)
            for _ in range(self.poll_interval):
                if not self.running:
                    break
                time.sleep(1)

    def _scan_network(self):
        """Scan for active nodes in the network"""
        try:
            self.logger.info("Scanning Duco network for active nodes...")
            self.active_nodes = self.duco_client.get_active_nodes()
            self.last_network_scan = datetime.now()
            self.logger.info(f"Found {len(self.active_nodes)} active nodes: {self.active_nodes}")
        except Exception as e:
            self.logger.error(f"Error scanning network: {e}")

    def _poll_system(self):
        """Poll DucoBox system data"""
        try:
            # Get system information
            system_info = self.duco_client.get_system_info()

            if not system_info:
                self.logger.warning("No system info retrieved")
                return

            # Get temperatures (DucoBox Energy only)
            temps = self.duco_client.get_temperatures()

            # Get filter info
            filter_remaining = self.duco_client.get_filter_remaining_time()
            filter_status = self.duco_client.get_filter_status()

            # Create DucoBoxSystem object
            ducobox = DucoBoxSystem(
                device_id="ducobox_main",
                node_type=NodeType.DUCOBOX,

                # System status
                status=system_info.get('ventilation_status').value if system_info.get('ventilation_status') else None,
                ventilation_mode=system_info.get('ventilation_mode'),

                # Air quality
                humidity_level=system_info.get('humidity'),
                co2_level=system_info.get('co2'),
                air_quality_rh=system_info.get('air_quality_rh'),
                air_quality_co2=system_info.get('air_quality_co2'),

                # Filter
                remaining_filter_time=filter_remaining,
                filter_status=filter_status.value if filter_status else None,

                # Temperatures
                temperature_oda=temps.get('outdoor_air'),
                temperature_sup=temps.get('supply_air'),
                temperature_eta=temps.get('extract_air'),
                temperature_eha=temps.get('exhaust_air'),

                # System info
                api_version=system_info.get('api_version'),
                remaining_write_actions=system_info.get('remaining_write_actions')
            )

            # Publish to Redis
            success = self.redis_publisher.publish_ducobox(ducobox)

            if success:
                self.stats['system_updates'] += 1
                self.logger.debug(
                    f"Published DucoBox: Mode={ducobox.ventilation_mode}, "
                    f"Humidity={ducobox.humidity_level}%, CO2={ducobox.co2_level}ppm"
                )
            else:
                self.logger.error("Failed to publish DucoBox data")

        except Exception as e:
            self.logger.error(f"Error polling system: {e}", exc_info=True)
            self.stats['errors'] += 1

    def _poll_nodes(self):
        """Poll all active nodes"""
        if not self.active_nodes:
            return

        nodes_to_publish = []

        for node_id in self.active_nodes:
            try:
                # Get node type first
                node_type = self.duco_client.get_node_type(node_id)
                if not node_type:
                    self.logger.debug(f"Could not determine type for node {node_id}")
                    continue

                # Skip the DucoBox itself (node_id 1 is usually the box)
                # It's already published in _poll_system()
                if node_type == NodeType.DUCOBOX:
                    self.logger.debug(f"Skipping node {node_id} (is DucoBox system)")
                    continue

                # Get node info
                node_info = self.duco_client.get_node_info(node_id)

                if not node_info or 'type' not in node_info:
                    self.logger.debug(f"No valid info for node {node_id}")
                    continue

                # Create DucoNode object only with non-null values
                node = DucoNode(
                    device_id=f"node_{node_id}",
                    node_id=node_id,
                    node_type=node_type,
                    node_type_name=node_type.name,

                    # Parameters from node_info (only if present)
                    remaining_time_current_mode=node_info.get('remaining_time_seconds'),
                    flow_rate=node_info.get('flow_level_percent'),
                    air_quality_rh=node_info.get('air_quality_rh_percent'),
                    air_quality_co2=node_info.get('air_quality_co2_percent'),
                    humidity_level=node_info.get('humidity_percent'),
                    co2_level=node_info.get('co2_ppm')
                )

                # Only publish if node has at least some data
                has_data = any([
                    node.remaining_time_current_mode is not None,
                    node.flow_rate is not None,
                    node.air_quality_rh is not None,
                    node.air_quality_co2 is not None,
                    node.humidity_level is not None,
                    node.co2_level is not None
                ])

                if has_data:
                    nodes_to_publish.append(node)
                    self.logger.debug(
                        f"Node {node_id} ({node_type.name}): "
                        f"humidity={node.humidity_level}, co2={node.co2_level}"
                    )
                else:
                    self.logger.debug(f"Node {node_id} ({node_type.name}) has no data")

            except Exception as e:
                self.logger.error(f"Error polling node {node_id}: {e}", exc_info=True)
                self.stats['errors'] += 1

        # Publish all nodes in batch
        if nodes_to_publish:
            count = self.redis_publisher.publish_duco_network(nodes_to_publish)
            self.stats['node_updates'] += count
            self.logger.info(f"Published {count} nodes to Redis")
        else:
            self.logger.warning("No nodes with data to publish")

    def poll_now(self):
        """Trigger an immediate poll (in addition to scheduled polls)"""
        if not self.running:
            self.logger.warning("Service not running")
            return

        try:
            self.logger.info("Manual poll triggered")
            self._poll_system()
            self._poll_nodes()
        except Exception as e:
            self.logger.error(f"Error in manual poll: {e}")

    def set_ventilation_mode(self, mode: VentilationMode) -> bool:
        """
        Set ventilation mode and trigger immediate poll.

        Args:
            mode: VentilationMode to set

        Returns:
            True if successful
        """
        try:
            success = self.duco_client.set_ventilation_mode(mode)

            if success:
                self.logger.info(f"Set ventilation mode to {mode.name}")
                # Poll immediately to update Redis with new state
                time.sleep(1)  # Give device time to update
                self.poll_now()
                return True
            else:
                self.logger.error(f"Failed to set ventilation mode to {mode.name}")
                return False

        except Exception as e:
            self.logger.error(f"Error setting ventilation mode: {e}")
            return False

    def identify_node(self, node_id: int, duration: float = 3.0) -> bool:
        """
        Identify a node by turning on its blue light.

        Args:
            node_id: Node ID to identify
            duration: How long to keep identification on (seconds)

        Returns:
            True if successful
        """
        try:
            success = self.duco_client.identify_node(node_id, enable=True, force=True)

            if success:
                self.logger.info(f"Identifying node {node_id} for {duration} seconds")
                time.sleep(duration)
                self.duco_client.identify_node(node_id, enable=False, force=True)
                return True
            else:
                self.logger.error(f"Failed to identify node {node_id}")
                return False

        except Exception as e:
            self.logger.error(f"Error identifying node: {e}")
            return False

    def get_statistics(self) -> Dict[str, Any]:
        """Get service statistics"""
        return {
            **self.stats,
            'running': self.running,
            'active_nodes': self.active_nodes,
            'poll_interval': self.poll_interval
        }

    def get_system_summary(self) -> Optional[Dict[str, Any]]:
        """Get current system summary from Redis"""
        return self.redis_publisher.get_ducobox()

    def get_node_summary(self, node_id: int) -> Optional[Dict[str, Any]]:
        """Get current node summary from Redis"""
        return self.redis_publisher.get_duco_node(node_id)

    def get_all_nodes_summary(self) -> List[Dict[str, Any]]:
        """Get all nodes from Redis"""
        return self.redis_publisher.get_all_duco_nodes()


# ============================================================================
# Usage Example
# ============================================================================

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Initialize Duco Modbus client
    duco_client = DucoModbusClient(
        host="10.141.1.64",
        register_offset=0
    )

    if not duco_client.connect():
        print("Failed to connect to DucoBox")
        exit(1)

    print("Connected to DucoBox")

    # Initialize Redis publisher
    redis_publisher = UnifiedRedisPublisher(
        redis_host='localhost',
        redis_port=6379,
        key_prefix='smarthome',
        enable_pubsub=True
    )

    # Initialize polling service
    duco_service = DucoPollingService(
        duco_client=duco_client,
        redis_publisher=redis_publisher,
        poll_interval=30  # Poll every 30 seconds
    )

    # Start service
    duco_service.start()

    print("Duco polling service started.")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            time.sleep(10)
            # Print statistics every 10 seconds
            stats = duco_service.get_statistics()
            print(f"\nStats: Polls={stats['polls']}, "
                  f"System Updates={stats['system_updates']}, "
                  f"Node Updates={stats['node_updates']}, "
                  f"Errors={stats['errors']}")

            # Print current system status
            system = duco_service.get_system_summary()
            if system:
                print(f"System: Mode={system.get('ventilation_mode_name')}, "
                      f"Humidity={system.get('humidity_level')}%, "
                      f"CO2={system.get('co2_level')}ppm")

    except KeyboardInterrupt:
        print("\nStopping service...")
        duco_service.stop()
        duco_client.disconnect()
        print("Service stopped.")