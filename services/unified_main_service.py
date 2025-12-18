"""
Unified Smart Home Service
Manages both Niko Home Control (event-driven) and Duco ventilation (polling)
"""
import logging
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv

from modules.duco import DucoModbusClient
from services.duco_polling_service import DucoPollingService

from datastructures.niko import NikoDataConverter, BaseDevice as NikoBaseDevice, Location
from modules.niko_home_control import NikoHomeControlAPI
from core.publisher import UnifiedRedisPublisher


class NikoCallbackService:
    """
    Event-driven service for Niko Home Control.
    Listens to MQTT callbacks and automatically updates Redis.
    """

    def __init__(
            self,
            niko_api: NikoHomeControlAPI,
            redis_publisher: UnifiedRedisPublisher,
            logger: Optional[logging.Logger] = None
    ):
        """
        Initialize Niko callback service.

        Args:
            niko_api: Authenticated NikoHomeControlAPI instance
            redis_publisher: UnifiedRedisPublisher instance
            logger: Optional logger
        """
        self.niko_api = niko_api
        self.redis_publisher = redis_publisher
        self.logger = logger or logging.getLogger(__name__)
        self.running = False
        self.devices_cache: Dict[str, NikoBaseDevice] = {}
        self.locations_cache: Dict[str, Location] = {}

        # Statistics
        self.stats = {
            'device_updates': 0,
            'location_updates': 0,
            'errors': 0,
            'last_update': None
        }

    def start(self):
        """Start the callback service"""
        if self.running:
            self.logger.warning("Service already running")
            return

        self.running = True
        self.logger.info("Starting Niko callback service")

        # Register callbacks
        self.niko_api.register_device_callback(self._on_device_event)
        self.niko_api.register_location_callback(self._on_location_event)
        self.niko_api.register_notification_callback(self._on_notification_event)
        self.niko_api.register_system_callback(self._on_system_event)
        self.niko_api.register_error_callback(self._on_error_event)

        # Initial sync - load all devices and locations
        self._initial_sync()

        self.logger.info("Niko callback service started successfully")

    def stop(self):
        """Stop the callback service"""
        if not self.running:
            return

        self.running = False
        self.logger.info("Stopping Niko callback service")
        # Callbacks remain registered but we just mark as not running

    def _initial_sync(self):
        """Perform initial sync of all devices and locations"""
        self.logger.info("Performing initial sync...")

        try:
            # Sync devices
            devices_data = self.niko_api.list_devices()
            self.logger.info(f"Initial sync: Found {len(devices_data)} devices")

            device_objects = []
            for device_data in devices_data:
                try:
                    device = NikoDataConverter.create_device(device_data)
                    self.devices_cache[device.uuid] = device
                    device_objects.append(device)
                except Exception as e:
                    self.logger.error(f"Error converting device: {e}")

            # Publish all devices
            count = self.redis_publisher.publish_all_niko_devices(device_objects)
            self.logger.info(f"Published {count} devices to Redis")

            # Sync locations
            locations_data = self.niko_api.list_locations()
            self.logger.info(f"Initial sync: Found {len(locations_data)} locations")

            for location_data in locations_data:
                try:
                    location = Location(
                        uuid=location_data.get("Uuid", ""),
                        name=location_data.get("Name", ""),
                        index=int(location_data.get("Index", 0)),
                        icon=location_data.get("Icon", "general")
                    )
                    self.locations_cache[location.uuid] = location
                    self.redis_publisher.publish_niko_location(location)
                except Exception as e:
                    self.logger.error(f"Error converting location: {e}")

            self.logger.info("Initial sync complete")

        except Exception as e:
            self.logger.error(f"Error during initial sync: {e}", exc_info=True)

    def _on_device_event(self, event_data: Dict[str, Any]):
        """
        Handle device events from Niko MQTT.

        Event format:
        {
            'method': 'devices.changed' or 'devices.added' or 'devices.removed',
            'devices': [list of device dicts],
            'topic': 'hobby/control/devices/evt'
        }
        """
        if not self.running:
            return

        method = event_data.get('method', '')
        devices = event_data.get('devices', [])

        self.logger.debug(f"Received device event: {method}, {len(devices)} devices")

        try:
            for device_data in devices:
                device_uuid = device_data.get('Uuid')

                if method == 'devices.removed':
                    # Remove from cache and Redis
                    if device_uuid in self.devices_cache:
                        del self.devices_cache[device_uuid]
                    self.redis_publisher.delete_device('niko_device', device_uuid=device_uuid)
                    self.logger.info(f"Removed device {device_uuid}")

                else:
                    # Add or update device
                    try:
                        device = NikoDataConverter.create_device(device_data)
                        self.devices_cache[device.uuid] = device

                        # Publish to Redis
                        success = self.redis_publisher.publish_niko_device(device)

                        if success:
                            self.stats['device_updates'] += 1
                            self.stats['last_update'] = datetime.now().isoformat()

                            action = "Updated" if method == 'devices.changed' else "Added"
                            self.logger.info(
                                f"{action} device: {device.name} ({device.device_type}) "
                                f"UUID: {device.uuid}"
                            )

                            # Log property changes if it's an update
                            if method == 'devices.changed' and device.properties:
                                self.logger.debug(f"Properties: {device.properties}")
                        else:
                            self.logger.error(f"Failed to publish device {device.uuid}")

                    except Exception as e:
                        self.logger.error(f"Error processing device {device_uuid}: {e}")
                        self.stats['errors'] += 1

        except Exception as e:
            self.logger.error(f"Error handling device event: {e}", exc_info=True)
            self.stats['errors'] += 1

    def _on_location_event(self, event_data: Dict[str, Any]):
        """Handle location events from Niko MQTT"""
        if not self.running:
            return

        method = event_data.get('method', '')
        locations = event_data.get('locations', [])

        self.logger.debug(f"Received location event: {method}, {len(locations)} locations")

        try:
            for location_data in locations:
                location_uuid = location_data.get('Uuid')

                if method == 'locations.removed':
                    if location_uuid in self.locations_cache:
                        del self.locations_cache[location_uuid]
                    self.redis_publisher.delete_device('niko_location', location_uuid=location_uuid)
                    self.logger.info(f"Removed location {location_uuid}")

                else:
                    try:
                        location = Location(
                            uuid=location_uuid,
                            name=location_data.get("Name", ""),
                            index=int(location_data.get("Index", 0)),
                            icon=location_data.get("Icon", "general")
                        )
                        self.locations_cache[location.uuid] = location

                        success = self.redis_publisher.publish_niko_location(location)

                        if success:
                            self.stats['location_updates'] += 1
                            action = "Updated" if method == 'locations.changed' else "Added"
                            self.logger.info(f"{action} location: {location.name}")

                    except Exception as e:
                        self.logger.error(f"Error processing location {location_uuid}: {e}")
                        self.stats['errors'] += 1

        except Exception as e:
            self.logger.error(f"Error handling location event: {e}", exc_info=True)
            self.stats['errors'] += 1

    def _on_notification_event(self, event_data: Dict[str, Any]):
        """Handle notification events"""
        if not self.running:
            return

        method = event_data.get('method', '')
        notifications = event_data.get('notifications', [])

        self.logger.info(f"Notification event: {method}, {len(notifications)} notifications")

        # Could store notifications in Redis if needed
        for notification in notifications:
            self.logger.info(f"Notification: {notification.get('Text', 'N/A')}")

    def _on_system_event(self, event_data: Dict[str, Any]):
        """Handle system events (time, system info)"""
        if not self.running:
            return

        method = event_data.get('method', '')

        if method == 'time.published':
            time_info = event_data.get('time_info', {})
            self.logger.debug(f"Time update: {time_info}")

        elif method == 'systeminfo.published':
            system_info = event_data.get('system_info', {})
            self.logger.debug(f"System info update: {system_info}")

    def _on_error_event(self, event_data: Dict[str, Any]):
        """Handle error events from MQTT"""
        error_code = event_data.get('error_code')
        error_message = event_data.get('error_message')
        method = event_data.get('method')

        self.logger.error(
            f"MQTT Error - Method: {method}, Code: {error_code}, "
            f"Message: {error_message}"
        )
        self.stats['errors'] += 1

    def get_statistics(self) -> Dict[str, Any]:
        """Get service statistics"""
        return {
            **self.stats,
            'running': self.running,
            'cached_devices': len(self.devices_cache),
            'cached_locations': len(self.locations_cache)
        }

    def get_device(self, device_uuid: str) -> Optional[NikoBaseDevice]:
        """Get device from cache"""
        return self.devices_cache.get(device_uuid)

    def get_location(self, location_uuid: str) -> Optional[Location]:
        """Get location from cache"""
        return self.locations_cache.get(location_uuid)

    def list_devices(self) -> List[NikoBaseDevice]:
        """List all cached devices"""
        return list(self.devices_cache.values())

    def list_locations(self) -> List[Location]:
        """List all cached locations"""
        return list(self.locations_cache.values())

    def get_devices_by_location(self) -> Dict[str, List[NikoBaseDevice]]:
        """Get devices organized by location"""
        result = {}

        # Initialize with all locations
        for location in self.locations_cache.values():
            result[location.name] = []

        # Add devices to their locations
        for device in self.devices_cache.values():
            location_name = device.location_name or "Unknown"
            if location_name not in result:
                result[location_name] = []
            result[location_name].append(device)

        return result

    def get_devices_by_type(self) -> Dict[str, List[NikoBaseDevice]]:
        """Get devices organized by type"""
        result = {}

        for device in self.devices_cache.values():
            device_type = device.device_type
            if device_type not in result:
                result[device_type] = []
            result[device_type].append(device)

        return result

class SmartHomeService:
    """
    Unified service managing both Niko and Duco systems.
    """

    def __init__(
            self,
            # Redis config
            redis_host: str = 'localhost',
            redis_port: int = 6379,
            redis_db: int = 0,
            
            # Niko config
            niko_host: Optional[str] = None,
            niko_username: Optional[str] = None,
            niko_jwt_token: Optional[str] = None,
            niko_ca_cert: Optional[str] = None,
            
            # Duco config
            duco_host: Optional[str] = None,
            duco_port: int = 502,
            duco_register_offset: int = 0,
            duco_poll_interval: int = 30,
            
            # General config
            key_prefix: str = 'smarthome',
            enable_niko: bool = True,
            enable_duco: bool = True
    ):
        """
        Initialize smart home service.

        Args:
            redis_host: Redis server hostname
            redis_port: Redis server port
            redis_db: Redis database number
            niko_host: Niko controller hostname
            niko_username: Niko MQTT username
            niko_jwt_token: Niko JWT token
            niko_ca_cert: Path to Niko CA certificate
            duco_host: Duco controller hostname
            duco_port: Duco Modbus port
            duco_register_offset: Duco register offset
            duco_poll_interval: Duco polling interval (seconds)
            key_prefix: Redis key prefix
            enable_niko: Enable Niko service
            enable_duco: Enable Duco service
        """
        self.logger = logging.getLogger(__name__)
        self.running = False
        
        # Initialize Redis publisher
        self.redis_publisher = UnifiedRedisPublisher(
            redis_host=redis_host,
            redis_port=redis_port,
            redis_db=redis_db,
            key_prefix=key_prefix,
            enable_pubsub=True,
            logger=self.logger
        )
        
        # Initialize services
        self.niko_service: Optional[NikoCallbackService] = None
        self.duco_service: Optional[DucoPollingService] = None
        
        # Initialize Niko if enabled
        if enable_niko and niko_host and niko_username and niko_jwt_token:
            try:
                self.logger.info("Initializing Niko Home Control...")
                self.niko_api = NikoHomeControlAPI(
                    host=niko_host,
                    username=niko_username,
                    jwt_token=niko_jwt_token,
                    ca_cert_path=niko_ca_cert
                )
                
                self.niko_service = NikoCallbackService(
                    niko_api=self.niko_api,
                    redis_publisher=self.redis_publisher,
                    logger=self.logger
                )
                self.logger.info("Niko Home Control initialized")
                
            except Exception as e:
                self.logger.error(f"Failed to initialize Niko: {e}", exc_info=True)
        else:
            self.logger.info("Niko Home Control disabled")
        
        # Initialize Duco if enabled
        if enable_duco and duco_host:
            try:
                self.logger.info("Initializing Duco ventilation...")
                self.duco_client = DucoModbusClient(
                    host=duco_host,
                    port=duco_port,
                    register_offset=duco_register_offset
                )
                
                if self.duco_client.connect():
                    self.duco_service = DucoPollingService(
                        duco_client=self.duco_client,
                        redis_publisher=self.redis_publisher,
                        poll_interval=duco_poll_interval,
                        logger=self.logger
                    )
                    self.logger.info("Duco ventilation initialized")
                else:
                    self.logger.error("Failed to connect to Duco")
                    
            except Exception as e:
                self.logger.error(f"Failed to initialize Duco: {e}", exc_info=True)
        else:
            self.logger.info("Duco ventilation disabled")

    def start(self):
        """Start all enabled services"""
        if self.running:
            self.logger.warning("Service already running")
            return
        
        self.running = True
        self.logger.info("Starting Smart Home Service...")
        
        # Start Niko service
        if self.niko_service:
            try:
                self.niko_service.start()
                self.logger.info("✓ Niko service started")
            except Exception as e:
                self.logger.error(f"Failed to start Niko service: {e}")
        
        # Start Duco service
        if self.duco_service:
            try:
                self.duco_service.start()
                self.logger.info("✓ Duco service started")
            except Exception as e:
                self.logger.error(f"Failed to start Duco service: {e}")
        
        self.logger.info("Smart Home Service started successfully")

    def stop(self):
        """Stop all services gracefully"""
        if not self.running:
            return
        
        self.running = False
        self.logger.info("Stopping Smart Home Service...")
        
        # Stop Niko service
        if self.niko_service:
            try:
                self.niko_service.stop()
                self.niko_api.close()
                self.logger.info("✓ Niko service stopped")
            except Exception as e:
                self.logger.error(f"Error stopping Niko service: {e}")
        
        # Stop Duco service
        if self.duco_service:
            try:
                self.duco_service.stop()
                self.duco_client.disconnect()
                self.logger.info("✓ Duco service stopped")
            except Exception as e:
                self.logger.error(f"Error stopping Duco service: {e}")
        
        self.logger.info("Smart Home Service stopped")

    def get_statistics(self) -> dict:
        """Get statistics from all services"""
        stats = {
            'running': self.running,
            'niko': None,
            'duco': None
        }
        
        if self.niko_service:
            stats['niko'] = self.niko_service.get_statistics()
        
        if self.duco_service:
            stats['duco'] = self.duco_service.get_statistics()
        
        return stats

    def print_status(self):
        """Print current status of all services"""
        print("\n" + "=" * 60)
        print("SMART HOME SERVICE STATUS")
        print("=" * 60)
        
        stats = self.get_statistics()
        
        print(f"\nService Running: {stats['running']}")
        
        # Niko status
        if stats['niko']:
            print("\n--- Niko Home Control ---")
            print(f"Running: {stats['niko']['running']}")
            print(f"Devices cached: {stats['niko']['cached_devices']}")
            print(f"Locations cached: {stats['niko']['cached_locations']}")
            print(f"Device updates: {stats['niko']['device_updates']}")
            print(f"Location updates: {stats['niko']['location_updates']}")
            print(f"Errors: {stats['niko']['errors']}")
            if stats['niko']['last_update']:
                print(f"Last update: {stats['niko']['last_update']}")
        else:
            print("\n--- Niko Home Control: DISABLED ---")
        
        # Duco status
        if stats['duco']:
            print("\n--- Duco Ventilation ---")
            print(f"Running: {stats['duco']['running']}")
            print(f"Active nodes: {len(stats['duco']['active_nodes'])}")
            print(f"Poll interval: {stats['duco']['poll_interval']}s")
            print(f"Total polls: {stats['duco']['polls']}")
            print(f"System updates: {stats['duco']['system_updates']}")
            print(f"Node updates: {stats['duco']['node_updates']}")
            print(f"Errors: {stats['duco']['errors']}")
            if stats['duco']['last_poll']:
                print(f"Last poll: {stats['duco']['last_poll']}")
            
            # Show current system state
            if self.duco_service:
                system = self.duco_service.get_system_summary()
                if system:
                    print("\nCurrent System State:")
                    print(f"  Mode: {system.get('ventilation_mode_name', 'N/A')}")
                    print(f"  Humidity: {system.get('humidity_level', 'N/A')}%")
                    print(f"  CO2: {system.get('co2_level', 'N/A')} ppm")
                    if system.get('temperature_oda'):
                        print(f"  Outdoor temp: {system.get('temperature_oda')}°C")
        else:
            print("\n--- Duco Ventilation: DISABLED ---")
        
        print("\n" + "=" * 60 + "\n")


def setup_logging(log_level: str = 'INFO', log_file: Optional[str] = None):
    """Setup logging configuration"""
    handlers = [logging.StreamHandler()]
    
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )


def main():
    """Main entry point"""
    # Load environment variables
    load_dotenv()
    
    # Setup logging
    log_level = os.getenv('LOG_LEVEL', 'INFO')
    log_file = os.getenv('LOG_FILE')
    setup_logging(log_level, log_file)
    
    logger = logging.getLogger(__name__)
    
    # Initialize service with environment variables
    service = SmartHomeService(
        # Redis config
        redis_host=os.getenv('REDIS_HOST', 'localhost'),
        redis_port=int(os.getenv('REDIS_PORT', 6379)),
        redis_db=int(os.getenv('REDIS_DB', 0)),
        
        # Niko config
        niko_host=os.getenv('NIKO_HOSTNAME'),
        niko_username=os.getenv('NIKO_USERNAME', 'hobby'),
        niko_jwt_token=os.getenv('JWT_TOKEN'),
        niko_ca_cert=os.getenv('NIKO_CA_CERT'),
        
        # Duco config
        duco_host=os.getenv('DUCO_HOST'),
        duco_port=int(os.getenv('DUCO_PORT', 502)),
        duco_register_offset=int(os.getenv('DUCO_REGISTER_OFFSET', 0)),
        duco_poll_interval=int(os.getenv('DUCO_POLL_INTERVAL', 30)),
        
        # General config
        key_prefix=os.getenv('REDIS_KEY_PREFIX', 'smarthome'),
        enable_niko=os.getenv('ENABLE_NIKO', 'true').lower() == 'true',
        enable_duco=os.getenv('ENABLE_DUCO', 'true').lower() == 'true'
    )
    
    # Setup signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        service.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start service
    service.start()
    
    print("\n" + "=" * 60)
    print("SMART HOME SERVICE")
    print("=" * 60)
    print("\nService started successfully!")
    print("Press Ctrl+C to stop.\n")
    
    # Status update interval
    status_interval = 30  # seconds
    
    try:
        while True:
            time.sleep(status_interval)
            service.print_status()
            
    except KeyboardInterrupt:
        print("\nShutting down...")
        service.stop()
        print("Service stopped.")


if __name__ == "__main__":
    main()
