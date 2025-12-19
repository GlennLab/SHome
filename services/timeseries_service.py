"""
Time Series Storage Service
Stores sensor measurements from Niko and Duco in TimescaleDB with compression

Features:
- Automatic data collection from Redis
- TimescaleDB hypertables for efficient time-series storage
- Automatic compression of old data
- Retention policies
- Downsampling for long-term storage
- Query helpers for retrieving data

Database: TimescaleDB (PostgreSQL extension for time-series data)
"""

import logging
import time
import threading
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
import json

import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql

from core.publisher import UnifiedRedisPublisher


@dataclass
class MeasurementPoint:
    """Single measurement point"""
    timestamp: datetime
    device_id: str
    device_type: str
    location: Optional[str]
    measurement_type: str
    value: float
    unit: str
    metadata: Optional[Dict[str, Any]] = None


class TimeSeriesDatabase:
    """
    TimescaleDB database manager for time-series data.
    Handles schema creation, compression, and retention policies.
    """

    # Table definitions with hypertables
    SCHEMA_SQL = """
    -- Enable TimescaleDB extension
    CREATE EXTENSION IF NOT EXISTS timescaledb;

    -- Main measurements table (hypertable)
    CREATE TABLE IF NOT EXISTS measurements (
        time TIMESTAMPTZ NOT NULL,
        device_id TEXT NOT NULL,
        device_type TEXT NOT NULL,
        location TEXT,
        measurement_type TEXT NOT NULL,
        value DOUBLE PRECISION NOT NULL,
        unit TEXT NOT NULL,
        metadata JSONB
    );

    -- Create hypertable (partitioned by time)
    SELECT create_hypertable('measurements', 'time', 
        chunk_time_interval => INTERVAL '1 day',
        if_not_exists => TRUE
    );

    -- Indexes for fast queries
    CREATE INDEX IF NOT EXISTS idx_measurements_device_time 
        ON measurements (device_id, time DESC);
    CREATE INDEX IF NOT EXISTS idx_measurements_type_time 
        ON measurements (measurement_type, time DESC);
    CREATE INDEX IF NOT EXISTS idx_measurements_location_time 
        ON measurements (location, time DESC);

    -- Continuous aggregate for hourly averages
    CREATE MATERIALIZED VIEW IF NOT EXISTS measurements_hourly
    WITH (timescaledb.continuous) AS
    SELECT 
        time_bucket('1 hour', time) AS bucket,
        device_id,
        device_type,
        location,
        measurement_type,
        AVG(value) as avg_value,
        MIN(value) as min_value,
        MAX(value) as max_value,
        COUNT(*) as sample_count
    FROM measurements
    GROUP BY bucket, device_id, device_type, location, measurement_type
    WITH NO DATA;

    -- Continuous aggregate for daily averages
    CREATE MATERIALIZED VIEW IF NOT EXISTS measurements_daily
    WITH (timescaledb.continuous) AS
    SELECT 
        time_bucket('1 day', time) AS bucket,
        device_id,
        device_type,
        location,
        measurement_type,
        AVG(value) as avg_value,
        MIN(value) as min_value,
        MAX(value) as max_value,
        COUNT(*) as sample_count
    FROM measurements
    GROUP BY bucket, device_id, device_type, location, measurement_type
    WITH NO DATA;

    -- Enable compression on measurements table
    ALTER TABLE measurements SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'device_id, measurement_type',
        timescaledb.compress_orderby = 'time DESC'
    );

    -- Add compression policy (compress data older than 7 days)
    SELECT add_compression_policy('measurements', INTERVAL '7 days', if_not_exists => TRUE);

    -- Add retention policy (drop raw data older than 90 days)
    SELECT add_retention_policy('measurements', INTERVAL '90 days', if_not_exists => TRUE);

    -- Refresh policies for continuous aggregates
    SELECT add_continuous_aggregate_policy('measurements_hourly',
        start_offset => INTERVAL '3 hours',
        end_offset => INTERVAL '1 hour',
        schedule_interval => INTERVAL '1 hour',
        if_not_exists => TRUE
    );

    SELECT add_continuous_aggregate_policy('measurements_daily',
        start_offset => INTERVAL '3 days',
        end_offset => INTERVAL '1 day',
        schedule_interval => INTERVAL '1 day',
        if_not_exists => TRUE
    );
    """

    def __init__(
            self,
            host: str = 'localhost',
            port: int = 5432,
            database: str = 'smarthome',
            user: str = 'smarthome',
            password: str = '',
            logger: Optional[logging.Logger] = None
    ):
        """
        Initialize TimescaleDB connection.

        Args:
            host: PostgreSQL host
            port: PostgreSQL port
            database: Database name
            user: Database user
            password: Database password
            logger: Optional logger
        """
        self.connection_params = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }
        self.logger = logger or logging.getLogger(__name__)
        self.conn: Optional[psycopg2.extensions.connection] = None

    def connect(self) -> bool:
        """Connect to TimescaleDB"""
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            self.conn.autocommit = False
            self.logger.info("Connected to TimescaleDB")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to TimescaleDB: {e}")
            return False

    def disconnect(self):
        """Disconnect from TimescaleDB"""
        if self.conn:
            self.conn.close()
            self.conn = None

    def initialize_schema(self) -> bool:
        """Initialize database schema with hypertables and policies"""
        if not self.conn:
            return False

        try:
            with self.conn.cursor() as cur:
                # Execute schema creation (split by semicolon for individual statements)
                for statement in self.SCHEMA_SQL.split(';'):
                    statement = statement.strip()
                    if statement:
                        try:
                            cur.execute(statement)
                        except Exception as e:
                            # Some statements might fail if already exist, that's ok
                            self.logger.debug(f"Statement warning: {e}")
                
                self.conn.commit()
                self.logger.info("Database schema initialized successfully")
                return True

        except Exception as e:
            self.logger.error(f"Failed to initialize schema: {e}", exc_info=True)
            self.conn.rollback()
            return False

    def insert_measurement(self, measurement: MeasurementPoint) -> bool:
        """Insert a single measurement"""
        return self.insert_measurements([measurement])

    def insert_measurements(self, measurements: List[MeasurementPoint]) -> bool:
        """Insert multiple measurements in batch"""
        if not self.conn or not measurements:
            return False

        try:
            with self.conn.cursor() as cur:
                # Prepare data for bulk insert
                data = [
                    (
                        m.timestamp,
                        m.device_id,
                        m.device_type,
                        m.location,
                        m.measurement_type,
                        m.value,
                        m.unit,
                        json.dumps(m.metadata) if m.metadata else None
                    )
                    for m in measurements
                ]

                # Bulk insert
                execute_values(
                    cur,
                    """
                    INSERT INTO measurements 
                    (time, device_id, device_type, location, measurement_type, value, unit, metadata)
                    VALUES %s
                    ON CONFLICT DO NOTHING
                    """,
                    data
                )

                self.conn.commit()
                self.logger.debug(f"Inserted {len(measurements)} measurements")
                return True

        except Exception as e:
            self.logger.error(f"Failed to insert measurements: {e}", exc_info=True)
            self.conn.rollback()
            return False

    def query_measurements(
            self,
            device_id: Optional[str] = None,
            measurement_type: Optional[str] = None,
            location: Optional[str] = None,
            start_time: Optional[datetime] = None,
            end_time: Optional[datetime] = None,
            limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Query measurements with filters"""
        if not self.conn:
            return []

        try:
            # Build query dynamically
            query_parts = ["SELECT * FROM measurements WHERE 1=1"]
            params = []

            if device_id:
                query_parts.append("AND device_id = %s")
                params.append(device_id)

            if measurement_type:
                query_parts.append("AND measurement_type = %s")
                params.append(measurement_type)

            if location:
                query_parts.append("AND location = %s")
                params.append(location)

            if start_time:
                query_parts.append("AND time >= %s")
                params.append(start_time)

            if end_time:
                query_parts.append("AND time <= %s")
                params.append(end_time)

            query_parts.append("ORDER BY time DESC LIMIT %s")
            params.append(limit)

            query = " ".join(query_parts)

            with self.conn.cursor() as cur:
                cur.execute(query, params)
                columns = [desc[0] for desc in cur.description]
                results = [dict(zip(columns, row)) for row in cur.fetchall()]
                return results

        except Exception as e:
            self.logger.error(f"Failed to query measurements: {e}", exc_info=True)
            return []

    def query_hourly_aggregates(
            self,
            device_id: Optional[str] = None,
            measurement_type: Optional[str] = None,
            start_time: Optional[datetime] = None,
            end_time: Optional[datetime] = None,
            limit: int = 168  # 7 days of hourly data
    ) -> List[Dict[str, Any]]:
        """Query hourly aggregated data"""
        if not self.conn:
            return []

        try:
            query_parts = ["SELECT * FROM measurements_hourly WHERE 1=1"]
            params = []

            if device_id:
                query_parts.append("AND device_id = %s")
                params.append(device_id)

            if measurement_type:
                query_parts.append("AND measurement_type = %s")
                params.append(measurement_type)

            if start_time:
                query_parts.append("AND bucket >= %s")
                params.append(start_time)

            if end_time:
                query_parts.append("AND bucket <= %s")
                params.append(end_time)

            query_parts.append("ORDER BY bucket DESC LIMIT %s")
            params.append(limit)

            query = " ".join(query_parts)

            with self.conn.cursor() as cur:
                cur.execute(query, params)
                columns = [desc[0] for desc in cur.description]
                results = [dict(zip(columns, row)) for row in cur.fetchall()]
                return results

        except Exception as e:
            self.logger.error(f"Failed to query hourly aggregates: {e}", exc_info=True)
            return []

    def get_compression_stats(self) -> Dict[str, Any]:
        """Get compression statistics"""
        if not self.conn:
            return {}

        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        pg_size_pretty(before_compression_total_bytes) as before_compression,
                        pg_size_pretty(after_compression_total_bytes) as after_compression,
                        ROUND(100 - (after_compression_total_bytes::numeric / 
                              before_compression_total_bytes::numeric * 100), 2) as compression_ratio
                    FROM timescaledb_information.compression_settings
                    WHERE hypertable_name = 'measurements';
                """)
                
                result = cur.fetchone()
                if result:
                    return {
                        'before_compression': result[0],
                        'after_compression': result[1],
                        'compression_ratio_percent': result[2]
                    }
                return {}

        except Exception as e:
            self.logger.error(f"Failed to get compression stats: {e}")
            return {}


class TimeSeriesCollector:
    """
    Collects sensor data from Redis and stores in TimescaleDB.
    Runs as a background service.
    """

    def __init__(
            self,
            redis_publisher: UnifiedRedisPublisher,
            timeseries_db: TimeSeriesDatabase,
            collection_interval: int = 60,  # seconds
            logger: Optional[logging.Logger] = None
    ):
        """
        Initialize time series collector.

        Args:
            redis_publisher: Redis publisher to read from
            timeseries_db: TimescaleDB instance to write to
            collection_interval: How often to collect data (seconds)
            logger: Optional logger
        """
        self.redis_publisher = redis_publisher
        self.timeseries_db = timeseries_db
        self.collection_interval = collection_interval
        self.logger = logger or logging.getLogger(__name__)
        
        self.running = False
        self.thread: Optional[threading.Thread] = None
        
        # Statistics
        self.stats = {
            'collections': 0,
            'measurements_stored': 0,
            'errors': 0,
            'last_collection': None
        }

    def start(self):
        """Start the collection service"""
        if self.running:
            self.logger.warning("Service already running")
            return

        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        self.logger.info(f"Time series collector started (interval: {self.collection_interval}s)")

    def stop(self):
        """Stop the collection service"""
        if not self.running:
            return

        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        self.logger.info("Time series collector stopped")

    def _run_loop(self):
        """Main collection loop"""
        while self.running:
            try:
                self._collect_and_store()
                self.stats['collections'] += 1
                self.stats['last_collection'] = datetime.now().isoformat()

            except Exception as e:
                self.logger.error(f"Error in collection loop: {e}", exc_info=True)
                self.stats['errors'] += 1

            # Wait for next collection
            for _ in range(self.collection_interval):
                if not self.running:
                    break
                time.sleep(1)

    def _collect_and_store(self):
        """Collect data from Redis and store in TimescaleDB"""
        measurements = []
        
        # Collect Niko temperature/humidity sensors
        niko_measurements = self._collect_niko_measurements()
        measurements.extend(niko_measurements)
        
        # Collect Duco system data
        duco_measurements = self._collect_duco_measurements()
        measurements.extend(duco_measurements)
        
        # Store all measurements
        if measurements:
            success = self.timeseries_db.insert_measurements(measurements)
            if success:
                self.stats['measurements_stored'] += len(measurements)
                self.logger.info(f"Stored {len(measurements)} measurements")
            else:
                self.logger.error("Failed to store measurements")
                self.stats['errors'] += 1

    def _collect_niko_measurements(self) -> List[MeasurementPoint]:
        """Collect measurements from Niko devices"""
        measurements = []
        timestamp = datetime.now()
        
        try:
            # Get all Niko devices
            devices = self.redis_publisher.get_all_niko_devices()
            
            for device in devices:
                device_id = device.get('uuid', '')
                device_type = device.get('device_type', '')
                location = device.get('location_name', 'Unknown')
                properties = device.get('properties', {})
                
                # Extract temperature
                if 'AmbientTemperature' in properties:
                    temp = properties['AmbientTemperature']
                    if temp is not None:
                        measurements.append(MeasurementPoint(
                            timestamp=timestamp,
                            device_id=device_id,
                            device_type=device_type,
                            location=location,
                            measurement_type='temperature',
                            value=float(temp),
                            unit='°C',
                            metadata={'source': 'niko', 'name': device.get('name')}
                        ))
                
                # Extract humidity
                if 'Humidity' in properties:
                    humidity = properties['Humidity']
                    if humidity is not None:
                        measurements.append(MeasurementPoint(
                            timestamp=timestamp,
                            device_id=device_id,
                            device_type=device_type,
                            location=location,
                            measurement_type='humidity',
                            value=float(humidity),
                            unit='%',
                            metadata={'source': 'niko', 'name': device.get('name')}
                        ))
                
                # Extract heat index if available
                if 'HeatIndex' in properties:
                    heat_index = properties['HeatIndex']
                    if heat_index is not None:
                        measurements.append(MeasurementPoint(
                            timestamp=timestamp,
                            device_id=device_id,
                            device_type=device_type,
                            location=location,
                            measurement_type='heat_index',
                            value=float(heat_index),
                            unit='°C',
                            metadata={'source': 'niko', 'name': device.get('name')}
                        ))

        except Exception as e:
            self.logger.error(f"Error collecting Niko measurements: {e}")

        return measurements

    def _collect_duco_measurements(self) -> List[MeasurementPoint]:
        """Collect measurements from Duco system"""
        measurements = []
        timestamp = datetime.now()
        
        try:
            # Get DucoBox system data
            ducobox = self.redis_publisher.get_ducobox()
            if ducobox:
                device_id = 'ducobox_main'
                location = 'Ventilation System'
                
                # System humidity
                if ducobox.get('humidity_level') is not None:
                    measurements.append(MeasurementPoint(
                        timestamp=timestamp,
                        device_id=device_id,
                        device_type='ducobox',
                        location=location,
                        measurement_type='humidity',
                        value=float(ducobox['humidity_level']),
                        unit='%',
                        metadata={'source': 'duco'}
                    ))
                
                # System CO2
                if ducobox.get('co2_level') is not None:
                    measurements.append(MeasurementPoint(
                        timestamp=timestamp,
                        device_id=device_id,
                        device_type='ducobox',
                        location=location,
                        measurement_type='co2',
                        value=float(ducobox['co2_level']),
                        unit='ppm',
                        metadata={'source': 'duco'}
                    ))
                
                # Air quality metrics
                if ducobox.get('air_quality_rh') is not None:
                    measurements.append(MeasurementPoint(
                        timestamp=timestamp,
                        device_id=device_id,
                        device_type='ducobox',
                        location=location,
                        measurement_type='air_quality_rh',
                        value=float(ducobox['air_quality_rh']),
                        unit='%',
                        metadata={'source': 'duco'}
                    ))
                
                if ducobox.get('air_quality_co2') is not None:
                    measurements.append(MeasurementPoint(
                        timestamp=timestamp,
                        device_id=device_id,
                        device_type='ducobox',
                        location=location,
                        measurement_type='air_quality_co2',
                        value=float(ducobox['air_quality_co2']),
                        unit='%',
                        metadata={'source': 'duco'}
                    ))
                
                # Temperatures (DucoBox Energy)
                temp_fields = {
                    'temperature_oda': 'outdoor_air_temp',
                    'temperature_sup': 'supply_air_temp',
                    'temperature_eta': 'extract_air_temp',
                    'temperature_eha': 'exhaust_air_temp'
                }
                
                for field, measurement_type in temp_fields.items():
                    if ducobox.get(field) is not None:
                        measurements.append(MeasurementPoint(
                            timestamp=timestamp,
                            device_id=device_id,
                            device_type='ducobox',
                            location=location,
                            measurement_type=measurement_type,
                            value=float(ducobox[field]),
                            unit='°C',
                            metadata={'source': 'duco'}
                        ))
                
                # Flow rate (derived from ventilation mode)
                if ducobox.get('flow_rate') is not None:
                    measurements.append(MeasurementPoint(
                        timestamp=timestamp,
                        device_id=device_id,
                        device_type='ducobox',
                        location=location,
                        measurement_type='flow_rate',
                        value=float(ducobox['flow_rate']),
                        unit='%',
                        metadata={'source': 'duco'}
                    ))
            
            # Get Duco node data
            nodes = self.redis_publisher.get_all_duco_nodes()
            for node in nodes:
                node_id = f"node_{node.get('node_id')}"
                node_type = node.get('node_type_name', 'unknown')
                location = f"Node {node.get('node_id')}"
                
                # Node humidity
                if node.get('humidity_level') is not None:
                    measurements.append(MeasurementPoint(
                        timestamp=timestamp,
                        device_id=node_id,
                        device_type=f'duco_{node_type}',
                        location=location,
                        measurement_type='humidity',
                        value=float(node['humidity_level']),
                        unit='%',
                        metadata={'source': 'duco', 'node_type': node_type}
                    ))
                
                # Node CO2
                if node.get('co2_level') is not None:
                    measurements.append(MeasurementPoint(
                        timestamp=timestamp,
                        device_id=node_id,
                        device_type=f'duco_{node_type}',
                        location=location,
                        measurement_type='co2',
                        value=float(node['co2_level']),
                        unit='ppm',
                        metadata={'source': 'duco', 'node_type': node_type}
                    ))
                
                # Node flow rate
                if node.get('flow_rate') is not None:
                    measurements.append(MeasurementPoint(
                        timestamp=timestamp,
                        device_id=node_id,
                        device_type=f'duco_{node_type}',
                        location=location,
                        measurement_type='flow_rate',
                        value=float(node['flow_rate']),
                        unit='%',
                        metadata={'source': 'duco', 'node_type': node_type}
                    ))

        except Exception as e:
            self.logger.error(f"Error collecting Duco measurements: {e}")

        return measurements

    def get_statistics(self) -> Dict[str, Any]:
        """Get collector statistics"""
        return {
            **self.stats,
            'running': self.running,
            'collection_interval': self.collection_interval
        }


# ============================================================================
# Standalone Execution
# ============================================================================

def main():
    """Main entry point for standalone execution"""
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    # Get configuration
    db_host = os.getenv('TIMESCALE_HOST', 'localhost')
    db_port = int(os.getenv('TIMESCALE_PORT', 5432))
    db_name = os.getenv('TIMESCALE_DB', 'smarthome')
    db_user = os.getenv('TIMESCALE_USER', 'smarthome')
    db_password = os.getenv('TIMESCALE_PASSWORD', '')
    
    redis_host = os.getenv('REDIS_HOST', 'localhost')
    redis_port = int(os.getenv('REDIS_PORT', 6379))
    key_prefix = os.getenv('REDIS_KEY_PREFIX', 'smarthome')
    
    collection_interval = int(os.getenv('COLLECTION_INTERVAL', 60))
    
    logger.info("Starting Time Series Storage Service...")
    
    try:
        # Initialize TimescaleDB
        logger.info(f"Connecting to TimescaleDB at {db_host}:{db_port}...")
        timeseries_db = TimeSeriesDatabase(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password,
            logger=logger
        )
        
        if not timeseries_db.connect():
            logger.error("Failed to connect to TimescaleDB")
            return
        
        logger.info("✓ Connected to TimescaleDB")
        
        # Initialize schema
        logger.info("Initializing database schema...")
        if timeseries_db.initialize_schema():
            logger.info("✓ Schema initialized")
        else:
            logger.error("Failed to initialize schema")
            return
        
        # Initialize Redis publisher
        logger.info(f"Connecting to Redis at {redis_host}:{redis_port}...")
        redis_publisher = UnifiedRedisPublisher(
            redis_host=redis_host,
            redis_port=redis_port,
            key_prefix=key_prefix
        )
        logger.info("✓ Connected to Redis")
        
        # Initialize collector
        collector = TimeSeriesCollector(
            redis_publisher=redis_publisher,
            timeseries_db=timeseries_db,
            collection_interval=collection_interval,
            logger=logger
        )
        
        # Start collector
        collector.start()
        
        print("\n" + "=" * 60)
        print("TIME SERIES STORAGE SERVICE")
        print("=" * 60)
        print(f"\nCollection interval: {collection_interval} seconds")
        print("Data compression: Enabled (7 days)")
        print("Retention: 90 days (raw data)")
        print("\nPress Ctrl+C to stop.\n")
        
        # Main loop - print stats
        while True:
            time.sleep(60)
            
            stats = collector.get_statistics()
            compression_stats = timeseries_db.get_compression_stats()
            
            print("\n" + "-" * 60)
            print("SERVICE STATISTICS")
            print("-" * 60)
            print(f"Collections: {stats['collections']}")
            print(f"Measurements stored: {stats['measurements_stored']}")
            print(f"Errors: {stats['errors']}")
            if stats['last_collection']:
                print(f"Last collection: {stats['last_collection']}")
            
            if compression_stats:
                print("\nCOMPRESSION STATISTICS")
                print(f"Before compression: {compression_stats.get('before_compression', 'N/A')}")
                print(f"After compression: {compression_stats.get('after_compression', 'N/A')}")
                print(f"Compression ratio: {compression_stats.get('compression_ratio_percent', 'N/A')}%")
            
            print("-" * 60 + "\n")
            
    except KeyboardInterrupt:
        print("\n\nStopping service...")
        collector.stop()
        timeseries_db.disconnect()
        print("Service stopped.")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)


if __name__ == "__main__":
    main()
