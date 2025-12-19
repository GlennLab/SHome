"""
Time Series Storage Service - FIXED UNIQUE CONSTRAINT VERSION
"""

import logging
import time
import threading
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from dataclasses import dataclass
import json

import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import Error as Psycopg2Error

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

    -- Create unique index for ON CONFLICT clause
    -- Note: We create an index instead of constraint for hypertables
    CREATE UNIQUE INDEX IF NOT EXISTS measurements_unique_idx 
    ON measurements (time, device_id, measurement_type);

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
            try:
                self.conn.close()
            except:
                pass
            finally:
                self.conn = None

    def _ensure_connection(self) -> bool:
        """Ensure we have a working connection"""
        if not self.conn or self.conn.closed:
            return self.connect()
        return True

    def initialize_schema(self) -> bool:
        """Initialize database schema with hypertables and policies"""
        if not self._ensure_connection():
            return False

        try:
            with self.conn.cursor() as cur:
                # First drop the unique index if it exists (to avoid conflicts)
                try:
                    cur.execute("DROP INDEX IF EXISTS measurements_unique_idx;")
                except:
                    pass

                # Execute schema creation
                for statement in self.SCHEMA_SQL.split(';'):
                    statement = statement.strip()
                    if statement:
                        try:
                            cur.execute(statement)
                        except Exception as e:
                            # Some statements might fail if already exist, that's ok
                            if "already exists" not in str(e).lower() and "duplicate" not in str(e).lower():
                                self.logger.debug(f"Schema statement warning: {e}")

                self.conn.commit()
                self.logger.info("Database schema initialized successfully")

                # Set up compression and policies separately
                self._setup_compression_and_policies()

                return True

        except Exception as e:
            self.logger.error(f"Failed to initialize schema: {e}", exc_info=True)
            self._safe_rollback()
            return False

    def _setup_compression_and_policies(self):
        """Set up compression and policies after schema is created"""
        if not self._ensure_connection():
            return

        try:
            with self.conn.cursor() as cur:
                # Enable compression on measurements table
                try:
                    cur.execute("""
                        ALTER TABLE measurements SET (
                            timescaledb.compress,
                            timescaledb.compress_segmentby = 'device_id, measurement_type',
                            timescaledb.compress_orderby = 'time DESC'
                        );
                    """)
                    self.logger.info("Compression enabled on measurements table")
                except Exception as e:
                    self.logger.warning(f"Could not enable compression: {e}")

                # Add compression policy (compress data older than 7 days)
                try:
                    cur.execute("""
                        SELECT add_compression_policy('measurements', INTERVAL '7 days', if_not_exists => TRUE);
                    """)
                    self.logger.info("Compression policy added")
                except Exception as e:
                    self.logger.warning(f"Could not add compression policy: {e}")
                    # Try older syntax
                    try:
                        cur.execute("""
                            SELECT add_compression_policy('measurements', INTERVAL '7 days');
                        """)
                    except:
                        pass

                # Add retention policy (drop raw data older than 90 days)
                try:
                    cur.execute("""
                        SELECT add_retention_policy('measurements', INTERVAL '90 days', if_not_exists => TRUE);
                    """)
                    self.logger.info("Retention policy added")
                except Exception as e:
                    self.logger.warning(f"Could not add retention policy: {e}")
                    # Try older syntax
                    try:
                        cur.execute("""
                            SELECT add_retention_policy('measurements', INTERVAL '90 days');
                        """)
                    except:
                        pass

                self.conn.commit()
                self.logger.info("Compression and policies configured")

        except Exception as e:
            self.logger.error(f"Failed to setup compression/policies: {e}")
            self._safe_rollback()

    def _safe_rollback(self):
        """Safely rollback transaction and ensure clean state"""
        if self.conn and not self.conn.closed:
            try:
                self.conn.rollback()
                self.logger.debug("Transaction rolled back")
            except Exception as e:
                self.logger.debug(f"Could not rollback: {e}")
                # If rollback fails, we need to reconnect
                self.disconnect()
                self.connect()

    def insert_measurements(self, measurements: List[MeasurementPoint]) -> bool:
        """Insert multiple measurements in batch"""
        if not measurements:
            return True  # Nothing to insert is not an error

        if not self._ensure_connection():
            return False

        # Ensure timestamps are timezone aware
        for m in measurements:
            if m.timestamp.tzinfo is None:
                m.timestamp = m.timestamp.replace(tzinfo=timezone.utc)

        retry_count = 0
        max_retries = 2

        while retry_count <= max_retries:
            try:
                with self.conn.cursor() as cur:
                    # Prepare data for bulk insert
                    data = []
                    for m in measurements:
                        # Ensure metadata is JSON serializable
                        metadata = m.metadata
                        if metadata is not None:
                            try:
                                # Convert any non-serializable objects
                                metadata = json.loads(json.dumps(metadata, default=str))
                            except:
                                metadata = {"raw": str(metadata)}

                        data.append((
                            m.timestamp,
                            m.device_id,
                            m.device_type,
                            m.location,
                            m.measurement_type,
                            m.value,
                            m.unit,
                            json.dumps(metadata) if metadata else None
                        ))

                    # Bulk insert with ON CONFLICT DO NOTHING
                    # Using the unique index we created
                    execute_values(
                        cur,
                        """
                        INSERT INTO measurements 
                        (time, device_id, device_type, location, measurement_type, value, unit, metadata)
                        VALUES %s
                        ON CONFLICT (time, device_id, measurement_type) DO NOTHING
                        """,
                        data,
                        template="(%s, %s, %s, %s, %s, %s, %s, %s)"
                    )

                    self.conn.commit()
                    inserted_count = cur.rowcount
                    if inserted_count > 0:
                        self.logger.debug(f"Inserted {inserted_count} measurements (skipped {len(measurements) - inserted_count} duplicates)")
                    return True

            except (Psycopg2Error, Exception) as e:
                error_msg = str(e).lower()
                self.logger.error(f"Failed to insert measurements (attempt {retry_count + 1}): {e}")

                # Check if it's a unique constraint error - we need to handle this differently
                if "unique constraint" in error_msg or "duplicate key" in error_msg:
                    # Try without ON CONFLICT clause, just insert and let it fail
                    # We'll catch individual failures but continue with others
                    try:
                        self._safe_rollback()
                        success = self._insert_measurements_without_conflict(measurements)
                        if success:
                            return True
                    except Exception as conflict_error:
                        self.logger.error(f"Conflict resolution failed: {conflict_error}")

                # Rollback and retry
                self._safe_rollback()

                retry_count += 1
                if retry_count > max_retries:
                    # Last retry failed, reconnect and try one more time
                    self.logger.warning("Reconnecting to database after multiple failures")
                    self.disconnect()
                    time.sleep(1)
                    if not self.connect():
                        return False

                    # Try one final time with new connection
                    success = self._insert_measurements_simple(measurements)
                    if success:
                        return True
                    else:
                        self.logger.error("Final insertion attempt failed")
                        self._safe_rollback()
                        return False

                # Wait a bit before retry
                time.sleep(0.5)

        return False

    def _insert_measurements_without_conflict(self, measurements: List[MeasurementPoint]) -> bool:
        """Insert measurements without ON CONFLICT clause - handle duplicates manually"""
        if not self._ensure_connection():
            return False

        try:
            with self.conn.cursor() as cur:
                inserted_count = 0

                for m in measurements:
                    # Prepare metadata
                    metadata = m.metadata
                    if metadata is not None:
                        try:
                            metadata = json.loads(json.dumps(metadata, default=str))
                        except:
                            metadata = {"raw": str(metadata)}

                    try:
                        cur.execute("""
                            INSERT INTO measurements 
                            (time, device_id, device_type, location, measurement_type, value, unit, metadata)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            m.timestamp,
                            m.device_id,
                            m.device_type,
                            m.location,
                            m.measurement_type,
                            m.value,
                            m.unit,
                            json.dumps(metadata) if metadata else None
                        ))
                        inserted_count += 1
                    except psycopg2.errors.UniqueViolation:
                        # Duplicate entry, skip it
                        self.conn.rollback()
                        # Start a new transaction for the next insert
                        continue
                    except Exception as e:
                        self.logger.debug(f"Failed to insert single measurement: {e}")
                        self.conn.rollback()
                        continue
                    else:
                        self.conn.commit()

                if inserted_count > 0:
                    self.logger.debug(f"Inserted {inserted_count} measurements individually")
                return True

        except Exception as e:
            self.logger.error(f"Failed in individual insert: {e}")
            self._safe_rollback()
            return False

    def _insert_measurements_simple(self, measurements: List[MeasurementPoint]) -> bool:
        """Simple insert without ON CONFLICT - last resort"""
        if not self._ensure_connection():
            return False

        try:
            with self.conn.cursor() as cur:
                # Prepare data for bulk insert
                data = []
                for m in measurements:
                    # Ensure metadata is JSON serializable
                    metadata = m.metadata
                    if metadata is not None:
                        try:
                            # Convert any non-serializable objects
                            metadata = json.loads(json.dumps(metadata, default=str))
                        except:
                            metadata = {"raw": str(metadata)}

                    data.append((
                        m.timestamp,
                        m.device_id,
                        m.device_type,
                        m.location,
                        m.measurement_type,
                        m.value,
                        m.unit,
                        json.dumps(metadata) if metadata else None
                    ))

                # Bulk insert WITHOUT ON CONFLICT
                execute_values(
                    cur,
                    """
                    INSERT INTO measurements 
                    (time, device_id, device_type, location, measurement_type, value, unit, metadata)
                    VALUES %s
                    """,
                    data,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s)"
                )

                self.conn.commit()
                inserted_count = cur.rowcount
                self.logger.debug(f"Inserted {inserted_count} measurements (simple insert)")
                return True

        except Exception as e:
            self.logger.error(f"Simple insert failed: {e}")
            self._safe_rollback()
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
        if not self._ensure_connection():
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

    def get_compression_stats(self) -> Dict[str, Any]:
        """Get compression statistics"""
        if not self._ensure_connection():
            return {}

        try:
            with self.conn.cursor() as cur:
                # Try different queries for different TimescaleDB versions
                queries = [
                    """
                    SELECT 
                        cs.uncompressed_heap_size as before_compression,
                        cs.compressed_heap_size as after_compression,
                        ROUND(100 - (cs.compressed_heap_size::numeric / 
                              NULLIF(cs.uncompressed_heap_size::numeric, 0) * 100), 2) as compression_ratio
                    FROM timescaledb_information.compressed_chunk_stats cs
                    JOIN timescaledb_information.hypertables h 
                        ON cs.hypertable_name = h.hypertable_name
                    WHERE h.hypertable_name = 'measurements'
                    LIMIT 1;
                    """,
                    """
                    SELECT 
                        pg_size_pretty(uncompressed_total_bytes) as before_compression,
                        pg_size_pretty(compressed_total_bytes) as after_compression,
                        ROUND(100 - (compressed_total_bytes::numeric / 
                              NULLIF(uncompressed_total_bytes::numeric, 0) * 100), 2) as compression_ratio
                    FROM timescaledb_information.compressed_chunk_stats
                    WHERE hypertable_name = 'measurements'
                    LIMIT 1;
                    """
                ]

                for query in queries:
                    try:
                        cur.execute(query)
                        result = cur.fetchone()
                        if result:
                            return {
                                'before_compression': result[0],
                                'after_compression': result[1],
                                'compression_ratio_percent': result[2]
                            }
                    except Exception as e:
                        self.logger.debug(f"Compression stats query failed: {e}")
                        continue

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
                self.stats['last_collection'] = datetime.now(timezone.utc).isoformat()

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

        try:
            # Collect Niko temperature/humidity sensors
            niko_measurements = self._collect_niko_measurements()
            measurements.extend(niko_measurements)

            # Collect Duco system data
            duco_measurements = self._collect_duco_measurements()
            measurements.extend(duco_measurements)

            # Store all measurements
            if measurements:
                # Ensure all timestamps are timezone aware
                current_time = datetime.now(timezone.utc)
                for m in measurements:
                    if m.timestamp.tzinfo is None:
                        m.timestamp = current_time

                success = self.timeseries_db.insert_measurements(measurements)
                if success:
                    self.stats['measurements_stored'] += len(measurements)
                    self.logger.debug(f"Collected {len(measurements)} measurements")
                else:
                    self.logger.error("Failed to store measurements")
                    self.stats['errors'] += 1
            else:
                self.logger.debug("No measurements to store")

        except Exception as e:
            self.logger.error(f"Error collecting data: {e}", exc_info=True)
            self.stats['errors'] += 1

    def _collect_niko_measurements(self) -> List[MeasurementPoint]:
        """Collect measurements from Niko devices"""
        measurements = []
        timestamp = datetime.now(timezone.utc)

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
                    if self._is_valid_measurement(temp):
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
                    if self._is_valid_measurement(humidity):
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
                    if self._is_valid_measurement(heat_index):
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
            self.logger.error(f"Error collecting Niko measurements: {e}", exc_info=True)

        return measurements

    def _collect_duco_measurements(self) -> List[MeasurementPoint]:
        """Collect measurements from Duco system"""
        measurements = []
        timestamp = datetime.now(timezone.utc)

        try:
            # Get DucoBox system data
            ducobox = self.redis_publisher.get_ducobox()
            if ducobox:
                device_id = 'ducobox_main'
                location = 'Ventilation System'

                # System humidity
                if self._is_valid_measurement(ducobox.get('humidity_level')):
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
                if self._is_valid_measurement(ducobox.get('co2_level')):
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
                if self._is_valid_measurement(ducobox.get('air_quality_rh')):
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

                if self._is_valid_measurement(ducobox.get('air_quality_co2')):
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
                    value = ducobox.get(field)
                    if self._is_valid_measurement(value):
                        measurements.append(MeasurementPoint(
                            timestamp=timestamp,
                            device_id=device_id,
                            device_type='ducobox',
                            location=location,
                            measurement_type=measurement_type,
                            value=float(value),
                            unit='°C',
                            metadata={'source': 'duco'}
                        ))

                # Flow rate (derived from ventilation mode)
                if self._is_valid_measurement(ducobox.get('flow_rate')):
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
                if self._is_valid_measurement(node.get('humidity_level')):
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
                if self._is_valid_measurement(node.get('co2_level')):
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
                if self._is_valid_measurement(node.get('flow_rate')):
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
            self.logger.error(f"Error collecting Duco measurements: {e}", exc_info=True)

        return measurements

    def _is_valid_measurement(self, value) -> bool:
        """Check if a measurement value is valid for storage"""
        if value is None:
            return False

        try:
            # Check if it's a string that can be converted to float
            if isinstance(value, str):
                value = value.strip()
                if value == '':
                    return False
                # Try to convert to check if it's valid
                float(value)

            # Check for reasonable ranges (adjust as needed)
            num_value = float(value) if not isinstance(value, (int, float)) else value

            # Example: temperature between -50 and 100°C
            if -50 <= num_value <= 100:
                return True
            # Humidity between 0 and 100%
            if 0 <= num_value <= 100:
                return True
            # CO2 between 0 and 5000 ppm
            if 0 <= num_value <= 5000:
                return True

            return False

        except (ValueError, TypeError):
            return False

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
            else:
                print("\nCOMPRESSION STATISTICS")
                print("No compression data available yet (data may not be old enough)")

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