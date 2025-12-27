import datetime
import json
import logging
import sys
from typing import Optional

import redis
from PyQt6.QtCore import QTimer, Qt
from PyQt6.QtGui import QColor, QFont
from PyQt6.QtWidgets import (QGroupBox, QSizePolicy, QApplication, QWidget,
                             QVBoxLayout, QMainWindow, QScrollArea, QHBoxLayout,
                             QLabel, QGridLayout)

from widgets.multistat import SmartHomeWidget
# Import VentilationBoxWidget if it's in a different module
# If it's in the same directory, you can use:
from vent import VentilationBoxWidget


class WidgetPanel(QGroupBox):
    def __init__(self, title: str = None, parent=None, color: Optional[str] = None):
        super().__init__(title, parent)
        self.color = QColor(color) if color else QColor("black")

        # Use format() to insert the color into the stylesheet
        self.setStyleSheet(f"""
            QGroupBox {{
                font-weight: bold;
                font-size: 16px;
                border: 2px solid {self.color.name()};
                border-radius: 12px;
                margin-top: 1ex;
                margin-bottom: 1ex;
                margin-left: 1ex;
                margin-right: 1ex;
                padding-top: 15px;
                background: transparent;
            }}
            QGroupBox::title {{
                subcontrol-origin: margin;
                left: 25px;
                padding: 0 8px 0 8px;
                color: {self.color.name()};
            }}
        """)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)


class Dashboard(QMainWindow):
    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0):
        super().__init__()
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=True
        )
        self.logger = logging.getLogger(__name__)

        self.climate_data = {}
        self.ventilation_data = {}
        self.room_name_cache = {}

        self.timer = QTimer()
        self.timer.timeout.connect(self.fetch_data)
        self.timer.start(2000)  # Update every 2 seconds

        # Set window title and size
        self.setWindowTitle("Smart Home Dashboard")
        self.resize(1400, 900)

        # Central widget and main layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(20, 20, 20, 20)
        main_layout.setSpacing(20)

        # Create main grid for panels
        grid_widget = QWidget()
        grid_layout = QGridLayout(grid_widget)
        grid_layout.setSpacing(20)

        # Create climate panel (left side)
        climate_panel = self.create_climate_panel()
        grid_layout.addWidget(climate_panel, 0, 0)

        # Create ventilation panel (right side)
        ventilation_panel = self.create_ventilation_panel()
        grid_layout.addWidget(ventilation_panel, 0, 1)

        # Set column stretch factors
        grid_layout.setColumnStretch(0, 1)  # Climate panel
        grid_layout.setColumnStretch(1, 1)  # Ventilation panel

        main_layout.addWidget(grid_widget, 1)

        # Status bar
        self.status_bar = QLabel()
        self.status_bar.setStyleSheet("""
            background-color: #323232; 
            color: #CCCCCC; 
            padding: 8px; 
            border-radius: 4px;
            font-size: 11px;
        """)
        self.status_bar.setFont(QFont("Arial", 10))
        main_layout.addWidget(self.status_bar)

        # Initialize ventilation widget
        self.ventilation_widget = None

        self.fetch_data()
        self.update_status_bar()

    def update_status_bar(self):
        """Update status bar with connection info"""
        try:
            redis_status = "Verbonden" if self.redis_client.ping() else "Verbroken"
        except:
            redis_status = "Verbroken"

        status_text = f"Redis: {redis_status} | Duco: {'Beschikbaar' if self.ventilation_data else 'Geen data'} | Laatste refresh: {datetime.datetime.now().strftime('%H:%M:%S')}"
        self.status_bar.setText(status_text)

    def safe_json_parse(self, json_str):
        """Safely parse JSON that may contain multiple objects or erroneous data"""
        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            self.logger.warning(f"JSON parse fout: {e}, probeer eerste object te extraheren")

            try:
                start = json_str.find('{')
                end = json_str.rfind('}') + 1
                if start >= 0 and end > start:
                    extracted = json_str[start:end]
                    return json.loads(extracted)
            except:
                pass

            self.logger.error(f"Kon JSON data niet parsen: {json_str[:100]}...")
            return {}

    def extract_room_name(self, uuid, data):
        """Extract room name from data"""
        if isinstance(data, dict):
            # Duco format
            if 'parameters' in data and 'LocationName' in data['parameters']:
                return data['parameters']['LocationName']
            elif 'name' in data:
                # Check if it's a Duco node name, extract room name
                name = data['name']
                # Example: "Duco Node 2 (CO2 Room Sensor)" -> check if mapped
                if 'parameters' in data and 'LocationName' in data['parameters']:
                    return data['parameters']['LocationName']
                # Try to extract from name
                if '(' in name and ')' in name:
                    # Could have room info in parentheses
                    return name
                return name
        # Default
        return f"Kamer {uuid[:8]}"

    def create_climate_panel(self):
        """Create climate panel for room sensors"""
        panel = WidgetPanel('Klimaat', color='#FFA500')
        panel.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

        layout = QVBoxLayout(panel)
        layout.setContentsMargins(15, 20, 15, 15)

        # Scroll area for climate widgets
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)

        self.climate_container = QWidget()
        self.climate_layout = QHBoxLayout(self.climate_container)
        self.climate_layout.setSpacing(15)
        self.climate_layout.setContentsMargins(10, 10, 10, 10)
        self.climate_layout.setAlignment(Qt.AlignmentFlag.AlignTop)

        scroll_area.setWidget(self.climate_container)
        layout.addWidget(scroll_area)

        return panel

    def create_ventilation_panel(self):
        """Create ventilation panel for Duco system"""
        panel = WidgetPanel('Ventilatie', color='#1E90FF')  # Dodger Blue
        panel.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

        layout = QVBoxLayout(panel)
        layout.setContentsMargins(15, 20, 15, 15)

        # Create ventilation widget container
        self.ventilation_container = QWidget()
        ventilation_layout = QVBoxLayout(self.ventilation_container)
        ventilation_layout.setContentsMargins(0, 0, 0, 0)
        ventilation_layout.setSpacing(10)

        # Info label (will show when no data is available)
        self.ventilation_info_label = QLabel("Wachten op ventilatie data...")
        self.ventilation_info_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.ventilation_info_label.setStyleSheet("""
            color: #666666; 
            font-size: 14px; 
            font-style: italic;
            padding: 20px;
        """)
        ventilation_layout.addWidget(self.ventilation_info_label)

        # Add to scroll area
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setWidget(self.ventilation_container)
        layout.addWidget(scroll_area)

        return panel

    def parse_ventilation_data(self):
        """Parse ventilation data from Redis and extract values for widget"""
        try:
            # Get Duco system data
            duco_system_json = self.redis_client.get('duco:system')
            if not duco_system_json:
                self.logger.debug("Geen Duco systeem data gevonden in Redis")
                return None

            system_data = self.safe_json_parse(duco_system_json)
            if not system_data:
                return None

            # Extract temperatures
            temperatures = system_data.get('temperatures', {})

            # Extract filter status
            filter_info = system_data.get('filter', {})
            filter_status = filter_info.get('status', 'ok').lower()
            filter_days = filter_info.get('remaining_days', 175)

            # Extract ventilation status
            ventilation_info = system_data.get('ventilation', {})
            fan_speed = 0  # Default

            # Try to get fan speed from active nodes or system data
            # You might need to adjust this based on your actual data structure
            fan_speed = ventilation_info.get('fan_speed', 50)

            # Try alternative locations for fan speed
            if fan_speed == 0:
                # Check if there's a mode that indicates speed
                mode = ventilation_info.get('mode', 'auto')
                if 'boost' in mode.lower():
                    fan_speed = 80
                elif 'low' in mode.lower():
                    fan_speed = 30
                elif 'medium' in mode.lower():
                    fan_speed = 50
                elif 'high' in mode.lower():
                    fan_speed = 70

            # Extract humidity (might not be in system data, try from nodes)
            humidity = 50  # Default

            # Try to get humidity from active nodes
            active_nodes = self.redis_client.keys('duco:node:*')
            for node_key in active_nodes:
                node_json = self.redis_client.get(node_key)
                if node_json:
                    node_data = self.safe_json_parse(node_json)
                    if node_data and 'properties' in node_data:
                        props = node_data['properties']
                        if 'Humidity' in props:
                            humidity = props['Humidity']
                            break

            # Prepare data for ventilation widget
            ventilation_values = {
                'oda_temp': temperatures.get('outdoor_air', 12.3),  # Outdoor Air
                'sup_temp': temperatures.get('supply_air', 20.5),  # Supply Air to house
                'eha_temp': temperatures.get('extract_air', 22.0),  # Extract Air from house
                'eta_temp': temperatures.get('exhaust_air', 14.6),  # Exhaust Air to outside
                'fan_speed': fan_speed,
                'humidity': humidity,
                'mode': ventilation_info.get('mode', 'auto').lower(),
                'filter_status': filter_status,
                'days_to_replace': filter_days
            }

            self.logger.debug(f"Parsed ventilation data: {ventilation_values}")
            return ventilation_values

        except Exception as e:
            self.logger.error(f"Fout bij parsen ventilatie data: {e}")
            return None

    def update_ventilation_display(self):
        """Update ventilation display with current data"""
        if not self.ventilation_data:
            # Show info label if no data
            if self.ventilation_widget:
                self.ventilation_widget.hide()
            self.ventilation_info_label.show()
            self.ventilation_info_label.setText("Geen ventilatie data beschikbaar")
            return

        # Hide info label
        self.ventilation_info_label.hide()

        # Create or update ventilation widget
        if not self.ventilation_widget:
            # Create new ventilation widget
            self.ventilation_widget = VentilationBoxWidget(
                eha_temp=self.ventilation_data.get('eha_temp', None),
                eta_temp=self.ventilation_data.get('eta_temp', None),
                sup_temp=self.ventilation_data.get('sup_temp', None),
                oda_temp=self.ventilation_data.get('oda_temp', None),
                fan_speed=self.ventilation_data.get('fan_speed', None),
                humidity=self.ventilation_data.get('humidity', None),
                mode=self.ventilation_data.get('mode', None),
                filter_status=self.ventilation_data.get('filter_status', None),
                days_to_replace=self.ventilation_data.get('days_to_replace', None),
                name="DUCO VENTILATIE",
                flow_amplitude=1.2,
                flow_frequency=1.5,
                wave_spacing=5
            )

            # Add to layout
            layout = self.ventilation_container.layout()
            layout.insertWidget(0, self.ventilation_widget)
        else:
            # Update existing widget
            self.ventilation_widget.update_values(
                eha_temp=self.ventilation_data.get('eha_temp'),
                eta_temp=self.ventilation_data.get('eta_temp'),
                sup_temp=self.ventilation_data.get('sup_temp'),
                oda_temp=self.ventilation_data.get('oda_temp'),
                fan_speed=self.ventilation_data.get('fan_speed'),
                humidity=self.ventilation_data.get('humidity'),
                mode=self.ventilation_data.get('mode'),
                filter_status=self.ventilation_data.get('filter_status'),
                days_to_replace=self.ventilation_data.get('days_to_replace')
            )

    def update_climate_display(self):
        """Update climate display with current data"""
        # Clear existing widgets
        for i in reversed(range(self.climate_layout.count())):
            widget = self.climate_layout.itemAt(i).widget()
            if widget:
                widget.setParent(None)

        if not self.climate_data:
            no_data_label = QLabel("Geen kamerklimaat data beschikbaar")
            no_data_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            no_data_label.setStyleSheet("color: #666666; font-size: 14px;")
            self.climate_layout.addWidget(no_data_label)
            return

        # Group data per room (use room_name as key)
        rooms_data = {}

        for uuid, data in self.climate_data.items():
            # Extract room name
            room_name = self.extract_room_name(uuid, data)

            if room_name not in rooms_data:
                rooms_data[room_name] = {
                    'name': room_name,
                    'temperature': 0,
                    'humidity': 0,
                    'co2': None
                }

            # Update values from this data source
            self.update_room_data(rooms_data[room_name], data)

        # Add widgets for each room
        for room_name, room_data in rooms_data.items():
            climate_widget = SmartHomeWidget(room_name=room_data['name'])
            climate_widget.update_values(
                room_data['name'],
                room_data['temperature'],
                room_data['humidity'],
                room_data['co2']
            )

            self.climate_layout.addWidget(climate_widget)

    def update_room_data(self, room_dict, new_data):
        """Update room data with values from new_data"""
        if not isinstance(new_data, dict):
            return

        if 'properties' in new_data:
            props = new_data['properties']

            # Update temperature if available
            temp_keys = ['AmbientTemperature', 'Temperature', 'temp', 'temperature']
            for key in temp_keys:
                if key in props:
                    try:
                        room_dict['temperature'] = float(props[key])
                        break
                    except (ValueError, TypeError):
                        continue

            # Update humidity if available
            humidity_keys = ['Humidity', 'humidity', 'humidity_percent', 'RH']
            for key in humidity_keys:
                if key in props:
                    try:
                        room_dict['humidity'] = float(props[key])
                        break
                    except (ValueError, TypeError):
                        continue

            # Update CO2 if available
            co2_keys = ['CO2', 'co2', 'co2_ppm', 'carbon_dioxide']
            for key in co2_keys:
                if key in props:
                    try:
                        room_dict['co2'] = float(props[key])
                        break
                    except (ValueError, TypeError):
                        continue

    def fetch_data(self):
        """Fetch data from Redis and update displays"""
        try:
            # Fetch climate data for rooms
            climate_keys = self.redis_client.keys('climate:*')
            self.climate_data = {}

            for key in climate_keys:
                if key != 'climate:last_updated':
                    climate_json = self.redis_client.get(key)
                    if climate_json:
                        climate_data = self.safe_json_parse(climate_json)
                        if climate_data:
                            # Extract room name from key (e.g., climate:living_room -> living_room)
                            room_name = key.split(':', 1)[1]
                            self.climate_data[room_name] = climate_data

            # Alternative method: try a single climate key with all data
            if not self.climate_data:
                climate_all_json = self.redis_client.get('climate:all')
                if climate_all_json:
                    climate_all_data = self.safe_json_parse(climate_all_json)
                    if climate_all_data and isinstance(climate_all_data, dict):
                        self.climate_data = climate_all_data

            # NEW: Also fetch Duco climate data
            duco_climate_keys = self.redis_client.keys('duco:node:*')
            for key in duco_climate_keys:
                climate_json = self.redis_client.get(key)
                if climate_json:
                    duco_data = self.safe_json_parse(climate_json)
                    if duco_data and 'parameters' in duco_data:
                        # This data is already mapped to a Niko room
                        room_uuid = duco_data.get('uuid')  # duco_<node_id>
                        self.climate_data[room_uuid] = duco_data

            # Fetch ventilation system data
            ventilation_data = self.parse_ventilation_data()
            if ventilation_data:
                self.ventilation_data = ventilation_data
                self.logger.info("Ventilatie data geladen")
            else:
                self.ventilation_data = {}
                self.logger.debug("Geen ventilatie data beschikbaar")

            # Update displays
            self.update_climate_display()
            self.update_ventilation_display()
            self.update_status_bar()

        except Exception as e:
            self.logger.error(f"Fout bij ophalen data: {e}")
            self.status_bar.setText(f"Fout: {str(e)}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Create and show dashboard
    app = QApplication(sys.argv)
    app.setStyle('Fusion')

    dashboard = Dashboard()
    dashboard.show()

    try:
        # Run the application
        sys.exit(app.exec())
    finally:
        # Clean up
        pass
