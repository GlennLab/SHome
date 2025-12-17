import sys
from enum import StrEnum

from PyQt6.QtCore import QPointF, Qt, QRectF
from PyQt6.QtGui import QPainter, QColor, QPen, QFont, QLinearGradient, QConicalGradient
from PyQt6.QtWidgets import QWidget, QApplication


class Units(StrEnum):
    DEGREE = "°C"
    FAHRENHEIT = "°F"
    RELATIVE_HUMIDITY = "%RV"
    PARTS_PER_MILLION = "ppm"


class SmartHomeWidget(QWidget):
    """Combined sensor widget with gradient background and colored CO2 arcs"""

    # Color thresholds
    TEMP_COLD = 18
    TEMP_COOL = 20
    TEMP_WARM = 24
    TEMP_HOT = 28

    HUMIDITY_LOW = 30
    HUMIDITY_GOOD_LOW = 40
    HUMIDITY_GOOD_HIGH = 60
    HUMIDITY_HIGH = 70

    CO2_GOOD = 600
    CO2_MODERATE = 800
    CO2_ELEVATED = 1000
    CO2_HIGH = 1500

    def __init__(self, parent=None, temperature: float = None,
                 humidity: float = None, co2: int = None):
        super().__init__(parent)
        self.temperature = temperature
        self.humidity = humidity
        self.co2 = co2
        self.temperature_unit = Units.DEGREE
        self.humidity_unit = Units.RELATIVE_HUMIDITY
        self.co2_unit = Units.PARTS_PER_MILLION

        self.setMinimumSize(250, 250)

    def get_temperature_color(self):
        """Return color based on temperature level"""
        if self.temperature is None:
            return QColor(180, 180, 180)  # Neutral gray
        if self.temperature < self.TEMP_COLD:
            return QColor(100, 150, 255)  # Cold blue
        elif self.temperature < self.TEMP_COOL:
            return QColor(150, 200, 255)  # Cool light blue
        elif self.temperature < self.TEMP_WARM:
            return QColor(144, 238, 144)  # Comfortable green
        elif self.temperature < self.TEMP_HOT:
            return QColor(255, 200, 100)  # Warm orange
        else:
            return QColor(255, 100, 100)  # Hot red

    def get_humidity_color(self):
        """Return color based on humidity level"""
        if self.humidity is None:
            return QColor(180, 180, 180)  # Neutral gray
        if self.humidity < self.HUMIDITY_LOW:
            return QColor(255, 200, 150)  # Dry orange
        elif self.humidity < self.HUMIDITY_GOOD_LOW:
            return QColor(200, 230, 180)  # Light yellow-green
        elif self.humidity <= self.HUMIDITY_GOOD_HIGH:
            return QColor(150, 220, 255)  # Good light blue
        elif self.humidity <= self.HUMIDITY_HIGH:
            return QColor(100, 180, 255)  # Moist blue
        else:
            return QColor(80, 150, 230)  # Too humid dark blue

    def get_co2_color(self):
        """Return color based on CO2 level"""
        if self.co2 is None:
            return QColor(180, 180, 180)  # Neutral gray
        if self.co2 < self.CO2_GOOD:
            return QColor(100, 255, 100)  # Excellent bright green
        elif self.co2 < self.CO2_MODERATE:
            return QColor(144, 238, 144)  # Good green
        elif self.co2 < self.CO2_ELEVATED:
            return QColor(255, 220, 100)  # Moderate yellow
        elif self.co2 < self.CO2_HIGH:
            return QColor(255, 160, 100)  # Elevated orange
        else:
            return QColor(255, 100, 100)  # High red

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)

        width = self.width()
        height = self.height()
        center_x = width / 2
        center_y = height / 2

        # Responsive sizing
        dot_radius = min(width, height) * 0.35
        arc_width = max(12, int(dot_radius * 0.15))
        arc_radius = dot_radius + arc_width / 2 + 5

        dot_center = QPointF(center_x, center_y)

        # Get colors for current values
        temp_color = self.get_temperature_color()
        humidity_color = self.get_humidity_color()
        co2_color = self.get_co2_color()

        # Draw CO2 arc segments around the circle
        # Create 8 segments around the bottom half (from 180° to 360° / -180° to 0°)
        pen = QPen()
        pen.setWidth(arc_width)
        pen.setCapStyle(Qt.PenCapStyle.RoundCap)
        painter.setPen(pen)
        painter.setBrush(Qt.BrushStyle.NoBrush)

        # CO2 arc spans top 240 degrees (rotated 180°, from -30° to 210°)
        start_angle = -30  # degrees (top area)
        span_angle = 240  # degrees
        segment_count = 12
        segment_span = span_angle / segment_count
        gap = 3  # degrees gap between segments

        rect = QRectF(
            center_x - arc_radius,
            center_y - arc_radius,
            arc_radius * 2,
            arc_radius * 2
        )

        for i in range(segment_count):
            # Calculate segment angle
            seg_start = start_angle + i * segment_span
            seg_span = segment_span - gap

            # Interpolate color based on position (darker at edges, brighter in middle)
            progress = abs(i - segment_count / 2) / (segment_count / 2)
            color = QColor(co2_color)
            # Fade edges
            color.setAlpha(int(255 * (1 - progress * 0.5)))

            pen.setColor(color)
            painter.setPen(pen)

            # Draw arc segment (Qt uses 1/16th degree units)
            painter.drawArc(rect, int(seg_start * 16), int(seg_span * 16))

        # Draw main circle with gradient background
        # Top half: temperature gradient (based on temp value)
        # Bottom half: humidity gradient (based on humidity value)
        gradient = QLinearGradient(center_x, center_y - dot_radius, center_x, center_y + dot_radius)

        # Top half - temperature gradient intensity based on how far from comfortable
        if self.temperature is not None:
            temp_intensity = abs(self.temperature - 22) / 10  # 22°C as ideal
            temp_intensity = min(temp_intensity, 1.0)
        else:
            temp_intensity = 0
        temp_light = QColor(temp_color.lighter(int(130 - temp_intensity * 30)))
        temp_mid = QColor(temp_color)
        temp_dark = QColor(temp_color.darker(int(105 + temp_intensity * 10)))

        gradient.setColorAt(0, temp_light)
        gradient.setColorAt(0.35, temp_mid)
        gradient.setColorAt(0.48, temp_dark)

        # Create smooth transition in the middle
        blend_color = QColor(
            (temp_color.red() + humidity_color.red()) // 2,
            (temp_color.green() + humidity_color.green()) // 2,
            (temp_color.blue() + humidity_color.blue()) // 2
        )
        gradient.setColorAt(0.5, blend_color)

        # Bottom half - humidity gradient intensity based on how far from optimal
        if self.humidity is not None:
            humidity_intensity = abs(self.humidity - 50) / 40  # 50% as ideal
            humidity_intensity = min(humidity_intensity, 1.0)
        else:
            humidity_intensity = 0
        humidity_light = QColor(humidity_color.lighter(int(105 + humidity_intensity * 10)))
        humidity_mid = QColor(humidity_color)
        humidity_dark = QColor(humidity_color.darker(int(115 + humidity_intensity * 25)))

        gradient.setColorAt(0.52, humidity_light)
        gradient.setColorAt(0.65, humidity_mid)
        gradient.setColorAt(1, humidity_dark)

        painter.setBrush(gradient)
        pen = QPen(QColor("white"))
        pen.setWidth(max(3, int(dot_radius * 0.08)))
        painter.setPen(pen)
        painter.drawEllipse(dot_center, dot_radius, dot_radius)

        # Draw horizontal divider line in the middle
        pen = QPen(QColor(255, 255, 255, 120))
        pen.setWidth(max(1, int(dot_radius * 0.02)))
        painter.setPen(pen)
        line_length = dot_radius * 0.85
        painter.drawLine(
            QPointF(center_x - line_length, center_y),
            QPointF(center_x + line_length, center_y)
        )

        # Font setup
        font_size = max(12, int(dot_radius * 0.24))
        font = QFont('Segoe UI', font_size, QFont.Weight.Bold)
        painter.setFont(font)
        font_metrics = painter.fontMetrics()

        # Draw temperature (top half)
        painter.setPen(QPen(QColor("white")))
        temp_text = f'{self.temperature:.1f}°' if self.temperature is not None else '-'
        text_width = font_metrics.horizontalAdvance(temp_text)
        text_height = font_metrics.height()
        x = center_x - text_width / 2
        y = center_y - dot_radius * 0.25
        painter.drawText(QPointF(x, y), temp_text)

        # Draw humidity (bottom half)
        humidity_text = f'{self.humidity:.0f}%' if self.humidity is not None else '-'
        text_width = font_metrics.horizontalAdvance(humidity_text)
        x = center_x - text_width / 2
        y = center_y + dot_radius * 0.35
        painter.drawText(QPointF(x, y), humidity_text)

        # Draw CO2 value in the open section at the bottom
        font_size_co2 = max(13, int(dot_radius * 0.2))
        font_co2 = QFont('Segoe UI', font_size_co2, QFont.Weight.Bold)
        painter.setFont(font_co2)

        # CO2 text positioned below circle in open arc section
        co2_text = f'{self.co2:.0f} {self.co2_unit.value}' if self.co2 is not None else '-'
        text_width = painter.fontMetrics().horizontalAdvance(co2_text)
        text_height = painter.fontMetrics().height()

        x = center_x - text_width / 2
        y = center_y + dot_radius + text_height - 5

        # Draw subtle glow effect for CO2 text
        painter.setPen(QPen(QColor(255, 255, 255, 60)))
        for offset in range(3, 0, -1):
            painter.drawText(QPointF(x - offset, y), co2_text)
            painter.drawText(QPointF(x + offset, y), co2_text)
            painter.drawText(QPointF(x, y - offset), co2_text)
            painter.drawText(QPointF(x, y + offset), co2_text)

        # Draw main CO2 text with shadow
        painter.setPen(QPen(QColor(0, 0, 0, 120)))
        painter.drawText(QPointF(x + 1, y + 1), co2_text)

        # Draw CO2 text in color
        painter.setPen(QPen(co2_color.lighter(130)))
        painter.drawText(QPointF(x, y), co2_text)

        # Draw small labels
        font_label = QFont('Segoe UI', max(8, int(dot_radius * 0.14)))
        painter.setFont(font_label)
        painter.setPen(QPen(QColor(255, 255, 255, 200)))

        # Temperature label (top)
        temp_label = "TEMP"
        text_width = painter.fontMetrics().horizontalAdvance(temp_label)
        x = center_x - text_width / 2
        y = center_y - dot_radius * 0.52
        painter.drawText(QPointF(x, y), temp_label)

        # Humidity label (bottom)
        hum_label = "HUMID"
        text_width = painter.fontMetrics().horizontalAdvance(hum_label)
        x = center_x - text_width / 2
        y = center_y + dot_radius * 0.6
        painter.drawText(QPointF(x, y), hum_label)

    def update_values(self, temperature: float = None, humidity: float = None, co2: int = None):
        """Update sensor values and refresh display"""
        if temperature is not None:
            self.temperature = temperature
        if humidity is not None:
            self.humidity = humidity
        if co2 is not None:
            self.co2 = co2
        self.update()


if __name__ == "__main__":
    app = QApplication(sys.argv)

    # Demo with different scenarios
    # Scenario 1: Good conditions
    window1 = SmartHomeWidget(temperature=22.0, humidity=50, co2=550)
    window1.setWindowTitle("Good Conditions")
    window1.resize(300, 300)
    window1.move(100, 100)
    window1.show()

    # Scenario 2: High CO2
    window2 = SmartHomeWidget(temperature=23.5, humidity=55, co2=1200)
    window2.setWindowTitle("High CO₂")
    window2.resize(300, 300)
    window2.move(450, 100)
    window2.show()

    # Scenario 3: Missing temperature
    window3 = SmartHomeWidget(temperature=None, humidity=45, co2=700)
    window3.setWindowTitle("Missing Temp")
    window3.resize(300, 300)
    window3.move(800, 100)
    window3.show()

    # Scenario 4: Missing CO2
    window4 = SmartHomeWidget(temperature=29.0, humidity=25, co2=None)
    window4.setWindowTitle("Missing CO₂")
    window4.resize(300, 300)
    window4.move(100, 450)
    window4.show()

    sys.exit(app.exec())