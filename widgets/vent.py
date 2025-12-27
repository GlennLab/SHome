"""
Ventilation Box Widget for Duco System
Displays air flow, temperatures, humidity, and filter status
Optimized with adjustable wave spacing
"""
import sys
from collections import deque
from PyQt6.QtCore import QPointF, Qt, QRectF, QLineF, QTimer
from PyQt6.QtGui import (QPainter, QColor, QPen, QFont, QLinearGradient,
                         QPolygonF, QPainterPath, QBrush, QRadialGradient)
from PyQt6.QtWidgets import QWidget, QApplication

class VentilationBoxWidget(QWidget):
    """
    Ventilation box widget showing air flow diagram with temperatures
    Air flow paths:
    - ODA (Outdoor Air) → SUP (Supply to house) - Fresh air inlet
    - EHA (Extract from house) → ETA (Exhaust to outside) - Stale air outlet
    """
    # Temperature color thresholds
    TEMP_VERY_COLD = 0
    TEMP_COLD = 10
    TEMP_COOL = 15
    TEMP_COMFORTABLE = 20
    TEMP_WARM = 25
    # Filter warning thresholds
    FILTER_CRITICAL_DAYS = 30
    FILTER_WARNING_DAYS = 60

    def __init__(self, parent=None,
                 eha_temp: float = None,  # Exhaust Air (from house)
                 eta_temp: float = None,  # Extract Air (to outside)
                 sup_temp: float = None,  # Supply Air (to house)
                 oda_temp: float = None,  # Outdoor Air (from outside)
                 fan_speed: int = None,  # 0-100%
                 humidity: int = None,  # RH %
                 mode: str = None,  # "auto" or "manual"
                 filter_status: str = None,  # "ok", "dirty", "inactive"
                 days_to_replace: int = None,
                 name: str = "VENTILATION",
                 flow_amplitude: float = 1.0,  # Wave amplitude multiplier (0.1-3.0)
                 flow_frequency: float = 1.0,  # Wave frequency multiplier (0.1-5.0)
                 wave_spacing: float = 1.0):  # NEW: Distance between waves (0.5-3.0)
        super().__init__(parent)
        self.eha_temp = eha_temp if eha_temp is not None else 22.0
        self.eta_temp = eta_temp if eta_temp is not None else 14.6
        self.sup_temp = sup_temp if sup_temp is not None else 20.5
        self.oda_temp = oda_temp if oda_temp is not None else 12.3
        self.fan_speed = fan_speed if fan_speed is not None else 50
        self.humidity = humidity if humidity is not None else 43
        self.mode = mode if mode is not None else "auto"
        self.filter_status = filter_status if filter_status is not None else "ok"
        self.days_to_replace = days_to_replace if days_to_replace is not None else 175
        self.name = name

        # Flow animation parameters
        self.flow_amplitude = flow_amplitude
        self.flow_frequency = flow_frequency
        self.wave_spacing = wave_spacing  # NEW: Controls distance between peaks

        # Animation state
        self.flow_offset = 0

        # Create wave pattern with current parameters
        self.wave_pattern = self.create_wave_pattern(200)  # More points for smoother waves

        # Animation timer
        self.flow_timer = QTimer(self)
        self.flow_timer.timeout.connect(self.animate_flow)
        self.flow_timer.start(100)  # Slower for better performance

        self.setMinimumSize(600, 450)

    def get_temp_color(self, temp: float) -> QColor:
        """Get color based on temperature"""
        if temp < self.TEMP_VERY_COLD:
            return QColor(50, 100, 200)  # Deep blue
        elif temp < self.TEMP_COLD:
            return QColor(100, 150, 255)  # Blue
        elif temp < self.TEMP_COOL:
            return QColor(150, 200, 255)  # Light blue
        elif temp < self.TEMP_COMFORTABLE:
            return QColor(144, 238, 144)  # Light green (comfortable)
        elif temp < self.TEMP_WARM:
            return QColor(255, 200, 100)  # Orange
        else:
            return QColor(255, 100, 100)  # Red (hot)

    def get_filter_color(self) -> QColor:
        """Get filter icon color based on remaining days"""
        if self.days_to_replace <= self.FILTER_CRITICAL_DAYS:
            return QColor(255, 50, 50)  # Red - urgent
        elif self.days_to_replace <= self.FILTER_WARNING_DAYS:
            return QColor(255, 165, 0)  # Orange - warning
        else:
            return QColor(100, 200, 100)  # Green - ok

    def draw_flow_arrow(self, painter: QPainter,
                        center: QPointF,
                        direction: QPointF,
                        size: float,
                        color: QColor):
        """Draw a clean triangular arrow head"""
        dx, dy = direction.x(), direction.y()
        length = (dx * dx + dy * dy) ** 0.5
        if length == 0:
            return
        dx /= length
        dy /= length
        px, py = -dy, dx
        arrow = QPolygonF([
            QPointF(center.x() + dx * size, center.y() + dy * size),
            QPointF(center.x() - dx * size + px * size * 0.6,
                    center.y() - dy * size + py * size * 0.6),
            QPointF(center.x() - dx * size - px * size * 0.6,
                    center.y() - dy * size - py * size * 0.6),
        ])
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(QBrush(color))
        painter.drawPolygon(arrow)

    def draw_arrow(self, painter: QPainter, start: QPointF, end: QPointF, color: QColor):
        """Draw an arrow from start to end"""
        # Draw line
        pen = QPen(color, 4, Qt.PenStyle.SolidLine)
        painter.setPen(pen)
        painter.drawLine(QLineF(start, end))
        # Calculate arrow head
        dx = end.x() - start.x()
        dy = end.y() - start.y()
        length = (dx * dx + dy * dy) ** 0.5
        if length > 0:
            # Normalize
            dx /= length
            dy /= length
            # Arrow head size
            arrow_size = 15
            # Perpendicular vector
            px = -dy
            py = dx
            # Arrow head points
            arrow_head = QPolygonF([
                end,
                QPointF(end.x() - dx * arrow_size + px * arrow_size / 2,
                        end.y() - dy * arrow_size + py * arrow_size / 2),
                QPointF(end.x() - dx * arrow_size - px * arrow_size / 2,
                        end.y() - dy * arrow_size - py * arrow_size / 2)
            ])
            painter.setBrush(QBrush(color))
            painter.setPen(Qt.PenStyle.NoPen)
            painter.drawPolygon(arrow_head)

    def draw_temperature_circle(self, painter: QPainter, center: QPointF,
                                radius: float, temp: float, label: str):
        """Draw a temperature circle with gradient and text"""
        # Get temperature color
        temp_color = self.get_temp_color(temp)
        # Create radial gradient
        gradient = QRadialGradient(center, radius)
        gradient.setColorAt(0, temp_color.lighter(120))
        gradient.setColorAt(0.6, temp_color)
        gradient.setColorAt(1, temp_color.darker(155))
        # Draw circle
        painter.setBrush(QBrush(gradient))
        painter.setPen(QPen(QColor(255, 255, 255, 220), 3))
        painter.drawEllipse(center, radius, radius)
        # Draw label (ODA, SUP, etc.) at top
        font_label = QFont('Segoe UI', int(radius * 0.24), QFont.Weight.Bold)
        painter.setFont(font_label)
        painter.setPen(QPen(QColor(255, 255, 255, 200)))
        label_rect = painter.fontMetrics().boundingRect(label)
        label_pos = QPointF(center.x() - label_rect.width() / 2,
                            center.y() - radius * 0.35)
        painter.drawText(label_pos, label)
        # Draw temperature text in center
        font_temp = QFont('Segoe UI', int(radius * 0.40), QFont.Weight.Bold)
        painter.setFont(font_temp)
        painter.setPen(QPen(QColor(255, 255, 255)))
        temp_text = f"{temp:.1f}°C"
        text_rect = painter.fontMetrics().boundingRect(temp_text)
        text_pos = QPointF(center.x() - text_rect.width() / 2,
                           center.y() + text_rect.height() / 3)
        painter.drawText(text_pos, temp_text)

    def create_wave_pattern(self, length: int) -> deque:
        """Create a deque with a smooth wave pattern with adjustable spacing"""
        pattern = deque(maxlen=length)

        # NEW: Adjust number of complete wave cycles based on spacing
        # Lower spacing = more waves, higher spacing = fewer waves (longer fades)
        cycles = 5 / self.wave_spacing  # Inverse relationship

        for i in range(length):
            # Create smooth wave based on position
            pos = (i / length) * cycles  # Adjusted by wave_spacing

            # Smooth triangle wave (0-1-0 pattern)
            if pos % 1.0 < 0.5:
                # Rise phase: 0 to 1
                val = (pos % 1.0) * 2
            else:
                # Fall phase: 1 to 0
                val = 2 - (pos % 1.0) * 2

            # Apply amplitude scaling
            alpha = int(val * 80 * self.flow_amplitude)  # Higher base value for better visibility
            alpha = max(0, min(255, alpha))

            pattern.append(alpha)
        return pattern

    def animate_flow(self):
        if self.fan_speed > 0:
            self.flow_offset -= int(2 * self.flow_frequency)
            if self.flow_offset >= len(self.wave_pattern):
                self.flow_offset = 0
            self.update()

    def draw_duct(self, painter: QPainter, start: QPointF, end: QPointF,
                  temp: float, direction: str):
        temp_color = self.get_temp_color(temp)
        dx = end.x() - start.x()
        dy = end.y() - start.y()
        length = (dx * dx + dy * dy) ** 0.5
        if length == 0:
            return
        ux, uy = dx / length, dy / length
        px, py = -uy, ux
        duct_width = 50

        # ---- DUCT SHAPE ----
        path = QPainterPath()
        path.moveTo(start.x() + px * duct_width / 2, start.y() + py * duct_width / 2)
        path.lineTo(end.x() + px * duct_width / 2, end.y() + py * duct_width / 2)
        path.lineTo(end.x() - px * duct_width / 2, end.y() - py * duct_width / 2)
        path.lineTo(start.x() - px * duct_width / 2, start.y() - py * duct_width / 2)
        path.closeSubpath()

        # ---- BASE COLOR ----
        painter.save()
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(QBrush(temp_color.darker(160)))
        painter.drawPath(path)
        painter.restore()

        # ---- ANIMATED WAVE GRADIENT ----
        painter.save()
        painter.setCompositionMode(QPainter.CompositionMode.CompositionMode_SourceOver)

        # Create gradient along the duct
        grad = QLinearGradient(start, end)

        pattern_len = len(self.wave_pattern)

        # NEW: Adjust number of gradient stops based on wave spacing
        # More stops for better quality with longer fades
        num_stops = max(30, int(50 / self.wave_spacing))

        for i in range(num_stops + 1):
            pos = i / num_stops

            # Calculate pattern index with offset for animation
            pattern_idx = int(pos * pattern_len + self.flow_offset) % pattern_len

            # Get alpha from pattern
            alpha = self.wave_pattern[pattern_idx]

            # Apply final scaling for visibility
            alpha = min(200, int(alpha * 1.2))

            # Set gradient color at this position
            # In draw_duct(), instead of white waves:
            wave_color = self.get_temp_color(temp).lighter(150)
            grad.setColorAt(pos, QColor(wave_color.red(), wave_color.green(),
                                        wave_color.blue(), alpha))
            # grad.setColorAt(pos, QColor(255, 255, 255, alpha))

        painter.setBrush(QBrush(grad))
        painter.drawPath(path)
        painter.restore()

        # ---- OUTLINE ----
        painter.setPen(QPen(QColor(80, 80, 80), 2))
        painter.setBrush(Qt.BrushStyle.NoBrush)
        painter.drawPath(path)

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        width = self.width()
        height = self.height()

        # Calculate scaling
        base_width = 600
        base_height = 450
        scale = min(width / base_width, height / base_height)

        # Center the drawing
        offset_x = (width - base_width * scale) / 2
        offset_y = (height - base_height * scale) / 2
        painter.translate(offset_x, offset_y)
        painter.scale(scale, scale)

        # Define positions - box centered, circles on edges
        circle_radius = 40
        center_x = base_width / 2
        center_y = base_height / 2
        box_width = 400
        box_height = 280

        # Draw ventilation box with rounded corners
        box_rect = QRectF(center_x - box_width / 2, center_y - box_height / 2,
                          box_width, box_height)

        # Draw inside/outside separator
        painter.setPen(QPen(QColor(150, 150, 150), 2, Qt.PenStyle.DashLine))
        separator_x = box_rect.left() - 30
        painter.drawLine(QLineF(separator_x, 50, separator_x, base_height - 50))

        # Background tints
        painter.fillRect(QRectF(0, 0, separator_x, base_height),
                         QColor(30, 40, 60, 80))
        painter.fillRect(QRectF(separator_x, 0, base_width - separator_x, base_height),
                         QColor(40, 60, 40, 80))

        # Labels
        font_label = QFont('Segoe UI', 14, QFont.Weight.Bold)
        painter.setFont(font_label)
        painter.setPen(QPen(QColor(180, 180, 180)))
        painter.drawText(QRectF(10, 20, 150, 30), Qt.AlignmentFlag.AlignLeft, "Buiten")
        painter.drawText(QRectF(separator_x + 200, 20, 200, 30),
                         Qt.AlignmentFlag.AlignLeft, "Binnen")

        # Box gradient
        box_gradient = QLinearGradient(box_rect.topLeft(), box_rect.bottomRight())
        box_gradient.setColorAt(0, QColor(70, 70, 80))
        box_gradient.setColorAt(1, QColor(50, 50, 60))
        painter.setBrush(QBrush(box_gradient))
        painter.setPen(QPen(QColor(255, 255, 255, 100), 3))
        painter.drawRoundedRect(box_rect, 20, 20)

        # Circle positions
        oda_center = QPointF(box_rect.left() + circle_radius + 25,
                             box_rect.top() + circle_radius + 25)
        eta_center = QPointF(box_rect.left() + circle_radius + 25,
                             box_rect.bottom() - circle_radius - 25)
        sup_center = QPointF(box_rect.right() - circle_radius - 25,
                             box_rect.top() + circle_radius + 25)
        eha_center = QPointF(box_rect.right() - circle_radius - 25,
                             box_rect.bottom() - circle_radius - 25)

        # Connection points
        outside_margin = 10
        oda_outside = QPointF(outside_margin, oda_center.y())
        eta_outside = QPointF(outside_margin, eta_center.y())
        inside_margin = base_width - 10
        sup_inside = QPointF(inside_margin, sup_center.y())
        eha_inside = QPointF(inside_margin, eha_center.y())
        margin_offset = 10

        # Draw ducts
        self.draw_duct(painter, oda_outside,
                       QPointF(oda_center.x() - circle_radius + margin_offset, oda_center.y()),
                       self.oda_temp, "in")
        self.draw_duct(painter,
                       QPointF(eta_center.x() - circle_radius + margin_offset, eta_center.y()),
                       eta_outside, self.eta_temp, "out")
        self.draw_duct(painter,
                       QPointF(sup_center.x() + circle_radius - margin_offset, sup_center.y()),
                       sup_inside, self.sup_temp, "out")
        self.draw_duct(painter, eha_inside,
                       QPointF(eha_center.x() + circle_radius - margin_offset, eha_center.y()),
                       self.eha_temp, "in")

        # Draw temperature circles
        self.draw_temperature_circle(painter, oda_center, circle_radius,
                                     self.oda_temp, "ODA")
        self.draw_temperature_circle(painter, eta_center, circle_radius,
                                     self.eta_temp, "ETA")
        self.draw_temperature_circle(painter, sup_center, circle_radius,
                                     self.sup_temp, "SUP")
        self.draw_temperature_circle(painter, eha_center, circle_radius,
                                     self.eha_temp, "EHA")

        # Center info panel
        info_center_x = center_x
        info_center_y = center_y
        vertical_spacing = 60
        top_y = info_center_y - vertical_spacing
        middle_y = info_center_y + 15
        bottom_y = info_center_y + vertical_spacing + 30
        icon_x_offset = -75
        text_x_offset = 0

        # Fan speed
        font_icon = QFont('Segoe UI', 40, QFont.Weight.Bold)
        painter.setFont(font_icon)
        painter.setPen(QPen(QColor(100, 180, 255)))
        painter.drawText(QPointF(info_center_x + icon_x_offset, top_y), '🌀')
        font_value = QFont('Segoe UI', 20, QFont.Weight.Bold)
        painter.setFont(font_value)
        painter.setPen(QPen(QColor(200, 220, 255)))
        painter.drawText(QPointF(info_center_x + text_x_offset, top_y - 5), f'{self.fan_speed} %')

        # Humidity
        painter.setFont(font_icon)
        painter.setPen(QPen(QColor(100, 200, 255)))
        painter.drawText(QPointF(info_center_x + icon_x_offset, middle_y), '💦')
        painter.setFont(font_value)
        painter.setPen(QPen(QColor(200, 220, 255)))
        painter.drawText(QPointF(info_center_x + text_x_offset, middle_y - 5), f'{self.humidity} %')

        # Filter status
        painter.setFont(font_icon)
        filter_color = self.get_filter_color()
        painter.setPen(QPen(filter_color))
        painter.drawText(QPointF(info_center_x + icon_x_offset, bottom_y), '▨')
        font_days = QFont('Segoe UI', 20, QFont.Weight.Bold)
        painter.setFont(font_days)
        if self.days_to_replace <= self.FILTER_CRITICAL_DAYS:
            text_color = QColor(255, 100, 100)
        elif self.days_to_replace <= self.FILTER_WARNING_DAYS:
            text_color = QColor(255, 200, 100)
        else:
            text_color = QColor(150, 150, 150)
        painter.setPen(QPen(text_color))
        painter.drawText(QPointF(info_center_x + text_x_offset - 18, bottom_y - 5),
                         f'{self.days_to_replace} days')

        # Mode indicator
        if self.mode:
            font_mode = QFont('Segoe UI', 16, QFont.Weight.Bold)
            painter.setFont(font_mode)
            mode_color = QColor(100, 255, 100) if self.mode.lower() == "auto" else QColor(255, 200, 100)
            painter.setPen(QPen(mode_color))
            mode_text = self.mode.upper()
            mode_rect = painter.fontMetrics().boundingRect(mode_text)
            painter.drawText(QPointF(center_x - mode_rect.width() / 2,
                                     box_rect.top() + 30), mode_text)

        # Filter status text
        if self.filter_status and self.filter_status.lower() != "ok":
            font_status = QFont('Segoe UI', 10, QFont.Weight.Bold)
            painter.setFont(font_status)
            if self.filter_status.lower() == "dirty":
                status_color = QColor(255, 150, 0)
                status_text = "⚠ FILTER DIRTY"
            elif self.filter_status.lower() == "inactive":
                status_color = QColor(150, 150, 150)
                status_text = "○ INACTIVE"
            else:
                status_color = QColor(200, 200, 200)
                status_text = self.filter_status.upper()
            painter.setPen(QPen(status_color))
            status_rect = painter.fontMetrics().boundingRect(status_text)
            painter.drawText(QPointF(center_x - status_rect.width() / 2,
                                     box_rect.bottom() - 10), status_text)

    def update_values(self, eha_temp=None, eta_temp=None, sup_temp=None,
                      oda_temp=None, fan_speed=None, humidity=None,
                      mode=None, filter_status=None, days_to_replace=None,
                      flow_amplitude=None, flow_frequency=None,
                      wave_spacing=None):  # NEW: wave_spacing parameter
        """Update widget values and refresh display"""
        if eha_temp is not None:
            self.eha_temp = eha_temp
        if eta_temp is not None:
            self.eta_temp = eta_temp
        if sup_temp is not None:
            self.sup_temp = sup_temp
        if oda_temp is not None:
            self.oda_temp = oda_temp
        if fan_speed is not None:
            self.fan_speed = fan_speed
        if humidity is not None:
            self.humidity = humidity
        if mode is not None:
            self.mode = mode
        if filter_status is not None:
            self.filter_status = filter_status
        if days_to_replace is not None:
            self.days_to_replace = days_to_replace

        # Update flow parameters and recreate wave pattern
        changed = False
        if flow_amplitude is not None:
            self.flow_amplitude = max(0.1, min(3.0, flow_amplitude))
            changed = True
        if flow_frequency is not None:
            self.flow_frequency = max(0.1, min(5.0, flow_frequency))
            changed = True
        if wave_spacing is not None:  # NEW: Handle wave_spacing
            self.wave_spacing = max(0.5, min(3.0, wave_spacing))  # Clamp to reasonable range
            changed = True

        if changed:
            self.wave_pattern = self.create_wave_pattern(200)

        self.update()

# Demo application with wave spacing examples
if __name__ == "__main__":
    app = QApplication(sys.argv)

    # Demo 1: Tight waves (short fades)
    window1 = VentilationBoxWidget(
        oda_temp=12.3, sup_temp=20.5, eha_temp=22.0, eta_temp=14.6,
        fan_speed=50, humidity=43, mode="auto", filter_status="ok", days_to_replace=175,
        flow_amplitude=1.0, flow_frequency=1.0, wave_spacing=5  # Tight spacing
    )
    window1.setWindowTitle("Tight Waves (Short Fades)")
    window1.resize(700, 500)
    window1.move(100, 100)
    window1.show()

    # Demo 2: Medium waves (balanced)
    window2 = VentilationBoxWidget(
        oda_temp=8.5, sup_temp=18.2, eha_temp=21.5, eta_temp=12.1,
        fan_speed=65, humidity=55, mode="manual", filter_status="ok", days_to_replace=45,
        flow_amplitude=1.5, flow_frequency=2, wave_spacing= 5 # Medium spacing
    )
    window2.setWindowTitle("Medium Waves (Balanced)")
    window2.resize(700, 500)
    window2.move(850, 100)
    window2.show()

    # Demo 3: Wide waves (long fades)
    window3 = VentilationBoxWidget(
        oda_temp=-2.3, sup_temp=16.8, eha_temp=23.5, eta_temp=8.4,
        fan_speed=80, humidity=62, mode="auto", filter_status="dirty", days_to_replace=15,
        flow_amplitude=0.8, flow_frequency=1.5, wave_spacing=5 # Wide spacing
    )
    window3.setWindowTitle("Wide Waves (Long Fades)")
    window3.resize(700, 500)
    window3.move(100, 650)
    window3.show()

    # Demo 4: Very wide waves (very long fades)
    window4 = VentilationBoxWidget(
        oda_temp=28.5, sup_temp=26.2, eha_temp=24.8, eta_temp=27.3,
        fan_speed=35, humidity=38, mode="auto", filter_status="ok", days_to_replace=120,
        flow_amplitude=1.2, flow_frequency=1.5, wave_spacing=5  # Very wide spacing
    )
    window4.setWindowTitle("Very Wide Waves (Very Long Fades)")
    window4.resize(700, 500)
    window4.move(850, 650)
    window4.show()

    sys.exit(app.exec())