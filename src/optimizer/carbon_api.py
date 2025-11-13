from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, List


@dataclass
class CarbonIntensity:
    value: float  # gCO2/kWh
    timestamp: datetime
    zone: str
    source: str

    def is_low(self, threshold: float = 250) -> bool:
        return self.value < threshold

    def is_high(self, threshold: float = 500) -> bool:
        return self.value > threshold


class CarbonAPI:
    def __init__(self, zone: str = "US-CAL-CISO"):
        self.zone = zone

    def get_current_intensity(self) -> CarbonIntensity:
        # simple fallback curve
        h = datetime.now().hour
        if 10 <= h <= 16:
            val = 220 + abs(h - 13) * 25
        elif 17 <= h <= 21:
            val = 520 + (h - 17) * 20
        else:
            val = 380
        return CarbonIntensity(val, datetime.now(), self.zone, "historical_pattern")

    def get_forecast(self, hours: int = 24) -> List[CarbonIntensity]:
        now = datetime.now()
        return [
            self.get_current_intensity().__class__(
                value=self.get_current_intensity().value,
                timestamp=now + timedelta(hours=i),
                zone=self.zone,
                source="forecast",
            )
            for i in range(hours)
        ]
