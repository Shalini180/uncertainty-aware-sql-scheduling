from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Any, List, Optional
import json
from datetime import datetime


@dataclass
class EnergyMetrics:
    energy_joules: float
    duration_ms: float
    power_watts: float

    def carbon_grams(self, carbon_intensity_g_per_kwh: float) -> float:
        # 1 kWh = 3_600_000 J
        kwh = self.energy_joules / 3_600_000
        return kwh * carbon_intensity_g_per_kwh


class MetricsCollector:
    def __init__(self) -> None:
        self.metrics_history: List[Dict[str, Any]] = []

    def record(
        self,
        *,
        query: str,
        variant: str,
        metrics: EnergyMetrics,
        carbon_intensity: float,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        row = {
            "ts": datetime.utcnow().isoformat(),
            "query": query,
            "variant": variant,
            "energy_joules": metrics.energy_joules,
            "duration_ms": metrics.duration_ms,
            "power_watts": metrics.power_watts,
            "carbon_grams": metrics.carbon_grams(carbon_intensity),
            "carbon_intensity": carbon_intensity,
        }
        if metadata:
            row["metadata"] = metadata
        self.metrics_history.append(row)

    def summary(self) -> Dict[str, Any]:
        if not self.metrics_history:
            return {}
        e = [r["energy_joules"] for r in self.metrics_history]
        d = [r["duration_ms"] for r in self.metrics_history]
        c = [r["carbon_grams"] for r in self.metrics_history]
        return {
            "count": len(self.metrics_history),
            "total_energy_joules": sum(e),
            "avg_duration_ms": sum(d) / len(d),
            "total_carbon_grams": sum(c),
        }

    def save(self, filename: Optional[str] = None) -> str:
        Path("data/results").mkdir(parents=True, exist_ok=True)
        if not filename:
            filename = f"data/results/metrics_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, "w") as f:
            json.dump(self.metrics_history, f, indent=2)
        return filename
