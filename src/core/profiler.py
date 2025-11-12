from dataclasses import dataclass


@dataclass
class EnergyMetrics:
    duration_ms: float = 0.0
    energy_joules: float = 0.0
    power_watts: float = 0.0
