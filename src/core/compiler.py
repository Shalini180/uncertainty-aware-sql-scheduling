from enum import Enum
from dataclasses import dataclass


class ExecutionStrategy(Enum):
    FAST = "fast"
    BALANCED = "balanced"
    EFFICIENT = "efficient"
    GPU = "gpu"


@dataclass
class QueryVariant:
    sql: str
    estimated_latency: float = 300.0
    estimated_energy: float = 10.0
