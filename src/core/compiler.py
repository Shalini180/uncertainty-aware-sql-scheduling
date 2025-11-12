from enum import Enum
from dataclasses import dataclass
from typing import Dict
from src.utils.query_parser import QueryAnalyzer


class ExecutionStrategy(Enum):
    FAST = "fast"
    BALANCED = "balanced"
    EFFICIENT = "efficient"
    GPU = "gpu"


@dataclass
class QueryVariant:
    sql: str
    estimated_latency: float  # ms
    estimated_energy: float  # J


class MultiVariantCompiler:
    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self.analyzer = QueryAnalyzer()

    def compile(self, sql: str) -> Dict[ExecutionStrategy, QueryVariant]:
        a = self.analyzer.analyze(sql)
        base_latency = (
            200.0
            + 120.0 * a["has_join"]
            + 100.0 * a["has_aggregation"]
            + 50.0 * a["has_sort"]
        )
        base_latency += 40.0 * (len(a["tables"]) - 1)
        base_energy = (
            8.0 + 4.0 * a["has_join"] + 3.0 * a["has_aggregation"] + 1.0 * a["has_sort"]
        )

        variants = {
            ExecutionStrategy.FAST: QueryVariant(
                sql, base_latency * 0.7, base_energy * 1.25
            ),
            ExecutionStrategy.BALANCED: QueryVariant(
                sql, base_latency * 1.0, base_energy * 1.0
            ),
            ExecutionStrategy.EFFICIENT: QueryVariant(
                sql, base_latency * 1.5, base_energy * 0.7
            ),
        }
        return variants
