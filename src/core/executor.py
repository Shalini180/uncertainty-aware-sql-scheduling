import duckdb
from typing import Any, Tuple, Dict
from time import perf_counter
from src.monitoring.metrics import EnergyMetrics
from src.optimizer.selector import ExecutionStrategy


class QueryExecutor:
    def __init__(self, db_path: str = ":memory:"):
        self.conn = duckdb.connect(db_path)
        # seed a tiny table if empty
        self.conn.execute("CREATE TABLE IF NOT EXISTS t AS SELECT * FROM range(1000)")

    def _run(self, sql: str, factor: float) -> Tuple[list, EnergyMetrics]:
        start = perf_counter()
        result = self.conn.execute(sql).fetchall()
        dur_ms = (perf_counter() - start) * 1000
        # simple energy model: energy ∝ duration * factor
        power = 15.0 * factor
        energy_j = power * (dur_ms / 1000.0)
        return result, EnergyMetrics(energy_j, dur_ms, power)

    def execute(self, sql: str, strategy: ExecutionStrategy):
        factor = {"fast": 1.4, "balanced": 1.0, "efficient": 0.6}[strategy.value]
        return self._run(sql, factor)

    def compare_variants(
        self, sql: str
    ) -> Dict[ExecutionStrategy, Tuple[list, EnergyMetrics]]:
        out = {}
        for s in ExecutionStrategy:
            out[s] = self.execute(sql, s)
        return out
