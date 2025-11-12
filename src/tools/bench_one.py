import sys
from src.core.executor import QueryExecutor
from src.core.compiler import ExecutionStrategy

if __name__ == "__main__":
    sql = sys.argv[1] if len(sys.argv) > 1 else "SELECT 42"
    ex = QueryExecutor()
    for strat in (
        ExecutionStrategy.FAST,
        ExecutionStrategy.BALANCED,
        ExecutionStrategy.EFFICIENT,
    ):
        rows, m = ex.execute(sql, strat)
        print(
            f"{strat.value:9}  rows={len(rows)}  t={m.duration_ms:.2f}ms  E={m.energy_joules:.4f}J  P={m.power_watts:.2f}W  [{m.backend}]"
        )
