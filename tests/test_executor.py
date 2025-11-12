from src.core.executor import QueryExecutor
from src.core.compiler import ExecutionStrategy


def test_select_runs_and_metrics_exist():
    ex = QueryExecutor()
    rows, metrics = ex.execute("SELECT 1 AS x", ExecutionStrategy.BALANCED)
    assert rows == [(1,)]
    assert metrics.duration_ms >= 0
    assert hasattr(metrics, "energy_joules")
    assert metrics.backend in ("rapl", "noop")
