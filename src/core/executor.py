import duckdb
from typing import List, Tuple, Any
from src.core.profiler import measure, to_metrics
from src.core.compiler import ExecutionStrategy


class QueryExecutor:
    def __init__(self, db_path: str = ":memory:"):
        self.con = duckdb.connect(db_path)

    def _apply_strategy(self, strategy: ExecutionStrategy):
        # Place to tune pragmas per strategy in future
        # e.g., self.con.execute("PRAGMA threads=...;")
        pass

    def execute(
        self, sql: str, strategy: ExecutionStrategy
    ) -> Tuple[List[tuple], object]:
        self._apply_strategy(strategy)
        with measure() as m:
            res = self.con.execute(sql).fetchall()
        return res, to_metrics(m)
