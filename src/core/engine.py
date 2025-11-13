from typing import Any, Tuple, Optional, Dict
from src.core.compiler import MultiVariantCompiler
from src.core.executor import QueryExecutor
from src.monitoring.metrics import EnergyMetrics
from src.optimizer.carbon_api import CarbonAPI
from src.optimizer.selector import (
    CarbonAwareSelector,
    QueryUrgency,
    SelectionContext,
    SelectionDecision,
    ExecutionStrategy,
)


class CarbonAwareQueryEngine:
    def __init__(self, db_path: str = ":memory:"):
        self.compiler = MultiVariantCompiler(db_path)
        self.executor = QueryExecutor(db_path)
        self.carbon_api = CarbonAPI()
        self.selector = CarbonAwareSelector(self.carbon_api)

    def execute_query(
        self,
        sql: str,
        urgency: QueryUrgency = QueryUrgency.MEDIUM,
        explain: bool = False,
    ) -> Tuple[Any, EnergyMetrics, Optional[SelectionDecision]]:
        carbon = self.carbon_api.get_current_intensity()
        variants = self.compiler.compile(sql)
        context = SelectionContext(sql, urgency, carbon, variants)
        decision = self.selector.select(context)
        if decision.should_defer:
            return None, None, decision
        result, metrics = self.executor.execute(sql, decision.selected_strategy)
        if explain:
            print(decision.explain())
            print(
                f"Energy: {metrics.energy_joules:.2f} J, Duration: {metrics.duration_ms:.2f} ms, Carbon: {metrics.carbon_grams(carbon.value):.4f} g"
            )
        return result, metrics, decision

    def compare_strategies(self, sql: str) -> Dict:
        carbon = self.carbon_api.get_current_intensity()
        results = self.executor.compare_variants(sql)
        return {
            s.value: {
                "energy_joules": m.energy_joules,
                "duration_ms": m.duration_ms,
                "power_watts": m.power_watts,
                "carbon_grams": m.carbon_grams(carbon.value),
                "result_count": len(r) if isinstance(r, list) else 1,
            }
            for s, (r, m) in results.items()
        }
