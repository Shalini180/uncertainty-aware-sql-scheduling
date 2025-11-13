import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from src.core.engine import CarbonAwareQueryEngine
from src.optimizer.selector import QueryUrgency
from evaluation.benchmarks.tpch_queries import TPCHBenchmark


class ExperimentRunner:
    def __init__(self, output_dir: str = "data/results"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.engine = CarbonAwareQueryEngine()
        self.benchmark = TPCHBenchmark(scale_factor=0.01)
        self.results: List[Dict] = []

    def setup(self):
        if not (Path("data/tpch") / "lineitem.parquet").exists():
            self.benchmark.generate_data()
        import duckdb

        conn = duckdb.connect(":memory:")
        self.benchmark.load_data(conn)
        conn.close()

    def run_baseline_comparison(self, iterations: int = 3):
        queries = self.benchmark.get_queries()
        for qn, sql in queries.items():
            for i in range(iterations):
                _, mf, _ = self.engine.execute_query(sql, QueryUrgency.HIGH)
                _, ma, dec = self.engine.execute_query(sql, QueryUrgency.MEDIUM)
                self.results += [
                    {
                        "query": qn,
                        "iteration": i,
                        "approach": "baseline",
                        "strategy": "fast",
                        "energy_joules": mf.energy_joules,
                        "duration_ms": mf.duration_ms,
                        "carbon_grams": 0.0,
                    },
                    {
                        "query": qn,
                        "iteration": i,
                        "approach": "carbon_aware",
                        "strategy": dec.selected_strategy.value,
                        "energy_joules": ma.energy_joules,
                        "duration_ms": ma.duration_ms,
                        "carbon_grams": 0.0,
                    },
                ]

    def run_carbon_intensity_experiments(self, iterations: int = 2):
        levels = [
            ("low", 200, QueryUrgency.LOW),
            ("medium", 400, QueryUrgency.MEDIUM),
            ("high", 600, QueryUrgency.HIGH),
        ]
        queries = list(self.benchmark.get_queries().items())[:2]
        for qn, sql in queries:
            for name, val, urg in levels:
                for i in range(iterations):
                    _, m, dec = self.engine.execute_query(sql, urg)
                    self.results.append(
                        {
                            "query": qn,
                            "iteration": i,
                            "carbon_level": name,
                            "carbon_intensity": val,
                            "urgency": urg.value,
                            "strategy": dec.selected_strategy.value,
                            "energy_joules": m.energy_joules,
                            "duration_ms": m.duration_ms,
                            "carbon_grams": 0.0,
                        }
                    )

    def run_urgency_experiments(self, iterations: int = 2):
        queries = list(self.benchmark.get_queries().items())[:2]
        for qn, sql in queries:
            for urg in [
                QueryUrgency.CRITICAL,
                QueryUrgency.HIGH,
                QueryUrgency.MEDIUM,
                QueryUrgency.LOW,
                QueryUrgency.BATCH,
            ]:
                for i in range(iterations):
                    res = self.engine.execute_query(sql, urg)
                    _, m, dec = res
                    if dec.should_defer:
                        self.results.append(
                            {
                                "query": qn,
                                "iteration": i,
                                "urgency": urg.value,
                                "strategy": dec.selected_strategy.value,
                                "defer_minutes": dec.defer_minutes,
                                "deferred": True,
                            }
                        )
                    else:
                        self.results.append(
                            {
                                "query": qn,
                                "iteration": i,
                                "urgency": urg.value,
                                "strategy": dec.selected_strategy.value,
                                "energy_joules": m.energy_joules,
                                "duration_ms": m.duration_ms,
                                "carbon_grams": 0.0,
                                "deferred": False,
                            }
                        )

    def save_results(self) -> str:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = self.output_dir / f"experiments_{ts}.csv"
        pd.DataFrame(self.results).to_csv(out, index=False)
        return str(out)

    def run_all_experiments(self):
        self.setup()
        self.run_baseline_comparison()
        self.run_carbon_intensity_experiments()
        self.run_urgency_experiments()
        path = self.save_results()
        print(f"Saved results: {path}")


if __name__ == "__main__":
    ExperimentRunner().run_all_experiments()
