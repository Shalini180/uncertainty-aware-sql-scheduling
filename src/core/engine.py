# src/core/engine.py
from typing import Any, Tuple, Optional
import logging
from datetime import datetime

# Import with compatibility
try:
    from src.core.profiler import EnergyProfiler, EnergyMetrics
except ImportError:
    # Fallback: Use your existing energy measurement code
    from src.core.profiler import EnergyMeasurement

    # Create simple wrapper
    class EnergyProfiler:
        def __init__(self):
            self.measurement = EnergyMeasurement()

        def profile(self, func, *args, **kwargs):
            """Profile a function's energy consumption"""
            import time

            start_time = time.time()
            result = func(*args, **kwargs)
            duration = (time.time() - start_time) * 1000  # ms

            # Get energy measurement
            energy = self.measurement.get_energy_usage()  # Adjust method name

            # Create simple metrics object
            class EnergyMetrics:
                def __init__(self, energy, duration):
                    self.energy_joules = energy
                    self.duration_ms = duration
                    self.power_watts = (energy / duration) * 1000 if duration > 0 else 0

                def carbon_grams(self, carbon_intensity):
                    kwh = self.energy_joules / 3_600_000
                    return kwh * carbon_intensity

            metrics = EnergyMetrics(energy, duration)
            return result, metrics


# src/core/engine.py
from typing import Any, Tuple, Optional
import logging
from datetime import datetime

from src.core.compiler import MultiVariantCompiler, ExecutionStrategy
from src.core.profiler import EnergyProfiler, EnergyMetrics
from src.optimizer.carbon_api import CarbonAPI
from src.optimizer.selector import (
    CarbonAwareSelector,
    QueryUrgency,
    SelectionContext,
    SelectionDecision,
)
from src.monitoring.metrics import MetricsCollector

logger = logging.getLogger(__name__)


class CarbonAwareQueryEngine:
    """
    Main query engine that orchestrates carbon-aware query execution

    Usage:
        engine = CarbonAwareQueryEngine(db_path='production.db')
        result, metrics, decision = engine.execute(sql, urgency=QueryUrgency.MEDIUM)
    """

    def __init__(self, db_path: str = ":memory:", enable_logging: bool = True):
        """
        Initialize the carbon-aware query engine

        Args:
            db_path: Path to DuckDB database file
            enable_logging: Whether to enable detailed logging
        """
        self.db_path = db_path

        # Initialize components
        self.compiler = MultiVariantCompiler(db_path)
        self.profiler = EnergyProfiler()
        self.carbon_api = CarbonAPI()
        self.selector = CarbonAwareSelector(self.carbon_api)
        self.metrics_collector = MetricsCollector()

        if enable_logging:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            )

        logger.info(f"CarbonAwareQueryEngine initialized with db_path={db_path}")

    def execute(
        self,
        sql: str,
        urgency: QueryUrgency = QueryUrgency.MEDIUM,
        explain: bool = False,
    ) -> Tuple[Any, Optional[EnergyMetrics], SelectionDecision]:
        """
        Execute a query with carbon-aware optimization

        Args:
            sql: SQL query string
            urgency: Query urgency level (affects variant selection)
            explain: Whether to print detailed explanation

        Returns:
            Tuple of (query_result, energy_metrics, selection_decision)
        """
        logger.info(f"Executing query with urgency={urgency.value}")

        # Step 1: Get current carbon intensity
        carbon = self.carbon_api.get_current_intensity()
        logger.info(f"Carbon intensity: {carbon.value:.0f} gCO2/kWh")

        # Step 2: Compile query into variants
        variants = self.compiler.compile(sql)
        logger.info(f"Generated {len(variants)} execution variants")

        # Step 3: Select optimal variant
        context = SelectionContext(
            query=sql,
            urgency=urgency,
            carbon_intensity=carbon,
            available_variants=variants,
        )
        decision = self.selector.select(context)

        if explain:
            print("\n" + "=" * 60)
            print("CARBON-AWARE EXECUTION DECISION")
            print("=" * 60)
            print(decision.explain())
            print("=" * 60 + "\n")

        # Step 4: Handle deferred execution
        if decision.should_defer:
            logger.info(f"Query deferred for {decision.defer_minutes} minutes")
            return None, None, decision

        # Step 5: Execute with selected variant
        logger.info(f"Executing with {decision.selected_strategy.value} variant")

        conn = self.compiler.get_connection(decision.selected_variant)

        def run_query():
            return conn.execute(sql).fetchall()

        result, metrics = self.profiler.profile(run_query)

        # Step 6: Record metrics
        self.metrics_collector.record(
            query=sql,
            variant=decision.selected_strategy.value,
            metrics=metrics,
            carbon_intensity=carbon.value,
            metadata={
                "urgency": urgency.value,
                "decision_reason": decision.reason,
                "timestamp": datetime.now().isoformat(),
            },
        )

        logger.info(
            f"Query completed: {metrics.duration_ms:.2f}ms, {metrics.energy_joules:.2f}J"
        )

        if explain:
            print(f"ACTUAL PERFORMANCE:")
            print(f"  Energy: {metrics.energy_joules:.2f} J")
            print(f"  Duration: {metrics.duration_ms:.2f} ms")
            print(f"  Power: {metrics.power_watts:.2f} W")
            print(f"  Carbon: {metrics.carbon_grams(carbon.value):.4f} g CO2")
            print()

        return result, metrics, decision

    def compare_strategies(self, sql: str) -> dict:
        """
        Execute query with all strategies and compare results

        Args:
            sql: SQL query to compare

        Returns:
            Dictionary with strategy comparisons
        """
        logger.info("Running strategy comparison")

        carbon = self.carbon_api.get_current_intensity()
        variants = self.compiler.compile(sql)

        results = {}

        for strategy, variant in variants.items():
            conn = self.compiler.get_connection(variant)

            def run_query():
                return conn.execute(sql).fetchall()

            result, metrics = self.profiler.profile(run_query)

            results[strategy.value] = {
                "energy_joules": metrics.energy_joules,
                "duration_ms": metrics.duration_ms,
                "power_watts": metrics.power_watts,
                "carbon_grams": metrics.carbon_grams(carbon.value),
                "result_count": len(result) if isinstance(result, list) else 1,
            }

        return results

    def get_statistics(self) -> dict:
        """Get execution statistics"""
        return self.metrics_collector.summary()

    def save_metrics(self, filename: str = None) -> str:
        """Save collected metrics to file"""
        return self.metrics_collector.save(filename)

    def close(self):
        """Clean up resources"""
        self.compiler.close_all()
        logger.info("Engine closed")


# Production example
if __name__ == "__main__":
    import duckdb

    # Create sample database
    print("Setting up sample database...")
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE orders AS 
        SELECT 
            range AS order_id,
            (range % 1000) AS customer_id,
            random() * 1000 AS amount,
            CAST('2024-01-01' AS DATE) + INTERVAL (range % 365) DAY AS date
        FROM range(100000)
    """
    )
    conn.close()

    # Initialize engine
    engine = CarbonAwareQueryEngine(":memory:")

    # Example queries with different urgencies
    queries = [
        (
            "SELECT COUNT(*) FROM orders",
            QueryUrgency.HIGH,
            "Simple count - high priority",
        ),
        (
            "SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id",
            QueryUrgency.MEDIUM,
            "Aggregation - medium priority",
        ),
        (
            "SELECT * FROM orders WHERE amount > 500 ORDER BY date DESC LIMIT 100",
            QueryUrgency.LOW,
            "Filtered sort - low priority",
        ),
    ]

    print("\n" + "=" * 80)
    print("CARBON-AWARE QUERY ENGINE - PRODUCTION EXAMPLE")
    print("=" * 80)

    for sql, urgency, description in queries:
        print(f"\n{'='*80}")
        print(f"Query: {description}")
        print(f"SQL: {sql[:60]}...")
        print(f"Urgency: {urgency.value}")
        print(f"{'='*80}")

        result, metrics, decision = engine.execute(sql, urgency, explain=True)

        if result:
            print(f"Results: {len(result)} rows")

    # Show statistics
    print("\n" + "=" * 80)
    print("EXECUTION STATISTICS")
    print("=" * 80)
    stats = engine.get_statistics()
    for key, value in stats.items():
        print(f"{key}: {value}")

    # Compare strategies
    print("\n" + "=" * 80)
    print("STRATEGY COMPARISON")
    print("=" * 80)
    comparison_sql = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id"
    comparison = engine.compare_strategies(comparison_sql)

    for strategy, metrics in comparison.items():
        print(f"\n{strategy.upper()}:")
        for metric, value in metrics.items():
            print(
                f"  {metric}: {value:.2f}"
                if isinstance(value, float)
                else f"  {metric}: {value}"
            )

    # Clean up
    engine.close()

    print("\n" + "=" * 80)
    print("DEMO COMPLETE")
    print("=" * 80)
