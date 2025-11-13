#!/usr/bin/env python3
"""Launcher for carbon-aware query engine"""

import os
import argparse
import duckdb

from src.core.engine import CarbonAwareQueryEngine
from src.optimizer.selector import QueryUrgency


def create_sample_db(db_path: str, row_count: int = 100_000):
    """Create and populate a sample 'orders' table at db_path."""
    if db_path != ":memory:":
        directory = os.path.dirname(db_path) or "."
        os.makedirs(directory, exist_ok=True)
        if os.path.exists(db_path):
            os.remove(db_path)

    print(f"Creating sample database at: {db_path} (rows: {row_count})")
    conn = duckdb.connect(database=db_path)

    conn.execute(
        f"""
        CREATE TABLE orders AS
        SELECT
            range AS order_id,
            (range % 1000) AS customer_id,
            random() * 1000 AS amount,
            CAST('2024-01-01' AS DATE) + INTERVAL (range % 365) DAY AS date
        FROM range({row_count})
    """
    )

    conn.close()
    print("Sample database created.")


def main():
    parser = argparse.ArgumentParser(description="Run carbon-aware query engine demo.")
    parser.add_argument(
        "--db-path",
        type=str,
        default="data/demo.db",
        help="Path to DuckDB database file, or ':memory:' for in-memory DB (default: data/demo.db)",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=100_000,
        help="Number of rows to generate in the sample orders table (default: 100000)",
    )
    args = parser.parse_args()

    db_path = args.db_path
    row_count = args.rows

    # Create sample DB
    create_sample_db(db_path=db_path, row_count=row_count)

    # Initialize engine
    print("\nInitializing carbon-aware query engine...")
    engine = CarbonAwareQueryEngine(db_path=db_path)

    # Example queries
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
    print("CARBON-AWARE QUERY ENGINE - DEMO")
    print("=" * 80)

    for sql, urgency, description in queries:
        print(f"\n{'='*80}")
        print(f"Query: {description}")
        print(f"SQL (preview): {sql[:120]}{'...' if len(sql) > 120 else ''}")
        print(f"Urgency: {urgency.value}")
        print(f"{'='*80}")

        result, metrics, decision = engine.execute(sql, urgency, explain=True)

        # Print brief results summary
        if result:
            print(f"Results: {len(result)} rows")

        # ---- UPDATED METRICS BLOCK (YOUR FIX APPLIED HERE) ----
        print("\nMetrics Summary:")
        print(f"  Energy: {metrics.energy_joules:.4f} J")
        print(f"  Duration: {metrics.duration_ms:.2f} ms")
        print(f"  Power: {metrics.power_watts:.4f} W")
        print(f"  CPU: {metrics.cpu_percent:.1f}%")
        print(f"  Memory: {metrics.memory_mb:.2f} MB")
        # -------------------------------------------------------

        if decision:
            print("Execution decision / plan summary:")
            print(f"  {decision}")

    # Engine statistics
    print("\n" + "=" * 80)
    print("EXECUTION STATISTICS")
    print("=" * 80)

    try:
        stats = engine.get_statistics()
        for key, value in stats.items():
            print(f"{key}: {value}")
    except Exception as e:
        print(f"Could not fetch statistics: {e!r}")

    # Strategy comparison
    print("\n" + "=" * 80)
    print("STRATEGY COMPARISON")
    print("=" * 80)

    comparison_sql = "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id"

    try:
        comparison = engine.compare_strategies(comparison_sql)
        for strategy, metrics_dict in comparison.items():
            print(f"\n{strategy.upper()}:")
            for metric, value in metrics_dict.items():
                if isinstance(value, float):
                    print(f"  {metric}: {value:.2f}")
                else:
                    print(f"  {metric}: {value}")
    except Exception as e:
        print(f"Could not run strategy comparison: {e!r}")

    # Cleanup
    engine.close()

    print("\n" + "=" * 80)
    print("DEMO COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
