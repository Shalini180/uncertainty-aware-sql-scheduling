# src/core/compiler.py
import duckdb
from enum import Enum
from typing import Dict, List, Optional
from dataclasses import dataclass
import os


class ExecutionStrategy(Enum):
    """Execution strategies for queries"""

    FAST = "fast"  # Latency-optimized (max threads, aggressive)
    EFFICIENT = "efficient"  # Energy-optimized (fewer threads)
    BALANCED = "balanced"  # Middle ground


@dataclass
class QueryVariant:
    """A compiled query variant with specific configuration"""

    strategy: ExecutionStrategy
    config: Dict
    estimated_energy: Optional[float] = None
    estimated_latency: Optional[float] = None

    def __str__(self):
        return f"{self.strategy.value}: threads={self.config.get('threads', 'N/A')}"


class MultiVariantCompiler:
    """Compiles queries into multiple execution variants"""

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self.conn_pool = {}
        self.max_threads = os.cpu_count() or 4

    def compile(self, sql: str) -> Dict[ExecutionStrategy, QueryVariant]:
        """
        Compile query into multiple variants

        Args:
            sql: SQL query string

        Returns:
            Dictionary mapping strategy to variant configuration
        """
        variants = {}

        # FAST variant: Maximum performance
        # FAST variant: Maximum performance
        variants[ExecutionStrategy.FAST] = QueryVariant(
            strategy=ExecutionStrategy.FAST,
            config={
                "threads": self.max_threads,
                "memory_limit": "4GB",
                "enable_optimizer": True,
            },
            estimated_latency=100.0,
            estimated_energy=150.0,
        )

        # EFFICIENT variant: Minimize energy
        variants[ExecutionStrategy.EFFICIENT] = QueryVariant(
            strategy=ExecutionStrategy.EFFICIENT,
            config={
                "threads": max(1, self.max_threads // 4),
                "memory_limit": "1GB",
                "enable_optimizer": True,
            },
            estimated_latency=200.0,
            estimated_energy=80.0,
        )

        # BALANCED variant: Trade-off
        variants[ExecutionStrategy.BALANCED] = QueryVariant(
            strategy=ExecutionStrategy.BALANCED,
            config={
                "threads": max(2, self.max_threads // 2),
                "memory_limit": "2GB",
                "enable_optimizer": True,
            },
            estimated_latency=140.0,
            estimated_energy=110.0,
        )
        return variants

    def get_connection(self, variant: QueryVariant) -> duckdb.DuckDBPyConnection:
        """
        Get or create a configured connection for a variant

        Args:
            variant: Query variant with configuration

        Returns:
            Configured DuckDB connection
        """
        strategy_key = variant.strategy.value

        if strategy_key not in self.conn_pool:
            conn = duckdb.connect(self.db_path)

            # Apply configuration
            try:
                conn.execute(f"SET threads TO {variant.config['threads']}")
            except:
                pass  # Some DuckDB versions don't support this

            try:
                conn.execute(f"SET memory_limit = '{variant.config['memory_limit']}'")
            except:
                pass

            # DuckDB doesn't have enable_parallelism setting
            # Thread count controls parallelism instead

            try:
                if variant.config.get("enable_optimizer", True):
                    conn.execute("SET enable_optimizer = true")
            except:
                pass

            # Optional: Set temp directory if specified
            if "temp_directory" in variant.config:
                try:
                    conn.execute(
                        f"SET temp_directory = '{variant.config['temp_directory']}'"
                    )
                except:
                    pass

            self.conn_pool[strategy_key] = conn

        return self.conn_pool[strategy_key]

    def close_all(self):
        """Close all pooled connections"""
        for conn in self.conn_pool.values():
            conn.close()
        self.conn_pool.clear()


# Test it
if __name__ == "__main__":
    compiler = MultiVariantCompiler()

    sql = "SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id"
    variants = compiler.compile(sql)

    print("Generated variants:")
    for strategy, variant in variants.items():
        print(f"  {variant}")
