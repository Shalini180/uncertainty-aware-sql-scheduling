# src/core/compiler.py
import duckdb
from enum import Enum
from typing import Dict, Optional
from dataclasses import dataclass
import os


class ExecutionStrategy(Enum):
    """Execution strategies for queries"""

    FAST = "fast"
    EFFICIENT = "efficient"
    BALANCED = "balanced"


@dataclass
class QueryVariant:
    """A compiled query variant with specific configuration"""

    strategy: ExecutionStrategy
    config: Dict
    sql: Optional[str] = None
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
        """Compile query into multiple variants"""
        variants = {}

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
            sql=sql,
        )

        # EFFICIENT variant: Minimize energy
        efficient_threads = max(1, self.max_threads // 4)
        variants[ExecutionStrategy.EFFICIENT] = QueryVariant(
            strategy=ExecutionStrategy.EFFICIENT,
            config={
                "threads": efficient_threads,
                "memory_limit": "1GB",
                "enable_optimizer": True,
            },
            sql=self._optimize_efficient_sql(sql, efficient_threads),
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
            sql=sql,
        )

        return variants

    def _optimize_efficient_sql(self, sql: str, threads: int) -> str:
        """Optimize SQL for efficient execution"""
        # Check for SELECT *
        if "select *" in sql.lower():
            import logging

            logging.getLogger(__name__).warning(
                "EFFICIENT plan detected 'SELECT *'. "
                "Explicit column selection is recommended for better performance."
            )

        # Add PRAGMA threads
        return f"PRAGMA threads={threads};\n{sql}"

    def get_connection(self, variant: QueryVariant) -> duckdb.DuckDBPyConnection:
        """Get or create a configured connection for a variant"""
        strategy_key = variant.strategy.value

        if strategy_key not in self.conn_pool:
            conn = duckdb.connect(self.db_path)

            try:
                conn.execute(f"SET threads TO {variant.config['threads']}")
            except:
                pass

            try:
                conn.execute(f"SET memory_limit = '{variant.config['memory_limit']}'")
            except:
                pass

            self.conn_pool[strategy_key] = conn

        return self.conn_pool[strategy_key]

    def close_all(self):
        """Close all pooled connections"""
        for conn in self.conn_pool.values():
            conn.close()
        self.conn_pool.clear()
