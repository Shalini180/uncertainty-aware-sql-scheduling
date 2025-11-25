import sys
import os

# Add src to path
sys.path.append(os.getcwd())

from src.optimizer.selector import (
    select_execution_strategy,
    Strategies,
    CarbonAwareSelector,
    SelectionContext,
    QueryUrgency,
)
from src.optimizer.carbon_api import CarbonIntensity
from src.core.compiler import MultiVariantCompiler, ExecutionStrategy
from datetime import datetime


def test_strategies():
    print("Testing Strategies...")

    # Test Case 1: Strategy A (Latency-First) - Not directly called by selector but available
    print(f"Strategy A (Latency-First): {Strategies.latency_first(0, 0)}")

    # Test Case 2: Strategy B (Carbon-Aware Deferred)
    # Low Carbon, Low Urgency -> Balanced
    res = Strategies.carbon_deferred(urgency=2, carbon=200)
    print(f"Strategy B (Low Carbon 200, Low Urgency): {res}")
    assert res == "balanced"

    # High Carbon, Low Urgency -> Defer
    res = Strategies.carbon_deferred(urgency=2, carbon=450)
    print(f"Strategy B (High Carbon 450, Low Urgency): {res}")
    assert res == "defer"

    # High Carbon, High Urgency -> Balanced
    res = Strategies.carbon_deferred(urgency=4, carbon=450)
    print(f"Strategy B (High Carbon 450, High Urgency): {res}")
    assert res == "balanced"

    # Test Case 3: Strategy C (Balanced Hybrid)
    print(f"Strategy C (Balanced Hybrid): {Strategies.balanced_hybrid(0, 0)}")

    print("Strategies Test Passed!")


def test_selector_integration():
    print("\nTesting Selector Integration...")
    selector = CarbonAwareSelector()

    # Mock context
    variants = {}  # Mock variants

    # High Carbon, Low Urgency (BATCH=1)
    ctx = SelectionContext(
        query="SELECT * FROM t",
        urgency=QueryUrgency.BATCH,
        carbon_intensity=CarbonIntensity(
            value=450, timestamp=datetime.now(), zone="test", source="mock"
        ),
        available_variants=variants,
    )

    # We need to mock available_variants because selector accesses it
    # But selector calls select_execution_strategy first
    # Let's see if it fails on variant access if we don't provide them
    # It does: variant = context.available_variants[strategy]

    # So we need to provide variants
    compiler = MultiVariantCompiler()
    variants = compiler.compile("SELECT * FROM t")
    ctx.available_variants = variants

    decision = selector.select(ctx)
    print(
        f"Decision for BATCH, 450gCO2: {decision.selected_strategy}, Defer: {decision.should_defer}"
    )

    assert decision.should_defer == True
    assert decision.selected_strategy == ExecutionStrategy.BALANCED

    print("Selector Integration Test Passed!")


def test_compiler_optimizations():
    print("\nTesting Compiler Optimizations...")
    compiler = MultiVariantCompiler()
    sql = "SELECT * FROM users"
    variants = compiler.compile(sql)

    efficient_variant = variants[ExecutionStrategy.EFFICIENT]
    print(f"Efficient SQL: {efficient_variant.sql}")

    assert "PRAGMA threads=" in efficient_variant.sql
    assert "SELECT * FROM users" in efficient_variant.sql

    print("Compiler Optimizations Test Passed!")


if __name__ == "__main__":
    test_strategies()
    test_selector_integration()
    test_compiler_optimizations()
