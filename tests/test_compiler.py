from src.core.compiler import MultiVariantCompiler, ExecutionStrategy


def test_variants_exist_and_scaled():
    c = MultiVariantCompiler()
    v = c.compile(
        "SELECT COUNT(*) FROM t JOIN u ON t.id=u.id GROUP BY t.id ORDER BY t.id"
    )
    assert set(
        [
            ExecutionStrategy.FAST,
            ExecutionStrategy.BALANCED,
            ExecutionStrategy.EFFICIENT,
        ]
    ).issubset(v.keys())
    assert (
        v[ExecutionStrategy.FAST].estimated_latency
        < v[ExecutionStrategy.BALANCED].estimated_latency
    )
    assert (
        v[ExecutionStrategy.EFFICIENT].estimated_energy
        < v[ExecutionStrategy.BALANCED].estimated_energy
    )
