from src.core.profiler import EnergyProfiler


def test_profiler_context():
    profiler = EnergyProfiler()

    # Test using context manager
    with profiler.profile_context() as ctx:
        s = sum(range(10_000))

    assert ctx.metrics.duration_ms > 0
    assert ctx.metrics.energy_joules >= 0
    assert ctx.metrics.power_watts >= 0
