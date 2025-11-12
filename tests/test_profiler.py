from src.core.profiler import measure, to_metrics


def test_profiler_context():
    with measure() as m:
        s = sum(range(10_000))
    met = to_metrics(m)
    assert met.duration_ms > 0
    assert met.energy_joules >= 0
    assert met.power_watts >= 0
