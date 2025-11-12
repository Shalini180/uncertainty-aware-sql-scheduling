from dataclasses import dataclass
from time import perf_counter

# Try pyRAPL (works on Linux/WSL with Intel/AMD RAPL)
try:
    import pyRAPL

    pyRAPL.setup()
    _HAS_RAPL = True
except Exception:
    _HAS_RAPL = False
    pyRAPL = None  # type: ignore


@dataclass
class EnergyMetrics:
    duration_ms: float
    energy_joules: float
    power_watts: float
    backend: str


class _EnergyCtx:
    def __enter__(self):
        self.t0 = perf_counter()
        self.backend = "rapl" if _HAS_RAPL else "noop"
        if _HAS_RAPL:
            self._m = pyRAPL.Measurement("query")
            self._m.begin()
        return self

    def __exit__(self, exc_type, exc, tb):
        t1 = perf_counter()
        self.duration_ms = (t1 - self.t0) * 1000.0

        if _HAS_RAPL:
            self._m.end()
            # pyRAPL gives microjoules
            pkg_uJ = getattr(self._m.result, "pkg", 0) or 0
            dram_uJ = getattr(self._m.result, "dram", 0) or 0
            self.energy_joules = (pkg_uJ + dram_uJ) / 1_000_000.0
        else:
            self.energy_joules = 0.0

        dt_s = max(1e-6, self.duration_ms / 1000.0)
        self.power_watts = self.energy_joules / dt_s


def measure():
    """Context manager: with measure() as m: ... -> use m.duration_ms etc."""
    return _EnergyCtx()


def to_metrics(ctx: _EnergyCtx) -> EnergyMetrics:
    return EnergyMetrics(
        duration_ms=ctx.duration_ms,
        energy_joules=ctx.energy_joules,
        power_watts=ctx.power_watts,
        backend=ctx.backend,
    )
