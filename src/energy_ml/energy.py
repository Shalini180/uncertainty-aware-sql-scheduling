import pyRAPL
from contextlib import contextmanager

pyRAPL.setup()


@contextmanager
def measure_energy(label="run"):
    meter = pyRAPL.Measurement(label)
    meter.begin()
    try:
        yield
    finally:
        meter.end()
        print(f"[{label}] Energy: {meter.result.pkg} uJ, DRAM: {meter.result.dram} uJ")
