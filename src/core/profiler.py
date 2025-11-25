# src/core/profiler.py
import time
import psutil
from dataclasses import dataclass
from typing import Callable, Any, Tuple, Optional, List
import statistics

# Try to import pyRAPL, fall back to estimation
try:
    import pyRAPL

    RAPL_AVAILABLE = True
except ImportError:
    RAPL_AVAILABLE = False
    print("Warning: pyRAPL not available. Using CPU-based energy estimation.")


@dataclass
class EnergyMetrics:
    """Energy and performance metrics from query execution"""

    energy_joules: float
    duration_ms: float
    cpu_percent: float
    memory_mb: float

    @property
    def power_watts(self) -> float:
        """Average power consumption in watts"""
        if self.duration_ms > 0:
            return (self.energy_joules * 1000) / self.duration_ms
        return 0.0

    def carbon_grams(self, carbon_intensity: float = 475.0) -> float:
        """
        Calculate carbon emissions in grams CO2

        Args:
            carbon_intensity: Grid carbon intensity in gCO2/kWh (default: US average)

        Returns:
            Carbon emissions in grams CO2
        """
        kwh = self.energy_joules / 3_600_000  # Joules to kWh
        return kwh * carbon_intensity


class EnergyProfiler:
    """Profile energy consumption of code execution"""

    def __init__(self, use_rapl: bool = True):
        """
        Initialize energy profiler

        Args:
            use_rapl: Whether to try using Intel RAPL (if available)
        """
        self.use_rapl = use_rapl and RAPL_AVAILABLE
        self.rapl_meter = None

        if self.use_rapl:
            try:
                pyRAPL.setup()
                self.rapl_meter = pyRAPL.Measurement("query")
                print("Energy profiling: Using Intel RAPL")
            except Exception as e:
                print(f"RAPL initialization failed: {e}")
                self.use_rapl = False

        if not self.use_rapl:
            print("Energy profiling: Using CPU estimation")

    def profile(self, func: Callable, *args, **kwargs) -> Tuple[Any, EnergyMetrics]:
        """
        Profile a function's energy consumption

        Args:
            func: Function to profile
            *args, **kwargs: Arguments to pass to function

        Returns:
            Tuple of (function_result, energy_metrics)
        """
        # Get process for CPU monitoring
        process = psutil.Process()
        cpu_start = process.cpu_percent()
        mem_start = process.memory_info().rss / 1024 / 1024  # MB

        # Start energy measurement
        if self.use_rapl:
            self.rapl_meter.begin()

        # Execute function
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        duration_ms = (time.perf_counter() - start_time) * 1000

        # End energy measurement
        if self.use_rapl:
            self.rapl_meter.end()
            # Convert microjoules to joules
            energy_joules = self.rapl_meter.result.pkg[0] / 1_000_000
        else:
            # Estimate based on CPU usage
            # Assume average CPU TDP of 65W, scale by usage
            cpu_avg = (cpu_start + process.cpu_percent()) / 2
            estimated_power = 65 * (cpu_avg / 100)  # Watts
            energy_joules = estimated_power * (duration_ms / 1000)

        # Get final CPU/memory stats
        cpu_end = process.cpu_percent()
        mem_end = process.memory_info().rss / 1024 / 1024

        metrics = EnergyMetrics(
            energy_joules=energy_joules,
            duration_ms=duration_ms,
            cpu_percent=(cpu_start + cpu_end) / 2,
            memory_mb=mem_end - mem_start,
        )

        return result, metrics

    def profile_with_uncertainty(
        self, func: Callable, iterations: int = 5, *args, **kwargs
    ) -> Tuple[Any, EnergyMetrics, float]:
        """
        Profile a function multiple times to measure energy uncertainty

        Args:
            func: Function to profile
            iterations: Number of times to run (default 5)
            *args, **kwargs: Arguments to pass to function

        Returns:
            Tuple of (result, average_metrics, energy_std_dev_joules)
        """
        results = []
        energy_readings = []
        duration_readings = []
        cpu_readings = []
        memory_readings = []

        # Warmup run
        func(*args, **kwargs)

        for _ in range(iterations):
            res, met = self.profile(func, *args, **kwargs)
            results.append(res)
            energy_readings.append(met.energy_joules)
            duration_readings.append(met.duration_ms)
            cpu_readings.append(met.cpu_percent)
            memory_readings.append(met.memory_mb)

        # Calculate statistics
        avg_energy = statistics.mean(energy_readings)
        std_dev_energy = (
            statistics.stdev(energy_readings) if len(energy_readings) > 1 else 0.0
        )

        avg_metrics = EnergyMetrics(
            energy_joules=avg_energy,
            duration_ms=statistics.mean(duration_readings),
            cpu_percent=statistics.mean(cpu_readings),
            memory_mb=statistics.mean(memory_readings),
        )

        return results[0], avg_metrics, std_dev_energy

    def profile_context(self):
        """Context manager for profiling"""
        return ProfilerContext(self)


class ProfilerContext:
    """Context manager for energy profiling"""

    def __init__(self, profiler: EnergyProfiler):
        self.profiler = profiler
        self.metrics = None
        self.process = psutil.Process()

    def __enter__(self):
        self.cpu_start = self.process.cpu_percent()
        self.mem_start = self.process.memory_info().rss / 1024 / 1024

        if self.profiler.use_rapl:
            self.profiler.rapl_meter.begin()

        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = (time.perf_counter() - self.start_time) * 1000

        if self.profiler.use_rapl:
            self.profiler.rapl_meter.end()
            energy_joules = self.profiler.rapl_meter.result.pkg[0] / 1_000_000
        else:
            cpu_avg = (self.cpu_start + self.process.cpu_percent()) / 2
            estimated_power = 65 * (cpu_avg / 100)
            energy_joules = estimated_power * (duration_ms / 1000)

        cpu_end = self.process.cpu_percent()
        mem_end = self.process.memory_info().rss / 1024 / 1024

        self.metrics = EnergyMetrics(
            energy_joules=energy_joules,
            duration_ms=duration_ms,
            cpu_percent=(self.cpu_start + cpu_end) / 2,
            memory_mb=mem_end - self.mem_start,
        )


# Test
if __name__ == "__main__":
    profiler = EnergyProfiler()

    def test_function():
        """Test function that does some work"""
        total = 0
        for i in range(1000000):
            total += i
        return total

    result, metrics = profiler.profile(test_function)

    print(f"Result: {result}")
    print(f"Energy: {metrics.energy_joules:.2f} J")
    print(f"Duration: {metrics.duration_ms:.2f} ms")
    print(f"Power: {metrics.power_watts:.2f} W")
    print(f"Carbon (US avg): {metrics.carbon_grams():.4f} g CO2")

    # Test context manager
    print("\nTesting context manager:")
    with profiler.profile_context() as ctx:
        sum([i**2 for i in range(100000)])

    print(f"Energy: {ctx.metrics.energy_joules:.2f} J")
    print(f"Duration: {ctx.metrics.duration_ms:.2f} ms")
