# Uncertainty-Aware SQL Scheduling (DuckDB)

A **research prototype** investigating how execution order, workload variance, and external uncertainty influence tail latency in analytical query workloads.

This project stems from production failures where systems appeared healthy under aggregate metrics (CPU utilization, throughput, mean latency) yet users experienced missed deadlines and severe tail-latency degradation. The prototype isolates **coordination effects** in a controlled setting—not sustained resource contention.

---

## Research Questions

1. **When does execution order become the dominant driver of tail latency?**
2. **Under what conditions does ordering sensitivity *not* emerge?**
3. **How do deterministic scheduling policies behave under noisy external signals?**

The system is designed to **surface and isolate variance** hidden by average-case metrics, not to optimize performance.

---

## Key Findings

| Condition | Tail Latency Impact |
|-----------|---------------------|
| High inter-query cost variance + loosely coupled admission/execution | Up to **3× increase** in p99 latency from small ordering changes |
| Same conditions, mean latency | Changed **< 5%** (masking reliability risk) |
| Homogeneous query costs | Minimal ordering sensitivity |
| Tightly synchronized queueing | Minimal ordering sensitivity |
| Noisy external signals (e.g., carbon forecasts) | Deterministic deferral reduced throughput with no tail improvement |

**Takeaway:** Ordering sensitivity is *conditional*, not universal—it emerges only under specific architectural and workload assumptions.

---

## Architecture

```
                    ┌────────────────────┐
                    │    SQL Client      │
                    └─────────┬──────────┘
                              │
                              ▼
                    ┌────────────────────┐
                    │   Query Analyzer   │
                    │  (structure, cost) │
                    └─────────┬──────────┘
                              │
                              ▼
            ┌─────────────────────────────────────┐
            │   Modified DuckDB Scheduler         │
            │                                     │
            │  • Admission / Execution Split      │
            │  • Execution Order Control          │
            │  • Optional Deferral Logic          │
            └───────────┬─────────────────────────┘
                        │
          ┌─────────────┴─────────────┐
          │                           │
          ▼                           ▼
   ┌─────────────┐          ┌─────────────────┐
   │  Executor   │          │ External Signal │
   │  (DuckDB)   │          │ (e.g., Carbon)  │
   └──────┬──────┘          └─────────────────┘
          │
          ▼
   ┌────────────────────┐
   │ Metrics & Tracing  │
   │  • admission delay │
   │  • exec time       │
   │  • completion time │
   │  • tail latency    │
   └────────────────────┘
```

The architecture prioritizes **repeatability and traceability** over throughput.

---

## Getting Started

### Prerequisites

- Python 3.9+
- DuckDB 0.9+
- NumPy, Pandas (for analysis)

### Installation

```bash
git clone https://github.com/yourusername/uncertainty-sql-scheduler.git
cd uncertainty-sql-scheduler
pip install -r requirements.txt
```

### Running Your First Experiment

```bash
# Generate a workload trace with high cost variance
python scripts/generate_trace.py --variance high --queries 1000 -o traces/high_var.json

# Run with FIFO execution order
python scripts/run_experiment.py --trace traces/high_var.json --policy fifo

# Run with reversed execution order (same admission order)
python scripts/run_experiment.py --trace traces/high_var.json --policy reverse

# Compare tail latencies
python scripts/analyze.py --results results/fifo results/reverse --metric p99
```

---

## Experimental Design

### Isolating Execution-Order Effects

To distinguish structural scheduling effects from workload artifacts:

- **Arrival rates** held constant
- **Admission order** fixed via trace replay
- **Execution order** varied independently
- **Inter-query cost variance** controlled as an experimental variable

### Carbon-Aware Deferral Extension

The prototype treats carbon intensity as an **uncertain external signal**:

- Forecasts are probabilistic, not deterministic
- Deferral decisions avoid tail-risk violations under forecast error
- Studies how noisy signals interact with scheduling policies

This extension explores carbon awareness as a **systems constraint**, not a standalone optimization.

---

## Repository Structure

```
├── src/
│   ├── scheduler/        # Modified DuckDB scheduling logic
│   ├── analyzer/         # Query cost estimation
│   └── instrumentation/  # Metrics collection
├── scripts/
│   ├── generate_trace.py # Workload trace generation
│   ├── run_experiment.py # Experiment runner
│   └── analyze.py        # Results analysis (distributional)
├── traces/               # Sample workload traces
└── experiments/          # Experiment configurations
```

---

## Scope and Limitations

### This project is:
- A systems research prototype
- Focused on scheduling, coordination, and tail latency
- Designed for controlled experimentation and trace replay

### This project is not:
- A production-ready database
- A performance tuning tool
- A green-computing product

### Known limitations:
- Scheduling state is not persistent
- Results depend on workload construction and cost variance assumptions
- Energy/carbon measurements are approximate and region-dependent

These limitations are intentional to preserve experimental clarity.

---

## Citation

If you use this work in your research, please cite:

```bibtex
@software{uncertainty_sql_scheduler,
  author = {Shalini},
  title = {Uncertainty-Aware SQL Scheduling},
  year = {2025},
  url = {https://github.com/Shalini180/uncertainty-aware-sql-scheduling}
}
```

---

## Related Work

- [DuckDB](https://duckdb.org/) - The underlying analytical database
- Tail latency literature: Dean & Barroso, "The Tail at Scale" (2013)
- Carbon-aware computing: Wiesner et al., "Let's Wait Awhile" (2021)

---

## License

MIT License. See [LICENSE](LICENSE) for details.

---

## Contact

For questions about the research or collaboration inquiries, please open an issue or reach out directly.
