# ADR 0001 — High-level Architecture

- **Goal**: Carbon-aware SQL execution with multi-variant strategies.
- **Modules**:
  - utils/query_parser: extracts features from SQL
  - core/compiler: produces FAST/BALANCED/EFFICIENT variants
  - core/executor+profiler: runs variant and measures metrics
  - optimizer: carbon API + selector
  - energy_ml: demo UI (Streamlit)
- **Why**: separation of concerns; easy benchmarking & selection.
- **Alternatives**: single-pipeline executor (rejected, less flexible)
