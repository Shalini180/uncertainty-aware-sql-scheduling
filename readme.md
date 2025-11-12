Carbon-Aware SQL Query Engine

This project demonstrates how a database execution engine can incorporate real-time carbon-intensity awareness into query planning.
Instead of optimizing only for speed, the system considers:

Query complexity

Energy use

Carbon intensity of the power grid

User-defined urgency

Its goal is simple:

Reduce emissions from data workloads by selecting low-carbon execution strategies or deferring non-urgent queries.

This repo serves as a proof-of-concept for sustainable computing and environmentally informed workload scheduling.

✅ What It Does

Analyzes SQL query structure (joins, aggregation, complexity)

Builds multiple execution plans (Fast / Balanced / Efficient)

Estimates execution cost + energy usage

Pulls real-time grid carbon intensity (or uses fallback model)

Selects best execution plan given urgency + carbon

Optionally defers non-urgent queries until a cleaner window

Provides structured result + explanation

DuckDB is used as the local execution backend.

🏛 System Architecture
High-Level Flow
SQL Query
   │
   ▼
Query Analyzer ──► Plan Compiler ──► Profiler
   │                                 │
   └─────────────► Carbon-Aware Selector ◄──── Carbon Data
                                     │
                                     ├── Run now → Executor (DuckDB)
                                     └── Defer → Wait Window

Component Breakdown
Component	Responsibility
Query Analyzer	Parse SQL; extract structural features (joins, aggregations, filters).
Plan Compiler	Generate execution variants (Fast / Balanced / Efficient).
Profiler	Estimate or measure time + energy usage.
Carbon Data Provider	Fetch real-time grid carbon intensity or use fallback model.
Selector (Decision Engine)	Choose plan based on urgency × emissions × performance; may defer.
Executor (DuckDB)	Execute final chosen plan.
Metrics Collector	Gather runtime + carbon/energy estimates.
UI (Streamlit)	Simple user-facing interface.
🔧 Requirements

Python 3.10+

DuckDB

Linux / WSL2 recommended for energy measurement

Optional: ElectricityMaps API token

If energy measurement hardware is unavailable, energy defaults to estimation.

📦 Install
git clone <repo-url>
cd carbon-aware-sql-engine
python -m venv venv
source venv/bin/activate    # Windows: .\venv\Scripts\activate
pip install -r requirements.txt


Optional .env:

ELECTRICITYMAPS_API_TOKEN=<token>
EM_ZONE=US-CAL-CISO

▶️ Usage
Python
from src.core.engine import CarbonAwareQueryEngine
from src.optimizer.selector import QueryUrgency

engine = CarbonAwareQueryEngine()
sql = "SELECT COUNT(*) FROM my_table"

result, metrics, decision = engine.execute_query(
    sql,
    urgency=QueryUrgency.MEDIUM,
    explain=True
)

print(result)
print(metrics)
print(decision.explain())

Streamlit UI
streamlit run src/energy_ml/decision_app.py

📂 Code Structure
src/
 ├─ core/
 │   ├─ engine.py       # Main entry point
 │   ├─ compiler.py     # Builds execution alternatives
 │   ├─ executor.py     # Runs queries with DuckDB
 │   └─ profiler.py     # Time + energy measurement
 ├─ optimizer/
 │   ├─ carbon_api.py   # Real-time carbon lookup
 │   └─ selector.py     # Decision + scheduling logic
 └─ utils/
     └─ query_parser.py # SQL analysis

📝 Example Output
Selected strategy: Efficient
Reason: High carbon → minimization preferred
Duration: 412 ms
Energy: ~7 J
Carbon: ~0.004 g CO₂

Non-urgent workload → deferred ~90 minutes

⚠ Notes

Energy accuracy depends on hardware (best on Linux/WSL2).

Carbon forecasting depends on provider + region.

Scheduling is non-persistent (PoC only).

📌 Goal

This project is intended to spark discussion on:

How carbon signals can guide database execution

How sustainable computing can be pushed into query engines

What a carbon-aware optimizer might look like in practice

It is not designed to replace a production system, but to illustrate how environmental signals can be integrated into query planning with minimal user involvement.
