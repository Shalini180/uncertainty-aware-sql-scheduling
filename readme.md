\# Carbon-Aware SQL Query Engine



\## Overview

This repository implements a carbon-aware SQL query execution engine designed to optimize both performance and sustainability. The system analyzes SQL queries, estimates computational characteristics, evaluates real-time carbon-intensity of the electricity grid, and selects an execution strategy that balances speed, energy efficiency, and carbon emissions. It supports multiple query strategies, measures runtime and approximate energy usage, estimates CO₂ emissions, and can defer execution of low-urgency workloads during high carbon periods. This work demonstrates how next-generation database systems can incorporate sustainability-aware decision-making without user intervention.



\## Motivation

Traditional query optimizers minimize latency and resource usage, but electricity generation fluctuates by time and region, affecting carbon emissions. Scheduling compute-heavy workloads during low-carbon intensity periods can significantly reduce environmental impact at scale. This project addresses:

\- How can a query engine integrate environmental awareness into its planning and execution logic?\[6]



\## Key Contributions

\- Query structure analysis (joins, aggregation, complexity)

\- Multi-variant query compilation

\- Execution time and energy measurement

\- Carbon-intensity estimation (API and fallback models)

\- Policy enforcement based on urgency and emissions

\- Optional deferral of non-urgent queries

\- Streamlit-based user interface



\## Architecture



```

+----------------+

|   SQL Query    |

+----------------+

&nbsp;       |

&nbsp;       v

+----------------+

| Query Analyzer |

+----------------+

&nbsp;       |

&nbsp;       v

+----------------------+

| Multi-Variant Plans  | (FAST / BALANCED / EFFICIENT)

+----------------------+

&nbsp;       |

&nbsp;       v

+---------------------+

| Carbon-Aware Select |

+---------------------+

&nbsp;       |

&nbsp; +-----+-----+

&nbsp; |           |

run now     defer

&nbsp; |           |

&nbsp; v           v

Execution   Wait Window

```



\## Module Descriptions



| Module         | Path/Script                        | Description                                            |

|----------------|------------------------------------|--------------------------------------------------------|

| Query Analyzer | src/utils/query\_parser.py          | Parses SQL, extracts joins, aggregation, complexity    |

| Compiler       | src/core/compiler.py               | Generates execution variants                          |

| Executor       | src/core/executor.py               | Executes queries using DuckDB                         |

| Profiler       | src/core/profiler.py               | Measures runtime, energy usage                        |

| Carbon API     | src/optimizer/carbon\_api.py        | Gets real-time CO₂ data or fallback estimations        |

| Selector       | src/optimizer/selector.py          | Selects best plan by carbon and urgency               |

| Streamlit UI   | src/energy\_ml/decision\_app.py      | Interactive query interface                            |



\## System Requirements

\- Python 3.10+

\- DuckDB

\- Linux or WSL2 recommended (for energy profiling)

\- Energy measurement (Intel/AMD RAPL support)

\- Optional: ElectricityMaps API key



If energy profiling is unavailable (e.g., Windows), system runs but energy usage is reported as 0.0.



\## Installation



```bash

git clone <repo-url>

cd carbon-aware-sql-engine

python -m venv venv

\# Windows:

.\\venv\\Scripts\\activate

\# Linux/Mac:

source venv/bin/activate

pip install -r requirements.txt

```



\### Optional: Carbon API

Create `.env` file:

```

ELECTRICITYMAPS\_API\_TOKEN=<your\_token>

EM\_ZONE=US-CAL-CISO

```

Without these, the engine uses a default time-of-day carbon model.



\## Usage



\*\*Python API Example:\*\*

```python

from src.core.engine import CarbonAwareQueryEngine

from src.optimizer.selector import QueryUrgency



engine = CarbonAwareQueryEngine()

sql = "SELECT COUNT(\*) FROM my\_table"

result, metrics, decision = engine.execute\_query(

&nbsp;   sql,

&nbsp;   urgency=QueryUrgency.MEDIUM,

&nbsp;   explain=True

)

print(result)

print(metrics)

print(decision.explain())

```



\*\*Streamlit UI:\*\*

```bash

streamlit run src/energy\_ml/decision\_app.py

```



UI features:

\- SQL input box

\- Urgency selection

\- Execution strategy and rationale

\- Metrics and carbon status\[5]\[4]



\## Example Output



```

Selected variant: efficient

Reason: High carbon intensity — minimizing energy

Duration: 412.65 ms

Energy: 7.32 J

Carbon: 0.004 g CO2



Non-urgent workload: Carbon high — deferring execution by ~90 minutes

```



\## Testing

```bash

pytest -q

```



\## Limitations

\- Accurate energy measurement requires RAPL-enabled hardware (best on Linux/WSL2)

\- Carbon API forecast accuracy is region-dependent

\- Query cost models are heuristic-based

\- Deferral model does not persist tasks; manual re-execution required



\## Future Work

\- ML-powered performance prediction

\- PostgreSQL/Spark integration

\- Improved cost modeling, queueing, and dynamic scheduling

\- Historical workload tracking, better carbon forecast algorithms



\## Academic Significance

This project:

\- Demonstrates sustainable computing principles

\- Applies carbon-aware query scheduling

\- Integrates optimization in real-time systems

\- Heuristic query planning with environmental data





\## Acknowledgements

\- DuckDB

\- ElectricityMaps API

\- pyRAPL





