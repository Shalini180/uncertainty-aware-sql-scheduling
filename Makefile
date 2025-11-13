.PHONY: setup run test fmt lint ci

setup:
python -m pip install -r requirements.txt

run:
streamlit run src/energy_ml/main.py

test:
pytest -q

fmt:
black src tests

ci: fmt test

.PHONY: dev install run ui eval analyze

install:
\tpython -m pip install -e . -r requirements.txt

run:
\tpython -m src.core.engine

ui:
\tstreamlit run src/energy_ml/decision_app.py

eval:
\tpython -m evaluation.run_experiments

analyze:
\tpython -m evaluation.analyze_results

