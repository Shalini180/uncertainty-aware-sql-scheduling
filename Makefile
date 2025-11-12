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
