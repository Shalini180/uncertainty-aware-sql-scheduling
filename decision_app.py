import streamlit as st
from src.core.engine import CarbonAwareQueryEngine
from src.optimizer.selector import QueryUrgency

st.set_page_config(page_title="Carbon-Aware SQL", page_icon="🌱", layout="wide")
st.title("🌱 Carbon-Aware SQL Demo")

engine = CarbonAwareQueryEngine()
sql = st.text_area("SQL", "SELECT COUNT(*) FROM t")
urg = st.selectbox(
    "Urgency", list(QueryUrgency), format_func=lambda u: u.value.capitalize(), index=2
)

if st.button("Run"):
    result, metrics, decision = engine.execute_query(sql, urg, explain=False)
    if decision.should_defer:
        st.warning(f"Deferred {decision.defer_minutes} min — {decision.reason}")
    else:
        st.success(
            f"Strategy: {decision.selected_strategy.value.upper()} — {decision.reason}"
        )
        st.metric("Energy (J)", f"{metrics.energy_joules:.2f}")
        st.metric("Duration (ms)", f"{metrics.duration_ms:.0f}")
