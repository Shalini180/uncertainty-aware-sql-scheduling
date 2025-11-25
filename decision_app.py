"""Professional Carbon-Aware SQL Demo Dashboard"""

import os
import json
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path

# Page configuration
st.set_page_config(
    page_title="Carbon-Aware SQL Engine",
    page_icon="🌱",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Constants
API_URL = os.getenv("API_URL", "http://localhost:8000")
API_KEY = os.getenv("ENERGY_ML_API_KEY", "")
DATA_DIR = Path(__file__).parent / "data"

# Custom CSS for professional styling
st.markdown(
    """
    <style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #2E7D32;
        text-align: center;
        padding: 1rem 0;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #555;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .decision-box {
        background-color: #e8f5e9;
        padding: 1.5rem;
        border-radius: 0.5rem;
        border-left: 4px solid #2E7D32;
        margin: 1rem 0;
    }
    .stButton>button {
        width: 100%;
        background-color: #2E7D32;
        color: white;
        font-weight: bold;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# Helper Functions
@st.cache_data(ttl=300)
def load_carbon_history():
    """Load historical carbon intensity data"""
    try:
        file_path = DATA_DIR / "carbon_history.json"
        with open(file_path, "r") as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
    except Exception as e:
        st.error(f"Error loading carbon history: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def fetch_query_history():
    """Fetch query execution history from API"""
    try:
        headers = {"X-API-Key": API_KEY}
        response = requests.get(f"{API_URL}/query/history", headers=headers, timeout=5)
        if response.status_code == 200:
            return response.json()
        return []
    except Exception as e:
        st.warning(f"Could not fetch query history: {e}")
        return []


@st.cache_data(ttl=60)
def fetch_emissions_summary():
    """Fetch emissions summary from API"""
    try:
        headers = {"X-API-Key": API_KEY}
        response = requests.get(
            f"{API_URL}/emissions/summary", headers=headers, timeout=5
        )
        if response.status_code == 200:
            return response.json()
        return None
    except Exception as e:
        st.warning(f"Could not fetch emissions summary: {e}")
        return None


def get_current_carbon_intensity():
    """Get current carbon intensity"""
    try:
        response = requests.get(f"{API_URL}/carbon/current", timeout=5)
        if response.status_code == 200:
            return response.json()
        return None
    except Exception:
        return None


def create_strategy_comparison_chart(history_data):
    """Create comparative bar chart for strategies"""
    if not history_data:
        return None

    # Aggregate by strategy (using decision_reason or selected_plan)
    df = pd.DataFrame(history_data)

    # For demo purposes, create mock strategy groupings
    # In production, you'd parse the actual strategy from decision_reason
    strategies = ["Latency-First", "Balanced Hybrid", "Carbon-Aware Deferred"]
    emissions_avg = [0.45, 0.38, 0.32]  # Mock data in gCO₂
    latency_avg = [120, 180, 220]  # Mock data in ms

    # If we have real data, try to compute actual averages
    if len(df) > 0 and "estimated_emissions_gco2" in df.columns:
        # Split data into thirds as mock strategies
        third = len(df) // 3
        emissions_avg = [
            df.iloc[:third]["estimated_emissions_gco2"].mean() if third > 0 else 0.45,
            (
                df.iloc[third : 2 * third]["estimated_emissions_gco2"].mean()
                if third > 0
                else 0.38
            ),
            (
                df.iloc[2 * third :]["estimated_emissions_gco2"].mean()
                if third > 0
                else 0.32
            ),
        ]
        if "execution_time_ms" in df.columns:
            latency_avg = [
                df.iloc[:third]["execution_time_ms"].mean() if third > 0 else 120,
                (
                    df.iloc[third : 2 * third]["execution_time_ms"].mean()
                    if third > 0
                    else 180
                ),
                df.iloc[2 * third :]["execution_time_ms"].mean() if third > 0 else 220,
            ]

    fig = go.Figure(
        data=[
            go.Bar(
                name="CO₂ Emissions (gCO₂)",
                x=strategies,
                y=emissions_avg,
                marker_color="#EF5350",
                text=[f"{v:.2f}" for v in emissions_avg],
                textposition="auto",
            ),
            go.Bar(
                name="Latency (ms)",
                x=strategies,
                y=latency_avg,
                marker_color="#42A5F5",
                yaxis="y2",
                text=[f"{v:.0f}" for v in latency_avg],
                textposition="auto",
            ),
        ]
    )

    fig.update_layout(
        title="Strategy Comparison: Emissions vs Latency",
        xaxis=dict(title="Strategy"),
        yaxis=dict(title="CO₂ Emissions (gCO₂)", side="left", showgrid=False),
        yaxis2=dict(title="Latency (ms)", side="right", overlaying="y", showgrid=False),
        barmode="group",
        hovermode="x unified",
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    return fig


def create_carbon_intensity_chart(carbon_df, deferred_queries=None):
    """Create time series chart for carbon intensity"""
    if carbon_df.empty:
        return None

    fig = go.Figure()

    # Main carbon intensity line
    fig.add_trace(
        go.Scatter(
            x=carbon_df["timestamp"],
            y=carbon_df["carbon_intensity_gco2_kwh"],
            mode="lines+markers",
            name="Carbon Intensity",
            line=dict(color="#2E7D32", width=2),
            marker=dict(size=6),
        )
    )

    # Add threshold line
    threshold = 400
    fig.add_hline(
        y=threshold,
        line_dash="dash",
        line_color="red",
        annotation_text=f"Threshold ({threshold} gCO₂/kWh)",
    )

    # Add deferred query markers if available
    if deferred_queries:
        deferred_times = [
            pd.to_datetime(q.get("executed_at"))
            for q in deferred_queries
            if q.get("executed_at")
        ]
        if deferred_times:
            # Find corresponding carbon intensities
            deferred_intensities = []
            for dt in deferred_times:
                closest_idx = (carbon_df["timestamp"] - dt).abs().idxmin()
                deferred_intensities.append(
                    carbon_df.loc[closest_idx, "carbon_intensity_gco2_kwh"]
                )

            fig.add_trace(
                go.Scatter(
                    x=deferred_times,
                    y=deferred_intensities,
                    mode="markers",
                    name="Deferred Query Executed",
                    marker=dict(size=12, color="orange", symbol="star"),
                )
            )

    fig.update_layout(
        title="Historical Carbon Intensity with Deferred Executions",
        xaxis_title="Time",
        yaxis_title="Carbon Intensity (gCO₂/kWh)",
        hovermode="x unified",
        height=400,
        showlegend=True,
    )

    return fig


# Main App
def main():
    # Header
    st.markdown(
        '<h1 class="main-header">🌱 Carbon-Aware SQL Engine</h1>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<p class="sub-header">Optimize your queries for minimal carbon footprint</p>',
        unsafe_allow_html=True,
    )

    # Current carbon intensity banner
    current_ci = get_current_carbon_intensity()
    if current_ci:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric(
                "Current Carbon Intensity",
                f"{current_ci['carbon_intensity_gco2_kwh']:.0f} gCO₂/kWh",
            )
        with col2:
            st.metric("Zone", current_ci["zone"])
        with col3:
            st.metric("Source", current_ci["source"])
        with col4:
            summary = fetch_emissions_summary()
            if summary:
                st.metric("Total Queries", summary["total_queries"])

    st.divider()

    # Visualizations Section
    st.header("📊 Performance Analytics")

    viz_col1, viz_col2 = st.columns(2)

    with viz_col1:
        st.subheader("Strategy Comparison")
        history = fetch_query_history()
        comparison_chart = create_strategy_comparison_chart(history)
        if comparison_chart:
            st.plotly_chart(comparison_chart, use_container_width=True)
        else:
            st.info(
                "No historical data available yet. Execute some queries to see comparisons!"
            )

    with viz_col2:
        st.subheader("Carbon Intensity Timeline")
        carbon_df = load_carbon_history()
        deferred = (
            [q for q in history if q.get("status") == "deferred"] if history else []
        )
        intensity_chart = create_carbon_intensity_chart(carbon_df, deferred)
        if intensity_chart:
            st.plotly_chart(intensity_chart, use_container_width=True)
        else:
            st.error("Carbon history data not available")

    st.divider()

    # Query Execution Section
    st.header("💻 Execute Carbon-Aware Query")

    input_col1, input_col2 = st.columns([3, 1])

    with input_col1:
        sql_query = st.text_area(
            "SQL Query",
            value="SELECT COUNT(*) FROM users WHERE created_at > '2024-01-01'",
            height=120,
            help="Enter your SQL query here",
        )

    with input_col2:
        urgency_options = {
            "Low": "low",
            "Medium": "medium",
            "High": "high",
            "Critical": "critical",
        }
        urgency_display = st.selectbox(
            "Urgency Level",
            options=list(urgency_options.keys()),
            index=1,
            help="Higher urgency may execute immediately despite high carbon intensity",
        )
        urgency = urgency_options[urgency_display]

        st.markdown("###")  # Spacing
        execute_button = st.button("🚀 Execute Query", use_container_width=True)

    # Execute query
    if execute_button:
        if not API_KEY:
            st.error(
                "❌ API Key not configured. Please set ENERGY_ML_API_KEY environment variable."
            )
        else:
            with st.spinner("Executing query..."):
                try:
                    headers = {"X-API-Key": API_KEY, "Content-Type": "application/json"}
                    payload = {"query": sql_query, "urgency": urgency, "explain": True}

                    response = requests.post(
                        f"{API_URL}/query/execute",
                        headers=headers,
                        json=payload,
                        timeout=30,
                    )

                    if response.status_code == 200:
                        result = response.json()

                        # Display decision
                        if result.get("deferred"):
                            st.warning(
                                f"⏸️ Query Deferred for {result.get('defer_minutes', 'N/A')} minutes"
                            )
                        else:
                            st.success("✅ Query Executed Successfully")

                        # Decision rationale box
                        st.markdown(
                            '<div class="decision-box">', unsafe_allow_html=True
                        )
                        st.markdown("### 🧠 Decision Rationale")

                        strategy = result.get("selected_plan", "N/A").upper()
                        st.markdown(
                            f"**The Carbon-Aware Selector chose the `{strategy}` strategy**"
                        )

                        st.markdown(
                            f"**Reasoning:** {result.get('decision_reason', 'N/A')}"
                        )

                        # Display metrics with formulas
                        carbon_intensity = result.get("carbon_intensity_gco2_kwh", 0)
                        threshold = 400  # You can make this dynamic

                        st.markdown("**Key Metrics:**")
                        metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(
                            4
                        )

                        with metric_col1:
                            st.metric(
                                "Carbon Intensity",
                                f"{carbon_intensity:.0f} gCO₂/kWh",
                            )
                        with metric_col2:
                            st.metric(
                                "Energy Consumed",
                                f"{result.get('energy_joules', 0):.2f} J",
                            )
                        with metric_col3:
                            st.metric(
                                "Execution Time",
                                f"{result.get('execution_time_ms', 0):.0f} ms",
                            )
                        with metric_col4:
                            forecast_unc = result.get(
                                "forecast_uncertainty_gco2_kwh", 0
                            )
                            energy_std = result.get("energy_std_dev_joules", 0)
                            if forecast_unc or energy_std:
                                st.metric(
                                    "Forecast Uncertainty",
                                    (
                                        f"{forecast_unc:.2f} gCO₂/kWh"
                                        if forecast_unc
                                        else "N/A"
                                    ),
                                )
                                st.metric(
                                    "Energy Std Dev",
                                    f"{energy_std:.2f} J" if energy_std else "N/A",
                                )

                        # Mathematical notation
                        if carbon_intensity and threshold:
                            st.markdown("**Decision Formula:**")
                            if carbon_intensity > threshold:
                                st.latex(
                                    rf"CI  ({carbon_intensity:.0f}) > Threshold ({threshold}) \Rightarrow Consider Deferral"
                                )
                            else:
                                st.latex(
                                    rf"CI ({carbon_intensity:.0f}) \leq Threshold ({threshold}) \Rightarrow Execute Now"
                                )

                        st.metric(
                            "Estimated CO₂ Emissions",
                            f"{result.get('estimated_emissions_gco2', 0):.4f} gCO₂",
                        )

                        st.markdown("</div>", unsafe_allow_html=True)

                        # Show result data if available
                        if result.get("result"):
                            with st.expander("📋 Query Results"):
                                st.json(result["result"])

                    elif response.status_code == 401:
                        st.error("❌ Authentication failed. Please check your API key.")
                    elif response.status_code == 429:
                        st.error(
                            "⚠️ Rate limit exceeded. Please wait before trying again."
                        )
                    else:
                        st.error(
                            f"❌ Request failed: {response.status_code} - {response.text}"
                        )

                except requests.exceptions.Timeout:
                    st.error(
                        "⏱️ Request timed out. The API server might be unavailable."
                    )
                except requests.exceptions.ConnectionError:
                    st.error(
                        "🔌 Connection error. Please ensure the API server is running at "
                        + API_URL
                    )
                except Exception as e:
                    st.error(f"❌ An error occurred: {str(e)}")

    # Footer
    st.divider()
    st.markdown(
        """
        <div style='text-align: center; color: #666; padding: 2rem 0;'>
            <p>🌱 Carbon-Aware SQL Engine • Optimizing for a greener future</p>
            <p style='font-size: 0.9rem;'>API Status: {} • Version 1.0.0</p>
        </div>
        """.format(
            "🟢 Connected" if current_ci else "🔴 Disconnected"
        ),
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
