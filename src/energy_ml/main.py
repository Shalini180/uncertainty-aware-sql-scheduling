import streamlit as st
import duckdb
import pandas as pd

st.title("Energy-aware ML Playground")
st.write("Hello! ?? This is a starter Streamlit app.")

con = duckdb.connect(":memory:")
df = pd.DataFrame({"x": [1, 2, 3], "y": [3, 2, 1]})
st.dataframe(df)
st.plotly_chart(
    {
        "data": [{"x": df["x"], "y": df["y"], "type": "scatter"}],
        "layout": {"title": "Sample"},
    }
)
