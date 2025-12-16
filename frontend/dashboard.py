import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from streamlit.runtime.scriptrunner import add_script_run_ctx
from streamlit_autorefresh import st_autorefresh


API_URL = "http://127.0.0.1:8000"

st.set_page_config(
    page_title="Quant Analytics Dashboard",
    layout="wide",
)
# Auto-refresh every 5 seconds
st_autorefresh(interval=5000, key="data_refresh")


st.title("📊 Quant Analytics – Stat Arb Monitor")

# ---------------- Sidebar Controls ----------------
st.sidebar.header("Controls")

symbol_x = st.sidebar.selectbox("Symbol X", ["BTCUSDT"])
symbol_y = st.sidebar.selectbox("Symbol Y", ["ETHUSDT"])

timeframe = st.sidebar.selectbox("Timeframe", ["1m", "5m"])
window = st.sidebar.slider("Rolling Window", 1, 50, 20)

z_threshold = st.sidebar.slider("Z-Score Threshold", 1.0, 3.0, 2.0)
corr_threshold = st.sidebar.slider("Correlation Threshold", 0.5, 0.9, 0.7)

# ---------------- Helper ----------------
def fetch(endpoint, params=None):
    r = requests.get(f"{API_URL}{endpoint}", params=params)
    r.raise_for_status()
    return pd.DataFrame(r.json())

# ---------------- Hedge Ratio ----------------
st.subheader("📐 Hedge Ratio (OLS)")

hr = requests.get(
    f"{API_URL}/hedge-ratio",
    params={
        "symbol_x": symbol_x,
        "symbol_y": symbol_y,
        "timeframe": timeframe,
    },
).json()

st.json(hr)

# ---------------- Spread & Z-score ----------------
st.subheader("📉 Spread & Z-Score")

spread_df = fetch(
    "/spread",
    {
        "symbol_x": symbol_x,
        "symbol_y": symbol_y,
        "timeframe": timeframe,
    },
)

z_df = fetch(
    "/zscore",
    {
        "symbol_x": symbol_x,
        "symbol_y": symbol_y,
        "timeframe": timeframe,
        "window": window,
    },
)

# Ensure timestamp exists
if "timestamp" not in spread_df.columns:
    spread_df = spread_df.reset_index()

if z_df.empty or "timestamp" not in z_df.columns:
    merged = spread_df.copy()
    merged["zscore"] = None
    st.warning("Not enough data yet to compute Z-score.")
else:
    merged = pd.merge(
        spread_df,
        z_df,
        on="timestamp",
        how="left",
    )


fig = go.Figure()

# --- Identify spread column safely ---
spread_col = None
for col in merged.columns:
    if col.lower().startswith("spread"):
        spread_col = col
        break

if spread_col is None:
    st.error("Spread data not available yet.")
else:
    fig.add_trace(
        go.Scatter(
            x=merged["timestamp"],
            y=merged[spread_col],
            name="Spread",
            line=dict(color="blue"),
        )
    )


fig.add_trace(
    go.Scatter(
        x=merged["timestamp"],
        y=merged.get("zscore"),
        name="Z-Score",
        yaxis="y2",
        line=dict(color="red"),
    )
)

fig.update_layout(
    yaxis=dict(title="Spread"),
    yaxis2=dict(
        title="Z-Score",
        overlaying="y",
        side="right",
    ),
    height=400,
)

st.plotly_chart(fig, use_container_width=True)

# ---------------- Rolling Correlation ----------------
st.subheader("🔗 Rolling Correlation")

corr_df = fetch(
    "/correlation",
    {
        "symbol_x": symbol_x,
        "symbol_y": symbol_y,
        "timeframe": timeframe,
        "window": window,
    },
)

if corr_df.empty or "timestamp" not in corr_df.columns:
    st.warning("Not enough data yet to compute rolling correlation.")
else:
    st.line_chart(corr_df.set_index("timestamp"))


# ---------------- Alerts ----------------
st.subheader("🚨 Alerts")

alerts = requests.get(
    f"{API_URL}/alerts",
    params={
        "symbol_x": symbol_x,
        "symbol_y": symbol_y,
        "timeframe": timeframe,
        "window": window,
        "z_threshold": z_threshold,
        "corr_threshold": corr_threshold,
    },
).json()

if alerts:
    st.error("⚠️ Active Alerts")
    st.json(alerts)
else:
    st.success("✅ No active alerts")

# ---------------- Export ----------------
st.subheader("⬇️ Export Data")

st.download_button(
    "Download Spread + Z-Score CSV",
    merged.to_csv(index=False),
    file_name="spread_zscore.csv",
)
