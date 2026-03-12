"""nesell analytics v2 - Main entry point."""
import streamlit as st
from lib.theme import setup_page, COLORS
from lib.data import load_daily_metrics

setup_page("nesell analytics")

st.markdown(
    """
<div style="padding: 2rem 0 1rem 0;">
    <h1 style="font-family: 'JetBrains Mono', monospace; font-size: 1.8rem; font-weight: 700;
               letter-spacing: 0.15em; color: #e2e8f0; margin: 0;">
        NESELL ANALYTICS
    </h1>
    <p style="font-family: 'JetBrains Mono', monospace; font-size: 0.75rem;
              color: #64748b; letter-spacing: 0.05em; margin-top: 4px;">
        ECOMMERCE INTELLIGENCE PLATFORM
    </p>
</div>
""",
    unsafe_allow_html=True,
)

# Data freshness check
df = load_daily_metrics(days=7)
if not df.empty:
    latest = df["date"].max()
    st.markdown(
        f'<div class="freshness-badge">Last data: {latest}</div>',
        unsafe_allow_html=True,
    )
else:
    st.warning("No data available. Run ETL: python3.11 -m etl.run")

st.markdown(
    """
<div style="margin-top: 2rem; font-family: 'JetBrains Mono', monospace; font-size: 0.85rem; color: #94a3b8; line-height: 2;">
    Navigate using the sidebar to access analytics modules:
    <br>
    <span style="color: #3b82f6;">01</span> COMMAND CENTER &mdash; Overview, KPIs, signals
    <br>
    <span style="color: #3b82f6;">02</span> P&L &mdash; Waterfall, contribution margins, fee decomposition
    <br>
    <span style="color: #3b82f6;">03</span> PRODUCTS &mdash; SKU profitability, portfolio quadrant, Pareto
    <br>
    <span style="color: #3b82f6;">04</span> MARKETS &mdash; Marketplace comparison, FX, heatmaps
    <br>
    <span style="color: #3b82f6;">05</span> AMAZON &mdash; Traffic, inventory, returns, BSR, pricing
    <br>
    <span style="color: #3b82f6;">06</span> TRENDS &mdash; MoM, seasonality, growth trajectory
</div>
""",
    unsafe_allow_html=True,
)
