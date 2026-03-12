import streamlit as st
import plotly.graph_objects as go
import plotly.io as pio

# Color palette - quant terminal inspired
COLORS = {
    "bg": "#0a0e1a",
    "card": "#111827",
    "border": "#1e293b",
    "text": "#e2e8f0",
    "muted": "#64748b",
    "primary": "#3b82f6",
    "success": "#10b981",
    "danger": "#ef4444",
    "warning": "#f59e0b",
    "info": "#06b6d4",
    # Chart colors
    "revenue": "#3b82f6",
    "cogs": "#ef4444",
    "fees": "#f59e0b",
    "profit": "#10b981",
    "cm1": "#06b6d4",
    "cm2": "#8b5cf6",
    "cm3": "#10b981",
}


def setup_page(title="nesell analytics"):
    st.set_page_config(
        page_title=title,
        page_icon="◆",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    inject_css()
    setup_plotly_theme()


def inject_css():
    css_path = "assets/style.css"
    try:
        with open(css_path) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    except FileNotFoundError:
        pass


def setup_plotly_theme():
    """Quant terminal Plotly theme."""
    pio.templates["quant"] = go.layout.Template(
        layout=go.Layout(
            paper_bgcolor=COLORS["bg"],
            plot_bgcolor=COLORS["bg"],
            font=dict(
                family="'JetBrains Mono', 'Fira Code', 'SF Mono', monospace",
                color=COLORS["text"],
                size=11,
            ),
            xaxis=dict(
                gridcolor=COLORS["border"],
                zerolinecolor=COLORS["border"],
                showgrid=True,
                gridwidth=1,
            ),
            yaxis=dict(
                gridcolor=COLORS["border"],
                zerolinecolor=COLORS["border"],
                showgrid=True,
                gridwidth=1,
            ),
            colorway=[
                COLORS["primary"],
                COLORS["success"],
                COLORS["warning"],
                COLORS["danger"],
                COLORS["info"],
                COLORS["cm2"],
            ],
            margin=dict(l=40, r=20, t=40, b=30),
            legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=10)),
            hoverlabel=dict(
                bgcolor=COLORS["card"],
                font_size=11,
                font_family="monospace",
            ),
        )
    )
    pio.templates.default = "quant"
