"""nesell-analytics Dashboard — Streamlit app."""
import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta

# --- Supabase REST API ---
from etl import config

_HEADERS = {
    "apikey": config.SUPABASE_KEY,
    "Authorization": f"Bearer {config.SUPABASE_KEY}",
}


def _get(table, params=None):
    url = f"{config.SUPABASE_URL}/rest/v1/{table}"
    resp = requests.get(url, headers=_HEADERS, params=params or {})
    resp.raise_for_status()
    return resp.json()


# --- Page config ---
st.set_page_config(
    page_title="nesell analytics",
    page_icon="📊",
    layout="wide",
)

st.title("nesell analytics")

# --- Date range selector ---
col1, col2, col3 = st.columns([1, 1, 2])
with col1:
    days = st.selectbox("Okres", [7, 14, 30, 60, 90], index=2, format_func=lambda x: f"Ostatnie {x} dni")
with col2:
    st.write("")  # spacer

cutoff = str(date.today() - timedelta(days=days))


# --- Load data ---
@st.cache_data(ttl=300)
def load_daily_metrics(cutoff_date):
    rows = _get("daily_metrics", {
        "select": "date,platform_id,sku,orders_count,units_sold,revenue,revenue_pln,cogs,platform_fees,shipping_cost,gross_profit,margin_pct",
        "date": f"gte.{cutoff_date}",
        "order": "date.asc",
    })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


@st.cache_data(ttl=300)
def load_platforms():
    rows = _get("platforms", {"select": "id,code,name"})
    return {r["id"]: r for r in rows}


@st.cache_data(ttl=300)
def load_orders(cutoff_date):
    rows = _get("orders", {
        "select": "id,platform_id,order_date,total_paid,currency,status",
        "order_date": f"gte.{cutoff_date}",
        "status": "neq.cancelled",
    })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


@st.cache_data(ttl=300)
def load_products():
    rows = _get("products", {"select": "sku,name,cost_pln,source,is_parent"})
    return pd.DataFrame(rows) if rows else pd.DataFrame()


platforms = load_platforms()
df = load_daily_metrics(cutoff)
orders_df = load_orders(cutoff)

if df.empty:
    st.warning("Brak danych metrycznych. Uruchom ETL: `python3.11 -m etl.run --aggregate`")
    st.stop()

# Map platform names
df["platform"] = df["platform_id"].map(lambda x: platforms.get(x, {}).get("code", f"#{x}"))

# --- KPIs ---
st.markdown("---")
total_revenue = df["revenue_pln"].sum()
total_cogs = df["cogs"].sum()
total_fees = df["platform_fees"].sum()
total_profit = df["gross_profit"].sum()
total_orders = df["orders_count"].sum()
total_units = df["units_sold"].sum()
avg_margin = (total_profit / total_revenue * 100) if total_revenue > 0 else 0

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Przychod (PLN)", f"{total_revenue:,.0f}")
k2.metric("COGS", f"{total_cogs:,.0f}")
k3.metric("Prowizje", f"{total_fees:,.0f}")
k4.metric("Zysk brutto", f"{total_profit:,.0f}")
k5.metric("Zamowienia", f"{int(total_orders):,}")
k6.metric("Marza", f"{avg_margin:.1f}%")

# --- Revenue & Profit trend ---
st.markdown("---")
st.subheader("Przychod i zysk dzienny")

daily = df.groupby("date").agg(
    revenue=("revenue_pln", "sum"),
    profit=("gross_profit", "sum"),
    cogs=("cogs", "sum"),
    fees=("platform_fees", "sum"),
    orders=("orders_count", "sum"),
).reset_index()
daily["date"] = pd.to_datetime(daily["date"])
daily = daily.sort_values("date")

fig = go.Figure()
fig.add_trace(go.Bar(x=daily["date"], y=daily["revenue"], name="Przychod", marker_color="#4CAF50"))
fig.add_trace(go.Bar(x=daily["date"], y=daily["cogs"], name="COGS", marker_color="#FF9800"))
fig.add_trace(go.Bar(x=daily["date"], y=daily["fees"], name="Prowizje", marker_color="#F44336"))
fig.add_trace(go.Scatter(x=daily["date"], y=daily["profit"], name="Zysk", mode="lines+markers",
                         line=dict(color="#2196F3", width=3)))
fig.update_layout(barmode="stack", height=400, margin=dict(l=0, r=0, t=30, b=0),
                  legend=dict(orientation="h", yanchor="bottom", y=1.02))
st.plotly_chart(fig, use_container_width=True)

# --- Platform breakdown ---
st.markdown("---")
col_plat, col_pie = st.columns([3, 2])

with col_plat:
    st.subheader("Platformy")
    plat = df.groupby("platform").agg(
        revenue=("revenue_pln", "sum"),
        profit=("gross_profit", "sum"),
        orders=("orders_count", "sum"),
        units=("units_sold", "sum"),
    ).reset_index()
    plat["margin"] = (plat["profit"] / plat["revenue"] * 100).round(1).fillna(0)
    plat = plat.sort_values("revenue", ascending=False)
    plat.columns = ["Platforma", "Przychod PLN", "Zysk PLN", "Zamowienia", "Sztuki", "Marza %"]
    st.dataframe(plat, use_container_width=True, hide_index=True)

with col_pie:
    st.subheader("Udzial w przychodzie")
    fig_pie = px.pie(plat, values="Przychod PLN", names="Platforma", hole=0.4)
    fig_pie.update_layout(height=350, margin=dict(l=0, r=0, t=30, b=0))
    st.plotly_chart(fig_pie, use_container_width=True)

# --- Platform trends ---
st.markdown("---")
st.subheader("Trend przychodow per platforma")

plat_daily = df.groupby(["date", "platform"]).agg(revenue=("revenue_pln", "sum")).reset_index()
plat_daily["date"] = pd.to_datetime(plat_daily["date"])

# 7-day rolling average
plat_trend = plat_daily.pivot(index="date", columns="platform", values="revenue").fillna(0)
plat_trend = plat_trend.rolling(7, min_periods=1).mean()

fig_trend = go.Figure()
for col in plat_trend.columns:
    fig_trend.add_trace(go.Scatter(x=plat_trend.index, y=plat_trend[col], name=col, mode="lines"))
fig_trend.update_layout(height=350, margin=dict(l=0, r=0, t=30, b=0),
                        yaxis_title="Przychod PLN (7d avg)",
                        legend=dict(orientation="h", yanchor="bottom", y=1.02))
st.plotly_chart(fig_trend, use_container_width=True)

# --- Top sellers ---
st.markdown("---")
st.subheader("Top produkty")

products_df = load_products()

top = df.groupby("sku").agg(
    revenue=("revenue_pln", "sum"),
    profit=("gross_profit", "sum"),
    units=("units_sold", "sum"),
    orders=("orders_count", "sum"),
).reset_index()

if not products_df.empty:
    top = top.merge(products_df[["sku", "name", "source"]], on="sku", how="left")
else:
    top["name"] = ""
    top["source"] = ""

top["margin"] = (top["profit"] / top["revenue"] * 100).round(1).fillna(0)
top = top.sort_values("revenue", ascending=False).head(30)
top_display = top[["sku", "name", "source", "units", "revenue", "profit", "margin"]].copy()
top_display.columns = ["SKU", "Nazwa", "Zrodlo", "Szt.", "Przychod PLN", "Zysk PLN", "Marza %"]
st.dataframe(top_display, use_container_width=True, hide_index=True)

# --- Orders count trend ---
st.markdown("---")
st.subheader("Zamowienia dziennie")

fig_orders = px.bar(daily, x="date", y="orders", color_discrete_sequence=["#2196F3"])
fig_orders.update_layout(height=300, margin=dict(l=0, r=0, t=30, b=0), yaxis_title="Zamowienia")
st.plotly_chart(fig_orders, use_container_width=True)

# --- Footer ---
st.markdown("---")
st.caption(f"Dane z {cutoff} do {date.today()} | Ostatnia aktualizacja: uruchom `python3.11 -m etl.run`")
