"""nesell-analytics Dashboard — Streamlit app."""
import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta
import calendar

# --- Supabase REST API ---
try:
    SUPABASE_URL = st.secrets["SUPABASE_URL"]
    SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
except Exception:
    from etl import config
    SUPABASE_URL = config.SUPABASE_URL
    SUPABASE_KEY = config.SUPABASE_KEY

_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
}


def _get(table, params=None):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    resp = requests.get(url, headers=_HEADERS, params=params or {})
    resp.raise_for_status()
    return resp.json()


# --- Page config ---
st.set_page_config(page_title="nesell analytics", page_icon="📊", layout="wide")

# --- Sidebar filters ---
st.sidebar.title("nesell analytics")
st.sidebar.markdown("---")

days = st.sidebar.selectbox("Okres", [7, 14, 30, 60, 90], index=2,
                            format_func=lambda x: f"Ostatnie {x} dni")
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
def load_products():
    rows = _get("products", {"select": "sku,name,cost_pln,cost_eur,source,is_parent,parent_sku"})
    return pd.DataFrame(rows) if rows else pd.DataFrame()


platforms = load_platforms()
df = load_daily_metrics(cutoff)
products_df = load_products()

if df.empty:
    st.warning("Brak danych. Uruchom ETL: `python3.11 -m etl.run`")
    st.stop()

df["platform"] = df["platform_id"].map(lambda x: platforms.get(x, {}).get("code", f"#{x}"))
df["date_dt"] = pd.to_datetime(df["date"])

# --- Sidebar: platform filter ---
all_platforms = sorted(df["platform"].unique())
selected_platforms = st.sidebar.multiselect("Platformy", all_platforms, default=all_platforms)

# --- Sidebar: SKU search ---
sku_search = st.sidebar.text_input("Szukaj SKU / produkt", "")

# Apply filters
mask = df["platform"].isin(selected_platforms)
if sku_search:
    sku_match = df["sku"].str.contains(sku_search, case=False, na=False)
    if not products_df.empty:
        matching_skus = products_df[products_df["name"].str.contains(sku_search, case=False, na=False)]["sku"].tolist()
        sku_match = sku_match | df["sku"].isin(matching_skus)
    mask = mask & sku_match

df_filtered = df[mask].copy()

if df_filtered.empty:
    st.warning("Brak danych dla wybranych filtrow.")
    st.stop()

# ==================== MAIN CONTENT ====================

# --- KPIs ---
total_revenue = df_filtered["revenue_pln"].sum()
total_cogs = df_filtered["cogs"].sum()
total_fees = df_filtered["platform_fees"].sum()
total_profit = df_filtered["gross_profit"].sum()
total_orders = df_filtered["orders_count"].sum()
total_units = df_filtered["units_sold"].sum()
avg_margin = (total_profit / total_revenue * 100) if total_revenue > 0 else 0

# COGS coverage warning
cogs_revenue = df_filtered[df_filtered["cogs"] > 0]["revenue_pln"].sum()
cogs_coverage = (cogs_revenue / total_revenue * 100) if total_revenue > 0 else 0
if cogs_coverage < 90:
    st.warning(f"COGS pokrywa tylko {cogs_coverage:.0f}% przychodu. "
               f"Produkty bez kosztow zawyżają marze. Realna marza (z COGS): "
               f"{df_filtered[df_filtered['cogs'] > 0]['gross_profit'].sum() / cogs_revenue * 100:.1f}%"
               if cogs_revenue > 0 else "")

# Previous period for deltas
prev_cutoff = str(date.today() - timedelta(days=days * 2))
prev_end = cutoff

@st.cache_data(ttl=300)
def load_prev_metrics(prev_start, prev_end_date):
    rows = _get("daily_metrics", {
        "select": "date,platform_id,sku,revenue_pln,gross_profit,cogs,platform_fees,orders_count,units_sold",
        "date": f"gte.{prev_start}",
        "order": "date.asc",
    })
    if not rows:
        return pd.DataFrame()
    pdf = pd.DataFrame(rows)
    pdf = pdf[pdf["date"] < prev_end_date]
    return pdf

df_prev = load_prev_metrics(prev_cutoff, prev_end)
if not df_prev.empty:
    df_prev["platform"] = df_prev["platform_id"].map(lambda x: platforms.get(x, {}).get("code", f"#{x}"))
    prev_mask = df_prev["platform"].isin(selected_platforms)
    if sku_search:
        prev_sku_match = df_prev["sku"].str.contains(sku_search, case=False, na=False)
        if not products_df.empty:
            prev_sku_match = prev_sku_match | df_prev["sku"].isin(matching_skus if sku_search else [])
        prev_mask = prev_mask & prev_sku_match
    df_prev_f = df_prev[prev_mask]
    prev_revenue = df_prev_f["revenue_pln"].sum()
    prev_profit = df_prev_f["gross_profit"].sum()
    prev_orders = df_prev_f["orders_count"].sum()
    prev_margin = (prev_profit / prev_revenue * 100) if prev_revenue > 0 else 0
    d_rev = f"{(total_revenue - prev_revenue) / prev_revenue * 100:+.0f}%" if prev_revenue > 0 else None
    d_prof = f"{(total_profit - prev_profit) / prev_profit * 100:+.0f}%" if prev_profit > 0 else None
    d_ord = f"{int(total_orders - prev_orders):+d}" if prev_orders > 0 else None
    d_margin = f"{avg_margin - prev_margin:+.1f}pp" if prev_revenue > 0 else None
else:
    d_rev = d_prof = d_ord = d_margin = None

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Przychod (PLN)", f"{total_revenue:,.0f}", delta=d_rev)
k2.metric("COGS", f"{total_cogs:,.0f}")
k3.metric("Prowizje", f"{total_fees:,.0f}")
k4.metric("Zysk brutto", f"{total_profit:,.0f}", delta=d_prof)
k5.metric("Zamowienia", f"{int(total_orders):,}", delta=d_ord)
k6.metric("Marza", f"{avg_margin:.1f}%", delta=d_margin)

# --- Tabs ---
tab_overview, tab_platforms, tab_products, tab_orders, tab_mom = st.tabs([
    "Przegląd", "Platformy", "Produkty", "Zamówienia", "Miesiąc vs miesiąc"
])

# ==================== TAB: OVERVIEW ====================
with tab_overview:
    daily = df_filtered.groupby("date").agg(
        revenue=("revenue_pln", "sum"),
        profit=("gross_profit", "sum"),
        cogs=("cogs", "sum"),
        fees=("platform_fees", "sum"),
        orders=("orders_count", "sum"),
    ).reset_index()
    daily["date"] = pd.to_datetime(daily["date"])
    daily = daily.sort_values("date")
    daily["margin"] = (daily["profit"] / daily["revenue"] * 100).round(1).fillna(0)

    # Revenue & Profit chart
    st.subheader("Przychod i zysk dzienny")
    fig = go.Figure()
    fig.add_trace(go.Bar(x=daily["date"], y=daily["revenue"], name="Przychod", marker_color="#4CAF50"))
    fig.add_trace(go.Bar(x=daily["date"], y=daily["cogs"], name="COGS", marker_color="#FF9800"))
    fig.add_trace(go.Bar(x=daily["date"], y=daily["fees"], name="Prowizje", marker_color="#F44336"))
    fig.add_trace(go.Scatter(x=daily["date"], y=daily["profit"], name="Zysk",
                             mode="lines+markers", line=dict(color="#2196F3", width=3)))
    fig.update_layout(barmode="stack", height=400, margin=dict(l=0, r=0, t=30, b=0),
                      legend=dict(orientation="h", yanchor="bottom", y=1.02))
    st.plotly_chart(fig, use_container_width=True)

    # Margin trend
    st.subheader("Trend marzy (%)")
    daily["margin_7d"] = daily["margin"].rolling(7, min_periods=1).mean()
    fig_margin = go.Figure()
    fig_margin.add_trace(go.Scatter(x=daily["date"], y=daily["margin"], name="Marza dzienna",
                                    mode="markers", marker=dict(size=5, color="#90CAF9", opacity=0.5)))
    fig_margin.add_trace(go.Scatter(x=daily["date"], y=daily["margin_7d"], name="Srednia 7d",
                                    mode="lines", line=dict(color="#2196F3", width=3)))
    fig_margin.update_layout(height=300, margin=dict(l=0, r=0, t=30, b=0),
                             yaxis_title="Marza %", yaxis=dict(range=[0, 100]),
                             legend=dict(orientation="h", yanchor="bottom", y=1.02))
    st.plotly_chart(fig_margin, use_container_width=True)

    # Orders per day
    st.subheader("Zamowienia dziennie")
    fig_orders = px.bar(daily, x="date", y="orders", color_discrete_sequence=["#2196F3"])
    fig_orders.update_layout(height=250, margin=dict(l=0, r=0, t=30, b=0), yaxis_title="Zamowienia")
    st.plotly_chart(fig_orders, use_container_width=True)


# ==================== TAB: PLATFORMS ====================
with tab_platforms:
    col_plat, col_pie = st.columns([3, 2])

    plat = df_filtered.groupby("platform").agg(
        revenue=("revenue_pln", "sum"),
        profit=("gross_profit", "sum"),
        cogs=("cogs", "sum"),
        fees=("platform_fees", "sum"),
        orders=("orders_count", "sum"),
        units=("units_sold", "sum"),
    ).reset_index()
    plat["margin"] = (plat["profit"] / plat["revenue"] * 100).round(1).fillna(0)
    plat = plat.sort_values("revenue", ascending=False)

    with col_plat:
        st.subheader("Podsumowanie platform")
        plat_display = plat[["platform", "revenue", "cogs", "fees", "profit", "margin", "orders", "units"]].copy()
        plat_display.columns = ["Platforma", "Przychod", "COGS", "Prowizje", "Zysk", "Marza %", "Zamowienia", "Szt."]
        st.dataframe(plat_display, use_container_width=True, hide_index=True)

    with col_pie:
        st.subheader("Udzial w przychodzie")
        fig_pie = px.pie(plat, values="revenue", names="platform", hole=0.4)
        fig_pie.update_layout(height=350, margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig_pie, use_container_width=True)

    # Platform trends
    st.subheader("Trend przychodow per platforma (7d avg)")
    plat_daily = df_filtered.groupby(["date", "platform"]).agg(revenue=("revenue_pln", "sum")).reset_index()
    plat_daily["date"] = pd.to_datetime(plat_daily["date"])
    plat_trend = plat_daily.pivot(index="date", columns="platform", values="revenue").fillna(0)
    plat_trend = plat_trend.rolling(7, min_periods=1).mean()

    fig_trend = go.Figure()
    for c in plat_trend.columns:
        fig_trend.add_trace(go.Scatter(x=plat_trend.index, y=plat_trend[c], name=c, mode="lines"))
    fig_trend.update_layout(height=350, margin=dict(l=0, r=0, t=30, b=0),
                            yaxis_title="Przychod PLN (7d avg)",
                            legend=dict(orientation="h", yanchor="bottom", y=1.02))
    st.plotly_chart(fig_trend, use_container_width=True)


# ==================== TAB: PRODUCTS ====================
with tab_products:
    st.subheader("Top produkty")

    top = df_filtered.groupby("sku").agg(
        revenue=("revenue_pln", "sum"),
        profit=("gross_profit", "sum"),
        cogs=("cogs", "sum"),
        units=("units_sold", "sum"),
        orders=("orders_count", "sum"),
    ).reset_index()

    if not products_df.empty:
        top = top.merge(products_df[["sku", "name", "source", "cost_pln"]], on="sku", how="left")
    else:
        top["name"] = ""
        top["source"] = ""
        top["cost_pln"] = 0

    top["margin"] = (top["profit"] / top["revenue"] * 100).round(1).fillna(0)
    top["avg_price"] = (top["revenue"] / top["units"]).round(2).fillna(0)

    sort_by = st.selectbox("Sortuj po", ["revenue", "profit", "units", "margin"], index=0,
                           format_func=lambda x: {"revenue": "Przychod", "profit": "Zysk",
                                                   "units": "Sztuki", "margin": "Marza"}[x])
    top = top.sort_values(sort_by, ascending=False).head(50)

    top_display = top[["sku", "name", "source", "units", "avg_price", "cost_pln", "revenue", "cogs", "profit", "margin"]].copy()
    top_display.columns = ["SKU", "Nazwa", "Zrodlo", "Szt.", "Sr. cena", "Koszt/szt", "Przychod", "COGS", "Zysk", "Marza %"]
    st.dataframe(top_display, use_container_width=True, hide_index=True)

    # Product scatter: revenue vs margin
    if len(top) > 3:
        st.subheader("Przychod vs marza")
        fig_scatter = px.scatter(top, x="revenue", y="margin", size="units", hover_name="sku",
                                 color="source", size_max=40,
                                 labels={"revenue": "Przychod PLN", "margin": "Marza %", "units": "Sztuki"})
        fig_scatter.update_layout(height=400, margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig_scatter, use_container_width=True)


# ==================== TAB: ORDERS ====================
with tab_orders:
    st.subheader("Zamówienia")

    @st.cache_data(ttl=300)
    def load_orders_detail(cutoff_date):
        rows = _get("orders", {
            "select": "id,external_id,platform_id,platform_order_id,order_date,status,shipping_country,total_paid,currency,shipping_cost",
            "order_date": f"gte.{cutoff_date}",
            "order": "order_date.desc",
        })
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    @st.cache_data(ttl=300)
    def load_order_items_all():
        rows = _get("order_items", {
            "select": "order_id,sku,name,quantity,unit_price,currency,asin",
        })
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    orders_detail = load_orders_detail(cutoff)
    items_all = load_order_items_all()

    if not orders_detail.empty:
        orders_detail["platform"] = orders_detail["platform_id"].map(
            lambda x: platforms.get(x, {}).get("code", f"#{x}"))
        orders_detail["date"] = pd.to_datetime(orders_detail["order_date"]).dt.strftime("%Y-%m-%d %H:%M")

        # Calculate COGS and profit per order
        if not items_all.empty and not products_df.empty:
            cost_map = dict(zip(products_df["sku"], products_df["cost_pln"].fillna(0)))
            items_all["cogs"] = items_all["sku"].map(cost_map).fillna(0) * items_all["quantity"]
            order_cogs = items_all.groupby("order_id").agg(
                items_count=("sku", "count"),
                total_cogs=("cogs", "sum"),
            ).reset_index()
            orders_detail = orders_detail.merge(order_cogs, left_on="id", right_on="order_id", how="left")
        else:
            orders_detail["items_count"] = 0
            orders_detail["total_cogs"] = 0

        orders_detail["total_cogs"] = orders_detail["total_cogs"].fillna(0)
        orders_detail["items_count"] = orders_detail["items_count"].fillna(0).astype(int)

        # Platform fee
        fee_map = {p["id"]: float(p.get("fee_pct", 0) or 0) for p in platforms.values()}
        orders_detail["fees"] = (orders_detail["total_paid"] * orders_detail["platform_id"].map(fee_map).fillna(0) / 100).round(2)
        orders_detail["profit"] = (orders_detail["total_paid"] - orders_detail["total_cogs"] - orders_detail["fees"]).round(2)
        orders_detail["margin"] = (orders_detail["profit"] / orders_detail["total_paid"] * 100).round(1).fillna(0)

        # Platform filter for orders tab
        plat_filter = st.multiselect("Filtruj platformy", sorted(orders_detail["platform"].unique()),
                                     default=sorted(orders_detail["platform"].unique()), key="orders_plat")
        orders_filtered = orders_detail[orders_detail["platform"].isin(plat_filter)]

        # KPIs for orders
        ok1, ok2, ok3, ok4 = st.columns(4)
        ok1.metric("Zamowien", f"{len(orders_filtered):,}")
        ok2.metric("Suma", f"{orders_filtered['total_paid'].sum():,.0f} {orders_filtered['currency'].mode().iloc[0] if len(orders_filtered) > 0 else ''}")
        ok3.metric("Sr. wartość", f"{orders_filtered['total_paid'].mean():,.0f}" if len(orders_filtered) > 0 else "0")
        ok4.metric("Sr. marza", f"{orders_filtered['margin'].mean():.1f}%" if len(orders_filtered) > 0 else "0%")

        # Orders table
        display_cols = ["date", "platform", "external_id", "shipping_country", "total_paid", "currency",
                        "total_cogs", "fees", "profit", "margin", "items_count", "status"]
        display_names = ["Data", "Platforma", "ID", "Kraj", "Kwota", "Waluta",
                         "COGS", "Prowizja", "Zysk", "Marza %", "Produkty", "Status"]

        orders_show = orders_filtered[display_cols].copy()
        orders_show.columns = display_names
        st.dataframe(orders_show, use_container_width=True, hide_index=True, height=500)

        # Order detail expander
        st.subheader("Szczegóły zamówienia")
        if not items_all.empty:
            order_id_input = st.text_input("Wpisz ID zamówienia (external_id)")
            if order_id_input:
                order_row = orders_detail[orders_detail["external_id"] == order_id_input]
                if not order_row.empty:
                    oid = order_row.iloc[0]["id"]
                    order_items = items_all[items_all["order_id"] == oid]
                    if not order_items.empty:
                        items_show = order_items[["sku", "name", "quantity", "unit_price", "currency", "asin", "cogs"]].copy()
                        items_show.columns = ["SKU", "Nazwa", "Szt.", "Cena/szt", "Waluta", "ASIN", "COGS"]
                        st.dataframe(items_show, use_container_width=True, hide_index=True)
                    else:
                        st.info("Brak pozycji dla tego zamówienia")
                else:
                    st.warning("Nie znaleziono zamówienia")
    else:
        st.info("Brak zamówień w wybranym okresie")


# ==================== TAB: MONTH vs MONTH ====================
with tab_mom:
    st.subheader("Porownanie miesiecy")

    # Load wider range for MoM
    @st.cache_data(ttl=300)
    def load_all_metrics():
        rows = _get("daily_metrics", {
            "select": "date,platform_id,sku,revenue_pln,gross_profit,cogs,platform_fees,orders_count,units_sold",
            "order": "date.asc",
        })
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    df_all = load_all_metrics()
    if df_all.empty:
        st.info("Brak danych")
    else:
        df_all["platform"] = df_all["platform_id"].map(lambda x: platforms.get(x, {}).get("code", f"#{x}"))
        df_all["date_dt"] = pd.to_datetime(df_all["date"])
        df_all["month"] = df_all["date_dt"].dt.to_period("M").astype(str)

        available_months = sorted(df_all["month"].unique(), reverse=True)

        if len(available_months) >= 2:
            col_m1, col_m2 = st.columns(2)
            with col_m1:
                month1 = st.selectbox("Miesiac 1 (nowszy)", available_months, index=0)
            with col_m2:
                month2 = st.selectbox("Miesiac 2 (starszy)", available_months, index=min(1, len(available_months)-1))

            m1 = df_all[df_all["month"] == month1]
            m2 = df_all[df_all["month"] == month2]

            def month_summary(mdf):
                return {
                    "Przychod": mdf["revenue_pln"].sum(),
                    "COGS": mdf["cogs"].sum(),
                    "Prowizje": mdf["platform_fees"].sum(),
                    "Zysk": mdf["gross_profit"].sum(),
                    "Zamowienia": int(mdf["orders_count"].sum()),
                    "Sztuki": int(mdf["units_sold"].sum()),
                    "Marza %": round(mdf["gross_profit"].sum() / mdf["revenue_pln"].sum() * 100, 1) if mdf["revenue_pln"].sum() > 0 else 0,
                }

            s1 = month_summary(m1)
            s2 = month_summary(m2)

            comp = pd.DataFrame({"Metryka": s1.keys(), month1: s1.values(), month2: s2.values()})
            comp["Zmiana"] = comp.apply(
                lambda r: f"{(r[month1] - r[month2]) / r[month2] * 100:+.1f}%" if r[month2] != 0 else "—", axis=1)
            st.dataframe(comp, use_container_width=True, hide_index=True)

            # Daily overlay
            st.subheader("Dzienny przychod — nakladka")
            m1_daily = m1.groupby(m1["date_dt"].dt.day).agg(revenue=("revenue_pln", "sum")).reset_index()
            m2_daily = m2.groupby(m2["date_dt"].dt.day).agg(revenue=("revenue_pln", "sum")).reset_index()
            m1_daily.columns = ["day", month1]
            m2_daily.columns = ["day", month2]

            overlay = m1_daily.merge(m2_daily, on="day", how="outer").sort_values("day").fillna(0)

            fig_mom = go.Figure()
            fig_mom.add_trace(go.Scatter(x=overlay["day"], y=overlay[month1], name=month1,
                                         mode="lines+markers", line=dict(width=3)))
            fig_mom.add_trace(go.Scatter(x=overlay["day"], y=overlay[month2], name=month2,
                                         mode="lines+markers", line=dict(width=2, dash="dash")))
            fig_mom.update_layout(height=350, margin=dict(l=0, r=0, t=30, b=0),
                                  xaxis_title="Dzien miesiaca", yaxis_title="Przychod PLN",
                                  legend=dict(orientation="h", yanchor="bottom", y=1.02))
            st.plotly_chart(fig_mom, use_container_width=True)

            # Platform comparison MoM
            st.subheader("Platformy — porownanie")
            p1 = m1.groupby("platform").agg(rev=("revenue_pln", "sum"), profit=("gross_profit", "sum")).reset_index()
            p2 = m2.groupby("platform").agg(rev=("revenue_pln", "sum"), profit=("gross_profit", "sum")).reset_index()
            p1.columns = ["Platform", f"Rev {month1}", f"Profit {month1}"]
            p2.columns = ["Platform", f"Rev {month2}", f"Profit {month2}"]
            pc = p1.merge(p2, on="Platform", how="outer").fillna(0)
            pc["Rev zmiana"] = pc.apply(
                lambda r: f"{(r[f'Rev {month1}'] - r[f'Rev {month2}']) / r[f'Rev {month2}'] * 100:+.1f}%"
                if r[f"Rev {month2}"] > 0 else "NEW", axis=1)
            st.dataframe(pc, use_container_width=True, hide_index=True)
        else:
            st.info("Potrzeba danych z co najmniej 2 miesiecy")

# --- Footer ---
st.sidebar.markdown("---")
st.sidebar.caption(f"Dane od {cutoff}")
st.sidebar.caption(f"Odswież: `python3.11 -m etl.run`")
