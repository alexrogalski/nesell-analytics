"""Amazon - Traffic, inventory, returns, BSR, pricing."""
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from lib.theme import setup_page, COLORS
from lib.data import (
    load_amazon_traffic, load_amazon_inventory, load_amazon_returns,
    load_amazon_bsr, load_amazon_pricing, get_marketplace_names,
)

setup_page("Amazon")

st.markdown('<div class="section-header">AMAZON DEEP DIVE</div>', unsafe_allow_html=True)

days = st.sidebar.selectbox(
    "PERIOD", [7, 14, 30, 60, 90], index=2,
    format_func=lambda x: f"{x}D", key="amz_period",
)

# Section selector
section = st.radio(
    "", ["Traffic", "Inventory", "Returns", "BSR & Pricing"],
    horizontal=True, key="amz_section",
)

mkt_names = get_marketplace_names()

# ==================== TRAFFIC ====================
if section == "Traffic":
    st.markdown('<div class="section-header">TRAFFIC & CONVERSION</div>', unsafe_allow_html=True)

    traf = load_amazon_traffic(days=days)
    if traf.empty:
        st.info("No traffic data. Run: python3.11 -m etl.run --reports")
        st.stop()

    traf["marketplace"] = traf["marketplace_id"].map(mkt_names).fillna(traf["marketplace_id"])

    # Split totals vs per-ASIN
    daily_traf = traf[traf["asin"] == "__TOTAL__"].copy()
    asin_traf = traf[traf["asin"] != "__TOTAL__"].copy()

    if not daily_traf.empty:
        daily_traf["date"] = pd.to_datetime(daily_traf["date"])
        for col in ["sessions", "page_views", "units_ordered", "buy_box_pct", "ordered_product_sales"]:
            if col in daily_traf.columns:
                daily_traf[col] = pd.to_numeric(daily_traf[col], errors="coerce").fillna(0)

        # KPIs
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("SESSIONS", f"{daily_traf['sessions'].sum():,.0f}")
        k2.metric("PAGE VIEWS", f"{daily_traf['page_views'].sum():,.0f}")
        k3.metric("UNITS ORDERED", f"{daily_traf['units_ordered'].sum():,.0f}")
        avg_bb = daily_traf["buy_box_pct"].mean()
        k4.metric("AVG BUY BOX %", f"{avg_bb:.1f}%")

        # Sessions per marketplace (stacked bar)
        import plotly.graph_objects as go
        sess_daily = daily_traf.groupby(["date", "marketplace"]).agg(sessions=("sessions", "sum")).reset_index()
        sess_pivot = sess_daily.pivot(index="date", columns="marketplace", values="sessions").fillna(0)

        fig_sess = go.Figure()
        palette = [COLORS["primary"], COLORS["success"], COLORS["warning"], COLORS["danger"], COLORS["info"], COLORS["cm2"]]
        for i, col in enumerate(sess_pivot.columns):
            fig_sess.add_trace(go.Bar(
                x=sess_pivot.index, y=sess_pivot[col], name=col,
                marker_color=palette[i % len(palette)],
            ))
        fig_sess.update_layout(barmode="stack", height=350, title="Daily Sessions by Marketplace")
        st.plotly_chart(fig_sess, use_container_width=True)

        # Buy Box trend
        bb_daily = daily_traf.groupby(["date", "marketplace"]).agg(buy_box=("buy_box_pct", "mean")).reset_index()
        from lib.charts import multi_line
        bb_pivot = bb_daily.pivot(index="date", columns="marketplace", values="buy_box").fillna(0).reset_index()
        mkt_cols = [c for c in bb_pivot.columns if c != "date"]
        if mkt_cols:
            fig_bb = multi_line(bb_pivot, "date", mkt_cols, title="Buy Box % Trend", height=300)
            fig_bb.update_layout(yaxis=dict(range=[0, 105]))
            st.plotly_chart(fig_bb, use_container_width=True)

        # Conversion rate
        conv = daily_traf.groupby("date").agg(sessions=("sessions", "sum"), units=("units_ordered", "sum")).reset_index()
        conv["conv_pct"] = np.where(conv["sessions"] > 0, conv["units"] / conv["sessions"] * 100, 0)

        from lib.charts import area_chart
        fig_conv = area_chart(conv, "date", "conv_pct", color=COLORS["info"], title="Conversion Rate %", height=280)
        st.plotly_chart(fig_conv, use_container_width=True)

    # Per-ASIN table
    if not asin_traf.empty:
        st.markdown('<div class="section-header">TOP ASINs</div>', unsafe_allow_html=True)
        for col in ["sessions", "page_views", "units_ordered", "ordered_product_sales", "buy_box_pct"]:
            if col in asin_traf.columns:
                asin_traf[col] = pd.to_numeric(asin_traf[col], errors="coerce").fillna(0)

        asin_agg = asin_traf.groupby(["asin", "marketplace"]).agg(
            sessions=("sessions", "sum"), page_views=("page_views", "sum"),
            buy_box=("buy_box_pct", "mean"), units=("units_ordered", "sum"),
            revenue=("ordered_product_sales", "sum"),
        ).reset_index()
        asin_agg["conv%"] = np.where(asin_agg["sessions"] > 0, asin_agg["units"] / asin_agg["sessions"] * 100, 0)
        asin_agg = asin_agg.sort_values("revenue", ascending=False).head(30)

        display = asin_agg.copy()
        display["buy_box"] = display["buy_box"].map(lambda x: f"{x:.1f}%")
        display["conv%"] = display["conv%"].map(lambda x: f"{x:.1f}%")
        display["revenue"] = display["revenue"].map(lambda x: f"{x:,.0f}")
        display.columns = ["ASIN", "Market", "Sessions", "Page Views", "Buy Box%", "Units", "Revenue", "Conv%"]
        st.dataframe(display, use_container_width=True, hide_index=True)


# ==================== INVENTORY ====================
elif section == "Inventory":
    st.markdown('<div class="section-header">FBA INVENTORY</div>', unsafe_allow_html=True)

    inv = load_amazon_inventory()
    if inv.empty:
        st.info("No inventory data. Run: python3.11 -m etl.run --reports")
        st.stop()

    inv["fulfillable_qty"] = pd.to_numeric(inv["fulfillable_qty"], errors="coerce").fillna(0).astype(int)
    latest_date = inv["snapshot_date"].max()
    inv_latest = inv[inv["snapshot_date"] == latest_date].copy()

    # KPIs
    k1, k2, k3 = st.columns(3)
    k1.metric("SKUs IN FBA", f"{inv_latest['sku'].nunique()}")
    k2.metric("COUNTRIES", f"{inv_latest['country'].nunique()}")
    k3.metric("TOTAL STOCK", f"{inv_latest['fulfillable_qty'].sum():,}")

    # Stock by country
    by_country = inv_latest.groupby("country").agg(
        skus=("sku", "nunique"), stock=("fulfillable_qty", "sum")
    ).reset_index().sort_values("stock", ascending=False)

    import plotly.graph_objects as go
    fig_inv = go.Figure(go.Bar(
        x=by_country["country"], y=by_country["stock"],
        marker_color=COLORS["success"],
        text=by_country["skus"].map(lambda x: f"{x} SKU"),
        textposition="outside",
    ))
    fig_inv.update_layout(height=300, title="Stock by Country", xaxis_title="", yaxis_title="Fulfillable Qty")
    st.plotly_chart(fig_inv, use_container_width=True)

    # Low stock alerts
    st.markdown('<div class="section-header">LOW STOCK ALERTS (< 5 units)</div>', unsafe_allow_html=True)
    low = inv_latest[inv_latest["fulfillable_qty"] < 5].sort_values("fulfillable_qty")
    if not low.empty:
        low_show = low[["sku", "product_name", "country", "fulfillable_qty"]].copy()
        low_show.columns = ["SKU", "Product", "Country", "Stock"]
        st.dataframe(low_show, use_container_width=True, hide_index=True)
    else:
        st.success("All SKUs have >= 5 units")

    # Full inventory table
    with st.expander(f"Full Inventory ({latest_date})"):
        inv_show = inv_latest[["sku", "asin", "product_name", "country", "fulfillable_qty"]].copy()
        inv_show.columns = ["SKU", "ASIN", "Product", "Country", "Stock"]
        inv_show = inv_show.sort_values(["SKU", "Country"])
        st.dataframe(inv_show, use_container_width=True, hide_index=True)


# ==================== RETURNS ====================
elif section == "Returns":
    st.markdown('<div class="section-header">AMAZON RETURNS</div>', unsafe_allow_html=True)

    rets = load_amazon_returns(days=days)
    if rets.empty:
        st.info("No returns data. Run: python3.11 -m etl.run --reports")
        st.stop()

    rets["quantity"] = pd.to_numeric(rets["quantity"], errors="coerce").fillna(0).astype(int)

    # KPIs
    k1, k2, k3 = st.columns(3)
    k1.metric("RETURNS", f"{len(rets)}")
    k2.metric("UNITS RETURNED", f"{rets['quantity'].sum():,}")
    k3.metric("UNIQUE SKUs", f"{rets['sku'].nunique()}")

    # Returns by reason
    by_reason = rets.groupby("reason").agg(count=("quantity", "sum")).reset_index()
    by_reason = by_reason.sort_values("count", ascending=True)

    import plotly.graph_objects as go
    fig_ret = go.Figure(go.Bar(
        y=by_reason["reason"], x=by_reason["count"],
        orientation="h", marker_color=COLORS["danger"],
    ))
    fig_ret.update_layout(height=max(250, len(by_reason) * 30), title="Return Reasons")
    st.plotly_chart(fig_ret, use_container_width=True)

    # Returns by SKU
    st.markdown('<div class="section-header">TOP RETURNED SKUs</div>', unsafe_allow_html=True)
    by_sku = rets.groupby(["sku", "product_name"]).agg(count=("quantity", "sum")).reset_index()
    by_sku = by_sku.sort_values("count", ascending=False).head(20)
    by_sku.columns = ["SKU", "Product", "Returns"]
    st.dataframe(by_sku, use_container_width=True, hide_index=True)

    # Returns timeline
    rets_dated = rets[rets["return_date"].notna()].copy()
    if not rets_dated.empty:
        rets_dated["date"] = pd.to_datetime(rets_dated["return_date"])
        ret_daily = rets_dated.groupby("date").agg(count=("quantity", "sum")).reset_index()

        from lib.charts import area_chart
        fig_ret_t = area_chart(ret_daily, "date", "count", color=COLORS["warning"], title="Returns Over Time", height=280)
        st.plotly_chart(fig_ret_t, use_container_width=True)

    # Full table
    with st.expander("Full Returns List"):
        ret_show = rets[["return_date", "order_id", "sku", "product_name", "quantity",
                         "reason", "detailed_disposition", "status"]].copy()
        ret_show.columns = ["Date", "Order ID", "SKU", "Product", "Qty", "Reason", "Disposition", "Status"]
        st.dataframe(ret_show, use_container_width=True, hide_index=True)


# ==================== BSR & PRICING ====================
elif section == "BSR & Pricing":
    st.markdown('<div class="section-header">BEST SELLERS RANK</div>', unsafe_allow_html=True)

    bsr = load_amazon_bsr()
    pricing = load_amazon_pricing()

    if not bsr.empty:
        bsr["marketplace"] = bsr["marketplace_id"].map(mkt_names).fillna(bsr["marketplace_id"])
        bsr["rank"] = pd.to_numeric(bsr["rank"], errors="coerce").fillna(0).astype(int)

        latest_bsr = bsr[bsr["snapshot_date"] == bsr["snapshot_date"].max()]

        bsr_show = latest_bsr[["asin", "marketplace", "category_name", "rank"]].copy()
        bsr_show.columns = ["ASIN", "Market", "Category", "Rank"]
        bsr_show = bsr_show.sort_values("Rank")
        st.dataframe(bsr_show, use_container_width=True, hide_index=True)

        # Top 10 by rank
        top_bsr = latest_bsr.sort_values("rank").head(10)
        if not top_bsr.empty:
            import plotly.graph_objects as go
            fig_bsr = go.Figure(go.Bar(
                x=top_bsr["asin"], y=top_bsr["rank"],
                marker_color=COLORS["primary"],
                text=top_bsr["rank"],
                textposition="outside",
            ))
            fig_bsr.update_layout(
                height=350, title="Top 10 ASINs by BSR",
                yaxis=dict(autorange="reversed", title="BSR Rank"),
            )
            st.plotly_chart(fig_bsr, use_container_width=True)

        # BSR trend for top ASINs
        st.markdown('<div class="section-header">BSR TREND (TOP 5 ASINs)</div>', unsafe_allow_html=True)
        top_asins = latest_bsr.sort_values("rank").head(5)["asin"].tolist()
        bsr_trend = bsr[bsr["asin"].isin(top_asins)].copy()
        if not bsr_trend.empty:
            bsr_trend["date"] = pd.to_datetime(bsr_trend["snapshot_date"])
            bsr_pivot = bsr_trend.pivot_table(index="date", columns="asin", values="rank", aggfunc="min")
            bsr_pivot = bsr_pivot.reset_index()
            asin_cols = [c for c in bsr_pivot.columns if c != "date"]
            if asin_cols:
                from lib.charts import multi_line
                fig_bsr_t = multi_line(bsr_pivot, "date", asin_cols, title="", height=350)
                fig_bsr_t.update_layout(yaxis=dict(autorange="reversed", title="BSR Rank"))
                st.plotly_chart(fig_bsr_t, use_container_width=True)
    else:
        st.info("No BSR data. Run: python3.11 -m etl.run --amzdata")

    # Pricing
    st.markdown('<div class="section-header">COMPETITIVE PRICING</div>', unsafe_allow_html=True)
    if not pricing.empty:
        pricing["marketplace"] = pricing["marketplace_id"].map(mkt_names).fillna(pricing["marketplace_id"])
        for col in ["landed_price", "listing_price", "shipping_price"]:
            if col in pricing.columns:
                pricing[col] = pd.to_numeric(pricing[col], errors="coerce").fillna(0)

        latest_pr = pricing[pricing["snapshot_date"] == pricing["snapshot_date"].max()]

        pr_show = latest_pr[["asin", "marketplace", "landed_price", "listing_price", "shipping_price", "currency", "condition_value"]].copy()
        pr_show.columns = ["ASIN", "Market", "Landed", "Listing", "Shipping", "Currency", "Condition"]
        for col in ["Landed", "Listing", "Shipping"]:
            pr_show[col] = pr_show[col].map(lambda x: f"{x:.2f}")
        pr_show = pr_show.sort_values(["ASIN", "Market"])
        st.dataframe(pr_show, use_container_width=True, hide_index=True)
    else:
        st.info("No pricing data. Run: python3.11 -m etl.run --amzdata")
