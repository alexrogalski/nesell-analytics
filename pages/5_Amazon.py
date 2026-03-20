"""Amazon - Traffic, inventory, returns, BSR, pricing."""
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from lib.theme import setup_page, COLORS
from lib.data import (
    load_amazon_traffic, load_amazon_inventory, load_amazon_returns,
    load_amazon_bsr, load_amazon_pricing, get_marketplace_names,
    load_refund_summary, load_amazon_restock, load_amazon_aged_inventory,
    load_buybox_history, load_inventory_velocity, load_daily_metrics,
)

setup_page("Amazon")

st.markdown('<div class="section-header">AMAZON DEEP DIVE</div>', unsafe_allow_html=True)

days = st.sidebar.selectbox(
    "PERIOD", [7, 14, 30, 60, 90], index=2,
    format_func=lambda x: f"{x}D", key="amz_period",
)

# Section selector
section = st.radio(
    "", ["Traffic", "Inventory", "Returns", "BSR & Pricing", "Restock", "Aged Inventory"],
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
        _traf_csv = display.to_csv(index=False).encode("utf-8")
        st.download_button("Download CSV", _traf_csv, "amazon_traffic_asins.csv", "text/csv", key="dl_amz_traffic")


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
        _inv_csv = inv_show.to_csv(index=False).encode("utf-8")
        st.download_button("Download CSV", _inv_csv, "amazon_inventory.csv", "text/csv", key="dl_amz_inventory")

    # --- Inventory Velocity ---
    st.markdown('<div class="section-header">INVENTORY VELOCITY</div>', unsafe_allow_html=True)

    velocity = load_inventory_velocity(days=days)
    if not velocity.empty:
        avg_str = velocity["sell_through_rate"].mean()
        high_doi = len(velocity[velocity["days_of_inventory"] > 180])
        low_doi = len(velocity[velocity["days_of_inventory"] < 14])

        v1, v2, v3 = st.columns(3)
        v1.metric("Avg Sell-Through Rate", f"{avg_str:.1f}%")
        v2.metric("SKUs DOI > 180d", f"{high_doi}",
                  delta="slow movers" if high_doi > 0 else None, delta_color="inverse")
        v3.metric("SKUs DOI < 14d", f"{low_doi}",
                  delta="restock needed" if low_doi > 0 else None, delta_color="inverse")

        # DOI status column
        vel_display = velocity.copy()
        vel_display["doi_status"] = vel_display["days_of_inventory"].apply(
            lambda d: "CRITICAL (<14d)" if d < 14 else
                      ("LOW (14-30d)" if d < 30 else
                       ("OK (30-90d)" if d <= 90 else
                        ("SLOW (90-180d)" if d <= 180 else "EXCESS (>180d)")))
        )
        vel_display = vel_display[["sku", "product_name", "current_stock", "avg_daily_sales",
                                    "sell_through_rate", "days_of_inventory", "doi_status"]].copy()
        vel_display.columns = ["SKU", "Product", "Stock", "Avg Daily Sales",
                               "Sell-Through %", "DOI (days)", "Status"]
        vel_display["Avg Daily Sales"] = vel_display["Avg Daily Sales"].map(lambda x: f"{x:.1f}")
        vel_display["Sell-Through %"] = vel_display["Sell-Through %"].map(lambda x: f"{x:.1f}%")
        vel_display["DOI (days)"] = vel_display["DOI (days)"].map(lambda x: f"{x:.0f}" if x < 999 else "999+")

        st.dataframe(vel_display, use_container_width=True, hide_index=True)
        _vel_csv = velocity.to_csv(index=False).encode("utf-8")
        st.download_button("Download CSV", _vel_csv, "inventory_velocity.csv", "text/csv", key="dl_inv_vel")

        # Bar charts: fastest and slowest selling
        fastest = velocity[velocity["avg_daily_sales"] > 0].head(10)
        slowest = velocity[velocity["days_of_inventory"] < 999].sort_values(
            "days_of_inventory", ascending=False
        ).head(10)

        b1, b2 = st.columns(2)
        with b1:
            if not fastest.empty:
                import plotly.graph_objects as go
                fig_fast = go.Figure(go.Bar(
                    y=fastest["sku"], x=fastest["days_of_inventory"],
                    orientation="h", marker_color=COLORS["success"],
                    text=fastest["days_of_inventory"].map(lambda x: f"{x:.0f}d"),
                    textposition="outside",
                ))
                fig_fast.update_layout(height=max(200, len(fastest) * 30),
                                       title="Fastest Selling (lowest DOI)")
                st.plotly_chart(fig_fast, use_container_width=True)

        with b2:
            if not slowest.empty:
                import plotly.graph_objects as go
                fig_slow = go.Figure(go.Bar(
                    y=slowest["sku"], x=slowest["days_of_inventory"],
                    orientation="h", marker_color=COLORS["danger"],
                    text=slowest["days_of_inventory"].map(lambda x: f"{x:.0f}d"),
                    textposition="outside",
                ))
                fig_slow.update_layout(height=max(200, len(slowest) * 30),
                                       title="Slowest Selling (highest DOI)")
                st.plotly_chart(fig_slow, use_container_width=True)
    else:
        st.info("No inventory velocity data. Need both inventory and sales data.")


# ==================== RETURNS ====================
elif section == "Returns":
    st.markdown('<div class="section-header">AMAZON RETURNS</div>', unsafe_allow_html=True)

    rets = load_amazon_returns(days=days)
    if rets.empty:
        st.info("No returns data. Run: python3.11 -m etl.run --reports")
        st.stop()

    rets["quantity"] = pd.to_numeric(rets["quantity"], errors="coerce").fillna(0).astype(int)

    # Load financial impact data
    refund_summary = load_refund_summary(days=days)
    est_refund_cost = refund_summary.get("estimated_refund_cost_pln", 0)
    refund_rate = refund_summary.get("refund_rate_pct", 0)

    # KPIs (expanded with financial impact)
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("RETURNS", f"{len(rets)}")
    k2.metric("UNITS RETURNED", f"{rets['quantity'].sum():,}")
    k3.metric("UNIQUE SKUs", f"{rets['sku'].nunique()}")
    k4.metric("EST. REVENUE LOST", f"{est_refund_cost:,.0f} PLN")
    k5.metric("REFUND RATE", f"{refund_rate:.1f}%")

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

    # Returns by SKU with estimated financial impact
    st.markdown('<div class="section-header">TOP RETURNED SKUs</div>', unsafe_allow_html=True)
    by_sku = rets.groupby(["sku", "product_name"]).agg(count=("quantity", "sum")).reset_index()
    by_sku = by_sku.sort_values("count", ascending=False).head(20)

    # Calculate per-SKU estimated revenue lost
    total_returned_units = rets["quantity"].sum()
    if total_returned_units > 0 and est_refund_cost > 0:
        avg_cost_per_returned_unit = est_refund_cost / total_returned_units
        by_sku["est_revenue_lost"] = by_sku["count"] * avg_cost_per_returned_unit
        by_sku.columns = ["SKU", "Product", "Returns", "Est. Revenue Lost (PLN)"]
        by_sku["Est. Revenue Lost (PLN)"] = by_sku["Est. Revenue Lost (PLN)"].map(lambda x: f"{x:,.0f}")
    else:
        by_sku.columns = ["SKU", "Product", "Returns"]
    st.dataframe(by_sku, use_container_width=True, hide_index=True)
    _ret_csv = by_sku.to_csv(index=False).encode("utf-8")
    st.download_button("Download CSV", _ret_csv, "amazon_returns_by_sku.csv", "text/csv", key="dl_amz_returns")

    # Financial impact summary
    if est_refund_cost > 0:
        st.markdown('<div class="section-header">FINANCIAL IMPACT</div>', unsafe_allow_html=True)
        fi1, fi2, fi3 = st.columns(3)
        fi1.metric("Estimated Revenue Lost", f"{est_refund_cost:,.0f} PLN")
        fi2.metric("Avg Cost per Return", f"{est_refund_cost / max(total_returned_units, 1):,.0f} PLN")
        fi3.metric("Refund Rate", f"{refund_rate:.1f}%")

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
        _bsr_csv = bsr_show.to_csv(index=False).encode("utf-8")
        st.download_button("Download CSV", _bsr_csv, "amazon_bsr.csv", "text/csv", key="dl_amz_bsr")

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

    # Buy Box Comparison (Our Price vs Buy Box)
    st.markdown('<div class="section-header">OUR PRICE vs BUY BOX</div>', unsafe_allow_html=True)
    if not pricing.empty:
        pricing["marketplace"] = pricing["marketplace_id"].map(mkt_names).fillna(pricing["marketplace_id"])
        for col in ["our_price", "buy_box_price", "buy_box_landed_price",
                     "lowest_fba_price", "lowest_fbm_price",
                     "landed_price", "listing_price", "shipping_price"]:
            if col in pricing.columns:
                pricing[col] = pd.to_numeric(pricing[col], errors="coerce")

        for col in ["num_offers_new", "num_offers_used"]:
            if col in pricing.columns:
                pricing[col] = pd.to_numeric(pricing[col], errors="coerce").fillna(0).astype(int)

        latest_pr = pricing[pricing["snapshot_date"] == pricing["snapshot_date"].max()].copy()
        snapshot_date = pricing["snapshot_date"].max()
        st.caption(f"Snapshot: {snapshot_date}")

        # Compute difference and status
        has_our = latest_pr["our_price"].notna() & (latest_pr["our_price"] > 0)
        has_bb = latest_pr["buy_box_price"].notna() & (latest_pr["buy_box_price"] > 0)

        latest_pr["diff_eur"] = np.where(
            has_our & has_bb,
            latest_pr["our_price"] - latest_pr["buy_box_price"],
            np.nan,
        )
        latest_pr["diff_pct"] = np.where(
            has_our & has_bb & (latest_pr["buy_box_price"] > 0),
            (latest_pr["our_price"] - latest_pr["buy_box_price"]) / latest_pr["buy_box_price"] * 100,
            np.nan,
        )

        def _bb_status(row):
            if pd.isna(row.get("buy_box_price")) or row.get("buy_box_price", 0) == 0:
                return "No BB"
            if pd.isna(row.get("our_price")) or row.get("our_price", 0) == 0:
                return "No Price"
            pct = row.get("diff_pct")
            if pd.isna(pct):
                return "N/A"
            if abs(pct) < 0.5:
                return "Won"
            if pct > 0:
                return "Lost"
            return "Won"
        latest_pr["status"] = latest_pr.apply(_bb_status, axis=1)

        # KPIs
        total_with_data = latest_pr[has_our & has_bb].shape[0]
        won = latest_pr[latest_pr["status"] == "Won"].shape[0]
        lost = latest_pr[latest_pr["status"] == "Lost"].shape[0]
        above_10 = latest_pr[has_our & has_bb & (latest_pr["diff_pct"] > 10)].shape[0]
        win_rate = (won / total_with_data * 100) if total_with_data > 0 else 0

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("BUY BOX WIN RATE", f"{win_rate:.0f}%")
        k2.metric("ASINs AT BUY BOX", f"{won}")
        k3.metric("ASINs ABOVE BB", f"{lost}")
        k4.metric("ASINs >10% ABOVE", f"{above_10}")

        # Build display table
        bb_show = latest_pr[["asin", "marketplace", "our_price", "buy_box_price",
                              "diff_eur", "diff_pct", "status", "num_offers_new"]].copy()
        bb_show.columns = ["ASIN", "Market", "Our Price", "Buy Box", "Diff (EUR)", "Diff (%)", "Status", "Sellers"]

        # Format numeric columns
        for col in ["Our Price", "Buy Box", "Diff (EUR)"]:
            bb_show[col] = bb_show[col].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
        bb_show["Diff (%)"] = bb_show["Diff (%)"].apply(lambda x: f"{x:+.1f}%" if pd.notna(x) else "N/A")

        bb_show = bb_show.sort_values("Status", ascending=True).reset_index(drop=True)

        # Color-coded status styling
        def _color_status(val):
            if val == "Won":
                return f"color: {COLORS['success']}; font-weight: bold"
            if val == "Lost":
                return f"color: {COLORS['danger']}; font-weight: bold"
            if val in ("No BB", "No Price"):
                return f"color: {COLORS['muted']}"
            return ""

        def _color_diff(val):
            if val == "N/A":
                return f"color: {COLORS['muted']}"
            try:
                pct = float(val.replace("%", "").replace("+", ""))
            except (ValueError, AttributeError):
                return ""
            if abs(pct) < 0.5:
                return f"color: {COLORS['success']}"
            if pct > 10:
                return f"color: {COLORS['danger']}; font-weight: bold"
            if pct > 0:
                return f"color: {COLORS['warning']}"
            return f"color: {COLORS['success']}"

        styled = bb_show.style.map(_color_status, subset=["Status"])
        styled = styled.map(_color_diff, subset=["Diff (%)"])
        st.dataframe(styled, use_container_width=True, hide_index=True)

        _bb_csv = bb_show.to_csv(index=False).encode("utf-8")
        st.download_button("Download CSV", _bb_csv, "buybox_comparison.csv", "text/csv", key="dl_bb_compare")

        # Buy Box Win Rate Trend
        st.markdown('<div class="section-header">BUY BOX WIN RATE TREND</div>', unsafe_allow_html=True)
        bb_history = load_buybox_history(days=days)
        if not bb_history.empty and len(bb_history) > 1:
            bb_hist_chart = bb_history.copy()
            bb_hist_chart["snapshot_date"] = pd.to_datetime(bb_hist_chart["snapshot_date"])

            fig_bb_trend = go.Figure()
            fig_bb_trend.add_trace(go.Scatter(
                x=bb_hist_chart["snapshot_date"], y=bb_hist_chart["win_rate_pct"],
                mode="lines", name="Win Rate %",
                line=dict(color=COLORS["muted"], width=1), opacity=0.4,
            ))
            fig_bb_trend.add_trace(go.Scatter(
                x=bb_hist_chart["snapshot_date"], y=bb_hist_chart["win_rate_7d"],
                mode="lines", name="7d MA",
                line=dict(color=COLORS["success"], width=2),
            ))
            fig_bb_trend.update_layout(
                height=300, title="Buy Box Win Rate Trend",
                yaxis=dict(range=[0, 105], title="Win Rate %"),
                legend=dict(orientation="h", y=-0.15),
            )
            st.plotly_chart(fig_bb_trend, use_container_width=True)

            # ASINs that lost Buy Box (last two snapshots)
            _snapshots = sorted(pricing["snapshot_date"].unique())
            if len(_snapshots) >= 2:
                _today_snap = _snapshots[-1]
                _yest_snap = _snapshots[-2]
                _today_pr = pricing[pricing["snapshot_date"] == _today_snap].copy()
                _yest_pr = pricing[pricing["snapshot_date"] == _yest_snap].copy()

                for _c in ["our_price", "buy_box_price"]:
                    if _c in _today_pr.columns:
                        _today_pr[_c] = pd.to_numeric(_today_pr[_c], errors="coerce")
                    if _c in _yest_pr.columns:
                        _yest_pr[_c] = pd.to_numeric(_yest_pr[_c], errors="coerce")

                # Won yesterday
                _yest_valid = _yest_pr[
                    _yest_pr["our_price"].notna() & (_yest_pr["our_price"] > 0) &
                    _yest_pr["buy_box_price"].notna() & (_yest_pr["buy_box_price"] > 0)
                ].copy()
                _yest_won = set(_yest_valid[_yest_valid["our_price"] <= _yest_valid["buy_box_price"] * 1.005]["asin"])

                # Lost today
                _today_valid = _today_pr[
                    _today_pr["our_price"].notna() & (_today_pr["our_price"] > 0) &
                    _today_pr["buy_box_price"].notna() & (_today_pr["buy_box_price"] > 0) &
                    _today_pr["asin"].isin(_yest_won)
                ].copy()
                _lost = _today_valid[_today_valid["our_price"] > _today_valid["buy_box_price"] * 1.005]

                if not _lost.empty:
                    _lost_display = _lost[["asin", "our_price", "buy_box_price"]].copy()
                    _lost_display["diff_pct"] = (
                        (_lost_display["our_price"] - _lost_display["buy_box_price"]) /
                        _lost_display["buy_box_price"] * 100
                    )
                    _lost_display.columns = ["ASIN", "Our Price", "Buy Box", "Diff %"]
                    _lost_display["Our Price"] = _lost_display["Our Price"].map(lambda x: f"{x:.2f}")
                    _lost_display["Buy Box"] = _lost_display["Buy Box"].map(lambda x: f"{x:.2f}")
                    _lost_display["Diff %"] = _lost_display["Diff %"].map(lambda x: f"{x:+.1f}%")
                    st.markdown(f"**ASINs that lost Buy Box** ({_yest_snap} -> {_today_snap})")
                    st.dataframe(_lost_display, use_container_width=True, hide_index=True)
        else:
            st.info("Not enough pricing snapshots for trend. Need at least 2 days of data.")

        # Expandable: full competitive pricing detail (old view)
        with st.expander("Full Pricing Detail (landed/listing/shipping)"):
            for col in ["landed_price", "listing_price", "shipping_price"]:
                if col in latest_pr.columns:
                    latest_pr[col] = pd.to_numeric(latest_pr[col], errors="coerce").fillna(0)
            detail_cols = ["asin", "marketplace"]
            for c in ["landed_price", "listing_price", "shipping_price", "currency", "condition_value"]:
                if c in latest_pr.columns:
                    detail_cols.append(c)
            pr_detail = latest_pr[detail_cols].copy()
            rename = {"asin": "ASIN", "marketplace": "Market", "landed_price": "Landed",
                      "listing_price": "Listing", "shipping_price": "Shipping",
                      "currency": "Currency", "condition_value": "Condition"}
            pr_detail.rename(columns=rename, inplace=True)
            for col in ["Landed", "Listing", "Shipping"]:
                if col in pr_detail.columns:
                    pr_detail[col] = pr_detail[col].map(lambda x: f"{x:.2f}")
            pr_detail = pr_detail.sort_values(["ASIN", "Market"])
            st.dataframe(pr_detail, use_container_width=True, hide_index=True)
    else:
        st.info("No pricing data. Run: python3.11 -m etl.competitor_prices --limit 50")


# ==================== RESTOCK ====================
elif section == "Restock":
    st.markdown('<div class="section-header">RESTOCK RECOMMENDATIONS</div>', unsafe_allow_html=True)

    restock = load_amazon_restock()
    if restock.empty:
        st.info("No restock data. Run: python3.11 -m etl.run --restock")
        st.stop()

    for col in ["recommended_qty", "days_of_cover"]:
        if col in restock.columns:
            restock[col] = pd.to_numeric(restock[col], errors="coerce").fillna(0)

    snapshot = restock["snapshot_date"].max()
    st.caption(f"Snapshot: {snapshot}")

    # KPIs
    k1, k2, k3 = st.columns(3)
    k1.metric("SKUs TO RESTOCK", f"{len(restock[restock['recommended_qty'] > 0])}")
    critical = restock[restock["days_of_cover"] < 14].shape[0]
    k2.metric("CRITICAL (< 14d)", f"{critical}")
    warning = restock[(restock["days_of_cover"] >= 14) & (restock["days_of_cover"] < 30)].shape[0]
    k3.metric("WARNING (14-30d)", f"{warning}")

    # Search filter
    search = st.text_input("Filter by SKU", key="restock_search")
    df_show = restock.copy()
    if search:
        df_show = df_show[df_show["sku"].str.contains(search, case=False, na=False)]

    display = df_show[["sku", "product_name", "days_of_cover", "recommended_qty", "reorder_date"]].copy()
    display["status"] = display["days_of_cover"].apply(
        lambda v: "🔴 CRITICAL" if v < 14 else ("🟡 WARNING" if v < 30 else "🟢 OK")
    )
    display = display[["sku", "product_name", "status", "days_of_cover", "recommended_qty", "reorder_date"]]
    display.columns = ["SKU", "Product", "Status", "Days of Cover", "Recommended Qty", "Reorder Date"]
    display = display.sort_values("Days of Cover", ascending=True).reset_index(drop=True)

    st.dataframe(display, use_container_width=True, hide_index=True)
    _restock_csv = display.to_csv(index=False).encode("utf-8")
    st.download_button("Download CSV", _restock_csv, "amazon_restock.csv", "text/csv", key="dl_amz_restock")


# ==================== AGED INVENTORY ====================
elif section == "Aged Inventory":
    st.markdown('<div class="section-header">FBA AGED INVENTORY</div>', unsafe_allow_html=True)

    aged = load_amazon_aged_inventory()
    if aged.empty:
        st.info("No aged inventory data. Run: python3.11 -m etl.run --aged")
        st.stop()

    age_cols = ["inv_age_0_to_90_days", "inv_age_91_to_180_days",
                "inv_age_181_to_270_days", "inv_age_271_plus_days"]
    for col in age_cols + ["qty_to_be_charged_ltsf", "days_of_supply"]:
        if col in aged.columns:
            aged[col] = pd.to_numeric(aged[col], errors="coerce").fillna(0)

    snapshot = aged["snapshot_date"].max()
    st.caption(f"Snapshot: {snapshot}")

    # Alert banner: any inventory in 181+ days
    at_risk = aged[aged["inv_age_181_to_270_days"] + aged["inv_age_271_plus_days"] > 0]
    if not at_risk.empty:
        total_at_risk = int(at_risk["inv_age_181_to_270_days"].sum() + at_risk["inv_age_271_plus_days"].sum())
        st.error(
            f"LTSF RISK: {len(at_risk)} SKUs with {total_at_risk} units aged 181+ days — "
            f"Long-Term Storage Fees incoming. Consider running a removal order or liquidation.",
            icon="🚨",
        )

    # KPIs
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("0-90 DAYS", f"{int(aged['inv_age_0_to_90_days'].sum()):,}")
    k2.metric("91-180 DAYS", f"{int(aged['inv_age_91_to_180_days'].sum()):,}")
    k3.metric("181-270 DAYS", f"{int(aged['inv_age_181_to_270_days'].sum()):,}")
    k4.metric("271+ DAYS", f"{int(aged['inv_age_271_plus_days'].sum()):,}")

    # Full inventory table
    display = aged[["sku", "product_name", "inv_age_0_to_90_days", "inv_age_91_to_180_days",
                    "inv_age_181_to_270_days", "inv_age_271_plus_days", "qty_to_be_charged_ltsf"]].copy()
    display.columns = ["SKU", "Product", "0-90d", "91-180d", "181-270d", "271+d", "Qty for LTSF"]
    display = display.sort_values("271+d", ascending=False).reset_index(drop=True)
    st.dataframe(display, use_container_width=True, hide_index=True)
    _aged_csv = display.to_csv(index=False).encode("utf-8")
    st.download_button("Download CSV", _aged_csv, "amazon_aged_inventory.csv", "text/csv", key="dl_amz_aged")

    # Bar chart: top 10 SKUs with most 181+ days inventory
    aged["ltsf_risk"] = aged["inv_age_181_to_270_days"] + aged["inv_age_271_plus_days"]
    top10 = aged[aged["ltsf_risk"] > 0].sort_values("ltsf_risk", ascending=False).head(10)

    if not top10.empty:
        st.markdown('<div class="section-header">TOP 10 SKUS — AGE BUCKET BREAKDOWN</div>', unsafe_allow_html=True)
        import plotly.graph_objects as go
        palette = {
            "0-90d":   COLORS["success"],
            "91-180d": COLORS["warning"],
            "181-270d": COLORS["danger"],
            "271+d":   "#7f1d1d",
        }
        labels = top10["sku"].tolist()
        fig_aged = go.Figure()
        for col_key, col_label in [
            ("inv_age_0_to_90_days", "0-90d"),
            ("inv_age_91_to_180_days", "91-180d"),
            ("inv_age_181_to_270_days", "181-270d"),
            ("inv_age_271_plus_days", "271+d"),
        ]:
            fig_aged.add_trace(go.Bar(
                name=col_label,
                x=labels,
                y=top10[col_key].tolist(),
                marker_color=palette[col_label],
            ))
        fig_aged.update_layout(
            barmode="stack",
            height=400,
            title="Age Buckets — Top 10 SKUs at LTSF Risk",
            xaxis_title="SKU",
            yaxis_title="Units",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig_aged, use_container_width=True)
