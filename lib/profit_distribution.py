"""Profit distribution histogram chart."""
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from lib.theme import COLORS
from lib.order_table import _fmt


def render_profit_distribution(filtered):
    """Render the profit distribution histogram with stats."""
    if len(filtered) > 5:
        profits = filtered["profit_pln"].dropna()
        profits = profits[
            profits.between(profits.quantile(0.02), profits.quantile(0.98))
        ]

        fig = go.Figure()
        pos_profits = profits[profits >= 0]
        neg_profits = profits[profits < 0]

        if len(neg_profits) > 0:
            fig.add_trace(go.Histogram(
                x=neg_profits, name="Loss",
                marker_color=COLORS["danger"],
                opacity=0.85,
                nbinsx=max(5, int(len(neg_profits) ** 0.5)),
            ))
        if len(pos_profits) > 0:
            fig.add_trace(go.Histogram(
                x=pos_profits, name="Profit",
                marker_color=COLORS["success"],
                opacity=0.85,
                nbinsx=max(5, int(len(pos_profits) ** 0.5)),
            ))

        fig.add_vline(
            x=0, line_dash="dot",
            line_color=COLORS["muted"], opacity=0.5,
        )
        median_profit = profits.median()
        fig.add_vline(
            x=median_profit, line_dash="dash",
            line_color=COLORS["primary"], opacity=0.7,
            annotation_text="Median: " + _fmt(median_profit) + " PLN",
            annotation_font=dict(size=10, color=COLORS["primary"]),
        )
        fig.update_layout(
            height=350,
            xaxis_title="Profit per order (PLN)",
            yaxis_title="Number of orders",
            barmode="overlay",
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
            ),
        )
        st.plotly_chart(fig, use_container_width=True)

        # Stats below chart - custom HTML
        stats_html = (
            '<div class="dist-stats-row">'
            + '<div class="dist-stat">'
            + '<div class="label">Median Profit</div>'
            + '<div class="value">' + _fmt(median_profit) + ' PLN</div>'
            + '</div>'
            + '<div class="dist-stat">'
            + '<div class="label">Avg Profit</div>'
            + '<div class="value">' + _fmt(profits.mean()) + ' PLN</div>'
            + '</div>'
            + '<div class="dist-stat">'
            + '<div class="label">Min</div>'
            + '<div class="value" style="color: '
            + COLORS["danger"] + ';">'
            + _fmt(profits.min()) + ' PLN</div>'
            + '</div>'
            + '<div class="dist-stat">'
            + '<div class="label">Max</div>'
            + '<div class="value" style="color: '
            + COLORS["success"] + ';">'
            + _fmt(profits.max()) + ' PLN</div>'
            + '</div>'
            + '</div>'
        )
        st.markdown(stats_html, unsafe_allow_html=True)
    else:
        st.markdown(
            '<div style="font-family: var(--font-mono);'
            ' font-size: 0.8rem; color: #64748b; padding: 16px 0;">'
            'Not enough orders for distribution chart (need at least 6).'
            '</div>',
            unsafe_allow_html=True,
        )
