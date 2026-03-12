"""Reusable chart builders for nesell-analytics dashboard."""
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np
from lib.theme import COLORS


def _hex_to_rgb(hex_color):
    """Convert hex color to RGB tuple."""
    h = hex_color.lstrip("#")
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


def sparkline(values, color=COLORS["primary"], height=40, width=120):
    """Create a tiny sparkline figure."""
    r, g, b = _hex_to_rgb(color)
    fig = go.Figure(
        go.Scatter(
            y=values,
            mode="lines",
            line=dict(color=color, width=1.5),
            fill="tozeroy",
            fillcolor=f"rgba({r},{g},{b},0.1)",
        )
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        height=height,
        width=width,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return fig


def area_chart(df, x, y, color=COLORS["primary"], title="", height=350):
    """Clean area chart with gradient fill."""
    r, g, b = _hex_to_rgb(color)
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df[x],
            y=df[y],
            mode="lines",
            line=dict(color=color, width=2),
            fill="tozeroy",
            fillcolor=f"rgba({r},{g},{b},0.15)",
            name=y,
        )
    )
    fig.update_layout(title=title, height=height, showlegend=False)
    return fig


def bar_chart(df, x, y, color=COLORS["primary"], title="", height=350, horizontal=False):
    """Clean bar chart."""
    if horizontal:
        fig = go.Figure(go.Bar(y=df[x], x=df[y], orientation="h", marker_color=color))
    else:
        fig = go.Figure(go.Bar(x=df[x], y=df[y], marker_color=color))
    fig.update_layout(title=title, height=height)
    return fig


def multi_line(df, x, y_cols, colors=None, title="", height=350, names=None):
    """Multiple line series on one chart."""
    fig = go.Figure()
    palette = colors or [
        COLORS["primary"],
        COLORS["success"],
        COLORS["warning"],
        COLORS["danger"],
        COLORS["info"],
        COLORS["cm2"],
    ]
    for i, col in enumerate(y_cols):
        fig.add_trace(
            go.Scatter(
                x=df[x],
                y=df[col],
                mode="lines",
                line=dict(color=palette[i % len(palette)], width=2),
                name=names[i] if names else col,
            )
        )
    fig.update_layout(title=title, height=height)
    return fig


def waterfall_chart(labels, values, title="", height=400):
    """Waterfall chart for P&L breakdown."""
    measures = []
    for i, v in enumerate(values):
        if i == 0:
            measures.append("absolute")
        elif i == len(values) - 1:
            measures.append("total")
        else:
            measures.append("relative")

    fig = go.Figure(
        go.Waterfall(
            x=labels,
            y=values,
            measure=measures,
            connector=dict(line=dict(color=COLORS["border"])),
            increasing=dict(marker=dict(color=COLORS["success"])),
            decreasing=dict(marker=dict(color=COLORS["danger"])),
            totals=dict(marker=dict(color=COLORS["cm3"])),
            textposition="outside",
            text=[f"{v:,.0f}" for v in values],
            textfont=dict(size=10),
        )
    )
    fig.update_layout(title=title, height=height, showlegend=False)
    return fig


def heatmap(z_data, x_labels, y_labels, title="", height=400, colorscale=None):
    """Heatmap for correlation or activity matrices."""
    if colorscale is None:
        colorscale = [
            [0, COLORS["bg"]],
            [0.5, COLORS["primary"]],
            [1, COLORS["success"]],
        ]
    fig = go.Figure(
        go.Heatmap(
            x=x_labels,
            y=y_labels,
            z=z_data,
            colorscale=colorscale,
            texttemplate="%{z:.0f}",
            textfont=dict(size=9),
        )
    )
    fig.update_layout(title=title, height=height)
    return fig


def treemap(labels, parents, values, colors=None, title="", height=400):
    """Treemap for hierarchical data."""
    fig = go.Figure(
        go.Treemap(
            labels=labels,
            parents=parents,
            values=values,
            marker=dict(colors=colors) if colors else {},
            textinfo="label+value+percent parent",
            textfont=dict(size=11),
        )
    )
    fig.update_layout(
        title=title, height=height, margin=dict(l=10, r=10, t=40, b=10)
    )
    return fig


def scatter_quadrant(df, x, y, size, color, hover_name, title="", height=450):
    """Scatter plot with quadrant lines for portfolio analysis."""
    fig = px.scatter(
        df,
        x=x,
        y=y,
        size=size,
        color=color,
        hover_name=hover_name,
        size_max=40,
        title=title,
    )
    # Add quadrant lines at median
    if len(df) > 0:
        x_mid = df[x].median()
        y_mid = df[y].median()
        fig.add_hline(y=y_mid, line_dash="dot", line_color=COLORS["muted"], opacity=0.5)
        fig.add_vline(x=x_mid, line_dash="dot", line_color=COLORS["muted"], opacity=0.5)
        # Quadrant labels
        x_range = df[x].max() - df[x].min()
        y_range = df[y].max() - df[y].min()
        labels = [
            ("STARS", x_mid + x_range * 0.25, y_mid + y_range * 0.25),
            ("CASH COWS", x_mid + x_range * 0.25, y_mid - y_range * 0.25),
            ("QUESTION MARKS", x_mid - x_range * 0.25, y_mid + y_range * 0.25),
            ("DOGS", x_mid - x_range * 0.25, y_mid - y_range * 0.25),
        ]
        for label, lx, ly in labels:
            fig.add_annotation(
                x=lx, y=ly, text=label,
                showarrow=False,
                font=dict(size=9, color=COLORS["muted"]),
                opacity=0.4,
            )
    fig.update_layout(height=height)
    return fig
