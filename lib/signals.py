"""Anomaly detection and signal generation for nesell-analytics dashboard."""
import pandas as pd
import numpy as np


def detect_anomalies(series, window=30, threshold=2.0):
    """Detect anomalies using z-score over rolling window."""
    if len(series) < window:
        return pd.Series([False] * len(series), index=series.index)
    rolling_mean = series.rolling(window, min_periods=7).mean()
    rolling_std = series.rolling(window, min_periods=7).std()
    z_scores = (series - rolling_mean) / rolling_std.replace(0, np.nan)
    return z_scores.abs() > threshold


def generate_signals(daily_df, product_df):
    """Generate trading-style signals for the dashboard."""
    signals = []

    if daily_df.empty:
        return signals

    # Revenue trend signal
    if len(daily_df) >= 14:
        last_7 = daily_df.tail(7)["revenue_pln"].mean()
        prev_7 = daily_df.iloc[-14:-7]["revenue_pln"].mean()
        if prev_7 > 0:
            change = ((last_7 - prev_7) / prev_7) * 100
            if abs(change) > 15:
                direction = "UP" if change > 0 else "DOWN"
                color = "success" if change > 0 else "danger"
                signals.append(
                    {
                        "type": color,
                        "title": f"Revenue {direction} {abs(change):.1f}%",
                        "detail": f"7d avg {last_7:,.0f} PLN vs prev {prev_7:,.0f} PLN",
                    }
                )

    # Margin compression signal (use weighted margin to avoid outlier days)
    if len(daily_df) >= 14 and "cm3" in daily_df.columns and "revenue_pln" in daily_df.columns:
        last_7 = daily_df.tail(7)
        prev_7 = daily_df.iloc[-14:-7]
        last_7_rev = last_7["revenue_pln"].sum()
        prev_7_rev = prev_7["revenue_pln"].sum()
        last_7_margin = (last_7["cm3"].sum() / last_7_rev * 100) if last_7_rev > 0 else 0
        prev_7_margin = (prev_7["cm3"].sum() / prev_7_rev * 100) if prev_7_rev > 0 else 0
        margin_delta = last_7_margin - prev_7_margin
        if abs(margin_delta) > 3 and abs(margin_delta) < 50:
            direction = "expanding" if margin_delta > 0 else "compressing"
            color = "success" if margin_delta > 0 else "warning"
            signals.append(
                {
                    "type": color,
                    "title": f"Margin {direction}: {margin_delta:+.1f}pp",
                    "detail": f"CM3 margin {last_7_margin:.1f}% (was {prev_7_margin:.1f}%)",
                }
            )

    # Daily anomalies
    if len(daily_df) >= 7:
        last_row = daily_df.iloc[-1]
        anomaly_cols = ["revenue_pln", "units", "orders_count"]
        for col in anomaly_cols:
            if col in daily_df.columns:
                anomalies = detect_anomalies(daily_df[col])
                if len(anomalies) > 0 and anomalies.iloc[-1]:
                    val = last_row[col]
                    mean = daily_df[col].rolling(30, min_periods=7).mean().iloc[-1]
                    signals.append(
                        {
                            "type": "info",
                            "title": f"Anomaly: {col.replace('_', ' ').title()}",
                            "detail": f"Today: {val:,.0f} vs 30d avg: {mean:,.0f}",
                        }
                    )

    # Top product movers
    if not product_df.empty and "cm3" in product_df.columns:
        top_products = product_df.head(3)
        for _, row in top_products.iterrows():
            if row.get("cm3", 0) > 1000:
                signals.append(
                    {
                        "type": "success",
                        "title": f"Top performer: {str(row.get('sku', 'N/A'))[:25]}",
                        "detail": f"CM3: {row['cm3']:,.0f} PLN, margin {row.get('cm3_pct', 0):.1f}%",
                    }
                )

    return signals[:10]
