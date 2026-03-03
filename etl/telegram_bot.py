"""Telegram bot: daily P&L summary and alerts."""
import requests
from datetime import date, timedelta
from . import config


def send_message(text: str):
    """Send Telegram message."""
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print("  [Telegram] No bot token/chat_id configured, skipping")
        return False
    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    resp = requests.post(url, json={
        "chat_id": config.TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
    })
    return resp.status_code == 200


def build_daily_report(conn) -> str:
    """Build daily P&L report text."""
    yesterday = date.today() - timedelta(days=1)

    with conn.cursor() as cur:
        # Overall yesterday
        cur.execute("""
            SELECT
                SUM(units_sold), SUM(revenue_pln), SUM(cogs),
                SUM(platform_fees), SUM(gross_profit)
            FROM daily_metrics WHERE date = %s
        """, (yesterday,))
        row = cur.fetchone()

        if not row or not row[0]:
            return f"*{yesterday}*: Brak danych za wczoraj."

        units, revenue, cogs, fees, profit = row

        # By platform
        cur.execute("""
            SELECT p.code, SUM(dm.units_sold), SUM(dm.revenue_pln), SUM(dm.gross_profit)
            FROM daily_metrics dm
            JOIN platforms p ON p.id = dm.platform_id
            WHERE dm.date = %s
            GROUP BY p.code
            ORDER BY SUM(dm.gross_profit) DESC
        """, (yesterday,))
        platforms = cur.fetchall()

        # Top 5 SKUs
        cur.execute("""
            SELECT dm.sku, SUM(dm.units_sold), SUM(dm.gross_profit)
            FROM daily_metrics dm
            WHERE dm.date = %s
            GROUP BY dm.sku
            ORDER BY SUM(dm.gross_profit) DESC
            LIMIT 5
        """, (yesterday,))
        top_skus = cur.fetchall()

        # 7-day trend
        cur.execute("""
            SELECT date, SUM(gross_profit)
            FROM daily_metrics
            WHERE date >= %s - 6
            GROUP BY date
            ORDER BY date
        """, (yesterday,))
        trend = cur.fetchall()

    margin = round(float(profit) / float(revenue) * 100, 1) if revenue else 0

    lines = [
        f"📊 *Nesell Daily P&L — {yesterday}*",
        "",
        f"💰 Revenue: *{float(revenue):,.2f} PLN*",
        f"📦 Units sold: *{int(units)}*",
        f"💵 COGS: {float(cogs):,.2f} PLN",
        f"🏷 Platform fees: {float(fees):,.2f} PLN",
        f"✅ *Gross Profit: {float(profit):,.2f} PLN* ({margin}%)",
        "",
        "📍 *By Platform:*",
    ]
    for code, pu, pr, pp in platforms:
        lines.append(f"  {code}: {int(pu)} units, {float(pp):,.2f} PLN profit")

    lines.append("")
    lines.append("🏆 *Top SKUs:*")
    for sku, su, sp in top_skus:
        lines.append(f"  {sku}: {int(su)} units, {float(sp):,.2f} PLN")

    if trend:
        lines.append("")
        lines.append("📈 *7-day trend:*")
        spark = ""
        prev = None
        for d, p in trend:
            if prev is not None:
                spark += "📈" if float(p) > float(prev) else "📉"
            prev = p
        lines.append(f"  {spark} ({float(trend[0][1]):,.0f} → {float(trend[-1][1]):,.0f} PLN)")

    return "\n".join(lines)


def send_daily_report(conn):
    """Build and send daily P&L via Telegram."""
    report = build_daily_report(conn)
    print(report)
    ok = send_message(report)
    print(f"  [Telegram] Sent: {ok}")
    return ok
