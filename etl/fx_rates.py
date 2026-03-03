"""Fetch FX rates from NBP API (Polish National Bank)."""
import requests
from datetime import date, timedelta
from . import db


NBP_BASE = "https://api.nbp.pl/api/exchangerates/rates/a"
CURRENCIES = ["EUR", "GBP", "SEK", "USD"]


def fetch_nbp_rate(currency: str, day: date) -> float | None:
    """Fetch single currency rate for a date from NBP."""
    url = f"{NBP_BASE}/{currency}/{day.isoformat()}/"
    resp = requests.get(url, headers={"Accept": "application/json"})
    if resp.status_code == 200:
        data = resp.json()
        return float(data["rates"][0]["mid"])
    elif resp.status_code == 404:
        # No trading day (weekend/holiday) — try previous days
        for i in range(1, 5):
            prev = day - timedelta(days=i)
            url2 = f"{NBP_BASE}/{currency}/{prev.isoformat()}/"
            resp2 = requests.get(url2, headers={"Accept": "application/json"})
            if resp2.status_code == 200:
                return float(resp2.json()["rates"][0]["mid"])
    return None


def fetch_nbp_range(currency: str, start: date, end: date) -> list[dict]:
    """Fetch rates for a date range."""
    url = f"{NBP_BASE}/{currency}/{start.isoformat()}/{end.isoformat()}/"
    resp = requests.get(url, headers={"Accept": "application/json"})
    if resp.status_code == 200:
        return [
            {"date": r["effectiveDate"], "currency": currency, "rate_pln": float(r["mid"])}
            for r in resp.json()["rates"]
        ]
    return []


def sync_fx_rates(conn, days_back: int = 90):
    """Sync FX rates for last N days."""
    end = date.today()
    start = end - timedelta(days=days_back)
    total = 0
    for curr in CURRENCIES:
        rates = fetch_nbp_range(curr, start, end)
        for r in rates:
            db.upsert_fx_rate(conn, r["date"], r["currency"], r["rate_pln"])
        total += len(rates)
        print(f"  FX {curr}: {len(rates)} rates synced")
    return total


def convert_to_pln(conn, amount: float, currency: str, day: date) -> float | None:
    """Convert amount to PLN using DB rates."""
    if currency == "PLN":
        return amount
    rate = db.get_fx_rate(conn, day, currency)
    if rate:
        return round(amount * rate, 2)
    return None
