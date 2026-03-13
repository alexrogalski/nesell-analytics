"""Fetch FX rates from NBP API (Polish National Bank)."""
import requests
import time
from datetime import date, timedelta
from . import db


NBP_BASE = "https://api.nbp.pl/api/exchangerates/rates/a"
CURRENCIES = ["EUR", "GBP", "SEK", "USD"]
REQUEST_TIMEOUT = 15  # seconds


def _get_with_retry(url: str, max_retries: int = 3, backoff: float = 2.0) -> requests.Response:
    """GET request with retry logic and timeout."""
    headers = {"Accept": "application/json"}
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            return resp
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt < max_retries - 1:
                wait = backoff * (attempt + 1)
                print(f"    [WARN] NBP request failed (attempt {attempt+1}/{max_retries}), retrying in {wait}s: {e}")
                time.sleep(wait)
            else:
                raise


def fetch_nbp_rate(currency: str, day: date) -> float | None:
    """Fetch single currency rate for a date from NBP."""
    url = f"{NBP_BASE}/{currency}/{day.isoformat()}/"
    resp = _get_with_retry(url)
    if resp.status_code == 200:
        data = resp.json()
        return float(data["rates"][0]["mid"])
    elif resp.status_code == 404:
        # No trading day (weekend/holiday) — try previous days
        for i in range(1, 5):
            prev = day - timedelta(days=i)
            url2 = f"{NBP_BASE}/{currency}/{prev.isoformat()}/"
            resp2 = _get_with_retry(url2)
            if resp2.status_code == 200:
                return float(resp2.json()["rates"][0]["mid"])
    return None


def fetch_nbp_range(currency: str, start: date, end: date) -> list[dict]:
    """Fetch rates for a date range."""
    url = f"{NBP_BASE}/{currency}/{start.isoformat()}/{end.isoformat()}/"
    resp = _get_with_retry(url)
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
