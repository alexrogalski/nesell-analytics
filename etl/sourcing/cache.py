"""Supabase-backed cache for sourcing lookups (marketplace prices, BSR, etc.).

Uses the same PostgREST client pattern as etl.db so there is no direct
PostgreSQL dependency.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from .. import db

# ---------------------------------------------------------------------------
# Table name
# ---------------------------------------------------------------------------

_TABLE = "sourcing_cache"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_cached(
    ean: str,
    platform: str,
    marketplace_id: str | None = None,
    ttl_hours: int = 24,
) -> dict | None:
    """Return cached data for *ean*+*platform*+*marketplace_id* if fresh enough.

    Returns the parsed JSON payload or ``None`` when the cache is stale or
    missing.
    """
    params: dict[str, str] = {
        "select": "data_json,fetched_at",
        "ean": f"eq.{ean}",
        "platform": f"eq.{platform}",
        "limit": "1",
    }
    if marketplace_id is not None:
        params["marketplace_id"] = f"eq.{marketplace_id}"
    else:
        params["marketplace_id"] = "is.null"

    rows = db._get(_TABLE, params)
    if not rows:
        return None

    row = rows[0]
    fetched_at = _parse_ts(row.get("fetched_at", ""))
    if fetched_at is None:
        return None

    cutoff = datetime.now(timezone.utc) - timedelta(hours=ttl_hours)
    if fetched_at < cutoff:
        return None

    data = row.get("data_json")
    if isinstance(data, str):
        data = json.loads(data)
    return data


def set_cached(
    ean: str,
    platform: str,
    data: dict,
    marketplace_id: str | None = None,
) -> None:
    """Insert or update the cache entry for *ean*+*platform*+*marketplace_id*."""
    row = {
        "ean": ean,
        "platform": platform,
        "marketplace_id": marketplace_id,
        "data_json": data,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
    db._post(_TABLE, [row], on_conflict="ean,platform,marketplace_id")


def clear_cache(
    ean: str | None = None,
    older_than_hours: int | None = None,
) -> int:
    """Delete cache rows matching the filter and return an approximate count.

    Parameters
    ----------
    ean : str | None
        If given, only delete rows for this EAN.
    older_than_hours : int | None
        If given, only delete rows fetched more than this many hours ago.

    Returns
    -------
    int
        Number of rows deleted (best-effort; PostgREST may not always return
        exact counts).
    """
    import requests

    params: dict[str, str] = {}
    if ean is not None:
        params["ean"] = f"eq.{ean}"
    if older_than_hours is not None:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(hours=older_than_hours)
        ).isoformat()
        params["fetched_at"] = f"lt.{cutoff}"

    # If no filters at all, delete everything (use a tautology filter)
    if not params:
        params["id"] = "gt.0"

    headers = dict(db._HEADERS)
    headers["Prefer"] = "return=representation"

    resp = requests.delete(db._url(_TABLE), headers=headers, params=params)
    if resp.status_code not in (200, 204):
        raise Exception(
            f"Cache clear error {resp.status_code}: {resp.text[:200]}"
        )

    if resp.text:
        try:
            deleted = resp.json()
            return len(deleted) if isinstance(deleted, list) else 0
        except (json.JSONDecodeError, ValueError):
            return 0
    return 0


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_ts(raw: str) -> datetime | None:
    """Parse an ISO-8601 timestamp from Supabase into an aware datetime."""
    if not raw:
        return None
    raw = raw.strip()
    # Supabase returns e.g. "2026-03-24T10:00:00+00:00" or with "Z"
    raw = raw.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None
