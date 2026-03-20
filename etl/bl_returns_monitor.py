"""BL Returns Monitor — detect unprocessed returns and alert via Discord.

Checks for:
  - Returns in status "Nowe" (101524) for more than ALERT_AFTER_HOURS (48h)
  - Returns with no refund_done for more than REFUND_AFTER_HOURS (72h)

Usage:
  python3.11 -m etl.bl_returns_monitor            # check + Discord alert
  python3.11 -m etl.bl_returns_monitor --days 30  # look back 30 days
  python3.11 -m etl.bl_returns_monitor --list     # just print, no Discord
"""
import argparse
import time
from datetime import datetime, timedelta

from .baselinker import bl_api
from .discord_notify import send_embed, GREEN, YELLOW, RED, ORANGE

# ── Config ──────────────────────────────────────────────────────────────────

STATUS_NEW       = 101524   # "Nowe"
STATUS_RECEIVED  = 101525   # "Odebrane"
STATUS_IN_PROG   = 101526   # "W trakcie"
STATUS_DONE      = 101527   # "Zakończone"
STATUS_REJECTED  = 101528   # "Odrzucone"

ALERT_AFTER_HOURS  = 48     # alert if return in "Nowe" status for > N hours
REFUND_AFTER_HOURS = 72     # alert if refund not processed for > N hours

LOOKBACK_DAYS = 30          # default lookback period

# ── Helpers ──────────────────────────────────────────────────────────────────

def get_all_returns(date_from_ts: int) -> list:
    """Fetch all returns since date_from_ts (paginated by date)."""
    all_returns = []
    params = {"date_from": date_from_ts}
    while True:
        data = bl_api("getOrderReturns", params)
        batch = data.get("returns", [])
        all_returns.extend(batch)
        if len(batch) < 100:
            break
        # paginate: date_from = last return's date_add + 1
        last_date = batch[-1].get("date_add", 0)
        params["date_from"] = last_date + 1
        time.sleep(0.7)
    return all_returns


def get_status_name(status_id: int, status_map: dict) -> str:
    return status_map.get(status_id, {}).get("name", f"Status {status_id}")


def ts_to_str(ts: int) -> str:
    if not ts:
        return "—"
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


def hours_ago(ts: int) -> float:
    return (time.time() - ts) / 3600


# ── Main ─────────────────────────────────────────────────────────────────────

def run(lookback_days: int = LOOKBACK_DAYS, list_only: bool = False):
    now = datetime.now()
    date_from = int((now - timedelta(days=lookback_days)).timestamp())

    print(f"\n{'='*60}")
    print(f"BL Returns Monitor — last {lookback_days} days")
    print(f"{'='*60}")

    # Fetch status list for readable names
    status_data = bl_api("getOrderReturnStatusList", {})
    status_map = {s["id"]: s for s in status_data.get("statuses", [])}

    returns = get_all_returns(date_from)
    print(f"Total returns found: {len(returns)}")

    # Categorize
    pending_new    = []  # "Nowe" > ALERT_AFTER_HOURS
    pending_refund = []  # no refund_done + status != Done/Rejected, > REFUND_AFTER_HOURS
    all_open       = []  # any non-Done status

    for r in returns:
        rid        = r.get("return_id")
        oid        = r.get("order_id")
        status_id  = r.get("status_id")
        date_add   = r.get("date_add") or 0
        refund     = r.get("refund_done")
        currency   = r.get("currency", "EUR")
        comments   = (r.get("admin_comments") or "")[:50]
        age_hours  = hours_ago(date_add)

        if status_id in (STATUS_DONE, STATUS_REJECTED):
            continue

        all_open.append({
            "return_id": rid, "order_id": oid,
            "status": get_status_name(status_id, status_map),
            "age_hours": round(age_hours, 1),
            "date_add": ts_to_str(date_add),
            "refund": refund, "currency": currency,
        })

        if status_id == STATUS_NEW and age_hours > ALERT_AFTER_HOURS:
            pending_new.append(all_open[-1])

        if refund is None and age_hours > REFUND_AFTER_HOURS:
            pending_refund.append(all_open[-1])

    # Output
    print(f"\nOpen returns:          {len(all_open)}")
    print(f"Awaiting action (>48h): {len(pending_new)}")
    print(f"No refund (>72h):      {len(pending_refund)}")

    if all_open:
        print("\nAll open returns:")
        for r in all_open[:20]:
            print(f"  Return #{r['return_id']} (Order #{r['order_id']}) — "
                  f"{r['status']} — {r['age_hours']}h ago — refund: {r['refund']}")

    if list_only:
        return

    # Discord notification
    if not all_open:
        send_embed(
            title="BL Returns — Brak otwartych zwrotów",
            description=f"Ostatnie {lookback_days} dni — wszystkie zwroty obsłużone.",
            color=GREEN,
            footer=now.strftime("%Y-%m-%d %H:%M"),
        )
        return

    fields = [
        {"name": "Wszystkie otwarte", "value": str(len(all_open)),     "inline": True},
        {"name": "Czeka na akcję",    "value": str(len(pending_new)),  "inline": True},
        {"name": "Bez zwrotu",        "value": str(len(pending_refund)), "inline": True},
    ]

    if pending_new:
        lines = [f"Return #{r['return_id']} (Order #{r['order_id']}) — {r['age_hours']}h"
                 for r in pending_new[:8]]
        fields.append({"name": f"Czeka na akcję (>{ALERT_AFTER_HOURS}h w 'Nowe')",
                        "value": "\n".join(lines), "inline": False})

    if pending_refund:
        lines = [f"Return #{r['return_id']} (Order #{r['order_id']}) — {r['age_hours']}h bez zwrotu"
                 for r in pending_refund[:8]]
        fields.append({"name": f"Brak zwrotu (>{REFUND_AFTER_HOURS}h)",
                        "value": "\n".join(lines), "inline": False})

    color = RED if pending_new or pending_refund else YELLOW
    send_embed(
        title=f"BL Returns — {len(all_open)} otwartych zwrotów",
        description=f"Ostatnie {lookback_days} dni. Wymagają uwagi: {len(pending_new)} szt.",
        color=color,
        fields=fields,
        footer=now.strftime("%Y-%m-%d %H:%M"),
    )


def main():
    p = argparse.ArgumentParser(description="BL Returns Monitor")
    p.add_argument("--days", type=int, default=LOOKBACK_DAYS)
    p.add_argument("--list", action="store_true", help="Print only, no Discord")
    args = p.parse_args()
    run(lookback_days=args.days, list_only=args.list)


if __name__ == "__main__":
    main()
