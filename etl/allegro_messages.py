"""Sync Allegro Message Center threads and messages to Supabase."""
import time
from datetime import datetime, timedelta, timezone

from . import db
from .allegro_fees import _load_allegro_token, _api_get, BASE, _headers

# Allegro messaging API uses a different Accept header for some endpoints
MSG_ACCEPT = "application/vnd.allegro.public.v1+json"


def sync_allegro_messages(days_back=30):
    """Main entry point: sync Allegro threads + messages."""
    token = _load_allegro_token()
    since = datetime.now(timezone.utc) - timedelta(days=days_back)

    # Get last sync timestamp for incremental sync
    last_sync = _get_last_sync()
    if last_sync and last_sync > since:
        since = last_sync
        print(f"  [allegro-msg] Incremental sync from {since.isoformat()}")
    else:
        print(f"  [allegro-msg] Full sync, last {days_back} days")

    # Sync regular message threads
    threads = _fetch_threads(token, since)
    print(f"  [allegro-msg] Found {len(threads)} threads with new messages")

    new_inbound = 0
    for i, thread in enumerate(threads):
        try:
            n = _sync_thread(token, thread, since)
            new_inbound += n
        except Exception as e:
            print(f"    [WARN] Thread {thread.get('id', '?')}: {e}")

        if (i + 1) % 20 == 0:
            print(f"    ... processed {i+1}/{len(threads)} threads")

    # Sync disputes/issues (requires sale:disputes scope, may not be available)
    issues = []
    try:
        issues = _fetch_issues(token, since)
        if issues:
            print(f"  [allegro-msg] Found {len(issues)} active issues")
            for issue in issues:
                try:
                    _sync_issue(token, issue, since)
                except Exception as e:
                    print(f"    [WARN] Issue {issue.get('id', '?')}: {e}")
    except Exception:
        pass  # Expected: 406 if app lacks sale:disputes scope

    print(f"  [allegro-msg] Done: {len(threads)} threads, {len(issues)} issues, {new_inbound} new inbound messages")
    return new_inbound


def _get_last_sync():
    """Get timestamp of most recent synced Allegro message."""
    rows = db._get("conversations", {
        "source": "eq.allegro",
        "select": "last_message_at",
        "order": "last_message_at.desc",
        "limit": "1",
    })
    if rows and rows[0].get("last_message_at"):
        ts = rows[0]["last_message_at"]
        if isinstance(ts, str):
            # Parse ISO format, handle Z suffix
            ts = ts.replace("Z", "+00:00")
            return datetime.fromisoformat(ts)
    return None


def _fetch_threads(token, since):
    """Paginate through Allegro message threads, stop when older than since."""
    threads = []
    offset = 0
    while True:
        data = _api_get(token, "/messaging/threads", params={
            "limit": 20,
            "offset": offset,
        })
        batch = data.get("threads", [])
        if not batch:
            break

        for t in batch:
            last_msg = t.get("lastMessageDateTime", "")
            if last_msg:
                msg_dt = datetime.fromisoformat(last_msg.replace("Z", "+00:00"))
                if msg_dt < since:
                    return threads  # All remaining are older
            threads.append(t)

        offset += len(batch)
        if len(batch) < 20:
            break
        time.sleep(0.5)  # Rate limit courtesy

    return threads


def _sync_thread(token, thread, since):
    """Sync a single thread: upsert conversation + fetch messages."""
    thread_id = thread["id"]
    interlocutor = thread.get("interlocutor", {})
    offer = thread.get("offer", {})

    # Build conversation record
    conv = {
        "source": "allegro",
        "source_thread_id": thread_id,
        "buyer_name": interlocutor.get("login", ""),
        "buyer_login": interlocutor.get("login", ""),
        "platform": "allegro",
    }

    # Try to match order
    related = thread.get("relatedObject", {})
    checkout_id = related.get("id") if related.get("type") == "CHECKOUT_FORM" else None
    if checkout_id:
        conv["external_order_id"] = checkout_id
        bl_order = _find_bl_order_allegro(checkout_id)
        if bl_order:
            conv["bl_order_id"] = bl_order

    # Fetch messages for this thread
    messages = _fetch_thread_messages(token, thread_id, since)

    if not messages:
        return 0

    # Determine conversation state from messages
    new_inbound = 0
    last_msg = messages[0]  # messages are newest-first from API
    conv["last_message_at"] = last_msg.get("createdAt", "")
    conv["message_count"] = len(messages)  # will be updated later to total

    is_last_inbound = last_msg.get("author", {}).get("isInterlocutor", False)
    conv["last_message_direction"] = "inbound" if is_last_inbound else "outbound"
    conv["needs_reply"] = is_last_inbound
    conv["status"] = "open" if is_last_inbound else "replied"

    # Upsert conversation
    result = db._post("conversations", [conv], on_conflict="source,source_thread_id")
    conv_id = result[0]["id"] if result else None
    if not conv_id:
        return 0

    # Upsert messages
    msg_rows = []
    for m in messages:
        is_buyer = m.get("author", {}).get("isInterlocutor", False)
        if is_buyer:
            new_inbound += 1
        msg_rows.append({
            "conversation_id": conv_id,
            "source_message_id": m["id"],
            "direction": "inbound" if is_buyer else "outbound",
            "sender_name": m.get("author", {}).get("login", ""),
            "body_text": m.get("text", ""),
            "attachments": [{"url": a.get("url", ""), "filename": a.get("fileName", "")}
                           for a in m.get("attachments", [])],
            "is_read": not m.get("author", {}).get("isInterlocutor", False),
            "sent_at": m.get("createdAt", datetime.now(timezone.utc).isoformat()),
        })

    if msg_rows:
        for i in range(0, len(msg_rows), 500):
            chunk = msg_rows[i:i+500]
            db._post("messages", chunk, on_conflict="conversation_id,source_message_id")

    # Update conversation with accurate message count
    total_count = _count_messages(conv_id)
    if total_count:
        db._patch("conversations", {"id": f"eq.{conv_id}"}, {"message_count": total_count})

    # Compute first_response_minutes
    _update_response_time(conv_id)

    return new_inbound


def _fetch_thread_messages(token, thread_id, since):
    """Fetch messages for a thread, using after filter for incremental."""
    messages = []
    params = {"limit": 20, "after": since.isoformat()}

    data = _api_get(token, f"/messaging/threads/{thread_id}/messages", params=params)
    batch = data.get("messages", [])
    messages.extend(batch)

    # Paginate if needed (rare, most threads have < 20 new messages)
    while len(batch) == 20:
        oldest = batch[-1].get("createdAt", "")
        if oldest:
            params["after"] = since.isoformat()
            params["offset"] = len(messages)
        data = _api_get(token, f"/messaging/threads/{thread_id}/messages", params=params)
        batch = data.get("messages", [])
        messages.extend(batch)
        time.sleep(0.3)

    return messages


def _fetch_issues(token, since):
    """Fetch active Allegro issues (disputes/claims)."""
    issues = []
    offset = 0
    while True:
        data = _api_get(token, "/sale/issues", params={
            "limit": 20,
            "offset": offset,
        })
        batch = data.get("issues", data.get("disputes", []))
        if not batch:
            break
        issues.extend(batch)
        offset += len(batch)
        if len(batch) < 20:
            break
        time.sleep(0.5)
    return issues


def _sync_issue(token, issue, since):
    """Sync a single Allegro issue/dispute."""
    issue_id = issue["id"]
    buyer = issue.get("buyer", {})
    checkout = issue.get("checkoutForm", {})

    conv = {
        "source": "allegro_issue",
        "source_thread_id": issue_id,
        "buyer_name": buyer.get("login", ""),
        "buyer_login": buyer.get("login", ""),
        "platform": "allegro",
        "category": "complaint",
        "priority": "high",
    }

    if checkout.get("id"):
        conv["external_order_id"] = checkout["id"]
        bl_order = _find_bl_order_allegro(checkout["id"])
        if bl_order:
            conv["bl_order_id"] = bl_order

    # Fetch issue chat messages
    data = _api_get(token, f"/sale/issues/{issue_id}/messages", params={"limit": 50})
    messages = data.get("messages", [])

    if messages:
        last_msg = messages[-1]
        conv["last_message_at"] = last_msg.get("createdAt", "")
        conv["message_count"] = len(messages)
        is_buyer = last_msg.get("author", {}).get("role") == "BUYER"
        conv["last_message_direction"] = "inbound" if is_buyer else "outbound"
        conv["needs_reply"] = is_buyer
        conv["status"] = "escalated"

    result = db._post("conversations", [conv], on_conflict="source,source_thread_id")
    conv_id = result[0]["id"] if result else None
    if not conv_id or not messages:
        return

    msg_rows = []
    for m in messages:
        is_buyer = m.get("author", {}).get("role") == "BUYER"
        msg_rows.append({
            "conversation_id": conv_id,
            "source_message_id": m.get("id", m.get("createdAt", "")),
            "direction": "inbound" if is_buyer else "outbound",
            "sender_name": m.get("author", {}).get("login", ""),
            "body_text": m.get("text", ""),
            "attachments": [{"url": a.get("url", ""), "filename": a.get("fileName", "")}
                           for a in m.get("attachments", [])],
            "is_read": True,
            "sent_at": m.get("createdAt", datetime.now(timezone.utc).isoformat()),
        })

    for i in range(0, len(msg_rows), 500):
        chunk = msg_rows[i:i+500]
        db._post("messages", chunk, on_conflict="conversation_id,source_message_id")


def _find_bl_order_allegro(checkout_form_id):
    """Find Baselinker order ID by Allegro checkout form ID."""
    rows = db._get("orders", {
        "select": "external_id",
        "platform_order_id": f"eq.{checkout_form_id}",
        "limit": "1",
    })
    if rows:
        return rows[0].get("external_id")
    return None


def _count_messages(conv_id):
    """Count total messages in a conversation."""
    rows = db._get("messages", {
        "conversation_id": f"eq.{conv_id}",
        "select": "id",
    })
    return len(rows)


def _update_response_time(conv_id):
    """Compute first_response_minutes for a conversation."""
    msgs = db._get("messages", {
        "conversation_id": f"eq.{conv_id}",
        "select": "direction,sent_at",
        "order": "sent_at.asc",
    })
    first_inbound = None
    first_outbound_after = None
    for m in msgs:
        if m["direction"] == "inbound" and not first_inbound:
            first_inbound = m["sent_at"]
        elif m["direction"] == "outbound" and first_inbound and not first_outbound_after:
            first_outbound_after = m["sent_at"]
            break

    if first_inbound and first_outbound_after:
        t1 = datetime.fromisoformat(first_inbound.replace("Z", "+00:00"))
        t2 = datetime.fromisoformat(first_outbound_after.replace("Z", "+00:00"))
        minutes = int((t2 - t1).total_seconds() / 60)
        db._patch("conversations", {"id": f"eq.{conv_id}"}, {"first_response_minutes": minutes})
