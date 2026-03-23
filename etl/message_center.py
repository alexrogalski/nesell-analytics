"""
Unified message center orchestrator.

Syncs customer conversations from all sources:
  - Allegro Message Center API
  - Amazon buyer messages via Gmail IMAP

Usage:
    python3.11 -m etl.run --messages          # sync all message sources
    python3.11 -m etl.run --messages --days 7 # last 7 days only
"""
from datetime import datetime, timezone

from . import db
from . import discord_notify


def sync_messages(days_back=30):
    """Sync messages from all sources, link shipping problems, send alerts."""
    new_inbound = 0
    errors = []

    # 1. Allegro messages
    try:
        from . import allegro_messages
        n = allegro_messages.sync_allegro_messages(days_back=days_back)
        new_inbound += (n or 0)
    except Exception as e:
        print(f"  [msg-center] Allegro sync failed: {e}")
        errors.append(f"Allegro: {e}")

    # 2. Amazon messages (IMAP)
    try:
        from . import amazon_messages
        n = amazon_messages.sync_amazon_messages(days_back=days_back)
        new_inbound += (n or 0)
    except Exception as e:
        print(f"  [msg-center] Amazon sync failed: {e}")
        errors.append(f"Amazon: {e}")

    # 3. Link shipping problems to conversations
    try:
        _link_shipping_problems()
    except Exception as e:
        print(f"  [msg-center] Shipping problem linkage failed: {e}")

    # 4. Discord alert for new messages needing reply
    if new_inbound > 0:
        try:
            _send_discord_alert(new_inbound)
        except Exception as e:
            print(f"  [msg-center] Discord alert failed: {e}")

    if errors:
        print(f"  [msg-center] Completed with {len(errors)} error(s): {'; '.join(errors)}")
    else:
        print(f"  [msg-center] All sources synced OK, {new_inbound} new inbound messages")


def _link_shipping_problems():
    """Link conversations to shipping_problems by bl_order_id or external_order_id."""
    # Get open conversations without shipping_problem_id
    convs = db._get("conversations", {
        "shipping_problem_id": "is.null",
        "bl_order_id": "not.is.null",
        "select": "id,bl_order_id,external_order_id",
        "status": "neq.closed",
    })
    if not convs:
        return

    linked = 0
    for conv in convs:
        bl_id = conv.get("bl_order_id")
        if not bl_id:
            continue

        # Check if there's an open shipping problem for this order
        problems = db._get("shipping_problems", {
            "bl_order_id": f"eq.{bl_id}",
            "status": "in.(open,in_progress)",
            "select": "id",
            "limit": "1",
        })
        if problems:
            db._patch("conversations", {"id": f"eq.{conv['id']}"}, {
                "shipping_problem_id": problems[0]["id"],
                "category": "shipping",
            })
            linked += 1

    if linked:
        print(f"  [msg-center] Linked {linked} conversations to shipping problems")


def _send_discord_alert(new_count):
    """Send Discord alert about new messages needing reply."""
    # Get the newest conversations needing reply
    convs = db._get("conversations", {
        "needs_reply": "eq.true",
        "status": "in.(open,escalated)",
        "select": "source,platform,buyer_name,buyer_login,external_order_id,last_message_at,category",
        "order": "last_message_at.desc",
        "limit": "5",
    })

    if not convs:
        return

    fields = []
    for c in convs:
        source = (c.get("source") or "").upper().replace("_", " ")
        platform = c.get("platform") or ""
        buyer = c.get("buyer_name") or c.get("buyer_login") or "?"
        order = c.get("external_order_id") or ""
        cat = c.get("category") or ""

        name = f"[{platform.upper()}] {buyer}"
        value = f"Order: {order}" if order else "No order linked"
        if cat:
            value += f" | {cat}"

        fields.append({"name": name[:256], "value": value[:1024], "inline": False})

    discord_notify.send_embed(
        title=f"Message Center: {new_count} new message(s) need reply",
        description=f"Top {len(convs)} conversations needing attention:",
        color=discord_notify.ORANGE,
        fields=fields,
        footer=f"Message Center | {datetime.now().strftime('%Y-%m-%d %H:%M')}",
    )


def get_open_conversations():
    """Get all conversations needing reply (for dashboard use)."""
    return db._get("conversations", {
        "needs_reply": "eq.true",
        "status": "in.(open,escalated)",
        "order": "last_message_at.desc",
        "select": "*",
    })


def get_conversation_messages(conv_id):
    """Get all messages for a conversation (for dashboard use)."""
    return db._get("messages", {
        "conversation_id": f"eq.{conv_id}",
        "order": "sent_at.asc",
        "select": "*",
    })
