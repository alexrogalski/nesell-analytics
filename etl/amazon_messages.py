"""Sync Amazon buyer messages from Gmail IMAP to Supabase."""
import imaplib
import email
import re
import hashlib
from datetime import datetime, timedelta, timezone, date
from email.header import decode_header
from email.utils import parsedate_to_datetime

from . import config, db

# IMAP credentials (support@nesell.co is alias for alexander@nesell.co)
_GMAIL_CREDS = config._load_env_file(config.KEYS_DIR / "nesell-support-gmail.env")
_IMAP_USER = _GMAIL_CREDS.get("NESELL_SUPPORT_IMAP_USER", "")
_IMAP_PASS = _GMAIL_CREDS.get("NESELL_SUPPORT_IMAP_PASSWORD", "")

# Fallback to main nesell-gmail.env
if not _IMAP_USER:
    _GMAIL_MAIN = config._load_env_file(config.KEYS_DIR / "nesell-gmail.env")
    _IMAP_USER = _GMAIL_MAIN.get("GMAIL_USER", "alexander@nesell.co")
    _IMAP_PASS = _GMAIL_MAIN.get("GMAIL_APP_PASSWORD", "")

IMAP_HOST = "imap.gmail.com"

# Amazon order ID pattern: 3-7-7
ORDER_ID_RE = re.compile(r'\b(\d{3}-\d{7}-\d{7})\b')

# Marketplace domain -> platform mapping
MARKETPLACE_MAP = {
    "marketplace.amazon.de": "amazon_de",
    "marketplace.amazon.fr": "amazon_fr",
    "marketplace.amazon.it": "amazon_it",
    "marketplace.amazon.es": "amazon_es",
    "marketplace.amazon.nl": "amazon_nl",
    "marketplace.amazon.se": "amazon_se",
    "marketplace.amazon.pl": "amazon_pl",
    "marketplace.amazon.com.be": "amazon_be",
    "marketplace.amazon.co.uk": "amazon_uk",
}


def sync_amazon_messages(days_back=30):
    """Main entry point: fetch Amazon buyer emails from IMAP."""
    if not _IMAP_PASS:
        print("  [amazon-msg] No IMAP password configured, skipping")
        return 0

    mail = _connect_imap()
    try:
        # Get already-processed email Message-IDs (stable, unlike IMAP sequence numbers)
        processed = _get_processed_message_ids()
        print(f"  [amazon-msg] {len(processed)} emails already synced")

        # Search for Amazon buyer messages
        new_inbound = 0

        # Search in INBOX for inbound buyer messages
        mail.select("INBOX")
        inbound_uids = _search_buyer_emails(mail, days_back)
        print(f"  [amazon-msg] Found {len(inbound_uids)} Amazon buyer emails (last {days_back}d)")

        for uid in inbound_uids:
            try:
                n = _process_email(mail, uid, "inbound", processed)
                new_inbound += n
            except Exception as e:
                print(f"    [WARN] Email UID {uid}: {e}")

        # Search Sent folder for outbound replies
        mail.select('"[Gmail]/Sent Mail"')
        outbound_uids = _search_outbound_emails(mail, days_back)
        print(f"  [amazon-msg] Found {len(outbound_uids)} sent replies to Amazon buyers")

        for uid in outbound_uids:
            try:
                _process_email(mail, uid, "outbound", processed)
            except Exception as e:
                print(f"    [WARN] Sent UID {uid}: {e}")

        print(f"  [amazon-msg] Done: {new_inbound} new inbound messages")
        return new_inbound
    finally:
        try:
            mail.logout()
        except Exception:
            pass


def _connect_imap():
    """Connect to Gmail IMAP."""
    mail = imaplib.IMAP4_SSL(IMAP_HOST, 993)
    mail.login(_IMAP_USER, _IMAP_PASS)
    return mail


def _get_processed_message_ids():
    """Get set of email Message-IDs already in messages table (stable across IMAP sessions)."""
    ids = set()
    offset = 0
    while True:
        rows = db._get("messages", {
            "select": "email_message_id",
            "email_message_id": "not.is.null",
            "limit": "1000",
            "offset": str(offset),
        })
        for r in rows:
            mid = r.get("email_message_id")
            if mid:
                ids.add(mid)
        if len(rows) < 1000:
            break
        offset += 1000
    return ids


def _search_buyer_emails(mail, days_back):
    """Search INBOX for Amazon buyer messages (returns stable IMAP UIDs)."""
    since_str = (date.today() - timedelta(days=days_back)).strftime("%d-%b-%Y")

    uids = set()
    for search_query in [
        f'(FROM "@marketplace.amazon" SINCE {since_str})',
        f'(TO "support@nesell.co" FROM "amazon" SINCE {since_str})',
    ]:
        status, data = mail.uid("search", None, search_query)
        if status == "OK" and data[0]:
            for uid in data[0].split():
                uids.add(uid.decode())
    return uids


def _search_outbound_emails(mail, days_back):
    """Search Sent folder for replies to Amazon buyers (returns stable IMAP UIDs)."""
    since_str = (date.today() - timedelta(days=days_back)).strftime("%d-%b-%Y")
    status, data = mail.uid("search", None, f'(TO "@marketplace.amazon" SINCE {since_str})')
    if status == "OK" and data[0]:
        return {uid.decode() for uid in data[0].split()}
    return set()


def _process_email(mail, uid, direction, processed_ids=None):
    """Process a single email: parse, match to order, upsert."""
    uid_bytes = uid.encode() if isinstance(uid, str) else uid
    status, data = mail.uid("fetch", uid_bytes, "(RFC822)")
    if status != "OK" or not data or not data[0]:
        return 0

    raw = data[0][1]
    msg = email.message_from_bytes(raw)

    # Parse headers
    from_addr = _decode_header(msg.get("From", ""))
    to_addr = _decode_header(msg.get("To", ""))
    subject = _decode_header(msg.get("Subject", ""))
    message_id = msg.get("Message-ID", "")
    date_str = msg.get("Date", "")

    # Skip if already processed (by Message-ID, stable across sessions)
    if processed_ids and message_id and message_id in processed_ids:
        return 0

    # Parse date
    try:
        sent_at = parsedate_to_datetime(date_str)
        if sent_at.tzinfo is None:
            sent_at = sent_at.replace(tzinfo=timezone.utc)
    except Exception:
        sent_at = datetime.now(timezone.utc)

    # Extract body
    body_text, body_html = _extract_body(msg)

    # Extract order ID
    order_id = _extract_order_id(subject, body_text or "")

    # Determine platform from sender domain
    platform = _extract_platform(from_addr if direction == "inbound" else to_addr)

    # Extract buyer name from From header
    buyer_name = _extract_name(from_addr) if direction == "inbound" else None

    # Build thread key
    if order_id:
        thread_key = f"amazon_email:{order_id}"
    else:
        # Fallback: hash of from+date_day
        day_str = sent_at.strftime("%Y-%m-%d")
        addr_hash = hashlib.md5(from_addr.encode()).hexdigest()[:8]
        thread_key = f"amazon_email:unknown:{addr_hash}:{day_str}"

    # Find or create conversation (without status fields — set those after msg insert)
    conv = {
        "source": "amazon_email",
        "source_thread_id": thread_key,
        "platform": platform or "amazon_de",
        "buyer_name": buyer_name or "",
        "buyer_login": _extract_email_addr(from_addr) if direction == "inbound" else "",
    }

    if order_id:
        conv["external_order_id"] = order_id
        bl_order = _find_bl_order(order_id)
        if bl_order:
            conv["bl_order_id"] = bl_order

    result = db._post("conversations", [conv], on_conflict="source,source_thread_id")
    conv_id = result[0]["id"] if result else None
    if not conv_id:
        return 0

    # Upsert message
    msg_row = {
        "conversation_id": conv_id,
        "source_message_id": message_id or f"uid:{uid}",
        "direction": direction,
        "sender_name": buyer_name or from_addr,
        "body_text": (body_text or "")[:10000],
        "body_html": (body_html or "")[:50000],
        "is_read": (direction == "outbound"),
        "email_uid": str(uid),
        "email_subject": (subject or "")[:500],
        "email_from": from_addr[:500] if from_addr else "",
        "email_message_id": message_id[:500] if message_id else "",
        "sent_at": sent_at.isoformat(),
    }

    db._post("messages", [msg_row], on_conflict="conversation_id,source_message_id")

    # Determine correct status from chronologically latest message in DB
    _refresh_conversation_status(conv_id)

    return 1 if direction == "inbound" else 0


def _refresh_conversation_status(conv_id):
    """Set conversation status based on the chronologically latest message."""
    msgs = db._get("messages", {
        "conversation_id": f"eq.{conv_id}",
        "select": "direction,sent_at",
        "order": "sent_at.desc",
        "limit": "1",
    })
    all_msgs = db._get("messages", {
        "conversation_id": f"eq.{conv_id}",
        "select": "id",
    })
    if not msgs:
        return
    latest = msgs[0]
    is_inbound = latest["direction"] == "inbound"
    db._patch("conversations", {"id": f"eq.{conv_id}"}, {
        "last_message_at": latest["sent_at"],
        "last_message_direction": latest["direction"],
        "needs_reply": is_inbound,
        "status": "open" if is_inbound else "replied",
        "message_count": len(all_msgs),
    })


def _decode_header(value):
    """Decode MIME-encoded header value."""
    if not value:
        return ""
    parts = decode_header(value)
    result = []
    for part, charset in parts:
        if isinstance(part, bytes):
            result.append(part.decode(charset or "utf-8", errors="replace"))
        else:
            result.append(part)
    return "".join(result)


def _extract_body(msg):
    """Extract plain text and HTML body from email."""
    text_body = None
    html_body = None

    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "text/plain" and not text_body:
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    text_body = payload.decode(charset, errors="replace")
            elif ct == "text/html" and not html_body:
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    html_body = payload.decode(charset, errors="replace")
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            ct = msg.get_content_type()
            decoded = payload.decode(charset, errors="replace")
            if ct == "text/html":
                html_body = decoded
            else:
                text_body = decoded

    return text_body, html_body


def _extract_order_id(subject, body):
    """Extract Amazon order ID (3-7-7 pattern) from subject or body."""
    # Check subject first
    m = ORDER_ID_RE.search(subject)
    if m:
        return m.group(1)
    # Then body (first match)
    m = ORDER_ID_RE.search(body[:2000])
    if m:
        return m.group(1)
    return None


def _extract_platform(addr):
    """Extract platform from Amazon marketplace email address."""
    addr_lower = addr.lower()
    for domain, platform in MARKETPLACE_MAP.items():
        if domain in addr_lower:
            return platform
    if "amazon" in addr_lower:
        return "amazon_de"  # Default
    return None


def _extract_name(from_header):
    """Extract display name from From header."""
    if "<" in from_header:
        return from_header.split("<")[0].strip().strip('"')
    return from_header


def _extract_email_addr(from_header):
    """Extract email address from From header."""
    if "<" in from_header and ">" in from_header:
        return from_header.split("<")[1].split(">")[0]
    return from_header


def _find_bl_order(amazon_order_id):
    """Find Baselinker order ID by Amazon order ID."""
    rows = db._get("orders", {
        "select": "external_id",
        "platform_order_id": f"eq.{amazon_order_id}",
        "limit": "1",
    })
    if rows:
        return rows[0].get("external_id")
    return None
