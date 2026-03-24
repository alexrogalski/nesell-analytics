"""Process customer messages with Claude Sonnet.

One AI call per message handles everything: translation, categorization,
analysis, and draft reply in both Polish and buyer's language.
"""
import json
import os
import re
import shutil
import subprocess
from datetime import datetime, timezone

from . import db

# Language code -> marketplace mapping
PLATFORM_LANG = {
    "allegro": "pl",
    "amazon_de": "de",
    "amazon_fr": "fr",
    "amazon_it": "it",
    "amazon_es": "es",
    "amazon_nl": "nl",
    "amazon_se": "sv",
    "amazon_pl": "pl",
    "amazon_be": "nl",
    "amazon_uk": "en",
}

MAX_AI_RETRIES = 3
MAX_OUTPUT_TOKENS = 800


# ── Anthropic SDK / Claude CLI ──

def _get_anthropic_client():
    """Get Anthropic client if API key is available."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        try:
            from . import config
            env = config._load_env_file(config.KEYS_DIR / "anthropic.env")
            api_key = env.get("ANTHROPIC_API_KEY", "")
        except Exception:
            pass
    if not api_key:
        try:
            import streamlit as st
            api_key = st.secrets.get("ANTHROPIC_API_KEY", "")
        except Exception:
            pass
    if not api_key:
        return None
    try:
        import anthropic
        return anthropic.Anthropic(api_key=api_key)
    except Exception:
        return None


def _call_ai(prompt):
    """Call Claude Sonnet via Anthropic SDK (primary) or CLI (fallback)."""
    client = _get_anthropic_client()
    if client:
        try:
            response = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=MAX_OUTPUT_TOKENS,
                messages=[{"role": "user", "content": prompt}],
            )
            return response.content[0].text.strip()
        except Exception as e:
            print(f"    [WARN] Anthropic SDK failed: {e}")

    if shutil.which("claude"):
        try:
            result = subprocess.run(
                ["claude", "-p", prompt, "--model", "sonnet", "--max-turns", "1"],
                capture_output=True, text=True, timeout=45,
            )
            return result.stdout.strip()
        except Exception as e:
            print(f"    [WARN] Claude CLI failed: {e}")

    return None


# ── Main pipeline: one Sonnet call per message ──

def process_messages(batch_size=20):
    """Process unprocessed inbound messages. One Sonnet call per message does:
    translation to PL, language detection, categorization, analysis, draft replies."""
    msgs = db._get("messages", {
        "ai_processed_at": "is.null",
        "direction": "eq.inbound",
        "body_text": "not.is.null",
        "select": "id,conversation_id,body_text,email_subject,ai_retry_count",
        "order": "sent_at.desc",
        "limit": str(batch_size),
    })

    if not msgs:
        print("  [msg-ai] No new messages to process")
        return 0

    # Filter out messages that exceeded retry limit
    eligible = []
    for m in msgs:
        retries = m.get("ai_retry_count") or 0
        if retries >= MAX_AI_RETRIES:
            db._patch("messages", {"id": f"eq.{m['id']}"}, {
                "ai_processed_at": datetime.now(timezone.utc).isoformat(),
                "ai_analysis": f"[AUTO-SKIP] Exceeded {MAX_AI_RETRIES} retries",
            })
            print(f"    Msg {m['id']}: skipped ({MAX_AI_RETRIES} retries exceeded)")
        else:
            eligible.append(m)

    if not eligible:
        print("  [msg-ai] All pending messages exceeded retry limit")
        return 0

    print(f"  [msg-ai] Processing {len(eligible)} messages with Sonnet")

    # Pre-fetch conversation data
    conv_cache = {}
    for m in eligible:
        cid = m["conversation_id"]
        if cid not in conv_cache:
            rows = db._get("conversations", {
                "id": f"eq.{cid}",
                "select": "id,source,platform,external_order_id,buyer_name,buyer_login",
                "limit": "1",
            })
            conv_cache[cid] = rows[0] if rows else {}

    processed = 0
    for m in eligible:
        try:
            _process_single(m, conv_cache.get(m["conversation_id"], {}))
            processed += 1
        except Exception as e:
            retries = (m.get("ai_retry_count") or 0) + 1
            db._patch("messages", {"id": f"eq.{m['id']}"}, {"ai_retry_count": retries})
            print(f"    [WARN] Msg {m['id']} failed (retry {retries}/{MAX_AI_RETRIES}): {e}")

    print(f"  [msg-ai] Done: {processed}/{len(eligible)}")
    return processed


def _process_single(msg, conv):
    """Process one message with a single Sonnet call."""
    msg_id = msg["id"]
    conv_id = msg["conversation_id"]
    body = _clean_email_body(msg.get("body_text") or "")
    subject = (msg.get("email_subject") or "").strip()

    if not body and not subject:
        db._patch("messages", {"id": f"eq.{msg_id}"}, {
            "ai_processed_at": datetime.now(timezone.utc).isoformat(),
        })
        return

    platform = conv.get("platform", "amazon_de")
    expected_lang = PLATFORM_LANG.get(platform, "de")
    order_id = conv.get("external_order_id", "")
    buyer = conv.get("buyer_name") or conv.get("buyer_login") or "?"

    # Get order context
    order_ctx = _get_order_context(order_id) if order_id else ""

    prompt = f"""You are customer support for "nesell" (e-commerce, Amazon/Allegro EU, hats and accessories).

BUYER: {buyer} | PLATFORM: {platform} | ORDER: {order_id or "none"}
SUBJECT: {subject[:120]}
MESSAGE:
{body[:1200]}
{f"ORDER DATA: {order_ctx[:500]}" if order_ctx else ""}

Respond with ONLY a JSON object (no markdown, no explanation):
{{
  "detected_language": "2-letter code (de/fr/it/es/nl/sv/pl/en)",
  "translation_pl": "full Polish translation of buyer's message (skip if already Polish)",
  "kategoria": "shipping|return|damage|question|complaint|other",
  "pilnosc": "low|normal|high|urgent",
  "analiza": "2-3 sentences in Polish: what is the problem, what we know from order data, recommended action",
  "draft_pl": "professional reply in Polish, warm but concise, signed Zespol nesell",
  "draft_{expected_lang}": "same reply translated to {expected_lang}"
}}"""

    output = _call_ai(prompt)
    if not output:
        raise RuntimeError("No AI backend available")

    # Parse JSON
    json_match = re.search(r'\{.*\}', output, re.DOTALL)
    if not json_match:
        raise ValueError("No JSON in AI output")
    data = json.loads(json_match.group())

    # Extract fields
    detected_lang = data.get("detected_language", expected_lang)
    translation_pl = data.get("translation_pl", "")
    category = data.get("kategoria", "")
    urgency = data.get("pilnosc", "normal")
    analysis = data.get("analiza", "")
    draft_pl = data.get("draft_pl", "")

    # Find the local language draft
    draft_local = data.get(f"draft_{expected_lang}", "") or data.get(f"draft_{detected_lang}", "")
    if not draft_local:
        for k, v in data.items():
            if k.startswith("draft_") and k != "draft_pl" and v:
                draft_local = v
                break

    # Update message
    msg_update = {
        "ai_processed_at": datetime.now(timezone.utc).isoformat(),
        "detected_language": detected_lang,
        "ai_analysis": analysis,
        "ai_retry_count": 0,
        "draft_reply": draft_pl,
        "draft_reply_local": draft_local or draft_pl,
    }
    if translation_pl and detected_lang != "pl":
        msg_update["translation_pl"] = translation_pl
    if draft_pl:
        msg_update["ai_draft_pl"] = draft_pl
    if draft_local:
        msg_update["ai_draft_local"] = draft_local

    db._patch("messages", {"id": f"eq.{msg_id}"}, msg_update)

    # Update conversation
    conv_update = {}
    if category and category in ("shipping", "return", "damage", "question", "complaint", "other"):
        conv_update["category"] = category
    if urgency and urgency in ("low", "normal", "high", "urgent"):
        conv_update["priority"] = urgency
    if conv_update:
        db._patch("conversations", {"id": f"eq.{conv_id}"}, conv_update)

    print(f"    Msg {msg_id}: {detected_lang} | {category}/{urgency} | {analysis[:60]}...")


# ── Helpers ──

def _clean_email_body(text):
    """Strip Amazon boilerplate, keep only the actual buyer message."""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    m = re.search(
        r'-{3,}\s*Message:?\s*-{3,}\s*\n(.*?)\n\s*-{3,}\s*End message\s*-{3,}',
        text, re.DOTALL | re.IGNORECASE,
    )
    if m:
        return m.group(1).strip()
    lines = []
    for line in text.split("\n"):
        s = line.strip().lower()
        if any(p in s for p in [
            "was this email helpful", "resolve case", "report questionable",
            "copyright", "sellercentral.", "you have received a message",
            "no-response-needed", "customattribute",
        ]):
            continue
        if re.match(r'^https?://', s):
            continue
        lines.append(line)
    return "\n".join(lines).strip()


def _get_order_context(order_id):
    """Build order context string for the prompt."""
    if not order_id:
        return ""

    order = None
    for field in ["platform_order_id", "external_id"]:
        rows = db._get("orders", {field: f"eq.{order_id}", "select": "*", "limit": "1"})
        if rows:
            order = rows[0]
            break

    if not order:
        return f"Order {order_id} NOT FOUND in database."

    parts = [f"Order {order_id}:"]
    parts.append(f"Date: {(order.get('order_date') or '')[:10]}")
    parts.append(f"Status: {order.get('status', '?')}")
    parts.append(f"Country: {order.get('shipping_country', '?')}")

    internal_id = order.get("id")
    bl_id = str(order.get("external_id") or internal_id)

    items = db._get("order_items", {"order_id": f"eq.{internal_id}", "select": "sku,name,quantity"})
    for it in (items or [])[:3]:
        parts.append(f"Item: {it.get('quantity', 1)}x {it.get('name', it.get('sku', '?'))}")

    shipping = db._get("shipping_costs", {"order_id": f"eq.{bl_id}", "select": "tracking_number,courier,ship_date", "limit": "1"})
    if shipping:
        s = shipping[0]
        parts.append(f"Shipped: {s.get('ship_date','')} via {s.get('courier','?')}, tracking: {s.get('tracking_number','none')}")
    else:
        parts.append("Shipping: NO DATA")

    problems = db._get("shipping_problems", {"bl_order_id": f"eq.{bl_id}", "select": "problem_type,problem_detail,severity,status", "limit": "1"})
    if problems:
        p = problems[0]
        parts.append(f"PROBLEM: {p.get('problem_type','?')} - {p.get('problem_detail','?')} ({p.get('severity','?')})")

    return " | ".join(parts)
