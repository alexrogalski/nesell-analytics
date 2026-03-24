"""Auto-translate messages and generate AI draft replies for customer conversations.

Uses Anthropic SDK (primary) or Claude CLI (fallback) for per-case AI analysis.
"""
import json
import os
import re
import shutil
import subprocess
from datetime import datetime, timezone

from deep_translator import GoogleTranslator

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

# Draft reply templates per category (Polish base, will be translated)
DRAFT_TEMPLATES = {
    "shipping": (
        "Dzien dobry,\n\n"
        "Dziekujemy za wiadomosc. Sprawdzamy status wysylki Panstwa zamowienia{order_ref}. "
        "Prosimy o cierpliwosc, odpowiemy najszybciej jak to mozliwe z aktualnymi informacjami.\n\n"
        "Pozdrawiamy,\nZespol nesell"
    ),
    "return": (
        "Dzien dobry,\n\n"
        "Dziekujemy za kontakt w sprawie zwrotu{order_ref}. "
        "Prosimy o wyslanie produktu na nasz adres magazynowy. "
        "Po otrzymaniu i sprawdzeniu towaru, zwrot zostanie przetworzony.\n\n"
        "Pozdrawiamy,\nZespol nesell"
    ),
    "damage": (
        "Dzien dobry,\n\n"
        "Bardzo przepraszamy za niedogodnosci{order_ref}. "
        "Prosimy o przeslanie zdjecia uszkodzonego produktu, abysmy mogli szybko rozwiazac problem. "
        "Oferujemy wymiane lub pelny zwrot.\n\n"
        "Pozdrawiamy,\nZespol nesell"
    ),
    "default": (
        "Dzien dobry,\n\n"
        "Dziekujemy za wiadomosc{order_ref}. "
        "Zapoznajemy sie z Panstwa zapytaniem i odpowiemy najszybciej jak to mozliwe.\n\n"
        "Pozdrawiamy,\nZespol nesell"
    ),
}

# Keywords for auto-categorization (multi-language)
CATEGORY_KEYWORDS = {
    "shipping": [
        "tracking", "shipment", "delivery", "delivered", "lieferung", "versand",
        "zustellung", "livraison", "consegna", "envio", "wysylka", "przesylka",
        "paczka", "dostawa", "kiedy", "where is", "wo ist", "ou est", "dove",
        "cuando", "niet ontvangen", "not received", "nie dostalem", "nie dotarlo",
    ],
    "return": [
        "return", "refund", "ruckgabe", "erstattung", "retour", "remboursement",
        "reso", "rimborso", "devolucion", "reembolso", "zwrot", "oddac",
    ],
    "damage": [
        "damaged", "broken", "defect", "beschadigt", "kaputt", "endommage",
        "danneggiato", "danado", "uszkodzony", "zepsuty", "rozbity",
    ],
}

MAX_AI_RETRIES = 3

# ── API cost guards ──
# Sonnet 4 pricing: ~$3/M input, $15/M output tokens
# With 1024 max_tokens output and ~500 token prompts:
# ~$0.002 per call → 50 calls/day = ~$0.10/day = ~$3/month
DAILY_API_CALL_LIMIT = 50       # max SDK calls per day (resets at midnight UTC)
MAX_PROMPT_CHARS = 2000          # truncate prompts to save input tokens
MAX_OUTPUT_TOKENS = 800          # enough for analysis + draft, not wasteful
BATCH_SIZE_CAP = 5               # max messages per cron run (spread load across runs)
_daily_call_count = 0
_daily_call_date = None


def process_messages(batch_size=50):
    """Process unprocessed inbound messages: translate + generate draft replies."""
    msgs = db._get("messages", {
        "ai_processed_at": "is.null",
        "direction": "eq.inbound",
        "select": "id,conversation_id,body_text,email_subject,source_message_id",
        "order": "sent_at.desc",
        "limit": str(batch_size),
    })

    if not msgs:
        print("  [msg-ai] No new messages to process")
        return 0

    print(f"  [msg-ai] Processing {len(msgs)} inbound messages")

    conv_ids = list({str(m["conversation_id"]) for m in msgs})
    convs = {}
    for cid in conv_ids:
        rows = db._get("conversations", {
            "id": f"eq.{cid}",
            "select": "id,platform,category,external_order_id",
            "limit": "1",
        })
        if rows:
            convs[rows[0]["id"]] = rows[0]

    processed = 0
    for m in msgs:
        try:
            _process_single_message(m, convs.get(m["conversation_id"], {}))
            processed += 1
        except Exception as e:
            print(f"    [WARN] Message {m['id']}: {e}")

    print(f"  [msg-ai] Done: {processed}/{len(msgs)} messages processed")
    return processed


def _process_single_message(msg, conv):
    """Translate message and generate draft reply."""
    body = (msg.get("body_text") or "").strip()
    subject = (msg.get("email_subject") or "").strip()
    text = f"{subject}\n{body}" if subject else body

    if not text or len(text) < 3:
        db._patch("messages", {"id": f"eq.{msg['id']}"}, {
            "ai_processed_at": datetime.now(timezone.utc).isoformat(),
        })
        return

    platform = conv.get("platform", "amazon_de")
    source_lang = PLATFORM_LANG.get(platform, "de")

    # Detect and translate
    translation_pl = None
    detected_lang = source_lang

    if source_lang != "pl":
        try:
            translator = GoogleTranslator(source="auto", target="pl")
            translation_pl = translator.translate(text[:4500])
            detected_lang = _detect_language(text, source_lang)
        except Exception as e:
            print(f"    [WARN] Translation failed for msg {msg['id']}: {e}")
            translation_pl = None
    else:
        detected_lang = "pl"

    # Auto-categorize if not already categorized
    category = conv.get("category")
    if not category:
        category = _auto_categorize(text, translation_pl)
        if category:
            db._patch("conversations", {"id": f"eq.{conv.get('id', msg['conversation_id'])}"}, {
                "category": category,
            })

    # Generate draft reply
    order_ref = ""
    order_id = conv.get("external_order_id")
    if order_id:
        order_ref = f" ({order_id})"

    template_key = category if category in DRAFT_TEMPLATES else "default"
    draft_pl = DRAFT_TEMPLATES[template_key].format(order_ref=order_ref)

    # Translate draft to buyer's language
    draft_local = draft_pl
    if source_lang != "pl":
        try:
            translator = GoogleTranslator(source="pl", target=source_lang)
            draft_local = translator.translate(draft_pl)
        except Exception:
            draft_local = draft_pl

    # Save to DB
    update = {
        "detected_language": detected_lang,
        "ai_processed_at": datetime.now(timezone.utc).isoformat(),
        "draft_reply": draft_pl,
        "draft_reply_local": draft_local,
    }
    if translation_pl:
        update["translation_pl"] = translation_pl

    db._patch("messages", {"id": f"eq.{msg['id']}"}, update)


def _detect_language(text, fallback="de"):
    """Detect language using langdetect (with word-matching fallback)."""
    try:
        from langdetect import detect
        lang = detect(text[:1000])
        # Map langdetect codes to our codes
        lang_map = {"de": "de", "fr": "fr", "it": "it", "es": "es",
                     "nl": "nl", "sv": "sv", "pl": "pl", "en": "en"}
        return lang_map.get(lang, fallback)
    except Exception:
        pass
    # Fallback: simple word matching
    text_lower = text.lower()[:500]
    scores = {
        "de": sum(1 for w in ["und", "die", "der", "das", "ist", "ich", "nicht", "ein", "mit", "auf"] if f" {w} " in f" {text_lower} "),
        "fr": sum(1 for w in ["et", "les", "des", "une", "est", "pas", "pour", "que", "dans", "avec"] if f" {w} " in f" {text_lower} "),
        "it": sum(1 for w in ["che", "non", "per", "una", "del", "con", "sono", "questo", "anche"] if f" {w} " in f" {text_lower} "),
        "es": sum(1 for w in ["que", "los", "las", "por", "una", "con", "para", "del", "pero"] if f" {w} " in f" {text_lower} "),
        "nl": sum(1 for w in ["het", "een", "van", "dat", "niet", "met", "maar", "voor", "zijn"] if f" {w} " in f" {text_lower} "),
        "sv": sum(1 for w in ["och", "att", "det", "som", "inte", "med", "har", "kan", "den"] if f" {w} " in f" {text_lower} "),
        "pl": sum(1 for w in ["nie", "jest", "sie", "jak", "tak", "ale", "czy", "tego", "aby"] if f" {w} " in f" {text_lower} "),
        "en": sum(1 for w in ["the", "and", "for", "that", "with", "this", "not", "but", "have"] if f" {w} " in f" {text_lower} "),
    }
    best = max(scores, key=scores.get)
    return best if scores[best] >= 2 else fallback


def _auto_categorize(text, translation=None):
    """Auto-categorize message based on keywords."""
    combined = f"{text} {translation or ''}".lower()
    scores = {}
    for cat, keywords in CATEGORY_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in combined)
        if score > 0:
            scores[cat] = score
    if scores:
        return max(scores, key=scores.get)
    return None


# ── AI analysis (Anthropic SDK primary, Claude CLI fallback) ──

def _get_anthropic_client():
    """Get Anthropic client if API key is available."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        # Try loading from file
        try:
            from . import config
            env = config._load_env_file(config.KEYS_DIR / "anthropic.env")
            api_key = env.get("ANTHROPIC_API_KEY", "")
        except Exception:
            pass
    if not api_key:
        # Try Streamlit secrets
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


def _check_daily_limit():
    """Check and enforce daily API call limit. Returns True if call is allowed."""
    global _daily_call_count, _daily_call_date
    today = datetime.now(timezone.utc).date()
    if _daily_call_date != today:
        _daily_call_count = 0
        _daily_call_date = today
    if _daily_call_count >= DAILY_API_CALL_LIMIT:
        return False
    _daily_call_count += 1
    return True


def _call_ai(prompt):
    """Call Claude via Anthropic SDK (primary) or CLI (fallback). Returns response text.
    Enforces daily call limit and prompt size cap."""
    # Enforce prompt size limit
    if len(prompt) > MAX_PROMPT_CHARS:
        prompt = prompt[:MAX_PROMPT_CHARS] + "\n[truncated]"

    # Check daily limit
    if not _check_daily_limit():
        print(f"    [RATE-LIMIT] Daily API limit reached ({DAILY_API_CALL_LIMIT} calls). Skipping until tomorrow.")
        return None

    # Try Anthropic SDK first
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

    # Fallback: Claude CLI (free, no limit)
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


def analyze_with_claude(batch_size=10):
    """Analyze unprocessed inbound messages using AI (SDK or CLI).
    Batch size is capped to BATCH_SIZE_CAP to spread API usage across cron runs."""
    effective_batch = min(batch_size, BATCH_SIZE_CAP)
    msgs = db._get("messages", {
        "ai_analysis": "is.null",
        "direction": "eq.inbound",
        "body_text": "not.is.null",
        "select": "id,conversation_id,body_text,email_subject,detected_language,translation_pl,ai_retry_count",
        "order": "sent_at.desc",
        "limit": str(effective_batch),
    })

    if not msgs:
        print("  [msg-ai-claude] No messages to analyze")
        return 0

    # Filter out messages that exceeded retry limit
    eligible = []
    for m in msgs:
        retries = m.get("ai_retry_count") or 0
        if retries >= MAX_AI_RETRIES:
            # Mark as permanently failed
            db._patch("messages", {"id": f"eq.{m['id']}"}, {
                "ai_analysis": f"[AUTO-SKIP] Exceeded {MAX_AI_RETRIES} retry attempts",
            })
            print(f"    Msg {m['id']}: skipped (exceeded {MAX_AI_RETRIES} retries)")
        else:
            eligible.append(m)

    if not eligible:
        print("  [msg-ai-claude] All pending messages exceeded retry limit")
        return 0

    print(f"  [msg-ai-claude] Analyzing {len(eligible)} messages with AI")

    analyzed = 0
    for m in eligible:
        try:
            _analyze_single(m)
            analyzed += 1
        except Exception as e:
            # Increment retry counter
            retries = (m.get("ai_retry_count") or 0) + 1
            db._patch("messages", {"id": f"eq.{m['id']}"}, {
                "ai_retry_count": retries,
            })
            print(f"    [WARN] AI analysis failed for msg {m['id']} (retry {retries}/{MAX_AI_RETRIES}): {e}")

    print(f"  [msg-ai-claude] Done: {analyzed}/{len(eligible)} analyzed")
    return analyzed


def _analyze_single(msg):
    """Analyze a single message using AI."""
    msg_id = msg["id"]
    conv_id = msg["conversation_id"]
    body = msg.get("body_text") or ""
    subject = msg.get("email_subject") or ""
    translation = msg.get("translation_pl") or ""
    detected_lang = msg.get("detected_language") or "de"

    body_clean = _clean_email_body(body)

    conv = db._get("conversations", {
        "id": f"eq.{conv_id}",
        "select": "source,platform,external_order_id,buyer_name,buyer_login,status",
        "limit": "1",
    })
    conv = conv[0] if conv else {}

    order_id = conv.get("external_order_id", "")
    order_ctx = _get_order_context(order_id) if order_id else "Brak powiazanego zamowienia."

    buyer = conv.get("buyer_name") or conv.get("buyer_login") or "?"
    platform = conv.get("platform", "?")

    msg_text = body_clean[:800]
    tl_clean = _clean_email_body(translation)[:500] if translation else ""

    prompt = f"""E-commerce customer service for "nesell" (Amazon/Allegro EU). Analyze and draft reply.

BUYER: {buyer} | PLATFORM: {platform} | ORDER: {order_id or "none"}
SUBJECT: {subject[:100]}
MESSAGE: {msg_text}
{f"PL TRANSLATION: {tl_clean}" if tl_clean else ""}
ORDER CONTEXT: {order_ctx[:500]}

Reply as JSON only:
{{"analiza":"2-3 sentences PL: what is the problem, what we know, what to do","kategoria":"shipping|return|damage|question|complaint|other","pilnosc":"low|normal|high|urgent","draft_pl":"full professional reply in Polish","draft_{detected_lang}":"same reply in {detected_lang}"}}"""

    output = _call_ai(prompt)
    if not output:
        raise RuntimeError("No AI backend available (no API key and no claude CLI in PATH)")

    # Parse JSON from output
    json_match = re.search(r'\{.*\}', output, re.DOTALL)
    if not json_match:
        raise ValueError(f"No JSON in AI output")
    data = json.loads(json_match.group())

    analysis = data.get("analiza", "")
    category = data.get("kategoria", "")
    urgency = data.get("pilnosc", "normal")
    draft_pl = data.get("draft_pl", "")
    draft_local = data.get(f"draft_{detected_lang}", "") or data.get("draft_de", "") or data.get("draft_fr", "") or data.get("draft_it", "") or data.get("draft_es", "") or data.get("draft_nl", "")

    if not draft_local:
        for k, v in data.items():
            if k.startswith("draft_") and k != "draft_pl" and v:
                draft_local = v
                break

    update = {"ai_analysis": analysis, "ai_retry_count": 0}
    if draft_pl:
        update["ai_draft_pl"] = draft_pl
    if draft_local:
        update["ai_draft_local"] = draft_local
    db._patch("messages", {"id": f"eq.{msg_id}"}, update)

    conv_update = {}
    if category and category in ("shipping", "return", "damage", "question", "complaint", "other"):
        conv_update["category"] = category
    if urgency and urgency in ("low", "normal", "high", "urgent"):
        conv_update["priority"] = urgency
    if conv_update:
        db._patch("conversations", {"id": f"eq.{conv_id}"}, conv_update)

    print(f"    Msg {msg_id}: {category}/{urgency} - {analysis[:60]}...")


def _clean_email_body(text):
    """Strip Amazon boilerplate for analysis."""
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
    """Build order context string for AI prompt."""
    if not order_id:
        return "Brak numeru zamowienia."

    order = None
    for field in ["platform_order_id", "external_id"]:
        rows = db._get("orders", {field: f"eq.{order_id}", "select": "*", "limit": "1"})
        if rows:
            order = rows[0]
            break

    if not order:
        return f"Zamowienie {order_id} NIE ZNALEZIONE w bazie danych (moze FBA Amazon lub sprzed rozpoczecia synca)."

    ctx_parts = [f"Zamowienie {order_id} ZNALEZIONE w bazie:"]
    ctx_parts.append(f"- Data zamowienia: {(order.get('order_date') or '')[:10]}")
    ctx_parts.append(f"- Status: {order.get('status', '?')}")
    ctx_parts.append(f"- Kraj wysylki: {order.get('shipping_country', '?')}")
    ctx_parts.append(f"- Metoda dostawy: {order.get('delivery_method', '?')}")

    internal_id = order.get("id")
    bl_id = str(order.get("external_id") or internal_id)

    items = db._get("order_items", {
        "order_id": f"eq.{internal_id}",
        "select": "sku,name,quantity",
    })
    if items:
        for it in items[:3]:
            ctx_parts.append(f"- Produkt: {it.get('quantity', 1)}x {it.get('name', it.get('sku', '?'))}")

    shipping = db._get("shipping_costs", {
        "order_id": f"eq.{bl_id}",
        "select": "tracking_number,courier,ship_date,cost_pln",
        "limit": "1",
    })
    if shipping:
        s = shipping[0]
        ctx_parts.append(f"- WYSYLKA: {s.get('ship_date','')} kurierem {s.get('courier','?')}, tracking: {s.get('tracking_number','brak')}")
    else:
        ctx_parts.append("- WYSYLKA: BRAK DANYCH (nie znaleziono w shipping_costs)")

    problems = db._get("shipping_problems", {
        "bl_order_id": f"eq.{bl_id}",
        "select": "problem_type,problem_detail,severity,status",
        "limit": "1",
    })
    if problems:
        p = problems[0]
        ctx_parts.append(f"- PROBLEM DOSTAWY: {p.get('problem_type','?')} - {p.get('problem_detail','?')} (severity: {p.get('severity','?')}, status: {p.get('status','?')})")

    return "\n".join(ctx_parts)
