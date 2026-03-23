"""Auto-translate messages and generate draft replies for customer conversations."""
import re
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


def process_messages(batch_size=50):
    """Process unprocessed inbound messages: translate + generate draft replies."""
    # Get inbound messages without translations
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

    # Get conversation info for platform/language detection
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
            # Try to detect actual language
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
    """Simple language detection based on common words."""
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
