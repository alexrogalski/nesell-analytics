"""Messages - Inbox-style helpdesk for Allegro + Amazon."""
import re
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from lib.theme import setup_page, COLORS

setup_page("Messages")

from lib.data import _get


# ── Data loading ──

@st.cache_data(ttl=120)
def load_conversations(days=30):
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    rows = _get("conversations", {
        "select": "*",
        "last_message_at": f"gte.{cutoff}",
        "order": "last_message_at.desc",
    })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


@st.cache_data(ttl=60)
def load_messages_for_conv(conv_id):
    rows = _get("messages", {
        "conversation_id": f"eq.{conv_id}",
        "select": "id,direction,sender_name,body_text,email_subject,sent_at,is_read,attachments,translation_pl,detected_language,draft_reply,draft_reply_local,email_from",
        "order": "sent_at.asc",
    })
    return rows or []


# ── Helpers ──

def _time_ago(ts_str):
    if not ts_str:
        return ""
    try:
        ts = pd.to_datetime(ts_str, utc=True)
        delta = pd.Timestamp.now(tz="UTC") - ts
        if delta.days > 0:
            return f"{delta.days}d"
        hours = int(delta.total_seconds() / 3600)
        if hours > 0:
            return f"{hours}h"
        return f"{int(delta.total_seconds() / 60)}m"
    except Exception:
        return ""


def _esc(text):
    return (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _clean_body(text):
    """Strip Amazon email boilerplate, keep only the actual buyer message."""
    if not text:
        return ""

    # Normalize line endings
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    # Extract between Amazon delimiters: --- Message: --- ... --- End message ---
    m = re.search(
        r'-{3,}\s*Message:?\s*-{3,}\s*\n(.*?)\n\s*-{3,}\s*End message\s*-{3,}',
        text, re.DOTALL | re.IGNORECASE
    )
    if m:
        return m.group(1).strip()

    # For translated text, look for Polish equivalents
    m = re.search(
        r'-{3,}\s*Wiadomo[sś][cć]:?\s*-{3,}\s*\n(.*?)\n\s*-{3,}\s*(?:Koniec|Zakończ)',
        text, re.DOTALL | re.IGNORECASE
    )
    if m:
        return m.group(1).strip()

    # Fallback: strip all known boilerplate
    lines = text.split("\n")
    cleaned = []
    skip = False
    for line in lines:
        s = line.strip().lower()
        # Skip everything after "end message" delimiter
        if re.match(r'^-{3,}\s*end', s):
            skip = True
            continue
        if skip:
            continue
        # Skip header boilerplate
        if any(p in s for p in [
            "was this email helpful",
            "resolve case",
            "report questionable activity",
            "copyright", "© 20",
            "sellercentral.",
            "this message was sent to",
            "you have received a message",
            "otrzymałeś wiadomość",
            "otrzymales wiadomosc",
            "amazon.com/messaging/",
            "/gp/satisfaction/",
            "no-response-needed",
            "customattribute",
            "czy ten e-mail był pomocny",
            "czy ten email byl pomocny",
            "rozwiąż sprawę",
            "rozwiaz sprawe",
            "zgłoś wątpliwą aktywność",
            "zglos watpliwa aktywnosc",
        ]):
            continue
        if re.match(r'^https?://', s):
            continue
        if re.match(r'^-{3,}\s*(message|wiadomo)', s):
            continue
        # Skip order/product header line (# 123-456-789:)
        if re.match(r'^#\s*\d{3}-\d{7}-\d{7}', s):
            continue
        # Skip ASIN product lines
        if re.match(r'^\d+\s*/\s+.*\[asin:', s):
            continue
        cleaned.append(line)

    result = "\n".join(cleaned).strip()
    result = re.sub(r'^[\s\-]+\n', '', result)
    result = re.sub(r'\n[\s\-]+$', '', result)
    return result


def _send_allegro_reply(thread_id, text):
    """Send reply via Allegro Messaging API."""
    try:
        from etl.allegro_fees import _load_allegro_token, _headers, BASE
        import requests
        token = _load_allegro_token()
        h = _headers(token)
        h["Content-Type"] = "application/vnd.allegro.public.v1+json"
        resp = requests.post(
            f"{BASE}/messaging/threads/{thread_id}/messages",
            headers=h,
            json={"text": text},
        )
        return resp.status_code in (200, 201), resp.text[:200]
    except Exception as e:
        return False, str(e)


def _send_amazon_reply(to_addr, subject, text, order_id=""):
    """Send reply via Gmail SMTP to Amazon buyer."""
    try:
        import smtplib
        from email.mime.text import MIMEText
        from etl import config

        creds = config._load_env_file(config.KEYS_DIR / "nesell-support-gmail.env")
        user = creds.get("NESELL_SUPPORT_IMAP_USER", "")
        password = creds.get("NESELL_SUPPORT_IMAP_PASSWORD", "")
        from_addr = creds.get("NESELL_SUPPORT_EMAIL", "support@nesell.co")

        if not user or not password:
            return False, "No SMTP credentials configured"

        reply_subject = subject if subject.lower().startswith("re:") else f"Re: {subject}"

        msg = MIMEText(text, "plain", "utf-8")
        msg["From"] = f"nesell support <{from_addr}>"
        msg["To"] = to_addr
        msg["Subject"] = reply_subject

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(user, password)
            server.send_message(msg)

        return True, "Sent"
    except Exception as e:
        return False, str(e)


def _mark_replied(conv_id):
    """Mark conversation as replied in DB."""
    try:
        from etl import db
        db._patch("conversations", {"id": f"eq.{conv_id}"}, {
            "needs_reply": False,
            "status": "replied",
            "last_message_direction": "outbound",
        })
    except Exception:
        pass


# ── Sidebar ──

period_map = {7: "7D", 14: "14D", 30: "30D", 90: "90D"}
days = st.sidebar.selectbox("Period", list(period_map.keys()), index=2,
                            format_func=lambda x: period_map[x], key="msg_period")
df = load_conversations(days=days)

if df.empty:
    st.warning("No conversations. Run: python3.11 -m etl.run --messages")
    st.stop()

all_sources = sorted(df["source"].dropna().unique())
selected_sources = st.sidebar.multiselect("Source", all_sources, default=all_sources, key="msg_src")

status_opts = ["all", "open", "replied", "closed", "escalated"]
selected_status = st.sidebar.selectbox("Status", status_opts, key="msg_status")
needs_reply_only = st.sidebar.checkbox("Needs reply only", key="msg_nr")
search_q = st.sidebar.text_input("Search", key="msg_q")

filtered = df[df["source"].isin(selected_sources)].copy()
if selected_status != "all":
    filtered = filtered[filtered["status"] == selected_status]
if needs_reply_only:
    filtered = filtered[filtered["needs_reply"] == True]
if search_q:
    q = search_q.lower()
    filtered = filtered[
        filtered["buyer_name"].astype(str).str.lower().str.contains(q, na=False)
        | filtered["buyer_login"].astype(str).str.lower().str.contains(q, na=False)
        | filtered["external_order_id"].astype(str).str.lower().str.contains(q, na=False)
    ]
filtered = filtered.sort_values(["needs_reply", "last_message_at"], ascending=[False, False]).reset_index(drop=True)

# ── Header ──
needs_reply_count = int(filtered["needs_reply"].sum()) if "needs_reply" in filtered.columns else 0
nr_color = "#ef4444" if needs_reply_count > 0 else "#10b981"
st.html(f'''<div style="display:flex; gap:20px; align-items:baseline; padding:6px 0 10px; font-family:monospace; border-bottom:1px solid #1e293b; margin-bottom:8px;">
  <span style="font-size:17px; font-weight:700; color:#e2e8f0;">WIADOMOSCI</span>
  <span style="font-size:12px; color:#64748b;">{len(filtered)} konwersacji</span>
  <span style="font-size:12px; font-weight:700; color:{nr_color};">{needs_reply_count} do odpowiedzi</span>
</div>''')

# ============================================================
# TWO-PANEL LAYOUT
# ============================================================
list_col, thread_col = st.columns([2, 5], gap="medium")

with list_col:
    conv_ids = filtered["id"].tolist()
    if not conv_ids:
        st.info("Brak konwersacji.")
        st.stop()

    # Clean conversation labels
    labels = {}
    for _, row in filtered.iterrows():
        cid = row["id"]
        buyer = row.get("buyer_name") or row.get("buyer_login") or "?"
        if len(buyer) > 20:
            buyer = buyer[:18] + ".."
        plat = (row.get("platform") or "").replace("amazon_", "").upper()
        src = "ALG" if row.get("source") == "allegro" else "AMZ"
        nr = row.get("needs_reply", False)
        status = row.get("status", "")
        time = _time_ago(row.get("last_message_at"))
        dot = "● " if nr else "   "

        labels[cid] = f"{dot}{buyer}  ·  {src} {plat}  ·  {time}"

    default_idx = 0
    if "sel_conv" in st.session_state and st.session_state.sel_conv in conv_ids:
        default_idx = conv_ids.index(st.session_state.sel_conv)

    selected = st.radio(
        "conv", conv_ids, index=default_idx,
        format_func=lambda x: labels.get(x, "?"),
        key="conv_r", label_visibility="collapsed",
    )
    st.session_state.sel_conv = selected


with thread_col:
    if not selected:
        st.stop()

    conv_row = filtered[filtered["id"] == selected]
    if conv_row.empty:
        st.stop()
    conv_row = conv_row.iloc[0]

    buyer = conv_row.get("buyer_name") or conv_row.get("buyer_login") or "?"
    order = conv_row.get("external_order_id") or ""
    platform = (conv_row.get("platform") or "").upper().replace("_", " ")
    status = conv_row.get("status", "open")
    cat = conv_row.get("category") or ""
    nr = conv_row.get("needs_reply", False)
    source = conv_row.get("source", "")
    thread_id = conv_row.get("source_thread_id", "")

    s_colors = {"open": "#f59e0b", "replied": "#10b981", "closed": "#64748b", "escalated": "#ef4444"}
    s_col = s_colors.get(status, "#64748b")
    nr_html = '<span style="background:#ef444420; color:#ef4444; padding:2px 8px; border-radius:4px; font-size:11px; font-weight:600;">DO ODPOWIEDZI</span>' if nr else ""
    cat_html = f'<span style="background:#334155; color:#94a3b8; padding:2px 6px; border-radius:3px; font-size:10px;">{_esc(cat)}</span>' if cat else ""

    # Conversation header
    st.html(f'''<div style="padding:10px 14px; background:#111827; border:1px solid #1e293b; border-radius:8px; margin-bottom:10px; font-family:monospace;">
      <div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap;">
        <span style="font-size:15px; font-weight:700; color:#e2e8f0;">{_esc(buyer)}</span>
        <span style="font-size:11px; color:#64748b;">{_esc(platform)}</span>
        <span style="background:{s_col}20; color:{s_col}; padding:2px 8px; border-radius:4px; font-size:11px; font-weight:600;">{_esc(status)}</span>
        {nr_html} {cat_html}
      </div>
      <div style="font-size:11px; color:#64748b; margin-top:3px;">Zamowienie: {_esc(order) if order else "brak"}</div>
    </div>''')

    # ── Messages ──
    messages = load_messages_for_conv(selected)
    last_inbound_draft = None
    last_email_subject = ""
    last_email_from = ""

    if messages:
        for m in messages:
            direction = m.get("direction", "")
            is_inbound = direction == "inbound"
            sender = m.get("sender_name") or ("Kupujacy" if is_inbound else "Ty")
            body_raw = m.get("body_text") or ""
            body = _clean_body(body_raw)
            subject = m.get("email_subject") or ""
            sent = m.get("sent_at") or ""
            translation_pl = m.get("translation_pl") or ""
            detected_lang = m.get("detected_language") or ""
            time_display = _time_ago(sent)

            if is_inbound:
                last_email_subject = subject
                last_email_from = m.get("email_from") or ""

            border_color = "#ef4444" if is_inbound else "#3b82f6"
            bg = "#1e293b" if is_inbound else "#131d2e"
            label_color = "#ef4444" if is_inbound else "#3b82f6"
            dir_label = "KUPUJACY" if is_inbound else "TY"

            # Decide what to show as main text
            is_foreign = detected_lang and detected_lang != "pl" and is_inbound
            if is_foreign and translation_pl:
                # Show Polish translation as main text
                main_text = _esc(_clean_body(translation_pl)).replace("\n", "<br>")
                lang_note = f'<span style="font-size:10px; color:#8b5cf6; margin-left:8px;">tlumaczenie z {detected_lang.upper()}</span>'
            else:
                main_text = _esc(body).replace("\n", "<br>")
                lang_note = ""

            subject_block = f'<div style="font-size:11px; color:#06b6d4; margin-bottom:4px; font-weight:600;">{_esc(subject)}</div>' if subject else ""

            st.html(f'''<div style="margin-bottom:5px; font-family:monospace;">
              <div style="background:{bg}; border-left:3px solid {border_color}; border-radius:0 6px 6px 0; padding:10px 14px;">
                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:4px;">
                  <span style="font-size:11px; font-weight:700; color:{label_color};">{dir_label}{lang_note}</span>
                  <span style="font-size:10px; color:#64748b;">{_esc(sender)} · {time_display}</span>
                </div>
                {subject_block}
                <div style="font-size:12px; color:#e2e8f0; line-height:1.6; word-break:break-word;">{main_text if main_text.strip() else "<span style='color:#475569;'>Brak tresci</span>"}</div>
              </div>
            </div>''')

            # Show original in expander (only for foreign inbound with translation)
            if is_foreign and translation_pl:
                with st.expander(f"Pokaz oryginal ({detected_lang.upper()})"):
                    st.text(_clean_body(body_raw))

            # Track last inbound draft for reply
            if is_inbound and m.get("draft_reply"):
                last_inbound_draft = m

    # ── Reply section ──
    if nr and last_inbound_draft:
        st.html('<div style="height:1px; background:#1e293b; margin:12px 0;"></div>')

        draft_pl = last_inbound_draft.get("draft_reply") or ""
        draft_local = last_inbound_draft.get("draft_reply_local") or draft_pl
        d_lang = (last_inbound_draft.get("detected_language") or "pl").upper()

        # Show draft in Polish (always visible)
        if draft_pl:
            st.html(f'''<div style="padding:10px 14px; background:#1a1a2e; border-left:3px solid #8b5cf6; border-radius:0 6px 6px 0; margin-bottom:10px; font-family:monospace;">
              <div style="font-size:10px; font-weight:700; color:#8b5cf6; margin-bottom:4px;">PROPONOWANA ODPOWIEDZ (PL)</div>
              <div style="font-size:12px; color:#e2e8f0; line-height:1.6;">{_esc(draft_pl).replace(chr(10), "<br>")}</div>
            </div>''')

        # Editable text area in buyer's language (this gets sent)
        st.html(f'<div style="font-size:11px; font-weight:600; color:#64748b; font-family:monospace; margin-bottom:2px;">Tresc do wyslania ({d_lang}):</div>')
        reply_text = st.text_area(
            f"reply_{d_lang}",
            value=draft_local,
            height=120,
            key=f"reply_{selected}",
            label_visibility="collapsed",
        )

        send_col, status_col = st.columns([1, 3])
        with send_col:
            send_clicked = st.button("Wyslij", key=f"send_{selected}", type="primary")

        if send_clicked and reply_text.strip():
            with status_col:
                with st.spinner("Wysylanie..."):
                    if source == "allegro":
                        ok, msg = _send_allegro_reply(thread_id, reply_text)
                    elif source == "amazon_email":
                        ok, msg = _send_amazon_reply(
                            last_email_from, last_email_subject,
                            reply_text, order
                        )
                    else:
                        ok, msg = False, f"Unknown source: {source}"

                    if ok:
                        _mark_replied(selected)
                        st.success("Wyslano!")
                        load_conversations.clear()
                        load_messages_for_conv.clear()
                    else:
                        st.error(f"Blad: {msg}")

    elif not nr:
        st.html('<div style="text-align:center; padding:8px; font-size:11px; color:#10b981; font-family:monospace;">Odpowiedziano</div>')
