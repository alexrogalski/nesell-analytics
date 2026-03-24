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
        "select": "id,direction,sender_name,body_text,email_subject,sent_at,is_read,attachments,translation_pl,detected_language,draft_reply,draft_reply_local,email_from,ai_analysis,ai_draft_pl,ai_draft_local",
        "order": "sent_at.asc",
    })
    return rows or []


@st.cache_data(ttl=120)
def load_order_context(order_id):
    """Look up order details, items, shipping, and problems."""
    if not order_id:
        return None

    ctx = {"order_id": order_id}

    # Find order (try both fields)
    order = None
    for field in ["platform_order_id", "external_id"]:
        rows = _get("orders", {field: f"eq.{order_id}", "select": "*", "limit": "1"})
        if rows:
            order = rows[0]
            break

    if not order:
        ctx["found"] = False
        return ctx

    ctx["found"] = True
    ctx["order_date"] = order.get("order_date", "")
    ctx["status"] = order.get("status", "")
    ctx["shipping_country"] = order.get("shipping_country", "")
    ctx["delivery_method"] = order.get("delivery_method", "")
    ctx["bl_order_id"] = order.get("external_id") or order.get("id")
    internal_id = order.get("id")

    # Order items
    items = _get("order_items", {
        "order_id": f"eq.{internal_id}",
        "select": "sku,name,quantity",
    })
    ctx["items"] = items

    # Shipping info
    bl_id = str(ctx["bl_order_id"])
    shipping = _get("shipping_costs", {
        "order_id": f"eq.{bl_id}",
        "select": "tracking_number,courier,destination_country,ship_date,cost_pln,cost_source",
        "limit": "1",
    })
    if not shipping:
        shipping = _get("shipping_costs", {
            "external_order_id": f"eq.{order_id}",
            "select": "tracking_number,courier,destination_country,ship_date,cost_pln,cost_source",
            "limit": "1",
        })
    ctx["shipping"] = shipping[0] if shipping else None

    # Shipping problems
    problems = _get("shipping_problems", {
        "external_order_id": f"eq.{order_id}",
        "select": "problem_type,problem_detail,severity,status,resolution,delivery_attempts",
        "limit": "1",
    })
    if not problems and bl_id:
        problems = _get("shipping_problems", {
            "bl_order_id": f"eq.{bl_id}",
            "select": "problem_type,problem_detail,severity,status,resolution,delivery_attempts",
            "limit": "1",
        })
    ctx["problem"] = problems[0] if problems else None

    return ctx


def _build_smart_draft(ctx, message_text, detected_lang):
    """Generate context-aware draft reply based on order data."""
    lang = (detected_lang or "de").lower()
    order_id = ctx.get("order_id", "")
    ref = f" ({order_id})" if order_id else ""

    if not ctx or not ctx.get("found"):
        # Order not in our system
        draft_pl = (
            f"Dzien dobry,\n\n"
            f"Dziekujemy za wiadomosc{ref}. "
            f"Nie znalezlismy tego zamowienia w naszym systemie, co moze oznaczac ze zostalo zrealizowane przez Amazon (FBA). "
            f"Prosimy o kontakt bezposrednio z Amazon w celu uzyskania informacji o statusie przesylki.\n\n"
            f"Pozdrawiamy,\nZespol nesell"
        )
        return draft_pl

    status = ctx.get("status", "")
    ship = ctx.get("shipping")
    problem = ctx.get("problem")
    items = ctx.get("items", [])
    order_date = ctx.get("order_date", "")[:10]

    item_names = ", ".join(i.get("name", i.get("sku", "?"))[:40] for i in items[:2]) if items else "?"

    if problem:
        # There's a known shipping problem
        p_type = problem.get("problem_type", "")
        p_detail = problem.get("problem_detail", "")
        attempts = problem.get("delivery_attempts", 0)
        p_status = problem.get("status", "")

        draft_pl = (
            f"Dzien dobry,\n\n"
            f"Dziekujemy za wiadomosc dot. zamowienia{ref}.\n\n"
            f"Sprawdzilismy status i widzimy problem z dostawa: {p_detail}. "
        )
        if attempts and attempts >= 2:
            draft_pl += f"Kurier podjal juz {attempts} prob dostawy. "
        if p_status == "open":
            draft_pl += "Aktywnie pracujemy nad rozwiazaniem tego problemu. "
        draft_pl += (
            f"\nProponujemy wyslanie zamowienia ponownie lub pelny zwrot. "
            f"Prosimy o informacje, ktora opcja jest preferowana.\n\n"
            f"Pozdrawiamy,\nZespol nesell"
        )
        return draft_pl

    if ship:
        tracking = ship.get("tracking_number", "")
        courier = ship.get("courier", "DPD").upper()
        ship_date = ship.get("ship_date", "")

        if "nie" in (message_text or "").lower() or "nicht" in (message_text or "").lower() or "not" in (message_text or "").lower() or "pas" in (message_text or "").lower():
            # Customer says not received but we have shipping info
            draft_pl = (
                f"Dzien dobry,\n\n"
                f"Dziekujemy za wiadomosc dot. zamowienia{ref}.\n\n"
                f"Zamowienie zostalo wyslane {ship_date} kurierem {courier}"
            )
            if tracking:
                draft_pl += f", numer przesylki: {tracking}"
            draft_pl += (
                f".\n\nSprawdzamy aktualny status przesylki u kuriera. "
                f"Jesli paczka nie dotrze w ciagu 3 dni roboczych, wysylamy zamowienie ponownie lub oferujemy pelny zwrot.\n\n"
                f"Pozdrawiamy,\nZespol nesell"
            )
            return draft_pl

        # General inquiry about shipped order
        draft_pl = (
            f"Dzien dobry,\n\n"
            f"Dziekujemy za wiadomosc dot. zamowienia{ref}.\n\n"
            f"Zamowienie zostalo wyslane {ship_date} kurierem {courier}"
        )
        if tracking:
            draft_pl += f" (tracking: {tracking})"
        draft_pl += f".\n\nCzy mozemy pomoc w czymkolwiek jeszcze?\n\nPozdrawiamy,\nZespol nesell"
        return draft_pl

    if status == "confirmed":
        # Order confirmed but not shipped yet
        draft_pl = (
            f"Dzien dobry,\n\n"
            f"Dziekujemy za wiadomosc dot. zamowienia{ref}.\n\n"
            f"Zamowienie z dnia {order_date} jest w trakcie realizacji i zostanie wyslane najszybciej jak to mozliwe. "
            f"Powiadomimy o numerze przesylki.\n\n"
            f"Pozdrawiamy,\nZespol nesell"
        )
        return draft_pl

    # Fallback
    draft_pl = (
        f"Dzien dobry,\n\n"
        f"Dziekujemy za wiadomosc dot. zamowienia{ref} (status: {status}, data: {order_date}).\n\n"
        f"Sprawdzamy sprawe i odpowiemy najszybciej jak to mozliwe.\n\n"
        f"Pozdrawiamy,\nZespol nesell"
    )
    return draft_pl


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


def _mark_closed(conv_id):
    """Mark conversation as closed in DB."""
    try:
        from etl import db
        db._patch("conversations", {"id": f"eq.{conv_id}"}, {
            "needs_reply": False,
            "status": "closed",
        })
    except Exception:
        pass


def _call_ai_from_dashboard(prompt):
    """Call AI from dashboard: try Anthropic SDK, then Claude CLI."""
    import os
    # Try multiple sources for API key
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        try:
            api_key = st.secrets.get("ANTHROPIC_API_KEY", "")
        except Exception:
            pass
    if not api_key:
        try:
            from etl import config
            env = config._load_env_file(config.KEYS_DIR / "anthropic.env")
            api_key = env.get("ANTHROPIC_API_KEY", "")
        except Exception:
            pass
    if not api_key:
        # Load from Supabase app_config table (works on Streamlit Cloud without extra secrets)
        try:
            import requests as _req
            from lib.data import SUPABASE_URL, HEADERS
            resp = _req.get(
                f"{SUPABASE_URL}/rest/v1/app_config",
                headers=HEADERS,
                params={"key": "eq.ANTHROPIC_API_KEY", "select": "value", "limit": "1"},
                timeout=5,
            )
            if resp.status_code == 200:
                rows = resp.json()
                if rows:
                    api_key = rows[0].get("value", "")
        except Exception:
            pass

    if api_key:
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)
            # Cap prompt to 2000 chars, output to 800 tokens (cost control)
            capped_prompt = prompt[:2000]
            response = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=800,
                messages=[{"role": "user", "content": capped_prompt}],
            )
            return response.content[0].text.strip()
        except Exception as e:
            st.warning(f"SDK error: {e}")

    # Fallback: Claude CLI (local only)
    import shutil
    if shutil.which("claude"):
        try:
            import subprocess
            result = subprocess.run(
                ["claude", "-p", prompt, "--model", "sonnet", "--max-turns", "1"],
                capture_output=True, text=True, timeout=30,
            )
            return result.stdout.strip()
        except Exception:
            pass

    return None


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
    # Tab filter above conversation list
    tab_options = ["Do odpowiedzi", "Wszystkie", "Odpowiedziane"]
    if "msg_tab" not in st.session_state:
        st.session_state.msg_tab = "Do odpowiedzi"
    tab = st.radio("Filtr", tab_options, index=tab_options.index(st.session_state.msg_tab),
                   key="msg_tab_radio", horizontal=True, label_visibility="collapsed")
    st.session_state.msg_tab = tab

    if tab == "Do odpowiedzi":
        list_df = filtered[filtered["needs_reply"] == True]
    elif tab == "Odpowiedziane":
        list_df = filtered[filtered["status"] == "replied"]
    else:
        list_df = filtered

    list_df = list_df.reset_index(drop=True)
    conv_ids = list_df["id"].tolist()

    if not conv_ids:
        st.html(f'<div style="font-size:12px; color:#64748b; font-family:monospace; padding:20px; text-align:center;">Brak konwersacji w tej zakladce</div>')
        st.stop()

    # Clean conversation labels
    labels = {}
    for _, row in list_df.iterrows():
        cid = row["id"]
        buyer = row.get("buyer_name") or row.get("buyer_login") or "?"
        if len(buyer) > 20:
            buyer = buyer[:18] + ".."
        plat = (row.get("platform") or "").replace("amazon_", "").upper()
        src = "ALG" if row.get("source") == "allegro" else "AMZ"
        nr = row.get("needs_reply", False)
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
    </div>''')

    # ── Order context panel ──
    order_ctx = load_order_context(order) if order else None
    if order_ctx:
        if order_ctx.get("found"):
            o_date = (order_ctx.get("order_date") or "")[:10]
            o_status = order_ctx.get("status", "?")
            o_country = order_ctx.get("shipping_country", "?")
            ship = order_ctx.get("shipping")
            problem = order_ctx.get("problem")
            items = order_ctx.get("items", [])

            items_html = ""
            for it in items[:3]:
                name = _esc((it.get("name") or it.get("sku", "?"))[:50])
                qty = it.get("quantity", 1)
                items_html += f'<div style="font-size:11px; color:#94a3b8;">{qty}x {name}</div>'

            ship_html = '<span style="color:#f59e0b;">Brak danych o wysylce</span>'
            if ship:
                t = ship.get("tracking_number", "")
                c = ship.get("courier", "?").upper()
                sd = ship.get("ship_date", "")
                ship_html = f'<span style="color:#10b981;">Wyslano {sd} | {c} {t}</span>'

            problem_html = ""
            if problem:
                p_type = problem.get("problem_type", "")
                p_detail = _esc(problem.get("problem_detail", ""))
                p_sev = problem.get("severity", "")
                sev_color = "#ef4444" if p_sev == "critical" else "#f59e0b"
                problem_html = f'<div style="margin-top:4px; padding:4px 8px; background:#ef444415; border-radius:4px; font-size:11px; color:{sev_color};">PROBLEM: {p_type} - {p_detail}</div>'

            st.html(f'''<div style="padding:8px 12px; background:#0f172a; border:1px solid #1e293b; border-radius:6px; margin-bottom:8px; font-family:monospace; font-size:11px;">
              <div style="display:flex; gap:16px; flex-wrap:wrap; align-items:center;">
                <span style="color:#64748b;">Zamowienie: <span style="color:#e2e8f0; font-weight:600;">{_esc(order)}</span></span>
                <span style="color:#64748b;">Data: <span style="color:#e2e8f0;">{o_date}</span></span>
                <span style="color:#64748b;">Status: <span style="color:#e2e8f0;">{o_status}</span></span>
                <span style="color:#64748b;">Kraj: <span style="color:#e2e8f0;">{o_country}</span></span>
              </div>
              <div style="margin-top:4px;">{ship_html}</div>
              {items_html}
              {problem_html}
            </div>''')
        else:
            st.html(f'''<div style="padding:6px 12px; background:#0f172a; border:1px solid #1e293b; border-radius:6px; margin-bottom:8px; font-family:monospace; font-size:11px; color:#f59e0b;">
              Zamowienie {_esc(order)} nie znalezione w bazie (moze FBA lub sprzed synca)
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

    # ── Buyer history (other conversations with same buyer) ──
    buyer_login = conv_row.get("buyer_login") or ""
    if buyer_login:
        other_convs = _get("conversations", {
            "buyer_login": f"eq.{buyer_login}",
            "id": f"neq.{selected}",
            "select": "id,external_order_id,status,last_message_at,category",
            "order": "last_message_at.desc",
            "limit": "5",
        })
        if other_convs:
            hist_items = " | ".join(
                f"{(c.get('external_order_id') or '?')[:15]} ({c.get('status','?')})"
                for c in other_convs
            )
            st.html(f'''<div style="padding:4px 10px; background:#0f172a; border:1px solid #1e293b; border-radius:4px; margin-bottom:6px; font-family:monospace; font-size:10px; color:#64748b;">
              Historia kupujacego: {_esc(hist_items)}
            </div>''')

    # ── Reply section ──
    if nr and last_inbound_draft:
        st.html('<div style="height:1px; background:#1e293b; margin:12px 0;"></div>')

        d_lang = (last_inbound_draft.get("detected_language") or "pl").upper()

        # AI analysis panel
        ai_analysis = last_inbound_draft.get("ai_analysis") or ""
        if ai_analysis and not ai_analysis.startswith("[AUTO-SKIP]"):
            st.html(f'''<div style="padding:8px 12px; background:#0c1222; border-left:3px solid #06b6d4; border-radius:0 6px 6px 0; margin-bottom:8px; font-family:monospace;">
              <div style="font-size:10px; font-weight:700; color:#06b6d4; margin-bottom:3px;">ANALIZA AI</div>
              <div style="font-size:12px; color:#e2e8f0; line-height:1.5;">{_esc(ai_analysis)}</div>
            </div>''')

        # Prefer AI drafts over template-based
        ai_draft_pl = last_inbound_draft.get("ai_draft_pl") or ""
        ai_draft_local = last_inbound_draft.get("ai_draft_local") or ""

        if ai_draft_pl:
            draft_pl = ai_draft_pl
            draft_local = ai_draft_local or ai_draft_pl
        else:
            last_body = _clean_body(last_inbound_draft.get("body_text") or "")
            draft_pl = _build_smart_draft(order_ctx, last_body, d_lang.lower()) if order_ctx else (last_inbound_draft.get("draft_reply") or "")
            draft_local = draft_pl
            if d_lang != "PL":
                try:
                    from deep_translator import GoogleTranslator
                    draft_local = GoogleTranslator(source="pl", target=d_lang.lower()).translate(draft_pl)
                except Exception:
                    draft_local = last_inbound_draft.get("draft_reply_local") or draft_pl

        if draft_pl:
            st.html(f'''<div style="padding:10px 14px; background:#1a1a2e; border-left:3px solid #8b5cf6; border-radius:0 6px 6px 0; margin-bottom:10px; font-family:monospace;">
              <div style="font-size:10px; font-weight:700; color:#8b5cf6; margin-bottom:4px;">PROPONOWANA ODPOWIEDZ (PL)</div>
              <div style="font-size:12px; color:#e2e8f0; line-height:1.6;">{_esc(draft_pl).replace(chr(10), "<br>")}</div>
            </div>''')

        # Reply state: content in _rc, widget versioned via _rv
        rc_key = f"_rc_{selected}"
        rv_key = f"_rv_{selected}"
        rpl_key = f"_rpl_{selected}"

        if rc_key not in st.session_state:
            st.session_state[rc_key] = draft_local
        if rv_key not in st.session_state:
            st.session_state[rv_key] = 0

        # Show regenerated PL if available
        regen_pl = st.session_state.get(rpl_key)
        if regen_pl:
            st.html(f'''<div style="padding:10px 14px; background:#1a2e1a; border-left:3px solid #10b981; border-radius:0 6px 6px 0; margin-bottom:10px; font-family:monospace;">
              <div style="font-size:10px; font-weight:700; color:#10b981; margin-bottom:4px;">NOWA ODPOWIEDZ (PL)</div>
              <div style="font-size:12px; color:#e2e8f0; line-height:1.6;">{_esc(regen_pl).replace(chr(10), "<br>")}</div>
            </div>''')

        st.html(f'<div style="font-size:11px; font-weight:600; color:#64748b; font-family:monospace; margin-bottom:2px;">Tresc do wyslania ({d_lang}):</div>')
        widget_key = f"reply_{selected}_v{st.session_state[rv_key]}"
        reply_text = st.text_area(
            f"reply_{d_lang}",
            value=st.session_state[rc_key],
            height=120,
            key=widget_key,
            label_visibility="collapsed",
        )
        st.session_state[rc_key] = reply_text

        custom_prompt = st.text_input(
            "Dodatkowa instrukcja (opcjonalna, np. 'bardziej empatycznie', 'zaproponuj wymiane')",
            key=f"custom_prompt_{selected}",
            placeholder="Opcjonalnie: dodatkowe wytyczne dla AI...",
        )

        # Amazon SMTP warning
        if source == "amazon_email":
            st.html('<div style="font-size:10px; color:#f59e0b; font-family:monospace; padding:2px 0;">Uwaga: odpowiedzi Amazon wysylane przez Gmail. Lepiej odpowiadac przez Seller Central.</div>')

        btn_col1, btn_col2, btn_col3, btn_col4 = st.columns([1, 1, 1, 1])
        with btn_col1:
            send_clicked = st.button("Wyslij", key=f"send_{selected}", type="primary")
        with btn_col2:
            regen_clicked = st.button("Regeneruj", key=f"regen_{selected}")
        with btn_col3:
            close_clicked = st.button("Zamknij", key=f"close_{selected}")

        # Regeneruj: takes text from reply box + optional instruction
        if regen_clicked:
            user_text = reply_text.strip()
            extra = custom_prompt.strip()

            if not user_text and not extra:
                st.warning("Wpisz tekst odpowiedzi lub instrukcje dla AI.")
            else:
                _regen_local = None
                _regen_pl = None
                with st.spinner("Sonnet generuje..."):
                    try:
                        body_clean = _clean_body(last_inbound_draft.get("body_text") or "")

                        # Determine seller's intended message
                        modified_textarea = user_text and user_text != draft_local
                        seller_says = user_text if modified_textarea else extra

                        regen_prompt = (
                            f"TASK: Translate the seller's message to {d_lang} and Polish.\n\n"
                            f"SELLER SAYS: \"{seller_says}\"\n\n"
                            f"CONTEXT (read-only, do NOT copy into reply):\n"
                            f"BUYER MESSAGE: {body_clean[:400]}\n"
                            f"PLATFORM: {platform} | ORDER: {order}\n"
                            f"{f'STYLE NOTE: {extra}' if modified_textarea and extra else ''}\n\n"
                            f"STRICT RULES:\n"
                            f"- Output ONLY what the seller said, translated to {d_lang} and Polish\n"
                            f"- Fix grammar, make it sound professional\n"
                            f"- Do NOT add ANY information the seller did not write\n"
                            f"- Do NOT add assessments, promises, opinions, or solutions from the context\n"
                            f"- No em dashes. No filler phrases\n"
                            f"- Add greeting (Dzien dobry / Guten Tag / etc.) and sign-off: Pozdrawiamy, Zespol nesell\n\n"
                            f"Reply as JSON only: {{\"draft_pl\":\"Polish version\",\"draft_{d_lang.lower()}\":\"{d_lang} version\"}}"
                        )

                        output = _call_ai_from_dashboard(regen_prompt)
                        if output:
                            import json
                            json_match = re.search(r'\{.*\}', output, re.DOTALL)
                            if json_match:
                                data = json.loads(json_match.group())
                                _regen_local = data.get(f"draft_{d_lang.lower()}", "")
                                if not _regen_local:
                                    for k, v in data.items():
                                        if k.startswith("draft_") and k != "draft_pl" and v:
                                            _regen_local = v
                                            break
                                _regen_pl = data.get("draft_pl", "")
                                if not _regen_local:
                                    st.warning("AI nie zwrocilo odpowiedzi w jezyku kupujacego.")
                            else:
                                st.warning("AI nie zwrocilo JSON. Output: " + output[:200])
                        else:
                            st.error("Brak AI backendu. Sprawdz ANTHROPIC_API_KEY.")
                    except Exception as e:
                        st.error(f"Blad: {e}")

                # Outside spinner and try/except: update content + bump version
                if _regen_local:
                    st.session_state[rc_key] = _regen_local
                    st.session_state[rv_key] = st.session_state.get(rv_key, 0) + 1
                    if _regen_pl:
                        st.session_state[rpl_key] = _regen_pl
                    st.rerun()

        # Close conversation
        if close_clicked:
            _mark_closed(selected)
            st.success("Konwersacja zamknieta")
            load_conversations.clear()
            load_messages_for_conv.clear()

        if send_clicked and reply_text.strip():
            with btn_col4:
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

    else:
        # Not needing reply: show close button if still open
        action_col1, action_col2, _ = st.columns([1, 1, 3])
        if status == "replied":
            with action_col1:
                if st.button("Zamknij", key=f"close2_{selected}"):
                    _mark_closed(selected)
                    st.success("Zamknieta")
                    load_conversations.clear()
        if not nr:
            st.html('<div style="text-align:center; padding:4px; font-size:11px; color:#10b981; font-family:monospace;">Odpowiedziano</div>')
