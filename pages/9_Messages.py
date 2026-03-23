"""Messages - Inbox-style customer conversation hub."""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from lib.theme import setup_page, COLORS

setup_page("Messages")

# --- Data loading ---
from lib.data import _get


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
        "select": "direction,sender_name,body_text,email_subject,sent_at,is_read,attachments,translation_pl,detected_language,draft_reply,draft_reply_local",
        "order": "sent_at.asc",
    })
    return rows or []


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
    """Escape HTML entities."""
    return (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# --- Sidebar filters ---
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

# Apply filters
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

# Sort: needs_reply first, then by last_message_at
filtered = filtered.sort_values(
    ["needs_reply", "last_message_at"], ascending=[False, False]
).reset_index(drop=True)

# KPI bar (compact one-liner)
needs_reply_count = int(filtered["needs_reply"].sum()) if "needs_reply" in filtered.columns else 0
nr_color = "#ef4444" if needs_reply_count > 0 else "#10b981"
st.html(f'''<div style="display:flex; gap:24px; align-items:baseline; padding:8px 0 12px; font-family:monospace; border-bottom:1px solid #1e293b; margin-bottom:12px;">
  <span style="font-size:18px; font-weight:700; color:#e2e8f0;">MESSAGE CENTER</span>
  <span style="font-size:13px; color:#64748b;">{len(filtered)} conversations</span>
  <span style="font-size:13px; font-weight:700; color:{nr_color};">{needs_reply_count} needs reply</span>
  <span style="font-size:13px; color:#64748b;">{period_map.get(days,"")} period</span>
</div>''')

# ============================================================
# TWO-PANEL INBOX LAYOUT
# ============================================================
list_col, thread_col = st.columns([1, 2], gap="medium")

# --- LEFT PANEL: Conversation list ---
with list_col:
    # Build radio options
    conv_ids = filtered["id"].tolist()
    conv_labels = {}
    for _, row in filtered.iterrows():
        cid = row["id"]
        buyer = row.get("buyer_name") or row.get("buyer_login") or "?"
        if len(buyer) > 18:
            buyer = buyer[:16] + ".."
        order = row.get("external_order_id") or ""
        if len(order) > 15:
            order = order[-12:]
        src = "ALG" if row.get("source") == "allegro" else ("ISS" if row.get("source") == "allegro_issue" else "AMZ")
        platform = (row.get("platform") or "").replace("amazon_", "").replace("allegro", "alg").upper()
        status = row.get("status", "")
        nr = row.get("needs_reply", False)
        cat = row.get("category") or ""
        time = _time_ago(row.get("last_message_at"))
        msgs = int(row.get("message_count") or 0)

        dot = "● " if nr else "  "
        cat_tag = f" [{cat}]" if cat else ""
        conv_labels[cid] = f"{dot}{src} {platform} | {buyer}\n   {order}  {status}{cat_tag}  {msgs}msg  {time}"

    if not conv_ids:
        st.info("No conversations match filters.")
        st.stop()

    # Default to first conversation or session state
    default_idx = 0
    if "selected_conv" in st.session_state and st.session_state.selected_conv in conv_ids:
        default_idx = conv_ids.index(st.session_state.selected_conv)

    selected = st.radio(
        "Conversations",
        conv_ids,
        index=default_idx,
        format_func=lambda x: conv_labels.get(x, str(x)),
        key="conv_radio",
        label_visibility="collapsed",
    )
    st.session_state.selected_conv = selected

# --- RIGHT PANEL: Message thread + draft ---
with thread_col:
    if selected:
        # Conversation header
        conv_row = filtered[filtered["id"] == selected].iloc[0] if selected in filtered["id"].values else None
        if conv_row is not None:
            buyer = conv_row.get("buyer_name") or conv_row.get("buyer_login") or "?"
            order = conv_row.get("external_order_id") or ""
            platform = (conv_row.get("platform") or "").upper().replace("_", " ")
            status = conv_row.get("status", "open")
            cat = conv_row.get("category") or ""
            nr = conv_row.get("needs_reply", False)

            status_colors = {"open": "#f59e0b", "replied": "#10b981", "closed": "#64748b", "escalated": "#ef4444"}
            s_color = status_colors.get(status, "#64748b")
            nr_badge = '<span style="background:#ef444420; color:#ef4444; padding:2px 8px; border-radius:4px; font-size:11px; font-weight:600; margin-left:8px;">NEEDS REPLY</span>' if nr else ""
            cat_badge = f'<span style="background:#334155; color:#94a3b8; padding:2px 6px; border-radius:3px; font-size:10px; margin-left:6px;">{_esc(cat)}</span>' if cat else ""

            st.html(f'''<div style="padding:10px 14px; background:#111827; border:1px solid #1e293b; border-radius:8px; margin-bottom:12px; font-family:monospace;">
              <div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap;">
                <span style="font-size:15px; font-weight:700; color:#e2e8f0;">{_esc(buyer)}</span>
                <span style="font-size:12px; color:#64748b;">{_esc(platform)}</span>
                <span style="background:{s_color}20; color:{s_color}; padding:2px 8px; border-radius:4px; font-size:11px; font-weight:600;">{_esc(status)}</span>
                {nr_badge}{cat_badge}
              </div>
              <div style="font-size:11px; color:#64748b; margin-top:4px;">Order: {_esc(order) if order else "no order linked"}</div>
            </div>''')

        # Messages
        messages = load_messages_for_conv(selected)
        if messages:
            for m in messages:
                direction = m.get("direction", "")
                is_inbound = direction == "inbound"
                sender = m.get("sender_name") or ("Buyer" if is_inbound else "You")
                body = m.get("body_text") or ""
                subject = m.get("email_subject") or ""
                sent = m.get("sent_at") or ""
                translation_pl = m.get("translation_pl") or ""
                detected_lang = m.get("detected_language") or ""

                time_display = _time_ago(sent)
                border_color = "#ef4444" if is_inbound else "#3b82f6"
                bg = "#1e293b" if is_inbound else "#131d2e"
                label_color = "#ef4444" if is_inbound else "#3b82f6"
                dir_label = "BUYER" if is_inbound else "YOU"
                lang_tag = f' <span style="background:#334155; color:#94a3b8; padding:1px 5px; border-radius:3px; font-size:9px;">{detected_lang.upper()}</span>' if detected_lang and detected_lang != "pl" else ""

                body_safe = _esc(body[:3000]).replace("\n", "<br>")
                subject_block = f'<div style="font-size:11px; color:#06b6d4; margin-bottom:4px; font-weight:600;">{_esc(subject)}</div>' if subject else ""

                # Translation
                tl_block = ""
                if translation_pl and is_inbound and detected_lang != "pl":
                    tl_safe = _esc(translation_pl[:2000]).replace("\n", "<br>")
                    tl_block = f'''<div style="margin-top:8px; padding:8px 10px; background:#0f172a; border-left:2px solid #8b5cf6; border-radius:0 4px 4px 0;">
                      <div style="font-size:10px; font-weight:700; color:#8b5cf6; margin-bottom:2px;">PO POLSKU</div>
                      <div style="font-size:12px; color:#cbd5e1; line-height:1.5;">{tl_safe}</div>
                    </div>'''

                st.html(f'''<div style="margin-bottom:6px; font-family:monospace;">
                  <div style="background:{bg}; border-left:3px solid {border_color}; border-radius:0 6px 6px 0; padding:10px 14px;">
                    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:4px;">
                      <span style="font-size:11px; font-weight:700; color:{label_color};">{dir_label}{lang_tag}</span>
                      <span style="font-size:10px; color:#64748b;">{_esc(sender)} · {time_display}</span>
                    </div>
                    {subject_block}
                    <div style="font-size:12px; color:#e2e8f0; line-height:1.6; word-break:break-word;">{body_safe if body_safe else "<span style='color:#475569;'>No text content</span>"}</div>
                    {tl_block}
                  </div>
                </div>''')

            # Draft reply panel (for last inbound message)
            last_inbound = None
            for m in reversed(messages):
                if m.get("direction") == "inbound" and m.get("draft_reply"):
                    last_inbound = m
                    break

            if last_inbound:
                draft_pl = _esc(last_inbound.get("draft_reply") or "").replace("\n", "<br>")
                draft_local = _esc(last_inbound.get("draft_reply_local") or "").replace("\n", "<br>")
                d_lang = (last_inbound.get("detected_language") or "?").upper()

                st.html(f'''<div style="margin-top:12px; padding:14px; background:#1a1a2e; border:1px solid rgba(139,92,246,0.3); border-radius:8px; font-family:monospace;">
                  <div style="font-size:12px; font-weight:700; color:#8b5cf6; margin-bottom:10px;">DRAFT ODPOWIEDZI</div>
                  <div style="display:grid; grid-template-columns:1fr 1fr; gap:12px;">
                    <div>
                      <div style="font-size:10px; color:#64748b; margin-bottom:4px;">PL (dla Ciebie)</div>
                      <div style="font-size:12px; color:#e2e8f0; line-height:1.5; background:#111827; padding:10px; border-radius:6px;">{draft_pl}</div>
                    </div>
                    <div>
                      <div style="font-size:10px; color:#64748b; margin-bottom:4px;">{d_lang} (do wyslania kupujacemu)</div>
                      <div style="font-size:12px; color:#e2e8f0; line-height:1.5; background:#111827; padding:10px; border-radius:6px;">{draft_local}</div>
                    </div>
                  </div>
                </div>''')
        else:
            st.info("No messages in this conversation.")
