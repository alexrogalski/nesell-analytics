"""Messages - Customer conversation hub (Allegro + Amazon)."""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from lib.theme import setup_page, COLORS

setup_page("Messages")

st.markdown('<div class="section-header">MESSAGE CENTER</div>', unsafe_allow_html=True)

# --- Data loading (inline, no lib dependency) ---
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


@st.cache_data(ttl=120)
def load_messages_for_conv(conv_id):
    rows = _get("messages", {
        "conversation_id": f"eq.{conv_id}",
        "select": "direction,sender_name,body_text,email_subject,sent_at,is_read,attachments",
        "order": "sent_at.asc",
    })
    return rows or []


# --- Sidebar filters ---
st.sidebar.markdown('<div class="section-header">FILTERS</div>', unsafe_allow_html=True)

period_map = {7: "7D", 14: "14D", 30: "30D", 90: "90D"}
days = st.sidebar.selectbox("PERIOD", list(period_map.keys()), index=2,
                            format_func=lambda x: period_map[x], key="msg_period")

df = load_conversations(days=days)

if df.empty:
    st.warning("No conversations found. Run: python3.11 -m etl.run --messages")
    st.stop()

# Source filter
all_sources = sorted(df["source"].dropna().unique())
selected_sources = st.sidebar.multiselect("SOURCE", all_sources, default=all_sources, key="msg_src")

# Platform filter
all_platforms = sorted(df["platform"].dropna().unique())
selected_platforms = st.sidebar.multiselect("PLATFORM", all_platforms, default=all_platforms, key="msg_plat")

# Status filter
status_opts = ["all", "open", "replied", "closed", "escalated"]
selected_status = st.sidebar.selectbox("STATUS", status_opts, key="msg_status")

# Needs reply filter
needs_reply_only = st.sidebar.checkbox("Needs reply only", value=False, key="msg_needs_reply")

# Search
search_q = st.sidebar.text_input("SEARCH (buyer, order ID)", key="msg_search")

# --- Apply filters ---
filtered = df.copy()
filtered = filtered[filtered["source"].isin(selected_sources)]
filtered = filtered[filtered["platform"].isin(selected_platforms)]
if selected_status != "all":
    filtered = filtered[filtered["status"] == selected_status]
if needs_reply_only:
    filtered = filtered[filtered["needs_reply"] == True]
if search_q:
    q = search_q.lower()
    mask = (
        filtered["buyer_name"].astype(str).str.lower().str.contains(q, na=False)
        | filtered["buyer_login"].astype(str).str.lower().str.contains(q, na=False)
        | filtered["external_order_id"].astype(str).str.lower().str.contains(q, na=False)
    )
    filtered = filtered[mask]

# ============================================================
# KPI STRIP
# ============================================================
total_convs = len(filtered)
needs_reply_count = int(filtered["needs_reply"].sum()) if "needs_reply" in filtered.columns else 0
open_count = len(filtered[filtered["status"] == "open"]) if "status" in filtered.columns else 0
escalated_count = len(filtered[filtered["status"] == "escalated"]) if "status" in filtered.columns else 0
replied_count = len(filtered[filtered["status"] == "replied"]) if "status" in filtered.columns else 0

# Avg response time
avg_resp = None
if "first_response_minutes" in filtered.columns:
    resp_vals = filtered["first_response_minutes"].dropna()
    if len(resp_vals) > 0:
        avg_resp = resp_vals.mean()

# Source breakdown
allegro_count = len(filtered[filtered["source"] == "allegro"])
amazon_count = len(filtered[filtered["source"] == "amazon_email"])
issue_count = len(filtered[filtered["source"] == "allegro_issue"])

reply_color = COLORS["danger"] if needs_reply_count > 0 else COLORS["success"]
resp_display = f"{avg_resp:.0f} min" if avg_resp is not None else "N/A"

kpi_html = f'''
<div style="display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:10px; margin-bottom:20px;">
  <div style="background:{COLORS['card']}; border:1px solid {COLORS['border']}; border-radius:8px; padding:14px; text-align:center;">
    <div style="font-size:0.65rem; color:{COLORS['muted']}; text-transform:uppercase; letter-spacing:0.5px;">Conversations</div>
    <div style="font-size:1.5rem; font-weight:700; color:{COLORS['text']};">{total_convs}</div>
    <div style="font-size:0.6rem; color:{COLORS['muted']};">{period_map.get(days,"")} period</div>
  </div>
  <div style="background:{COLORS['card']}; border:1px solid {reply_color}40; border-radius:8px; padding:14px; text-align:center;">
    <div style="font-size:0.65rem; color:{COLORS['muted']}; text-transform:uppercase; letter-spacing:0.5px;">Needs Reply</div>
    <div style="font-size:1.5rem; font-weight:700; color:{reply_color};">{needs_reply_count}</div>
    <div style="font-size:0.6rem; color:{COLORS['muted']};">action required</div>
  </div>
  <div style="background:{COLORS['card']}; border:1px solid {COLORS['border']}; border-radius:8px; padding:14px; text-align:center;">
    <div style="font-size:0.65rem; color:{COLORS['muted']}; text-transform:uppercase; letter-spacing:0.5px;">Avg Response</div>
    <div style="font-size:1.5rem; font-weight:700; color:{COLORS['info']};">{resp_display}</div>
    <div style="font-size:0.6rem; color:{COLORS['muted']};">first reply</div>
  </div>
  <div style="background:{COLORS['card']}; border:1px solid {COLORS['border']}; border-radius:8px; padding:14px; text-align:center;">
    <div style="font-size:0.65rem; color:{COLORS['muted']}; text-transform:uppercase; letter-spacing:0.5px;">Open / Escalated</div>
    <div style="font-size:1.5rem; font-weight:700; color:{COLORS['warning']};">{open_count} / {escalated_count}</div>
    <div style="font-size:0.6rem; color:{COLORS['muted']};">{replied_count} replied</div>
  </div>
  <div style="background:{COLORS['card']}; border:1px solid {COLORS['border']}; border-radius:8px; padding:14px; text-align:center;">
    <div style="font-size:0.65rem; color:{COLORS['muted']}; text-transform:uppercase; letter-spacing:0.5px;">Sources</div>
    <div style="font-size:0.85rem; font-weight:600; color:{COLORS['text']};">
      <span style="color:#3b82f6;">Allegro {allegro_count}</span> &middot;
      <span style="color:#f59e0b;">Amazon {amazon_count}</span>
      {"&middot; <span style='color:#ef4444;'>Issues " + str(issue_count) + "</span>" if issue_count else ""}
    </div>
  </div>
</div>
'''
st.markdown(kpi_html, unsafe_allow_html=True)

# ============================================================
# CONVERSATIONS TABLE
# ============================================================
st.markdown('<div class="section-header">CONVERSATIONS</div>', unsafe_allow_html=True)


def _status_badge(status):
    colors = {
        "open": ("#f59e0b", "#451a03"),
        "replied": ("#10b981", "#064e3b"),
        "closed": ("#64748b", "#1e293b"),
        "escalated": ("#ef4444", "#450a0a"),
    }
    bg, _ = colors.get(status, ("#64748b", "#1e293b"))
    return f'<span style="background:{bg}20; color:{bg}; padding:2px 8px; border-radius:4px; font-size:0.65rem; font-weight:600;">{status}</span>'


def _source_badge(source):
    if source == "allegro":
        return '<span style="color:#3b82f6; font-weight:600;">ALG</span>'
    elif source == "amazon_email":
        return '<span style="color:#f59e0b; font-weight:600;">AMZ</span>'
    elif source == "allegro_issue":
        return '<span style="color:#ef4444; font-weight:600;">ISSUE</span>'
    return f'<span style="color:{COLORS["muted"]};">{source}</span>'


def _time_ago(ts_str):
    if not ts_str:
        return ""
    try:
        ts = pd.to_datetime(ts_str, utc=True)
        now = pd.Timestamp.now(tz="UTC")
        delta = now - ts
        if delta.days > 0:
            return f"{delta.days}d ago"
        hours = int(delta.total_seconds() / 3600)
        if hours > 0:
            return f"{hours}h ago"
        mins = int(delta.total_seconds() / 60)
        return f"{mins}m ago"
    except Exception:
        return ""


# Build table HTML
PAGE_SIZE = 30
if "msg_page" not in st.session_state:
    st.session_state.msg_page = 1
visible_count = st.session_state.msg_page * PAGE_SIZE
visible = filtered.head(visible_count)

rows_html = ""
for _, row in visible.iterrows():
    buyer = row.get("buyer_name") or row.get("buyer_login") or "?"
    order_id = row.get("external_order_id") or ""
    platform = (row.get("platform") or "").upper().replace("_", " ")
    status = row.get("status") or "open"
    source = row.get("source") or ""
    msg_count = int(row.get("message_count") or 0)
    last_msg = row.get("last_message_at") or ""
    needs_reply_flag = row.get("needs_reply", False)
    category = row.get("category") or ""
    direction = row.get("last_message_direction") or ""
    conv_id = row.get("id")

    reply_dot = f'<span style="color:{COLORS["danger"]}; font-weight:bold; margin-right:4px;">&#9679;</span>' if needs_reply_flag else ""
    dir_arrow = '<span style="color:#ef4444;">&#8592;</span>' if direction == "inbound" else '<span style="color:#10b981;">&#8594;</span>'
    cat_badge = f'<span style="font-size:0.6rem; color:{COLORS["muted"]}; margin-left:6px;">{category}</span>' if category else ""

    rows_html += f'''
    <tr style="border-bottom:1px solid {COLORS['border']}; cursor:pointer;" data-conv="{conv_id}">
      <td style="padding:8px 6px; white-space:nowrap;">{reply_dot}{_source_badge(source)}</td>
      <td style="padding:8px 6px; font-size:0.75rem;">{platform}</td>
      <td style="padding:8px 6px;">
        <div style="font-size:0.75rem; font-weight:600; color:{COLORS['text']};">{buyer}</div>
        <div style="font-size:0.65rem; color:{COLORS['muted']};">{order_id}</div>
      </td>
      <td style="padding:8px 6px;">{_status_badge(status)}{cat_badge}</td>
      <td style="padding:8px 6px; text-align:center; font-size:0.75rem; color:{COLORS['muted']};">{dir_arrow} {msg_count}</td>
      <td style="padding:8px 6px; text-align:right; font-size:0.65rem; color:{COLORS['muted']};">{_time_ago(last_msg)}</td>
    </tr>'''

table_html = f'''
<table style="width:100%; border-collapse:collapse; font-family:var(--font-mono); background:{COLORS['card']}; border-radius:8px; overflow:hidden;">
  <thead>
    <tr style="border-bottom:2px solid {COLORS['border']};">
      <th style="padding:10px 6px; text-align:left; font-size:0.6rem; color:{COLORS['muted']}; text-transform:uppercase; letter-spacing:1px;">Src</th>
      <th style="padding:10px 6px; text-align:left; font-size:0.6rem; color:{COLORS['muted']}; text-transform:uppercase; letter-spacing:1px;">Platform</th>
      <th style="padding:10px 6px; text-align:left; font-size:0.6rem; color:{COLORS['muted']}; text-transform:uppercase; letter-spacing:1px;">Buyer / Order</th>
      <th style="padding:10px 6px; text-align:left; font-size:0.6rem; color:{COLORS['muted']}; text-transform:uppercase; letter-spacing:1px;">Status</th>
      <th style="padding:10px 6px; text-align:center; font-size:0.6rem; color:{COLORS['muted']}; text-transform:uppercase; letter-spacing:1px;">Msgs</th>
      <th style="padding:10px 6px; text-align:right; font-size:0.6rem; color:{COLORS['muted']}; text-transform:uppercase; letter-spacing:1px;">Last</th>
    </tr>
  </thead>
  <tbody>
    {rows_html}
  </tbody>
</table>
'''
st.markdown(table_html, unsafe_allow_html=True)

# Show more
if visible_count < len(filtered):
    remaining = len(filtered) - visible_count
    if st.button(f"Show more ({remaining} remaining)", key="msg_show_more"):
        st.session_state.msg_page += 1
        st.rerun()

st.markdown(
    f'<div style="text-align:center; font-size:0.6rem; color:{COLORS["muted"]}; margin:8px 0;">'
    f'Showing {min(visible_count, len(filtered))} of {len(filtered)} conversations</div>',
    unsafe_allow_html=True,
)


# ============================================================
# CONVERSATION DETAIL (expandable)
# ============================================================
st.markdown('<div class="section-header">CONVERSATION DETAIL</div>', unsafe_allow_html=True)

# Build selectbox with conversation labels
if not filtered.empty:
    conv_options = {}
    for _, row in filtered.head(100).iterrows():
        conv_id = row.get("id")
        buyer = row.get("buyer_name") or row.get("buyer_login") or "?"
        order = row.get("external_order_id") or ""
        source = (row.get("source") or "").upper().replace("_", " ")
        status = row.get("status", "")
        needs = " [NEEDS REPLY]" if row.get("needs_reply") else ""
        label = f"[{source}] {buyer} | {order} ({status}){needs}"
        conv_options[conv_id] = label

    selected_conv_id = st.selectbox(
        "Select conversation",
        list(conv_options.keys()),
        format_func=lambda x: conv_options.get(x, str(x)),
        key="msg_conv_select",
    )

    if selected_conv_id:
        messages = load_messages_for_conv(selected_conv_id)
        if messages:
            for m in messages:
                direction = m.get("direction", "")
                is_inbound = direction == "inbound"
                sender = m.get("sender_name") or ("Buyer" if is_inbound else "You")
                body = m.get("body_text") or ""
                subject = m.get("email_subject") or ""
                sent = m.get("sent_at") or ""
                attachments = m.get("attachments") or []

                # Styling
                align = "flex-start" if is_inbound else "flex-end"
                bg = "#1e293b" if is_inbound else "#1a2744"
                border_color = COLORS["danger"] if is_inbound else COLORS["primary"]
                label_color = COLORS["danger"] if is_inbound else COLORS["primary"]
                dir_label = "INBOUND" if is_inbound else "OUTBOUND"

                subject_html = f'<div style="font-size:0.7rem; color:{COLORS["info"]}; margin-bottom:4px;">{subject}</div>' if subject else ""
                body_preview = body[:2000] if body else "<em style='color:#64748b;'>No text content</em>"
                body_preview = body_preview.replace("\n", "<br>")

                att_html = ""
                if attachments and len(attachments) > 0:
                    att_items = "".join(
                        f'<span style="font-size:0.6rem; color:{COLORS["info"]}; margin-right:8px;">&#128206; {a.get("filename","attachment")}</span>'
                        for a in attachments if isinstance(a, dict)
                    )
                    if att_items:
                        att_html = f'<div style="margin-top:6px;">{att_items}</div>'

                time_display = _time_ago(sent)

                msg_html = f'''
                <div style="display:flex; justify-content:{align}; margin-bottom:8px;">
                  <div style="max-width:80%; background:{bg}; border-left:3px solid {border_color}; border-radius:6px; padding:10px 14px;">
                    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
                      <span style="font-size:0.65rem; font-weight:700; color:{label_color};">{dir_label}</span>
                      <span style="font-size:0.6rem; color:{COLORS["muted"]}; margin-left:12px;">{sender} &middot; {time_display}</span>
                    </div>
                    {subject_html}
                    <div style="font-size:0.72rem; color:{COLORS["text"]}; line-height:1.5; word-break:break-word;">{body_preview}</div>
                    {att_html}
                  </div>
                </div>
                '''
                st.markdown(msg_html, unsafe_allow_html=True)
        else:
            st.info("No messages found for this conversation.")
