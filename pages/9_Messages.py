"""Messages - Customer conversation hub (Allegro + Amazon)."""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from lib.theme import setup_page, COLORS

setup_page("Messages")

st.markdown('<div class="section-header">MESSAGE CENTER</div>', unsafe_allow_html=True)

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


@st.cache_data(ttl=120)
def load_messages_for_conv(conv_id):
    rows = _get("messages", {
        "conversation_id": f"eq.{conv_id}",
        "select": "direction,sender_name,body_text,email_subject,sent_at,is_read,attachments,translation_pl,detected_language,draft_reply,draft_reply_local",
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

# Filters
all_sources = sorted(df["source"].dropna().unique())
selected_sources = st.sidebar.multiselect("SOURCE", all_sources, default=all_sources, key="msg_src")

all_platforms = sorted(df["platform"].dropna().unique())
selected_platforms = st.sidebar.multiselect("PLATFORM", all_platforms, default=all_platforms, key="msg_plat")

status_opts = ["all", "open", "replied", "closed", "escalated"]
selected_status = st.sidebar.selectbox("STATUS", status_opts, key="msg_status")

needs_reply_only = st.sidebar.checkbox("Needs reply only", value=False, key="msg_needs_reply")
search_q = st.sidebar.text_input("SEARCH (buyer, order ID)", key="msg_search")

# Apply filters
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
# KPI STRIP (native Streamlit)
# ============================================================
total_convs = len(filtered)
needs_reply_count = int(filtered["needs_reply"].sum()) if "needs_reply" in filtered.columns else 0
open_count = len(filtered[filtered["status"] == "open"]) if "status" in filtered.columns else 0
escalated_count = len(filtered[filtered["status"] == "escalated"]) if "status" in filtered.columns else 0
replied_count = len(filtered[filtered["status"] == "replied"]) if "status" in filtered.columns else 0

avg_resp = None
if "first_response_minutes" in filtered.columns:
    resp_vals = filtered["first_response_minutes"].dropna()
    if len(resp_vals) > 0:
        avg_resp = resp_vals.mean()

allegro_count = len(filtered[filtered["source"] == "allegro"])
amazon_count = len(filtered[filtered["source"] == "amazon_email"])

resp_display = f"{avg_resp:.0f} min" if avg_resp is not None else "N/A"

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Conversations", total_convs, delta=f"{period_map.get(days,'')} period", delta_color="off")
k2.metric("Needs Reply", needs_reply_count, delta="action required" if needs_reply_count > 0 else "all good", delta_color="inverse" if needs_reply_count > 0 else "normal")
k3.metric("Avg Response", resp_display, delta="first reply", delta_color="off")
k4.metric("Open / Escalated", f"{open_count} / {escalated_count}", delta=f"{replied_count} replied", delta_color="off")
k5.metric("Sources", f"ALG {allegro_count} | AMZ {amazon_count}")


# ============================================================
# CONVERSATIONS TABLE (native st.dataframe)
# ============================================================
st.markdown('<div class="section-header">CONVERSATIONS</div>', unsafe_allow_html=True)


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


# Prepare display dataframe
if not filtered.empty:
    display_df = pd.DataFrame()
    display_df["Reply"] = filtered["needs_reply"].map(lambda x: "!!" if x else "")
    display_df["Src"] = filtered["source"].map({"allegro": "ALG", "amazon_email": "AMZ", "allegro_issue": "ISSUE"}).fillna("?")
    display_df["Platform"] = filtered["platform"].str.upper().str.replace("_", " ")
    display_df["Buyer"] = filtered["buyer_name"].fillna(filtered["buyer_login"]).fillna("?")
    display_df["Order"] = filtered["external_order_id"].fillna("")
    display_df["Status"] = filtered["status"].fillna("open")
    display_df["Cat"] = filtered["category"].fillna("")
    display_df["Dir"] = filtered["last_message_direction"].map({"inbound": "<-", "outbound": "->"}).fillna("")
    display_df["Msgs"] = filtered["message_count"].fillna(0).astype(int)
    display_df["Last"] = filtered["last_message_at"].map(_time_ago)
    display_df.index = filtered["id"].values

    st.dataframe(
        display_df,
        use_container_width=True,
        height=min(len(display_df) * 35 + 38, 600),
        column_config={
            "Reply": st.column_config.TextColumn("!", width="small"),
            "Src": st.column_config.TextColumn("Src", width="small"),
            "Platform": st.column_config.TextColumn("Platform", width="small"),
            "Buyer": st.column_config.TextColumn("Buyer", width="medium"),
            "Order": st.column_config.TextColumn("Order", width="medium"),
            "Status": st.column_config.TextColumn("Status", width="small"),
            "Cat": st.column_config.TextColumn("Cat", width="small"),
            "Dir": st.column_config.TextColumn("Dir", width="small"),
            "Msgs": st.column_config.NumberColumn("Msgs", width="small"),
            "Last": st.column_config.TextColumn("Last", width="small"),
        },
    )

    st.caption(f"Showing {len(display_df)} conversations ({period_map.get(days,'')} period)")

# ============================================================
# CONVERSATION DETAIL
# ============================================================
st.markdown('<div class="section-header">CONVERSATION DETAIL</div>', unsafe_allow_html=True)

if not filtered.empty:
    conv_options = {}
    for _, row in filtered.head(100).iterrows():
        cid = row.get("id")
        buyer = row.get("buyer_name") or row.get("buyer_login") or "?"
        order = row.get("external_order_id") or ""
        src = (row.get("source") or "").upper().replace("_", " ")
        status = row.get("status", "")
        needs = " ** NEEDS REPLY **" if row.get("needs_reply") else ""
        label = f"[{src}] {buyer} | {order} ({status}){needs}"
        conv_options[cid] = label

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
                translation_pl = m.get("translation_pl") or ""
                detected_lang = m.get("detected_language") or ""

                time_display = _time_ago(sent)
                dir_icon = "<<" if is_inbound else ">>"
                dir_label = "INBOUND" if is_inbound else "OUTBOUND"
                lang_tag = f" [{detected_lang.upper()}]" if detected_lang and detected_lang != "pl" else ""

                # Message bubble via st.html
                border_color = "#ef4444" if is_inbound else "#3b82f6"
                bg = "#1e293b" if is_inbound else "#1a2744"
                label_color = "#ef4444" if is_inbound else "#3b82f6"

                body_safe = (body[:2000] if body else "<em>No text</em>").replace("\n", "<br>").replace("<", "&lt;").replace(">", "&gt;").replace("&lt;br&gt;", "<br>").replace("&lt;em&gt;", "<em>").replace("&lt;/em&gt;", "</em>")
                subject_safe = subject.replace("<", "&lt;").replace(">", "&gt;") if subject else ""

                subject_block = f'<div style="font-size:12px; color:#06b6d4; margin-bottom:4px;">{subject_safe}</div>' if subject_safe else ""

                # Translation block
                tl_block = ""
                if translation_pl and is_inbound and detected_lang != "pl":
                    tl_safe = translation_pl[:2000].replace("\n", "<br>").replace("<", "&lt;").replace(">", "&gt;").replace("&lt;br&gt;", "<br>")
                    tl_block = f'''<div style="margin-top:8px; padding:8px 10px; background:#0f172a; border-left:2px solid #8b5cf6; border-radius:4px;">
                      <div style="font-size:10px; font-weight:700; color:#8b5cf6; margin-bottom:3px;">TLUMACZENIE PL</div>
                      <div style="font-size:12px; color:#cbd5e1; line-height:1.5;">{tl_safe}</div>
                    </div>'''

                bubble_html = f'''<div style="margin-bottom:6px; font-family:monospace;">
                  <div style="max-width:90%; background:{bg}; border-left:3px solid {border_color}; border-radius:6px; padding:10px 14px;">
                    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:4px;">
                      <span style="font-size:11px; font-weight:700; color:{label_color};">{dir_label}{lang_tag}</span>
                      <span style="font-size:10px; color:#64748b;">{sender} | {time_display}</span>
                    </div>
                    {subject_block}
                    <div style="font-size:12px; color:#e2e8f0; line-height:1.5; word-break:break-word;">{body_safe}</div>
                    {tl_block}
                  </div>
                </div>'''

                st.html(bubble_html)

            # Draft reply panel
            last_inbound = None
            for m in reversed(messages):
                if m.get("direction") == "inbound" and m.get("draft_reply"):
                    last_inbound = m
                    break

            if last_inbound:
                draft_pl = (last_inbound.get("draft_reply") or "").replace("\n", "<br>")
                draft_local = (last_inbound.get("draft_reply_local") or "").replace("\n", "<br>")
                d_lang = (last_inbound.get("detected_language") or "?").upper()

                draft_html = f'''<div style="margin-top:12px; padding:14px; background:#1a1a2e; border:1px solid rgba(139,92,246,0.25); border-radius:8px; font-family:monospace;">
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
                </div>'''

                st.html(draft_html)
        else:
            st.info("No messages found for this conversation.")
