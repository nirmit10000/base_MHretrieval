"""
app.py — PID chat interface. Clean, light, Claude-inspired design.
"""

import streamlit as st
import pandas as pd
import io
import random
import string
from datetime import datetime
from query_engine import query
import config as C

st.set_page_config(
    page_title=C.APP_TITLE,
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ══════════════════════════════════════════════════════════════════════════════
# CSS
# ══════════════════════════════════════════════════════════════════════════════
CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap');

:root {
  --bg:           #FAF9F7;
  --surface:      #FFFFFF;
  --surface2:     #F5F4F1;
  --surface3:     #ECEAE5;
  --border:       #E5E3DF;
  --border2:      #CCC9C2;
  --accent:       #D97706;
  --accent-light: #FEF3C7;
  --accent-dark:  #B45309;
  --green:        #16a34a;
  --green-light:  #f0fdf4;
  --orange:       #c2410c;
  --orange-light: #fff7ed;
  --red:          #dc2626;
  --red-light:    #fef2f2;
  --teal:         #0891b2;
  --teal-light:   #ecfeff;
  --purple:       #7c3aed;
  --purple-light: #f5f3ff;
  --text:         #1A1A1A;
  --text2:        #3D3A35;
  --text3:        #6B6760;
  --text4:        #9B9790;
  --shadow-xs:    0 1px 2px rgba(0,0,0,.05);
  --shadow-sm:    0 1px 3px rgba(0,0,0,.08), 0 1px 2px rgba(0,0,0,.05);
}

/* ── Base reset ── */
* { font-family: 'Inter', sans-serif !important; box-sizing: border-box; margin: 0; }
html, body,
[data-testid="stApp"],
[data-testid="stAppViewContainer"],
[data-testid="stMain"],
[data-testid="stMainBlockContainer"] {
  background: var(--bg) !important;
}
#MainMenu, footer { visibility: hidden !important; display: none !important; }
header[data-testid="stHeader"] { visibility: hidden !important; height: 0 !important; }
/* Force sidebar always expanded — hide the collapse button */
[data-testid="stSidebarCollapseButton"] { display: none !important; }
[data-testid="collapsedControl"] { display: none !important; }
.block-container { padding: 0 !important; max-width: 100% !important; }
[data-testid="stAppViewContainer"] > section:last-child { padding-top: 58px !important; }

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 8px; }

/* ════════════════════ TOPBAR ════════════════════ */
.pid-topbar {
  position: fixed; top: 0; left: 0; right: 0; height: 52px; z-index: 9999;
  background: var(--surface); border-bottom: 1px solid var(--border);
  display: flex; align-items: center; justify-content: space-between;
  padding: 0 20px;
}
.pid-brand { display: flex; align-items: center; gap: 8px; }
.pid-logo {
  background: var(--accent-light); color: var(--accent-dark);
  font-family: 'IBM Plex Mono', monospace !important;
  font-size: 11px !important; font-weight: 600 !important;
  padding: 3px 8px; border-radius: 5px; letter-spacing: .04em;
}
.pid-name { font-size: 14px !important; font-weight: 600 !important; color: var(--text); }
.pid-chips { display: flex; gap: 6px; }
.pid-chip {
  font-family: 'IBM Plex Mono', monospace !important;
  font-size: 10px !important; color: var(--text4);
  background: var(--surface2); border: 1px solid var(--border);
  padding: 3px 8px; border-radius: 5px;
}

/* ════════════════════ SIDEBAR ════════════════════ */
[data-testid="stSidebar"] {
  background: var(--surface2) !important;
  box-shadow: 1px 0 0 0 var(--border) !important;
  border-right: none !important;
  min-width: 260px !important; max-width: 260px !important;
  padding-top: 52px !important;
}
[data-testid="stSidebar"] > div:first-child { padding: 16px 12px !important; }
.sid-label {
  font-size: 10px !important; font-weight: 600 !important;
  text-transform: uppercase; color: var(--text4) !important;
  letter-spacing: .07em; margin: 14px 0 5px 6px; display: block;
}
/* Sidebar nav buttons */
[data-testid="stSidebar"] .stButton > button {
  background: transparent !important;
  border: none !important;
  box-shadow: none !important;
  color: var(--text2) !important;
  font-size: 13px !important; font-weight: 500 !important;
  padding: 8px 12px !important; border-radius: 8px !important;
  text-align: left !important; width: 100% !important;
  justify-content: flex-start !important;
  transition: background .12s, color .12s !important;
  margin-bottom: 2px !important;
}
[data-testid="stSidebar"] .stButton > button:hover {
  background: var(--surface3) !important;
  color: var(--text) !important;
}
[data-testid="stSidebar"] .stButton > button[kind="primary"] {
  background: var(--accent-light) !important;
  border-left: 3px solid var(--accent) !important;
  color: var(--accent-dark) !important;
  font-weight: 600 !important;
  padding-left: 9px !important;
}
.sid-muted {
  opacity: .5; padding: 8px 12px; font-size: 13px; font-weight: 500;
  color: var(--text3); border-radius: 8px;
  margin-bottom: 2px; cursor: not-allowed;
}
.sid-session {
  padding: 6px 10px; border-radius: 6px; margin-bottom: 2px; cursor: default;
}
.sid-session:hover { background: var(--surface3); }
.sid-sid  { font-family:'IBM Plex Mono',monospace !important; font-size:10px !important; color:var(--text4); display:block; }
.sid-q    { font-size:12px !important; color:var(--text3); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:220px; display:block; }
.sid-none { font-size:12px; color:var(--text4); padding:4px 10px; }
.sid-meta { font-size:11px; color:var(--text3); padding:2px 10px; line-height:1.8; }

/* ════════════════════ STARTER / SUGGESTION BUTTONS ════════════════════ */
/* Buttons in main block (starters on empty page, suggestion pills) — NOT card action buttons */
[data-testid="stMainBlockContainer"] > div > div > div > div .stButton > button,
[data-testid="stMainBlockContainer"] > div > div > div .stButton > button {
  background: var(--surface) !important;
  border: 1px solid var(--border) !important;
  color: var(--text2) !important;
  font-size: 13px !important; font-weight: 400 !important;
  border-radius: 10px !important;
  padding: 10px 14px !important;
  text-align: left !important;
  white-space: normal !important;
  height: auto !important; min-height: unset !important;
  box-shadow: var(--shadow-xs) !important;
  line-height: 1.4 !important;
  transition: border-color .12s, background .12s !important;
}
[data-testid="stMainBlockContainer"] > div > div > div > div .stButton > button:hover,
[data-testid="stMainBlockContainer"] > div > div > div .stButton > button:hover {
  border-color: var(--accent) !important;
  background: var(--accent-light) !important;
  color: var(--accent-dark) !important;
}

/* ════════════════════ CHAT AREA ════════════════════ */
[data-testid="stMainBlockContainer"] {
  max-width: 860px !important; margin: 0 auto !important;
  padding: 28px 28px 90px !important;
}

/* ── User bubble ── */
.msg-user { display: flex; justify-content: flex-end; margin-bottom: 20px; }
.bub-user {
  background: #2D2D2D; color: #F5F4F1;
  padding: 10px 16px; border-radius: 18px 18px 4px 18px;
  max-width: 68%; font-size: 15px !important; line-height: 1.55;
}

/* ── Assistant answer — no bubble, full-width prose ── */
.msg-asst { margin-bottom: 8px; }
.bub-asst {
  background: transparent; color: var(--text);
  font-size: 15px !important; line-height: 1.7;
  max-width: 100%;
}
.bub-asst code {
  font-family: 'IBM Plex Mono', monospace !important;
  font-size: 12px !important; background: var(--surface2);
  padding: 1px 5px; border-radius: 3px; color: var(--text2);
}

/* ── Status row ── */
.s-row { display: flex; gap: 5px; flex-wrap: wrap; margin-bottom: 8px; }
.s-chip {
  font-family: 'IBM Plex Mono', monospace !important;
  font-size: 10px !important; padding: 2px 7px; border-radius: 4px;
  background: var(--surface2); border: 1px solid var(--border); color: var(--text3);
}
.s-ok   { background: var(--green-light);  color: var(--green);  border-color: #bbf7d0; }
.s-warn { background: var(--orange-light); color: var(--orange); border-color: #fed7aa; }

/* ── Citation row ── */
.cite-row { display: flex; flex-wrap: wrap; gap: 4px; margin-top: 8px; }
.cite-chip {
  font-family: 'IBM Plex Mono', monospace !important;
  font-size: 10px !important; color: var(--text4);
  background: var(--surface2); border: 1px solid var(--border);
  padding: 2px 7px; border-radius: 5px;
}

/* ── Empty state ── */
.empty-wrap {
  display: flex; flex-direction: column; align-items: center;
  padding: 72px 20px 36px; text-align: center;
}
.empty-title { font-size: 22px !important; font-weight: 600 !important; color: var(--text); margin-bottom: 8px; }
.empty-sub   { font-size: 14px !important; color: var(--text3); margin-bottom: 32px; max-width: 440px; line-height: 1.65; }

/* ════════════════════ CHAT INPUT ════════════════════ */
[data-testid="stBottom"],
[data-testid="stBottom"] > div,
[data-testid="stBottom"] > div > div,
[data-testid="stBottom"] > div > div > div {
  background: var(--bg) !important;
  box-shadow: none !important;
}
[data-testid="stBottom"]::before,
[data-testid="stBottom"]::after { display: none !important; }
[data-testid="stChatInput"],
[data-testid="stChatInput"] > div {
  background: var(--bg) !important;
  border-top: 1px solid var(--border) !important;
  padding: 10px 24px 12px !important;
  box-shadow: none !important;
}
[data-testid="stChatInput"] textarea {
  background: var(--surface) !important;
  border: 1.5px solid var(--border2) !important;
  border-radius: 12px !important;
  font-size: 15px !important; color: var(--text) !important;
  caret-color: var(--text) !important;
  box-shadow: none !important;
  padding: 12px 16px !important;
  min-height: 48px !important;
  line-height: 1.5 !important;
  resize: none !important;
}
[data-testid="stChatInput"] textarea:focus {
  border-color: var(--accent) !important;
  box-shadow: 0 0 0 3px rgba(217,119,6,.10) !important;
  outline: none !important;
}
[data-testid="stChatInput"] textarea::placeholder { color: var(--text4) !important; }
[data-testid="stChatInput"] button {
  background: var(--accent) !important;
  border-radius: 8px !important;
  border: none !important;
}
[data-testid="stChatInput"] button:hover { background: var(--accent-dark) !important; }

/* ════════════════════ DATA CARD (st.container border=True) ════════════════════ */
[data-testid="stVerticalBlockBorderWrapper"] {
  background: var(--surface) !important;
  border: 1px solid var(--border) !important;
  border-radius: 10px !important;
  box-shadow: var(--shadow-xs) !important;
  overflow: hidden !important;
  padding: 0 !important;
  margin-top: 12px !important;
}

/* Card header action buttons (CSV, Excel, ⤢) — high specificity to win over starter rule */
[data-testid="stMainBlockContainer"] [data-testid="stVerticalBlockBorderWrapper"] .stButton > button {
  background: var(--surface2) !important;
  border: 1px solid var(--border) !important;
  color: var(--text3) !important;
  font-size: 11px !important; font-weight: 500 !important;
  border-radius: 6px !important;
  padding: 4px 10px !important;
  text-align: center !important;
  white-space: nowrap !important;
  height: auto !important; min-height: unset !important;
  line-height: 1.4 !important;
  box-shadow: none !important;
}
[data-testid="stMainBlockContainer"] [data-testid="stVerticalBlockBorderWrapper"] .stButton > button:hover {
  background: var(--accent-light) !important;
  border-color: var(--accent) !important;
  color: var(--accent-dark) !important;
}
[data-testid="stMainBlockContainer"] [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stDownloadButton"] button {
  background: var(--surface2) !important;
  border: 1px solid var(--border) !important;
  color: var(--text3) !important;
  font-size: 11px !important; font-weight: 500 !important;
  border-radius: 6px !important;
  padding: 4px 10px !important;
  text-align: center !important;
  height: auto !important; min-height: unset !important;
  line-height: 1.4 !important;
  box-shadow: none !important;
}
[data-testid="stMainBlockContainer"] [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stDownloadButton"] button:hover {
  background: var(--accent-light) !important;
  border-color: var(--accent) !important;
  color: var(--accent-dark) !important;
}

/* Streamlit spinner override */
[data-testid="stSpinner"] > div { color: var(--text3) !important; font-size: 13px !important; }

/* SQL expander */
[data-testid="stExpander"] {
  border: 1px solid var(--border) !important;
  border-radius: 8px !important;
  background: var(--surface) !important;
}
</style>
"""

# ══════════════════════════════════════════════════════════════════════════════
# Session state
# ══════════════════════════════════════════════════════════════════════════════
def _new_sid():
    return "VA" + "".join(random.choices(string.digits, k=4))

if "session_id"    not in st.session_state: st.session_state.session_id    = _new_sid()
if "messages"      not in st.session_state: st.session_state.messages      = []
if "section"       not in st.session_state: st.session_state.section       = "mh_post2009"
if "sessions"      not in st.session_state: st.session_state.sessions      = []
if "pending_q"     not in st.session_state: st.session_state.pending_q     = None
if "expand_rows"   not in st.session_state: st.session_state.expand_rows   = None
if "expand_question" not in st.session_state: st.session_state.expand_question = ""
if "suggestions" not in st.session_state:
    st.session_state.suggestions = [
        "BJP winners in 2024 MH Vidhan Sabha voteshare > 60%",
        "Smallest margin PC in 2024 Lok Sabha Maharashtra",
        "NDA vs MVA total seats 2024 MH assembly",
        "Anti-incumbency results 2024 Maharashtra elections",
    ]

SECTION_META = {
    "mh_post2009": {"label": "MH  2009 – 2024", "db": C.POST_2009_DB, "scope_override": True},
    "mh_all":      {"label": "MH  All Years",    "db": C.FULL_DB,      "scope_override": False},
}

# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════
def _detect_level(q: str):
    q = q.lower()
    if any(x in q for x in ["pc ", "lok sabha", "parliamentary", "general election", " ge", "ls ", "mp seat", "pc-wise", "pc wise"]):
        return "PC",       "#dc2626", "#fef2f2"
    if any(x in q for x in ["assembly", "vidhan sabha", "mla", " ac ", "ac-wise", "vidhansabha"]):
        return "AC",       "#0891b2", "#ecfeff"
    if "district" in q:
        return "District", "#c2410c", "#fff7ed"
    if any(x in q for x in ["zone", "vidarbha", "marathwada", "konkan"]):
        return "Zone",     "#7c3aed", "#f5f3ff"
    return "State",        "#16a34a", "#f0fdf4"


def _gen_suggestions(q: str) -> list:
    q = q.lower()
    if any(x in q for x in ["lok sabha", " pc", " ge", "general election", "parliamentary"]):
        return [
            "Smallest margin PC 2024 Lok Sabha MH",
            "NDA vs INDI total votes 2024 MH Lok Sabha",
            "BJP PC-wise voteshare 2024 Maharashtra GE",
            "Highest turnout PC 2024 Maharashtra Lok Sabha",
        ]
    if any(x in q for x in ["swing", "change", "shift"]):
        return [
            "BJP swing > 10% ACs MH 2019 vs 2024",
            "Congress swing by district 2019 vs 2024",
            "NCP swing Western Maharashtra 2019 vs 2024",
            "Biggest swing seats 2024 MH Vidhan Sabha",
        ]
    if any(x in q for x in ["assembly", "vidhan sabha", " ae", "mla", "vidhan"]):
        return [
            "Closest contests 2024 Maharashtra Vidhan Sabha",
            "Party wise seat share 2024 MH assembly",
            "Turnout by district 2024 Maharashtra assembly",
            "Incumbents who lost 2024 MH assembly",
        ]
    return [
        "BJP winners 2024 MH Vidhan Sabha voteshare > 60%",
        "Smallest margin PC 2024 Lok Sabha Maharashtra",
        "NDA vs MVA seat count 2024 MH assembly",
        "Turnout comparison 2009 vs 2024 MH assembly",
    ]


def _citation_html(rows, db_used):
    if not rows:
        return ""
    years   = sorted({str(r.get("year","")) for r in rows if r.get("year")})
    eltypes = sorted({r.get("el_type","")   for r in rows if r.get("el_type")})
    db_lbl  = "Post-2009 DB" if db_used == C.POST_2009_DB else "Full DB 1951–2024"
    chips   = [
        '<span class="cite-chip">ECI · Maharashtra</span>',
        f'<span class="cite-chip">{" · ".join(years)}</span>'   if years   else "",
        f'<span class="cite-chip">{" / ".join(eltypes)}</span>' if eltypes else "",
        f'<span class="cite-chip">{db_lbl}</span>',
    ]
    return '<div class="cite-row">' + "".join(c for c in chips if c) + '</div>'




def _status_bar(db_used, verified, n_rows):
    db_lbl = "Post-2009" if db_used == C.POST_2009_DB else "Full DB"
    v_cls  = "s-ok" if verified else "s-warn"
    v_lbl  = "verified" if verified else "unverified"
    return (f'<div class="s-row">'
            f'<span class="s-chip">{db_lbl}</span>'
            f'<span class="s-chip {v_cls}">{v_lbl}</span>'
            f'<span class="s-chip">{n_rows} rows</span>'
            f'</div>')

# ══════════════════════════════════════════════════════════════════════════════
# Full-table dialog
# ══════════════════════════════════════════════════════════════════════════════
@st.dialog("Full Data View", width="large")
def _expand_dialog(rows, question):
    df = pd.DataFrame(rows)
    level, color, light = _detect_level(question)

    # ── Query header — always visible above the scrolling dataframe ──
    st.markdown(
        f'<div style="border-left:3px solid #D97706;padding:7px 12px;margin-bottom:12px;background:#FEF3C7;border-radius:0 6px 6px 0">'
        f'<span style="font-family:IBM Plex Mono,monospace;font-size:10px;font-weight:700;color:#B45309;letter-spacing:.06em">Q</span>'
        f'<span style="font-size:13px;color:#3D3A35;margin-left:8px;font-style:italic">&ldquo;{question}&rdquo;</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── Level pill + row count ──
    st.markdown(
        f'<span style="background:{light};color:{color};border:1px solid {color}30;'
        f'font-size:11px;font-weight:600;padding:2px 8px;border-radius:20px;'
        f'font-family:IBM Plex Mono,monospace">{level}</span>'
        f'<span style="font-size:13px;color:#71717a;margin-left:8px">{len(rows)} rows</span>',
        unsafe_allow_html=True,
    )
    df.index = range(1, len(df) + 1)
    st.dataframe(df, use_container_width=True, height=480)
    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
    c1, c2 = st.columns(2, gap="small")
    with c1:
        st.download_button(
            "Download CSV", df.to_csv(index=False).encode(),
            "electoral_data.csv", "text/csv", use_container_width=True,
        )
    with c2:
        buf = io.BytesIO()
        df.to_excel(buf, index=False, engine="openpyxl")
        st.download_button(
            "Download Excel", buf.getvalue(),
            "electoral_data.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# Render
# ══════════════════════════════════════════════════════════════════════════════
st.markdown(CSS, unsafe_allow_html=True)

# ── Topbar ──
model_chip = "llama-3.3-70b" if C.USE_GROQ else "gemini-3-flash"
st.markdown(f"""
<div class="pid-topbar">
  <div class="pid-brand">
    <span class="pid-logo">PID</span>
    <span class="pid-name">Political Intelligence</span>
  </div>
  <div class="pid-chips">
    <span class="pid-chip">{st.session_state.session_id}</span>
    <span class="pid-chip">{model_chip}</span>
  </div>
</div>""", unsafe_allow_html=True)

# ── Sidebar ──
with st.sidebar:
    st.markdown('<span class="sid-label">Maharashtra</span>', unsafe_allow_html=True)
    for key, meta in SECTION_META.items():
        active = st.session_state.section == key
        if st.button(meta["label"], key=f"nav_{key}",
                     type="primary" if active else "secondary",
                     use_container_width=True):
            st.session_state.section = key
            st.rerun()
    st.markdown('<div class="sid-muted">National (coming soon)</div>', unsafe_allow_html=True)

    st.markdown('<span class="sid-label" style="margin-top:18px">Session History</span>', unsafe_allow_html=True)
    if st.session_state.sessions:
        for s in reversed(st.session_state.sessions[-5:]):
            st.markdown(f"""<div class="sid-session">
              <span class="sid-sid">{s['id']} · {s['time']}</span>
              <span class="sid-q">{s['first_q']}</span>
            </div>""", unsafe_allow_html=True)
    else:
        st.markdown('<div class="sid-none">No previous sessions</div>', unsafe_allow_html=True)

    st.markdown('<span class="sid-label" style="margin-top:18px">Settings</span>', unsafe_allow_html=True)
    st.markdown(
        f'<div class="sid-meta">'
        f'SQL verify: {"on" if C.ENABLE_SQL_VERIFICATION else "off"}<br>'
        f'DB: {SECTION_META[st.session_state.section]["db"]}'
        f'</div>', unsafe_allow_html=True
    )
    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
    if st.button("New session", use_container_width=True):
        if st.session_state.messages:
            first_q = next((m["content"] for m in st.session_state.messages if m["role"] == "user"), "")
            st.session_state.sessions.append({
                "id":      st.session_state.session_id,
                "time":    datetime.now().strftime("%H:%M"),
                "first_q": (first_q[:36] + "…") if len(first_q) > 36 else first_q,
            })
        st.session_state.session_id = _new_sid()
        st.session_state.messages   = []
        st.session_state.suggestions = _gen_suggestions("")
        st.rerun()

# ── Main ──
meta = SECTION_META[st.session_state.section]

if not st.session_state.messages:
    st.markdown("""
    <div class="empty-wrap">
      <div class="empty-title">Maharashtra Electoral Intelligence</div>
      <div class="empty-sub">Ask anything about Maharashtra elections — Vidhan Sabha, Lok Sabha, candidates, margins, swings, and more.</div>
    </div>""", unsafe_allow_html=True)

    starters = [
        "BJP winners in 2024 MH Vidhan Sabha with voteshare above 60%",
        "Smallest margin PC in 2024 Lok Sabha Maharashtra",
        "NDA vs INDI total votes and seats in 2024 Maharashtra GE",
        "Anti-incumbency results in 2024 Maharashtra assembly",
    ]
    c1, c2 = st.columns(2, gap="small")
    for i, s in enumerate(starters):
        with (c1 if i % 2 == 0 else c2):
            if st.button(s, key=f"start_{i}", use_container_width=True):
                st.session_state.pending_q = s

else:
    for i, msg in enumerate(st.session_state.messages):
        if msg["role"] == "user":
            st.markdown(f'<div class="msg-user"><div class="bub-user">{msg["content"]}</div></div>',
                        unsafe_allow_html=True)
        else:
            rows     = msg.get("rows", [])
            question = msg.get("question", "")
            status   = _status_bar(msg.get("db_used",""), msg.get("verified", False), msg.get("rows_count", 0))
            answer   = msg["content"].replace("\n", "<br>")
            cite     = _citation_html(rows, msg.get("db_used",""))

            # Answer bubble
            st.markdown(
                f'<div class="msg-asst"><div class="bub-asst">{status}{answer}</div></div>',
                unsafe_allow_html=True,
            )

            # Data card — header with actions + native dataframe
            if rows:
                df  = pd.DataFrame(rows)
                buf = io.BytesIO()
                df.to_excel(buf, index=False, engine="openpyxl")
                level, color, light = _detect_level(question)

                with st.container(border=True):
                    # Header row: title + pill + actions
                    h_left, h_right = st.columns([5, 4], gap="small")
                    with h_left:
                        st.markdown(
                            f'<div style="display:flex;align-items:center;gap:8px;padding:4px 2px">'
                            f'<span style="font-size:12px;font-weight:600;color:var(--text2)">{len(rows)} rows</span>'
                            f'<span style="background:{light};color:{color};border:1px solid {color}30;'
                            f'font-size:10px;font-weight:600;padding:2px 7px;border-radius:20px;'
                            f'font-family:IBM Plex Mono,monospace">{level}</span>'
                            f'</div>',
                            unsafe_allow_html=True,
                        )
                    with h_right:
                        b1, b2, b3 = st.columns(3, gap="small")
                        with b1:
                            st.download_button(
                                "CSV", df.to_csv(index=False).encode(),
                                "electoral_data.csv", "text/csv",
                                key=f"csv_{i}", use_container_width=True,
                            )
                        with b2:
                            st.download_button(
                                "Excel", buf.getvalue(),
                                "electoral_data.xlsx",
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key=f"xlsx_{i}", use_container_width=True,
                            )
                        with b3:
                            if st.button("⤢", key=f"exp_{i}",
                                         use_container_width=True,
                                         help="Expand full table"):
                                _expand_dialog(rows, question)

                    # Native dataframe — sortable, scrollable
                    df.index = range(1, len(df) + 1)
                    st.dataframe(df, use_container_width=True,
                                 height=min(480, 35 * len(rows) + 38))

            # Citation chips
            if cite:
                st.markdown(cite, unsafe_allow_html=True)


    # Suggestion pills
    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)
    scols = st.columns(len(st.session_state.suggestions), gap="small")
    for i, sug in enumerate(st.session_state.suggestions):
        with scols[i]:
            if st.button(sug, key=f"sug_{i}", use_container_width=True):
                st.session_state.pending_q = sug

# ── Input ──
user_input = st.chat_input("Ask anything about Maharashtra elections…")

pending = st.session_state.pending_q
st.session_state.pending_q = None
question = user_input or pending

if question and question.strip():
    st.session_state.messages.append({"role": "user", "content": question})
    with st.spinner("Retrieving from electoral database…"):
        result = query(question, db_override=meta["db"] if meta["scope_override"] else None)
    if result["error"]:
        st.session_state.messages.append({
            "role": "assistant", "content": f"Something went wrong: {result['error']}",
            "rows": [], "sql": result.get("sql",""), "db_used": result["db_used"],
            "verified": False, "rows_count": 0, "question": question,
        })
    else:
        st.session_state.messages.append({
            "role":       "assistant",
            "content":    result["answer"],
            "rows":       result["rows"],
            "sql":        result["sql"],
            "db_used":    result["db_used"],
            "verified":   result["verified"],
            "rows_count": len(result["rows"]),
            "question":   question,
        })
        st.session_state.suggestions = _gen_suggestions(question)
    st.rerun()
