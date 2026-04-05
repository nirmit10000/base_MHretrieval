"""
party_resolver.py — Two jobs:
  1. Map NL party names → DB abbreviation codes
  2. Map NL election type references → correct el_type values
     and NL geography references → correct column + value
"""

import re
import sqlite3
import config as C
from rapidfuzz import process, fuzz

# ── Party name mappings ───────────────────────────────────────────────────────
KNOWN = {
    # INC
    "indian national congress": "INC", "congress": "INC", "inc": "INC",
    # BJP
    "bharatiya janata party": "BJP", "bjp": "BJP",
    # BSP
    "bahujan samaj party": "BSP", "bsp": "BSP", "mayawati": "BSP",
    # SP
    "samajwadi party": "SP", "mulayam": "SP",
    # CPI
    "communist party of india": "CPI", "cpi": "CPI",
    # CPM
    "communist party of india marxist": "CPM", "cpm": "CPM",
    # NCP
    "nationalist congress party": "NCP", "ncp": "NCP",
    "sharad pawar": "NCP", "ajit pawar": "NCP",
    # SHS
    "shiv sena": "SHS", "shivsena": "SHS", "shs": "SHS",
    "uddhav": "SHS", "shinde": "SHS",
    # MNS
    "maharashtra navnirman sena": "MNS", "mns": "MNS",
    "raj thackeray": "MNS",
    # TDP
    "telugu desam party": "TDP", "tdp": "TDP", "chandrababu": "TDP",
    # DMK
    "dravida munnetra kazhagam": "DMK", "dmk": "DMK",
    # ADMK
    "all india anna dravida munnetra kazhagam": "ADMK",
    "aiadmk": "ADMK", "admk": "ADMK",
    # RJD
    "rashtriya janata dal": "RJD", "rjd": "RJD", "lalu": "RJD",
    "lalu prasad": "RJD",
    # JDU
    "janata dal united": "JD(U)", "jdu": "JD(U)", "nitish": "JD(U)",
    # JDS
    "janata dal secular": "JD(S)", "jds": "JD(S)",
    # AITC
    "trinamool congress": "AITC", "tmc": "AITC",
    # SAD
    "akali dal": "SAD", "shiromani akali dal": "SAD", "sad": "SAD",
    # AAP
    "aam aadmi party": "AAP", "aap": "AAP",
    # Historical
    "jana sangh": "BJS", "bharatiya jana sangh": "BJS",
    "swatantra party": "SWA",
    "janata party": "JNP",
    "janata dal": "JD",
    # Others
    "independent": "IND", "ind": "IND",
    "vba": "VBA", "vanchit bahujan aghadi": "VBA",
    "mim": "AIMIM", "aimim": "AIMIM",
    "left front": "CPM",
}

# ── el_type alias mappings ─────────────────────────────────────────────────────
# Maps any NL phrase → correct el_type value
ELTYPE_MAP = {
    # ── GE / Lok Sabha / Parliamentary triggers ──
    "lok sabha": "GE", "loksabha": "GE", "lok-sabha": "GE",
    "general election": "GE", "general elections": "GE",
    "parliamentary election": "GE", "parliamentary elections": "GE",
    "parliament election": "GE", "parliament elections": "GE",
    "parliamentary constituency": "GE", "parliamentary seat": "GE",
    "parliamentary seats": "GE",
    "pc election": "GE", "pc level": "GE", "pc wise": "GE", "pc-wise": "GE",
    "pc seat": "GE", "pc seats": "GE", "pc margin": "GE", "pc margins": "GE",
    "national election": "GE", "national elections": "GE",
    "lok sabha election": "GE", "lok sabha elections": "GE",
    "lok sabha seat": "GE", "lok sabha seats": "GE",
    "ls election": "GE", "ls elections": "GE", "ls seat": "GE", "ls seats": "GE",
    "mp election": "GE", "mp elections": "GE", "mp seat": "GE", "mp seats": "GE",
    "member of parliament": "GE",
    "central election": "GE", "central elections": "GE",
    "federal election": "GE",
    # ── AE / Vidhan Sabha / Assembly triggers ──
    "vidhan sabha": "AE", "vidhansabha": "AE", "vidhan-sabha": "AE",
    "vidhan sabha election": "AE", "vidhan sabha elections": "AE",
    "vidhan sabha seat": "AE", "vidhan sabha seats": "AE",
    "vs election": "AE", "vs seat": "AE",
    "assembly election": "AE", "assembly elections": "AE",
    "assembly seat": "AE", "assembly seats": "AE",
    "assembly constituency": "AE", "assembly constituencies": "AE",
    "assembly segment": "AE", "assembly segments": "AE",
    "state election": "AE", "state elections": "AE",
    "state assembly": "AE", "state legislature": "AE",
    "legislative assembly": "AE",
    "ac election": "AE", "ac elections": "AE",
    "ac-wise": "AE", "ac level": "AE", "ac wise": "AE",
    "ac seat": "AE", "ac seats": "AE", "ac margin": "AE", "ac margins": "AE",
    "mla election": "AE", "mla elections": "AE",
    "mla seat": "AE", "mla seats": "AE",
    "member of legislative assembly": "AE",
    "constituency election": "AE",
    "vidhayak": "AE",
    # ── Bypoll triggers ──
    "bypoll": "AE-BP", "by-poll": "AE-BP", "by poll": "AE-BP",
    "byelection": "AE-BP", "bye election": "AE-BP", "by-election": "AE-BP",
    "upchunav": "AE-BP", "upa chunav": "AE-BP",
}


# ── Place name cache (loaded once at startup) ─────────────────────────────────
_AC_NAMES:       list[str] = []
_PC_NAMES:       list[str] = []
_DISTRICT_NAMES: list[str] = []
_ZONE_NAMES:     list[str] = []

def _load_place_names():
    global _AC_NAMES, _PC_NAMES, _DISTRICT_NAMES, _ZONE_NAMES
    if _AC_NAMES:
        return
    try:
        for db in (C.POST_2009_DB, C.FULL_DB):
            con = sqlite3.connect(db)
            cur = con.cursor()
            cur.execute("SELECT DISTINCT ac_name FROM mh_results WHERE ac_name IS NOT NULL")
            _AC_NAMES += [r[0] for r in cur.fetchall()]
            cur.execute("SELECT DISTINCT pc_name FROM mh_results WHERE pc_name IS NOT NULL")
            _PC_NAMES += [r[0] for r in cur.fetchall()]
            cur.execute("SELECT DISTINCT district FROM mh_results WHERE district IS NOT NULL")
            _DISTRICT_NAMES += [r[0] for r in cur.fetchall()]
            cur.execute("SELECT DISTINCT zone FROM mh_results WHERE zone IS NOT NULL")
            _ZONE_NAMES += [r[0] for r in cur.fetchall()]
            con.close()
        _AC_NAMES       = list(set(_AC_NAMES))
        _PC_NAMES       = list(set(_PC_NAMES))
        _DISTRICT_NAMES = list(set(_DISTRICT_NAMES))
        _ZONE_NAMES     = list(set(_ZONE_NAMES))
    except:
        pass

_STOPWORDS = {
    "the","and","for","with","from","that","this","over","list","data",
    "where","winner","winners","details","results","elections","election",
    "years","across","their","party","seats","votes","margin","vidhan",
    "sabha","lok","assembly","constituency","constituencies","total",
    "maharashtra","give","show","find","tell","what","which","does",
    "highest","lowest","most","least","best","worst","many","much",
    "north","south","east","west","central","rural","urban","rural",
    "level","wise","wise","seat","seats","vote","votes","lead","leads",
    "trail","trails","swing","share","count","number","name","names",
    "candidate","candidates","district","zone","region","area","state",
    "percent","percentage","turnout","incumbency","alliance","block",
}

def _fuzzy_resolve_places(query: str) -> list[str]:
    """Return hint strings for any AC/PC/district/zone names found via fuzzy match."""
    _load_place_names()
    hints = []
    seen = set()

    # ── District & Zone: original-case tokens + fuzz.ratio (best for short names) ──
    words_orig = re.findall(r'\b[a-zA-Z]{4,}\b', query)
    words_orig = [w for w in words_orig if w.lower() not in _STOPWORDS]
    all_words  = re.findall(r'\b[a-zA-Z]+\b', query)
    bigrams    = [f"{all_words[i]} {all_words[i+1]}" for i in range(len(all_words) - 1)]
    # Add trigrams for "East Vidarbha region" etc.
    trigrams   = [f"{all_words[i]} {all_words[i+1]} {all_words[i+2]}" for i in range(len(all_words) - 2)]
    geo_tokens = words_orig + bigrams + trigrams

    for token in geo_tokens:
        if _DISTRICT_NAMES:
            m, s, _ = process.extractOne(token, _DISTRICT_NAMES, scorer=fuzz.ratio)
            if s >= 82 and m not in seen:
                seen.add(m)
                hints.append(f"[DISTRICT: use district='{m}' in SQL]")

        if _ZONE_NAMES:
            m, s, _ = process.extractOne(token, _ZONE_NAMES, scorer=fuzz.ratio)
            if s >= 82 and m not in seen:
                seen.add(m)
                hints.append(f"[ZONE: use zone='{m}' in SQL]")

    # ── AC & PC: UPPERCASE tokens + fuzz.WRatio (best for long AC names) ──
    tokens_upper = [t.upper() for t in words_orig]
    for token in tokens_upper:
        if _AC_NAMES:
            m, s, _ = process.extractOne(token, _AC_NAMES, scorer=fuzz.WRatio)
            if s >= 88 and m not in seen:
                seen.add(m)
                hints.append(f"[AC NAME: use ac_name LIKE '%{m}%' in SQL]")

        if _PC_NAMES:
            m, s, _ = process.extractOne(token, _PC_NAMES, scorer=fuzz.WRatio)
            if s >= 88 and m not in seen:
                seen.add(m)
                hints.append(f"[PC NAME: use pc_name LIKE '%{m}%' in SQL]")

    return hints


def _all_db_parties() -> list[str]:
    try:
        con = sqlite3.connect(C.POST_2009_DB)
        cur = con.cursor()
        cur.execute("SELECT DISTINCT party FROM mh_results")
        parties = [r[0] for r in cur.fetchall() if r[0] and r[0].strip()]
        con.close()
        return parties
    except:
        return []


def resolve(user_query: str) -> str:
    """
    Enriches the user query with:
    1. Resolved party code hint
    2. Resolved el_type hint
    Returns enriched query string.
    """
    lower = user_query.lower()
    hints = []

    # ── Party resolution ──────────────────────────────────────────────────────
    party_resolved = False
    for phrase, code in KNOWN.items():
        if phrase in lower:
            hints.append(f"[PARTY: use party LIKE '%{code}%' in SQL]")
            party_resolved = True
            break

    if not party_resolved:
        # Regex: detect uppercase party codes already in query
        tokens = re.findall(r'\b[A-Z]{2,6}\b', user_query)
        db_parties = _all_db_parties()
        for token in tokens:
            if token in db_parties and token not in ("AC", "PC", "GE", "AE", "MH", "UP", "MP"):
                hints.append(f"[PARTY: use party LIKE '%{token}%' in SQL]")
                break

    # ── el_type resolution ────────────────────────────────────────────────────
    for phrase, eltype in ELTYPE_MAP.items():
        if phrase in lower:
            hints.append(f"[ELECTION TYPE: el_type='{eltype}' — do NOT use any other el_type value]")
            break

    # ── Geography hints ───────────────────────────────────────────────────────
    # If query mentions "AC-wise" in context of GE/PC → clarify
    if any(p in lower for p in ["ac-wise", "ac wise", "ac level", "assembly-wise", "segment wise"]):
        if any(p in lower for p in ["lok sabha", "general election", "pc", "parliamentary"]):
            hints.append("[GEO: AC-wise in GE context means filter el_type='GE' and show results grouped/ordered by ac_name]")

    # If query mentions leads/trails
    if any(p in lower for p in ["leads", "trails", "lead trail", "leading", "trailing"]):
        hints.append("[GEO: leads/trails means show top candidates per AC — winner (rank=1) and runner-up (rank=2) per ac_name]")

    # ── Place name fuzzy resolution ───────────────────────────────────────────
    hints += _fuzzy_resolve_places(user_query)

    if hints:
        return user_query + "\n" + "\n".join(hints)
    return user_query