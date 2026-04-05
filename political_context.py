"""
political_context.py — Political conditioning for SQL generation.

ADD NEW POINTS FREELY in plain English under the relevant section.
These get injected into every SQL generation + verification call.
"""

# ── Core Electoral Context ────────────────────────────────────────────────────
ELECTORAL_CONTEXT = [
    "BJP first contested Maharashtra assembly elections in 1990, and Lok Sabha in 1984.",
    "Shiv Sena (SHS) and BJP were alliance partners in Maharashtra from 1989 to 2019.",
    "In 2019, Shiv Sena split from BJP alliance after seat-sharing dispute post election.",
    "NCP split in 2023 — Ajit Pawar faction joined NDA, Sharad Pawar faction stayed opposition.",
    "In 2024 MH data, NCP (Ajit Pawar) and NCP (SP) (Sharad Pawar) may appear as different abbreviations.",
    "MNS (Maharashtra Navnirman Sena) was founded by Raj Thackeray in 2006 after splitting from SHS.",
    "INC and NCP formed Congress-NCP alliance from 1999 onwards in Maharashtra.",
    "Mahayuti = BJP + SHS (Shinde) + NCP (Ajit) — ruling alliance in 2024.",
    "MVA = Maha Vikas Aghadi = INC + SHS (UBT) + NCP (SP) + SP (Samajwadi Party) — opposition alliance in 2024 MH.",
    "In 2024 data, alliance column is 'NDA', 'INDI', or NULL (for truly independent/unaligned candidates).",
    "SP (Samajwadi Party) contested 2024 MH AE as part of MVA/INDI — their alliance value is correctly set to 'INDI' in DB.",
    "Vidarbha region = Nagpur, Amravati divisions — historically Congress stronghold.",
    "Western Maharashtra = Pune, Satara, Kolhapur — NCP stronghold historically.",
    "Mumbai = mixed — SHS/BJP strong in North, INC/NCP competitive in South and East.",
    "Marathwada = Aurangabad, Latur divisions — competitive between Congress and BJP.",
    "Konkan = coastal belt — SHS traditionally strong.",
]

# ── Data & Schema Context ─────────────────────────────────────────────────────
DATA_CONTEXT = [
    "Post-2009 data (new dataset) is richer — has district, pc_name, zone, incumbency, alliance.",
    "Pre-2009 data (old dataset) has ac_name/ac_no for AE, pc_name for GE — no district or zone.",
    "2009 data comes from new dataset only — treat it as post-2009 quality.",
    "Incumbency is reliable only from 2014 onwards — never use incumbency filter for 2009 or earlier.",
    "NOTA was introduced in Indian elections in 2013 — present from 2014 AE onwards in this data.",
    "Alliance column is only populated for 2024 AE and 2024 GE — NULL for all other years.",
    "vote_share denominator includes NOTA votes — NOTA is a valid vote cast.",
    "margin and margin_pct are only on the winner row (won=1) — NULL for all other rows.",
    "deposit_saved = 1 if candidate got >= 1/6th of total_votes_cast, NULL for NOTA.",
    "For GE (Lok Sabha) post-2009, ac_no and ac_name show which AC within the PC voted how — this is AC-wise leads/trails data.",
    "Bypolls (el_type AE-BP or GE-BP) are special elections — exclude unless user explicitly asks.",
    "Old MH GE data goes back to 1951 — old MH AE data goes back to 1962.",
    "District column is populated for ALL post-2009 data (both AE and GE).",
    "PC name (pc_name) is populated for ALL post-2009 data (both AE and GE).",
    "AC name (ac_name) is populated for ALL post-2009 data (both AE and GE).",
    "AC to PC to District mapping is complete and reliable for all post-2009 data.",
]

# ── el_type Mapping — CRITICAL ─────────────────────────────────────────────────
ELTYPE_CONTEXT = [
    "el_type column has ONLY these valid values: 'AE', 'GE', 'AE-BP', 'GE-BP'. NO OTHER VALUES EXIST.",
    "NEVER use el_type='PC', 'LS', 'VS', 'AC', 'Parliament', 'Assembly' or any other string.",

    # GE synonyms — all mean el_type='GE'
    "GE synonyms (all → el_type='GE'): Lok Sabha, Loksabha, LS, Parliamentary election, Parliament election, PC election, PC-wise, PC seat, PC margin, General election, National election, MP election, MP seat, Member of Parliament, Central election, Federal election, Parliamentary constituency.",

    # AE synonyms — all mean el_type='AE'
    "AE synonyms (all → el_type='AE'): Vidhan Sabha, Vidhansabha, VS, Assembly election, Assembly seat, Assembly constituency, Assembly segment, AC election, AC-wise, AC seat, AC margin, State election, State assembly, Legislative assembly, State legislature, MLA election, MLA seat, Member of Legislative Assembly, Vidhayak.",

    "Bypoll = AE-BP (assembly bypoll) or GE-BP (Lok Sabha bypoll). Upchunav = bypoll = AE-BP.",
    "AC-wise leads in a PC/GE election means: el_type='GE', grouped by ac_name — shows how each assembly segment voted in the Lok Sabha election.",
]

# ── Geography & Mapping Rules ─────────────────────────────────────────────────
GEOGRAPHY_CONTEXT = [
    "District → PC → AC hierarchy is fully mapped in post-2009 data.",
    "One district can have multiple PCs. One PC has ~6 ACs. ACs belong to exactly one district and one PC.",
    "To get district-level data: filter by district column.",
    "To get PC-level data: filter by pc_name column.",
    "To get AC-level data: filter by ac_name column.",
    "To aggregate GE results by district: GROUP BY district WHERE el_type='GE'.",
    "To aggregate GE results by PC: GROUP BY pc_name WHERE el_type='GE'.",
    "To show AC-wise breakdown within a PC: filter pc_name='X' AND el_type='GE' — each row is one AC's result.",
    "For 'leads and trails' in GE: show top candidates per AC within a PC, ordered by ac_name and rank.",
    "District names in DB (exact case): Ahmednagar, Akola, Amravati, Aurangabad, Beed, Bhandara, Buldhana, Chandrapur, Dhule, Gadchiroli, Gondiya, Hingoli, Jalgaon, Jalna, Kolhapur, Latur, Mumbai City, Mumbai Suburban, Nagpur, Nanded, Nandurbar, Nashik, Osmanabad, Palghar, Parbhani, Pune, Raigad, Ratnagiri, Sangli, Satara, Sindhudurg, Solapur, Thane, Wardha, Washim, Yavatmal.",
    "Mumbai has TWO districts in DB: 'Mumbai City' and 'Mumbai Suburban' — when user says 'Mumbai', always filter BOTH using: district IN ('Mumbai City', 'Mumbai Suburban')",
]

# ── Query Patterns ────────────────────────────────────────────────────────────
QUERY_PATTERNS = [
    "When user asks about 'Mahayuti' → use alliance='NDA' AND year=2024.",
    "When user asks about 'MVA' or 'Maha Vikas Aghadi' or 'INDI' → use alliance='INDI' AND year=2024.",
    "When user asks for BOTH NDA and MVA results together → use alliance IN ('NDA','INDI') AND year=2024, NOT just one alliance. Return results for both.",
    "When counting seats by alliance in 2024 → GROUP BY alliance WHERE alliance IN ('NDA','INDI') — always show both in same query.",
    "When user asks about 'Shiv Sena' without faction → use LIKE '%SHS%'.",
    "When user asks about 'NCP' without faction → use LIKE '%NCP%'.",
    "When user asks 'AC-wise leads' in a PC → el_type='GE', filter by pc_name, show all candidates ranked per AC.",
    "When user asks about 'leads and trails' → show winner (rank=1) and runner-up (rank=2) per AC.",
    "When user asks about a district for GE → filter district='X' AND el_type='GE'.",
    "When user asks about 'how many seats' → COUNT(DISTINCT ac_name) for AE, COUNT(DISTINCT pc_name) for GE.",
    "When user asks about incumbency → only valid for year >= 2014.",
    "When user asks about bellwether → ac_name where winner party matches state-winning party across elections.",
    "When user asks about anti-incumbency → incumbency=1 AND won=0.",
    "When user asks about wasted votes → votes for losing candidates (won=0).",
    "When user asks about swing → self-join on ac_name+el_type for AE, or pc_name+el_type for GE.",
]

# ── Custom Points (add during testing) ────────────────────────────────────────
CUSTOM_POINTS = [
    # Add plain English observations here as you test
    # e.g. "In Nashik district, MNS has historically been strong in urban ACs.",
]


# ── Compiled output ────────────────────────────────────────────────────────────
def get_political_context() -> str:
    sections = {
        "ELECTORAL CONTEXT": ELECTORAL_CONTEXT,
        "DATA & SCHEMA NOTES": DATA_CONTEXT,
        "el_type MAPPING — CRITICAL": ELTYPE_CONTEXT,
        "GEOGRAPHY & AC-PC-DISTRICT MAPPING": GEOGRAPHY_CONTEXT,
        "QUERY PATTERN RULES": QUERY_PATTERNS,
        "CUSTOM NOTES": [p for p in CUSTOM_POINTS if p.strip()],
    }
    out = ["=== POLITICAL & DATA CONTEXT ===\n"]
    for section, points in sections.items():
        if points:
            out.append(f"-- {section} --")
            for p in points:
                out.append(f"- {p}")
            out.append("")
    return "\n".join(out)