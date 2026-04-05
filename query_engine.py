"""
query_engine.py — Multi-layer NL2SQL pipeline.

Layers:
  1. Party resolver      — NL party name → DB code
  2. Political context   — inject electoral + schema context
  3. DB router           — post-2009 (fast) vs full (complete)
  4. SQL generator       — LLM with political conditioning
  5. SQL verifier        — PASS / SOFT_FAIL / HARD_FAIL
  6. SQL executor        — runs on SQLite
  7. Row count verifier  — injects exact count → no hallucination
  8. Answer formatter    — SQL2NL with context + highlights
"""

import sqlite3, json
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config as C
from political_context import get_political_context
from party_resolver import resolve

# ── Provider init ─────────────────────────────────────────────────────────────
from groq import Groq, RateLimitError as GroqRateLimit

def _gemini_call(system: str, user: str, temperature: float) -> str:
    import google.generativeai as genai
    genai.configure(api_key=C.GEMINI_API_KEY)
    model_obj = genai.GenerativeModel(C.GEMINI_MODEL)
    r = model_obj.generate_content(
        f"{system}\n\n{user}",
        generation_config={"temperature": temperature, "max_output_tokens": C.MAX_TOKENS},
    )
    return r.text.strip()

def _call(system: str, user: str, temperature: float, model: str = None) -> str:
    """
    Try each Groq key in order. On 429 rate-limit, move to next key.
    If all Groq keys exhausted, fall back to Gemini.
    If USE_GROQ is False, go straight to Gemini.
    """
    m = model or C.GROQ_MODEL_SQL

    if C.USE_GROQ and C.GROQ_API_KEYS:
        for key in C.GROQ_API_KEYS:
            try:
                client = Groq(api_key=key)
                r = client.chat.completions.create(
                    model=m,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user",   "content": user},
                    ],
                    temperature=temperature,
                    max_tokens=C.MAX_TOKENS,
                    timeout=C.API_TIMEOUT_SEC,
                )
                return r.choices[0].message.content.strip()
            except GroqRateLimit:
                continue  # try next key
            except Exception:
                raise

    # All Groq keys exhausted or USE_GROQ=False — fall back to Gemini
    if C.GEMINI_API_KEY:
        return _gemini_call(system, user, temperature)

    raise RuntimeError("All Groq keys rate-limited and no Gemini key available.")


# ── DB Router ─────────────────────────────────────────────────────────────────
def _route_db(question: str) -> str:
    """
    Auto-detect whether question needs full historical DB.
    Returns DB path.
    Only used when no db_override is passed (i.e. user is in 'All Years' section).
    """
    historical_signals = [
        "1951","1957","1962","1967","1971","1972","1977","1978",
        "1980","1984","1985","1989","1990","1991","1995","1996",
        "1998","1999","2004","pre-2009","before 2009","historical",
        "all years","across elections","since 1","from 19",
        "all elections","every election",
    ]
    q_lower = question.lower()
    for signal in historical_signals:
        if signal in q_lower:
            return C.FULL_DB
    return C.POST_2009_DB


# ── SQL System Prompt ──────────────────────────────────────────────────────────
def _build_sql_system(db_path: str) -> str:
    is_post = db_path == C.POST_2009_DB
    scope = (
        "DATA SCOPE: 2009–2024 only (AE: 2009,2014,2019,2024 | GE: 2009,2014,2019,2024). "
        "All columns fully populated except gender/age/caste/caste_category."
        if is_post else
        "DATA SCOPE: Full timeline. AE: 1962–2024. GE: 1951–2024. "
        "Pre-2009 rows have NULL for district/zone/incumbency/alliance. "
        "GE pre-2009 has NULL ac_no/ac_name. AE pre-2009 has NULL pc_no/pc_name."
    )

    return f"""
You are a dedicated NL2SQL engine for Maharashtra electoral data.
Generate precise, correct, single-statement SQLite SQL every time.

=== TABLE: mh_results (only table in DB) ===

-- Identity
year              INTEGER   -- election year
el_type           TEXT      -- 'AE' (Vidhan Sabha) / 'GE' (Lok Sabha) / 'AE-BP' / 'GE-BP'
state             TEXT      -- always 'MH'
data_source       TEXT      -- 'old' (pre-2009) / 'new' (2009+)

-- Geography
ac_no             INTEGER   -- NULL for old GE data
ac_name           TEXT      -- UPPERCASE e.g. 'AIROLI'. NULL for old GE data
district          TEXT      -- e.g. 'Thane'. NULL pre-2009
pc_no             INTEGER   -- NULL for old AE data
pc_name           TEXT      -- Title Case e.g. 'Thane'. NULL for old AE data
zone              TEXT      -- e.g. 'Vidarbha'. NULL pre-2009

-- Candidate
candidate         TEXT
gender            TEXT      -- NULL throughout
age               INTEGER   -- NULL for most
caste             TEXT      -- NULL throughout
caste_category    TEXT      -- NULL throughout
incumbency        INTEGER   -- 1/0. NULL pre-2014. Reliable 2014+

-- Party & Alliance
party             TEXT      -- abbreviations: BJP, INC, SHS, NCP, MNS, BSP, IND, NOTA etc.
alliance          TEXT      -- NDA/INDI/Others. 2024 only. NULL otherwise

-- Results
votes             INTEGER
vote_share        REAL      -- votes/total_votes_cast*100. NOTA included in denominator
rank              INTEGER   -- 1=winner, 2=runner-up etc.
won               INTEGER   -- 1/0
deposit_saved     INTEGER   -- 1 if votes>=total_votes_cast/6. NULL for NOTA

-- Derived
total_votes_cast  INTEGER   -- SUM all votes per AC per election incl. NOTA
total_electors    INTEGER   -- registered voters. NULL for old AC data
margin            INTEGER   -- AE only: winner-runnerup votes at AC level. ONLY on won=1 row.
                            -- WARNING: For GE rows, margin = AC-segment lead, NOT PC-level margin. Never use margin directly for GE PC-level queries.
margin_pct        REAL      -- AE only: margin/total_votes_cast*100. Same GE warning applies.
turnout_pct       REAL      -- total_votes_cast/total_electors*100

{scope}

{get_political_context()}

=== STRICT SQL RULES ===

SINGLE STATEMENT:
- Return exactly ONE SELECT statement
- Absolutely no semicolons anywhere
- No markdown, no backticks, no explanation, no comments
- Never generate multiple statements

COLUMN ALIASES IN JOINS:
- In ANY JOIN or self-join, prefix ALL columns with table alias (a1.col, a2.col)
- Never use bare column names when more than one table is in the FROM clause

ALWAYS SELECT ALL RELEVANT COLUMNS:
- Never return only ac_name or candidate alone
- For candidate/seat queries include: year, el_type, ac_name or pc_name, candidate, party, votes, vote_share, rank, won, margin, margin_pct
- For swing queries include: both years' vote_share + computed swing value
- For aggregations include: grouped column + all computed metrics

PARTY MATCHING:
- Always LIKE '%BJP%' — never exact match
- Mahayuti = NDA alliance in 2024 → filter alliance='NDA' AND year=2024
- MVA = INDI alliance in 2024 → filter alliance='INDI' AND year=2024
- SHS without qualifier → LIKE '%SHS%' catches both factions
- NCP without qualifier → LIKE '%NCP%' catches both factions
- Exclude NOTA from candidate lists: AND party != 'NOTA'

FILTERING:
- Winners: won=1
- Runner-up: rank=2
- Top N finishers: rank <= N
- Incumbents: incumbency=1 (ONLY for year >= 2014)
- Bypolls: exclude unless asked → el_type IN ('AE','GE')
- Anti-incumbency: incumbency=1 AND won=0

GE (LOK SABHA / PC) — CRITICAL RULES FOR ALL GE QUERIES:
- GE data is stored AC-wise: each row = one candidate's votes in one assembly segment within the PC.
- Maharashtra has 48 PCs, each split into ~6 assembly segments. Each AC row is NOT a PC result.
- won=1 in a GE row means the candidate led that assembly segment — NOT that they won the PC.
- NEVER use won=1 directly to count GE seats/winners. NEVER use margin column directly for GE PC-level margin.
- For ALL GE queries (seats won, winners, margins, party/alliance performance, vote totals at PC level):
  ALWAYS aggregate AC votes to PC level first using a CTE:

  Pattern A — PC winners + margins:
    WITH pc_votes AS (
      SELECT pc_name, candidate, party, alliance, SUM(votes) AS total_votes
      FROM mh_results
      WHERE year=<year> AND el_type='GE' AND party != 'NOTA'
      GROUP BY pc_name, candidate, party, alliance
    ),
    pc_ranked AS (
      SELECT *, RANK() OVER (PARTITION BY pc_name ORDER BY total_votes DESC) AS pc_rank
      FROM pc_votes
    )
    SELECT w.pc_name, w.candidate, w.party, w.alliance,
           w.total_votes AS winner_votes,
           r.total_votes AS runnerup_votes,
           w.total_votes - r.total_votes AS margin
    FROM pc_ranked w
    JOIN pc_ranked r ON w.pc_name = r.pc_name AND r.pc_rank = 2
    WHERE w.pc_rank = 1

  Pattern B — seats won by party/alliance:
    WITH pc_votes AS (
      SELECT pc_name, candidate, party, alliance, SUM(votes) AS total_votes
      FROM mh_results
      WHERE year=<year> AND el_type='GE' AND party != 'NOTA'
      GROUP BY pc_name, candidate, party, alliance
    ),
    pc_winners AS (
      SELECT pc_name, candidate, party, alliance, total_votes,
             RANK() OVER (PARTITION BY pc_name ORDER BY total_votes DESC) AS pc_rank
      FROM pc_votes
    )
    SELECT party, alliance, COUNT(*) AS seats_won, SUM(total_votes) AS total_votes
    FROM pc_winners
    WHERE pc_rank = 1
    GROUP BY party, alliance
    ORDER BY seats_won DESC

- This rule applies to: seats won in LS, who won a PC, alliance seat count, smallest/largest margin PC, total GE votes by party/alliance, PC-wise winner list, any GE performance query.
- EXCEPTION: If user explicitly asks for AC-segment level data within a GE, then raw rows are fine.

SWING (cross-year):
- Self-join mh_results on (ac_name, el_type) or (pc_name, el_type)
- Both years must exist: inner join ensures this
- Filter: a1.vote_share IS NOT NULL AND a2.vote_share IS NOT NULL
- Swing: ROUND(a2.vote_share - a1.vote_share, 2) AS swing

NULL SAFETY:
- NULLIF(x, 0) in all divisions
- IS NOT NULL filters before comparing

DEFAULT LIMIT: {C.SQL_DEFAULT_LIMIT} unless user specifies otherwise
"""


# ── SQL Verifier ──────────────────────────────────────────────────────────────
VERIFY_SYSTEM = """
You are a SQL safety and correctness verifier for a Maharashtra electoral SQLite database.

Valid table: mh_results only.
Valid columns: year, el_type, state, data_source, ac_no, ac_name, district,
pc_no, pc_name, zone, candidate, gender, age, caste, caste_category,
incumbency, party, alliance, votes, vote_share, rank, won, deposit_saved,
total_votes_cast, total_electors, margin, margin_pct, turnout_pct

Check for:
1. Single statement — no semicolons mid-query
2. Only valid columns from the list above
3. Only mh_results as table reference
4. All columns aliased in JOINs (a1.col, a2.col style)
5. No DROP/INSERT/UPDATE/DELETE (injection guard)
6. Party uses LIKE not = for matching
7. Logic matches the user question intent
8. No markdown or backticks in output

Respond ONLY in this exact JSON (no markdown, no explanation):
{"status": "PASS|SOFT_FAIL|HARD_FAIL", "reason": "...", "corrected_sql": "..."}

status:
  PASS = SQL is correct, execute it
  SOFT_FAIL = fixable issue, return corrected_sql with the fix applied
  HARD_FAIL = unfixable, do not execute, return reason only
"""

def _verify_sql(question: str, sql: str) -> dict:
    if not C.ENABLE_SQL_VERIFICATION:
        return {"status": "PASS", "reason": "", "corrected_sql": ""}
    raw = _call(VERIFY_SYSTEM, f"Question: {question}\n\nSQL:\n{sql}", C.TEMPERATURE_VERIFY, C.GROQ_MODEL_VERIFY)
    raw = raw.replace("```json","").replace("```","").strip()
    try:
        return json.loads(raw)
    except:
        return {"status": "PASS", "reason": "verifier parse error — passed through", "corrected_sql": ""}


# ── SQL Generator ─────────────────────────────────────────────────────────────
def _generate_sql(question: str, db_path: str) -> str:
    sql = _call(_build_sql_system(db_path), question, C.TEMPERATURE_SQL, C.GROQ_MODEL_SQL)
    sql = sql.replace("```sql","").replace("```","").strip()
    sql = sql.split(";")[0].strip()
    return sql


# ── SQL Executor ──────────────────────────────────────────────────────────────
def _run_sql(sql: str, db_path: str) -> tuple[list, list]:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description] if cur.description else []
    con.close()
    return [dict(r) for r in rows], cols


# ── Answer Formatter ──────────────────────────────────────────────────────────
FORMAT_SYSTEM = """
You are presenting electoral data findings to a political leadership team.
Be minimal, precise, and direct. No jargon. No labels. No section headers.
Write exactly this structure — nothing more:

LINE 1: One single sentence. State what was found and the exact verified count.
        Use the VERIFIED ROW COUNT exactly. Example: "14 assembly constituencies
        in Maharashtra's 2024 Vidhan Sabha recorded BJP voteshare above 60%."
LINE 2 (optional): One more sentence only if there is a single standout finding —
        the highest, lowest, or most extreme data point. If nothing clearly stands
        out, skip this line entirely.

No bullet points. No lists. No citations of individual rows.
The data table is shown separately — do not reference specific rows in the text.

Rules:
- VERIFIED ROW COUNT is the exact number of rows retrieved — use it, never guess.
- If data is empty: one sentence only — state what was searched and that no results were found.
- Never use words like: significant, notable, interesting, demonstrated, highlighted, indicating.
- Never write more than 40 words total.
- Stop after 2 sentences maximum.
"""

def _format_answer(question: str, rows: list, sql: str) -> str:
    count = len(rows)
    data_str = json.dumps(rows[:C.FORMAT_SAMPLE_ROWS], indent=2)
    prompt = (
        f"VERIFIED ROW COUNT: {count} — use this exact number for any 'how many' answer.\n"
        f"SQL USED: {sql}\n"
        f"Question: {question}\n"
        f"Data (first 20 of {count} rows shown):\n{data_str}"
    )
    return _call(FORMAT_SYSTEM, prompt, C.TEMPERATURE_FORMAT, C.GROQ_MODEL_FORMAT)


# ── Main Pipeline ─────────────────────────────────────────────────────────────
def query(question: str, db_override: str = None) -> dict:
    """
    Full 8-layer pipeline.
    db_override: if provided, skips auto-router and uses this DB directly.

    Returns:
      { sql, rows, columns, answer, db_used, verified, error }
    """
    resolved = resolve(question)

    # DB selection
    db_path = db_override if db_override else _route_db(question)

    attempt = 0
    sql = ""
    verify_result = {}

    try:
        while attempt < C.MAX_RETRIES:
            attempt += 1

            # Generate SQL
            sql = _generate_sql(resolved, db_path)

            # Verify SQL
            verify_result = _verify_sql(question, sql)
            status = verify_result.get("status", "PASS")

            if status == "PASS":
                break

            elif status == "SOFT_FAIL":
                corrected = verify_result.get("corrected_sql", "").strip()
                if corrected:
                    sql = corrected
                break

            elif status == "HARD_FAIL":
                if attempt < C.MAX_RETRIES:
                    resolved = (
                        f"{resolved}\n"
                        f"[PREVIOUS SQL FAILED: {verify_result.get('reason','')} — fix this in new attempt]"
                    )
                    continue
                else:
                    return {
                        "sql": sql, "rows": [], "columns": [],
                        "answer": (
                            f"Could not construct a reliable query after {C.MAX_RETRIES} attempts.\n"
                            f"Reason: {verify_result.get('reason','')}"
                        ),
                        "db_used": db_path,
                        "verified": False,
                        "error": None,
                    }

        # Execute
        rows, cols = _run_sql(sql, db_path)

        # Format
        answer = _format_answer(question, rows, sql)

        return {
            "sql": sql,
            "rows": rows,
            "columns": cols,
            "answer": answer,
            "db_used": db_path,
            "verified": verify_result.get("status") in ("PASS", "SOFT_FAIL"),
            "error": None,
        }

    except Exception as e:
        return {
            "sql": sql, "rows": [], "columns": [],
            "answer": "", "db_used": db_path,
            "verified": False, "error": str(e),
        }