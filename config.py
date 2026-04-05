"""
config.py — Single source of truth for all tunable parameters.
To switch provider: set USE_GROQ = True/False. That's it.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════════════════════════════════════
# PROVIDER TOGGLE
# ══════════════════════════════════════════════════════════════════════════════
USE_GROQ = True         # True = Groq primary | False = Gemini only

# ══════════════════════════════════════════════════════════════════════════════
# API KEYS
# Works both locally (.env) and on Streamlit Cloud (st.secrets)
# ══════════════════════════════════════════════════════════════════════════════
def _get(key: str) -> str:
    try:
        import streamlit as st
        val = st.secrets.get(key)
        if val:
            return val
    except Exception:
        pass
    return os.getenv(key)

# Groq keys tried in order: GROQ_API_KEY_1, _2, _3 ... then GROQ_API_KEY fallback
GROQ_API_KEYS = [v for v in (
    _get(f"GROQ_API_KEY_{i}") for i in range(1, 6)
) if v] or ([_get("GROQ_API_KEY")] if _get("GROQ_API_KEY") else [])

GROQ_API_KEY   = GROQ_API_KEYS[0] if GROQ_API_KEYS else None   # primary (compat)
GEMINI_API_KEY = _get("GEMINI_API_KEY")

if USE_GROQ and not GROQ_API_KEYS and not GEMINI_API_KEY:
    raise RuntimeError("No API keys found. Add GROQ_API_KEY_1 to .env or Streamlit secrets.")

# ══════════════════════════════════════════════════════════════════════════════
# MODELS
# ══════════════════════════════════════════════════════════════════════════════
GROQ_MODEL_SQL    = "llama-3.3-70b-versatile"   # SQL generation
GROQ_MODEL_VERIFY = "llama-3.3-70b-versatile"   # SQL verification
GROQ_MODEL_FORMAT = "llama-3.3-70b-versatile"   # answer formatting
GEMINI_MODEL      = "gemini-2.0-flash"           # Gemini fallback

# ══════════════════════════════════════════════════════════════════════════════
# GENERATION PARAMETERS
# ══════════════════════════════════════════════════════════════════════════════
MAX_TOKENS          = 1024   # hard cap for ALL LLM calls (SQL + verify + format)
TEMPERATURE_SQL     = 0.1    # low = deterministic SQL
TEMPERATURE_VERIFY  = 0.0    # zero = strict verification
TEMPERATURE_FORMAT  = 0.3    # slight variation allowed in prose
API_TIMEOUT_SEC     = 30     # seconds before Groq request times out

# ══════════════════════════════════════════════════════════════════════════════
# SQL PIPELINE
# ══════════════════════════════════════════════════════════════════════════════
ENABLE_SQL_VERIFICATION = True   # False = skip verify layer (faster, less safe)
MAX_RETRIES             = 2      # max SQL regeneration attempts on HARD_FAIL
SQL_DEFAULT_LIMIT       = 50     # LIMIT injected when user doesn't specify

# ══════════════════════════════════════════════════════════════════════════════
# ANSWER FORMATTER
# ══════════════════════════════════════════════════════════════════════════════
FORMAT_SAMPLE_ROWS  = 20   # how many rows are passed to the LLM for formatting
                           # (full table shown in UI regardless)

# ══════════════════════════════════════════════════════════════════════════════
# DATABASES
# Two DBs for latency optimisation:
#   POST_2009_DB — rich new data (2009–2024), all columns populated
#   FULL_DB      — complete timeline (1951–2024), fewer columns pre-2009
# ══════════════════════════════════════════════════════════════════════════════
POST_2009_DB = "electoral_mh_post2009.db"
FULL_DB      = "electoral_mh.db"

# ══════════════════════════════════════════════════════════════════════════════
# APP
# ══════════════════════════════════════════════════════════════════════════════
APP_TITLE        = "Maharashtra Electoral Intelligence"
MAX_ROWS_DISPLAY = 10000   # max rows Streamlit will render in the data table

# Legacy alias (kept so existing code that reads C.TEMPERATURE doesn't break)
TEMPERATURE = TEMPERATURE_SQL
