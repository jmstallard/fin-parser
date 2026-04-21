"""
fin_parser/config.py
Central config: loads .env, exposes typed settings used across all modules.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from project root (works from any working directory)
_ROOT = Path(__file__).parent.parent
load_dotenv(_ROOT / ".env")


def _require(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{key}' is not set. "
            f"Copy .env.example → .env and fill it in."
        )
    return value


# ── Anthropic ──────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY: str = _require("ANTHROPIC_API_KEY")
CLAUDE_MODEL: str = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-20250514")

# ── EDGAR ──────────────────────────────────────────────────────────────────
# SEC requires a descriptive User-Agent: "Name email@example.com"
EDGAR_USER_AGENT: str = _require("EDGAR_USER_AGENT")
EDGAR_BASE_URL: str = "https://data.sec.gov"
EDGAR_RATE_LIMIT_DELAY: float = 0.11  # SEC asks for max ~10 req/s

# ── Storage ────────────────────────────────────────────────────────────────
DATA_DIR: Path = Path(os.getenv("DATA_DIR", str(_ROOT / "data")))
RAW_DIR: Path = DATA_DIR / "raw"
PROCESSED_DIR: Path = DATA_DIR / "processed"
DB_PATH: Path = Path(os.getenv("DB_PATH", str(DATA_DIR / "fin_parser.db")))

# Ensure directories exist at import time
for _dir in (RAW_DIR, PROCESSED_DIR):
    _dir.mkdir(parents=True, exist_ok=True)
