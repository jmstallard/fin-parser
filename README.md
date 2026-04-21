# fin-parser

AI-powered financial filing parser — pulls 10-K/10-Q filings from SEC EDGAR,
extracts key metrics with Claude, and surfaces valuation signals and red flags.

## Architecture

```
Phase 1  Ingestion + extraction   EDGAR → pdfplumber → Claude → SQLite
Phase 2  Valuation + red flags    numpy-financial + Claude reasoning agents
Phase 3  Dashboard + comparables  Streamlit + embeddings
```

## Quick start

```bash
# 1. Clone and enter the project
git clone <your-repo>
cd fin-parser

# 2. Create a virtual environment
python -m venv .venv
source .venv/bin/activate

# 3. Install (editable mode so imports work during development)
pip install -e ".[dev]"

# 4. Set up environment variables
cp .env.example .env
# Edit .env — add your Anthropic API key and your name/email for EDGAR

# 5. Initialize the database
python -m fin_parser.db

# 6. Run the CLI
fin-parser fetch AAPL --form 10-K --limit 3
```

## Running tests

```bash
pytest
```

## Project structure

```
fin-parser/
├── fin_parser/
│   ├── config.py              # Env + settings
│   ├── db.py                  # SQLite schema + helpers
│   ├── cli.py                 # CLI entry point
│   ├── ingestion/
│   │   └── edgar.py           # EDGAR REST API client
│   ├── extraction/
│   │   └── claude_extractor.py  # Claude API → structured JSON (Phase 1)
│   ├── valuation/             # DCF, WACC, IRR (Phase 2)
│   ├── analysis/              # Red flag agent (Phase 2)
│   └── dashboard/             # Streamlit app (Phase 3)
├── data/
│   ├── raw/                   # Downloaded filings (gitignored)
│   └── processed/             # Intermediate JSON (gitignored)
├── tests/
├── .env.example
├── .gitignore
└── pyproject.toml
```

## Environment variables

| Variable | Required | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | Yes | Your Anthropic API key |
| `EDGAR_USER_AGENT` | Yes | `"Name email@example.com"` — required by SEC |
| `CLAUDE_MODEL` | No | Defaults to `claude-sonnet-4-20250514` |
| `DATA_DIR` | No | Defaults to `./data` |
| `DB_PATH` | No | Defaults to `./data/fin_parser.db` |
