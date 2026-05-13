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

# 6. Run the CLI — see "CLI commands" below for the full pipeline
fin-parser extract AAPL --form 10-K --limit 1
```

## CLI commands

The US pipeline is layered — `fetch`, `download`, and `extract` are not alternatives. Each one does everything the previous one does, then takes the next step:

| Command | Adds on top of the prior step | Hits network | Writes filing to disk | Calls Claude |
|---|---|---|---|---|
| `fetch` | Looks up filing metadata on EDGAR and saves it to the SQLite DB | yes | no | no |
| `download` | Also downloads the primary `.htm` document into `data/raw/` and records the local path | yes | yes | no |
| `extract` | Also runs the Claude extractor over the document and saves structured financial metrics | yes | yes | yes |
| `value` | Runs DCF + WACC valuation on the metrics already stored for a ticker | no | no | no |
| `redflag` | Runs the Claude red-flag agent on the metrics already stored for a ticker | no | no | yes |

In day-to-day use you only need `extract` — it subsumes `fetch` and `download`. The lower-level commands are there for inspecting what EDGAR returns before committing Claude API spend, or for debugging.

For Canadian issuers (SEDAR+ doesn't have an open submissions API), use `upload-ca` to register a local PDF instead of `fetch`/`download`/`extract`.

### Simple sequence for a US ticker

Using AAPL as an example:

```bash
# 1. Pull the latest 10-K, download it, and extract financial metrics with Claude.
#    (This one command replaces 'fetch' + 'download' + the Claude extraction step.)
fin-parser extract AAPL --form 10-K --limit 1

# 2. Run DCF + WACC valuation against the metrics just saved. Market cap in $M.
fin-parser value AAPL --market-cap 3200000

# 3. Run Claude red-flag analysis on the most recent extracted filing.
fin-parser redflag AAPL
```

If you only want to see what's available on EDGAR without burning API tokens, run `fin-parser fetch AAPL --form 10-K --limit 3` first — it lists and stores the filing metadata without downloading or parsing anything.

### Canadian filings (NI 43-101 / PEA / PFS / FS / MD&A)

```bash
fin-parser upload-ca \
    --ticker ABX --company "Barrick Gold" \
    --form "NI 43-101" --period 2025-03-31 \
    --pdf ~/Downloads/report.pdf
```

`upload-ca` registers the filing, auto-detects whether it's a mining technical report, and runs either the mining extractor or the standard financial extractor over the PDF. Once metrics are in the DB you can run `value` and `redflag` against the Canadian ticker the same way as US tickers.

## Dashboard

Once you've extracted at least one filing, launch the Streamlit dashboard to compare companies side by side:

```bash
streamlit run fin_parser/dashboard/app.py
```

It opens at http://localhost:8501 and reads directly from the SQLite DB — no extra config needed.

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
