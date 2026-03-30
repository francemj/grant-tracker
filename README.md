# Canadian Grant Finder

A web app and CLI pipeline that aggregates Canadian federal grant data, enriches it with LLM-extracted metadata, and helps organizations discover relevant funding opportunities through search, filtering, and a guided questionnaire.

**Live at [grant-finder.fly.dev](https://grant-finder.fly.dev)**

## How it works

1. **Crawl** — Three crawlers pull grant data from federal sources (ESDC, CKAN Open Data, Benefits Finder)
2. **Enrich** — Gemini 2.5 Flash extracts structured fields from raw text: funding ranges, eligibility, deadlines, categories, provinces, organization types, and relevance scores
3. **Serve** — A FastAPI web app lets users browse, filter, and discover grants matched to their profile

## Data Sources

| Source | Type | What it provides |
|--------|------|------------------|
| **CKAN Proactive Disclosure API** | Live JSON API | Active funding programs, real award amounts |
| **ESDC Funding Programs** | HTML scraping | Application status, deadlines, eligibility criteria |
| **Benefits Finder** | XLSX download | Broad program catalog with descriptions |

## Web App

The web interface provides:

- **Browse & search** with faceted filters (category, province, organization type, funding range, application status)
- **Grant detail pages** with full eligibility info, funding amounts, deadlines, and similar grants
- **Guided discovery** — a short questionnaire that matches grants to your organization's profile
- **HTMX-powered filtering** — no page reloads when adjusting filters

### Running locally

```bash
pip install -e .
grant-tracker web --port 8000
```

## CLI

### Crawl grant data

```bash
# Run all crawlers + enrichment
export GEMINI_API_KEY="your-key-here"
grant-tracker crawl

# Run a specific source
grant-tracker crawl --source esdc
```

### Enrich existing data

```bash
# Re-enrich unenriched rows
grant-tracker enrich

# Limit to specific sources
grant-tracker enrich --source ckan --source benefits-finder
```

### Refresh detail pages

Fetch live HTML for grants with URLs and re-enrich with updated text:

```bash
grant-tracker refresh-details
grant-tracker refresh-details --source benefits-finder --no-enrich
```

### List and export

```bash
grant-tracker list --search "disability"
grant-tracker export --format json -o grants.json
grant-tracker export --format csv -o grants.csv
```

## Deployment

The app runs on [Fly.io](https://fly.io) with a persistent volume for the SQLite database.

### Setup

```bash
fly launch --no-deploy
fly volumes create grants_data --size 1 --region yyz
fly secrets set GEMINI_API_KEY=your-key-here
fly deploy
```

### Seed the database

```bash
# Low-memory safe: crawl/write first, then enrich from DB in batches
fly ssh console -C "grant-tracker --db /data/grants.db crawl --enrich-after --chunk-size 100"

# If you still hit memory limits, skip enrichment during seeding and run it later
fly ssh console -C "grant-tracker --db /data/grants.db crawl --no-enrich --chunk-size 100"
```

### Automated updates

Two GitHub Actions workflows handle CI/CD:

- **Deploy on push** — every push to `main` triggers `fly deploy`
- **Weekly crawl** — runs every Monday at 6am UTC, explicitly wakes a scaled-to-zero Fly machine, runs `crawl --no-enrich`, then runs a standalone `enrich` pass

Both require a `FLY_API_TOKEN` repository secret.

## Tech Stack

- **Backend**: Python 3.11+, FastAPI, SQLite, Pydantic
- **Frontend**: Jinja2 templates, Tailwind CSS (CDN), HTMX
- **Enrichment**: Google Gemini 2.5 Flash
- **Crawling**: httpx, BeautifulSoup, openpyxl
- **Deployment**: Docker, Fly.io, GitHub Actions

## Development

```bash
pip install -e ".[dev]"
pytest
```
