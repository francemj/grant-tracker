# grant-tracker

A CLI tool that aggregates Canadian government grant data from multiple federal sources, designed to help not-for-profit organizations discover funding opportunities.

## Data Sources

| Source | Type | Freshness | What it provides |
|--------|------|-----------|------------------|
| **CKAN Proactive Disclosure API** | Live JSON API | Current (includes 2026 data) | Which programs are actively funding non-profits, real award amounts |
| **ESDC Funding Programs** | HTML scraping | Current | Application status, deadlines, eligibility criteria |
| **Innovation Canada Benefits Finder** | XLSX download | Snapshot (July 2025) | Broad program catalog with descriptions and eligibility |

## Installation

```bash
pip install -e .
```

## LLM Enrichment

After crawling, grant data is automatically enriched using **Gemini 2.0 Flash** to produce clean structured fields (funding ranges, eligibility, deadlines, relevance scores) from the raw scraped text.

Set your API key:

```bash
export GEMINI_API_KEY="your-key-here"
```

If no key is set, crawling still works but enrichment is skipped.

## Usage

### Crawl grant data

```bash
# Run all crawlers (enrichment runs automatically)
grant-tracker crawl

# Run a specific source
grant-tracker crawl --source esdc
grant-tracker crawl --source ckan
grant-tracker crawl --source benefits-finder
```

### List grants

```bash
# List all grants
grant-tracker list

# Filter by status
grant-tracker list --status accepting

# Search by keyword
grant-tracker list --search "disability"
```

### Export data

```bash
# Export as JSON
grant-tracker export --format json

# Export as CSV
grant-tracker export --format csv
```

## Development

```bash
pip install -e ".[dev]"
pytest
```
