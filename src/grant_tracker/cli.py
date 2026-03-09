from __future__ import annotations

import asyncio
import csv
import json
import sys
from pathlib import Path

import click
import structlog

from grant_tracker.db import DEFAULT_DB_PATH, GrantRepository
from grant_tracker.models import FundingLevel

structlog.configure(
    processors=[
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(0),
)

SOURCES = ("esdc", "ckan", "benefits-finder")


@click.group()
@click.option("--db", "db_path", type=click.Path(), default=str(DEFAULT_DB_PATH), help="Path to SQLite database file.")
@click.pass_context
def cli(ctx: click.Context, db_path: str) -> None:
    """Canadian grant tracker — aggregate federal grant data from multiple sources."""
    ctx.ensure_object(dict)
    ctx.obj["db_path"] = Path(db_path)


@cli.command()
@click.option("--source", type=click.Choice([*SOURCES, "all"]), default="all", help="Which crawler to run.")
@click.option("--no-details", is_flag=True, help="(ESDC) Skip fetching detail pages for faster crawling.")
@click.pass_context
def crawl(ctx: click.Context, source: str, no_details: bool) -> None:
    """Crawl grant sources, enrich with LLM, and store results."""
    db = GrantRepository(ctx.obj["db_path"])
    sources_to_run = SOURCES if source == "all" else (source,)

    total = 0
    for src in sources_to_run:
        click.echo(f"\n{'='*60}")
        click.echo(f"  Crawling: {src}")
        click.echo(f"{'='*60}")
        crawler = _make_crawler(src, no_details=no_details)
        grants = asyncio.run(crawler.crawl())
        count = db.upsert_many(grants)
        click.echo(f"  -> {count} grants upserted from {src}")
        total += count

    click.echo(f"\nCrawling complete. {total} total grants upserted.")

    unenriched = db.get_unenriched_grants()
    if unenriched:
        click.echo(f"\n{'='*60}")
        click.echo(f"  Enriching {len(unenriched)} grants with Gemini")
        click.echo(f"{'='*60}")
        try:
            from grant_tracker.enrichment import GeminiEnricher
            enricher = GeminiEnricher()
            enriched = asyncio.run(enricher.enrich_grants(unenriched))
            db.upsert_many(enriched)
            enriched_count = sum(1 for g in enriched if g.enriched)
            click.echo(f"  -> {enriched_count} grants enriched")
        except RuntimeError as exc:
            click.echo(f"  Skipping enrichment: {exc}")
        except Exception:
            click.echo("  Enrichment failed (see logs). Raw data was saved.")
            structlog.get_logger().error("enrichment failed", exc_info=True)
    else:
        click.echo("  All grants already enriched.")

    click.echo(f"\nDone. Database: {ctx.obj['db_path']}")
    click.echo(f"Total grants in database: {db.count()}")
    db.close()


@cli.command("list")
@click.option("--source", type=click.Choice(SOURCES), default=None, help="Filter by source.")
@click.option("--status", default=None, help="Filter by status (substring match).")
@click.option("--level", type=click.Choice([l.value for l in FundingLevel]), default=None, help="Filter by funding level.")
@click.option("--search", "keyword", default=None, help="Search title, description, org, eligibility.")
@click.option("--limit", type=int, default=50, help="Max results to display.")
@click.pass_context
def list_grants(ctx: click.Context, source: str | None, status: str | None, level: str | None, keyword: str | None, limit: int) -> None:
    """List grants stored in the database."""
    db = GrantRepository(ctx.obj["db_path"])

    if keyword:
        grants = db.search_grants(keyword)
    else:
        funding_level = FundingLevel(level) if level else None
        grants = db.get_grants(source=source, status=status, funding_level=funding_level)

    if not grants:
        click.echo("No grants found.")
        db.close()
        return

    for i, g in enumerate(grants[:limit]):
        click.echo(f"\n{'─'*60}")
        click.echo(f"  {g.title}")
        click.echo(f"  Org: {g.organization}")
        if g.url:
            click.echo(f"  URL: {g.url}")
        if g.status:
            click.echo(f"  Status: {g.status}")
        if g.deadline:
            click.echo(f"  Deadline: {g.deadline}")
        if g.funding_min or g.funding_max:
            lo = f"${g.funding_min:,}" if g.funding_min else "?"
            hi = f"${g.funding_max:,}" if g.funding_max else "?"
            click.echo(f"  Funding: {lo} – {hi}")
        if g.eligibility:
            click.echo(f"  Eligibility: {g.eligibility[:200]}")
        if g.description:
            click.echo(f"  Description: {g.description[:200]}")
        click.echo(f"  Source: {g.source} | Level: {g.funding_level.value}", nl=False)
        if g.enriched:
            click.echo(f" | Relevance: {g.relevance_score:.1f}", nl=False)
        click.echo()

    shown = min(len(grants), limit)
    click.echo(f"\n{'─'*60}")
    click.echo(f"Showing {shown} of {len(grants)} grants.")
    db.close()


@cli.command()
@click.option("--format", "fmt", type=click.Choice(["json", "csv"]), default="json", help="Export format.")
@click.option("--output", "-o", type=click.Path(), default=None, help="Output file path (default: stdout).")
@click.pass_context
def export(ctx: click.Context, fmt: str, output: str | None) -> None:
    """Export all grants as JSON or CSV."""
    db = GrantRepository(ctx.obj["db_path"])
    grants = db.get_grants()
    db.close()

    if not grants:
        click.echo("No grants to export.")
        return

    exclude_fields = {"raw_text", "raw_text_hash"}

    if fmt == "json":
        data = [g.model_dump(mode="json", exclude=exclude_fields) for g in grants]
        text = json.dumps(data, indent=2, ensure_ascii=False, default=str)
        if output:
            Path(output).write_text(text)
            click.echo(f"Exported {len(grants)} grants to {output}")
        else:
            click.echo(text)

    elif fmt == "csv":
        fields = [
            "title", "organization", "url", "description", "deadline",
            "funding_min", "funding_max", "eligibility", "funding_level",
            "contact_info", "source", "source_id", "status", "last_crawled",
            "enriched", "relevance_score",
        ]
        out = open(output, "w", newline="") if output else sys.stdout
        writer = csv.DictWriter(out, fieldnames=fields)
        writer.writeheader()
        for g in grants:
            row = g.model_dump(mode="json")
            writer.writerow({k: row.get(k, "") for k in fields})
        if output:
            out.close()
            click.echo(f"Exported {len(grants)} grants to {output}")


def _make_crawler(source: str, *, no_details: bool = False):
    if source == "esdc":
        from grant_tracker.crawlers.esdc import ESDCCrawler
        return ESDCCrawler(fetch_details=not no_details)
    elif source == "ckan":
        from grant_tracker.crawlers.ckan import CKANCrawler
        return CKANCrawler()
    elif source == "benefits-finder":
        from grant_tracker.crawlers.benefits_finder import BenefitsFinderCrawler
        return BenefitsFinderCrawler()
    else:
        raise click.BadParameter(f"Unknown source: {source}")
