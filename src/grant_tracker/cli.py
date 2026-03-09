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
from grant_tracker.url_resolver import build_url_lookup, resolve_ckan_urls
from grant_tracker.detail_fetcher import fetch_detail_text

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

    ckan_grants = db.get_ckan_grants_without_url()
    if ckan_grants:
        click.echo(f"\n{'='*60}")
        click.echo("  Resolving CKAN URLs from ESDC and Benefits Finder")
        click.echo(f"{'='*60}")
        esdc_grants = db.get_grants(source="esdc")
        bf_grants = db.get_grants(source="benefits-finder")
        esdc_lookup = build_url_lookup(esdc_grants, source="esdc")
        bf_lookup = build_url_lookup(bf_grants, source="benefits-finder")
        updated = resolve_ckan_urls(ckan_grants, esdc_lookup, bf_lookup)
        if updated:
            db.upsert_many(updated)
        click.echo(f"  -> Resolved URLs for {len(updated)} of {len(ckan_grants)} CKAN grants.")

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


@cli.command("resolve-urls")
@click.pass_context
def resolve_urls(ctx: click.Context) -> None:
    """Resolve URLs for CKAN grants by matching to ESDC and Benefits Finder titles."""
    db = GrantRepository(ctx.obj["db_path"])

    esdc_grants = db.get_grants(source="esdc")
    bf_grants = db.get_grants(source="benefits-finder")
    esdc_lookup = build_url_lookup(esdc_grants, source="esdc")
    bf_lookup = build_url_lookup(bf_grants, source="benefits-finder")

    ckan_grants = db.get_ckan_grants_without_url()
    if not ckan_grants:
        click.echo("No CKAN grants without URL to resolve.")
        db.close()
        return

    updated = resolve_ckan_urls(ckan_grants, esdc_lookup, bf_lookup)
    if updated:
        db.upsert_many(updated)

    click.echo(f"Resolved URLs for {len(updated)} of {len(ckan_grants)} CKAN grants.")
    db.close()


REFRESH_SOURCES_DEFAULT = ("benefits-finder", "ckan", "esdc")


@cli.command("refresh-details")
@click.option("--source", "sources", type=click.Choice(SOURCES), multiple=True, help="Sources to refresh (default: all).")
@click.option("--no-enrich", is_flag=True, help="Only update raw_text; skip Gemini enrichment.")
@click.pass_context
def refresh_details(ctx: click.Context, sources: tuple[str, ...], no_enrich: bool) -> None:
    """Fetch live detail pages for grants with a URL and optionally re-enrich them."""
    db = GrantRepository(ctx.obj["db_path"])

    sources_to_refresh = REFRESH_SOURCES_DEFAULT if not sources else sources
    grants = db.get_grants_for_detail_refresh(sources=sources_to_refresh)

    if not grants:
        click.echo("No grants to refresh.")
        db.close()
        return

    click.echo(f"\nRefreshing details for {len(grants)} grants (sources: {', '.join(sources_to_refresh)})...")

    async def run_refresh() -> list:
        updated = []
        for i, grant in enumerate(grants):
            text = await fetch_detail_text(grant.url, delay=1.0)
            if text is not None:
                grant.raw_text = text
                grant.raw_text_hash = grant.compute_raw_text_hash()
                updated.append(grant)
            if (i + 1) % 10 == 0 or (i + 1) == len(grants):
                click.echo(f"  Fetched {i + 1} / {len(grants)}...")
        return updated

    updated = asyncio.run(run_refresh())

    if updated:
        db.upsert_many(updated)
    click.echo(f"  -> Refreshed {len(updated)} of {len(grants)} grants.")

    if updated and not no_enrich:
        click.echo(f"\n  Enriching {len(updated)} refreshed grants with Gemini...")
        try:
            from grant_tracker.enrichment import GeminiEnricher
            enricher = GeminiEnricher()
            enriched = asyncio.run(enricher.enrich_grants(updated))
            db.upsert_many(enriched)
            enriched_count = sum(1 for g in enriched if g.enriched)
            click.echo(f"  -> {enriched_count} grants enriched.")
        except RuntimeError as exc:
            click.echo(f"  Skipping enrichment: {exc}")
        except Exception:
            click.echo("  Enrichment failed (see logs). Raw data was saved.")
            structlog.get_logger().error("enrichment failed", exc_info=True)

    click.echo(f"\nDone. Database: {ctx.obj['db_path']}")
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
        if g.enriched:
            click.echo(f"  Accepting applications: {'Yes' if g.accepting_applications else 'No'}")
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
            "enriched", "relevance_score", "accepting_applications",
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
