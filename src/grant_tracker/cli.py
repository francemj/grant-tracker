from __future__ import annotations

import asyncio
import csv
import json
import os
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
@click.option("--no-enrich", is_flag=True, help="Skip Gemini enrichment (and filtering) during crawl.")
@click.option(
    "--enrich-after",
    is_flag=True,
    help="Crawl + write raw rows first, then run DB-based enrichment (safer on low-memory machines).",
)
@click.option(
    "--chunk-size",
    type=int,
    default=100 if os.environ.get("FLY_APP_NAME") else 200,
    show_default=True,
    help="SQLite upsert chunk size.",
)
@click.pass_context
def crawl(ctx: click.Context, source: str, no_details: bool, no_enrich: bool, enrich_after: bool, chunk_size: int) -> None:
    """Crawl grant sources, enrich with LLM, filter out non-grants, and store results."""
    db = GrantRepository(ctx.obj["db_path"])
    if chunk_size <= 0:
        raise click.BadParameter("--chunk-size must be >= 1")

    requested_sources = SOURCES if source == "all" else (source,)
    # Always crawl ESDC + Benefits Finder before CKAN so we can resolve CKAN URLs
    # using DB-backed lookups without holding everything in memory.
    sources_to_run = tuple(s for s in ("esdc", "benefits-finder", "ckan") if s in requested_sources)

    total_crawled = 0
    total_upserted = 0

    for src in sources_to_run:
        click.echo(f"\n{'='*60}")
        click.echo(f"  Crawling: {src}")
        click.echo(f"{'='*60}")

        crawler = _make_crawler(src, no_details=no_details)
        grants = asyncio.run(crawler.crawl())
        total_crawled += len(grants)
        click.echo(f"  -> {len(grants)} grants crawled from {src}")

        # If we're doing a DB-first seed, write raw rows now and defer enrichment.
        if no_enrich or enrich_after:
            total_upserted += db.upsert_many(grants, chunk_size=chunk_size)
            continue

        # Resolve CKAN URLs using DB-backed lookups (ESDC/BF should already be upserted).
        if src == "ckan" and any((not (g.url and g.url.strip())) for g in grants):
            click.echo(f"\n{'='*60}")
            click.echo("  Resolving CKAN URLs from DB (ESDC and Benefits Finder)")
            click.echo(f"{'='*60}")
            esdc_lookup = build_url_lookup(db.get_grants(source="esdc"), source="esdc")
            bf_lookup = build_url_lookup(db.get_grants(source="benefits-finder"), source="benefits-finder")
            updated = resolve_ckan_urls(grants, esdc_lookup, bf_lookup)
            click.echo(f"  -> Resolved URLs for {len(updated)} of {len(grants)} CKAN grants.")

        # Enrich per-source, then filter before writing to SQLite.
        click.echo(f"\n{'='*60}")
        click.echo(f"  Enriching {len(grants)} rows with Gemini ({src})")
        click.echo(f"{'='*60}")

        try:
            from grant_tracker.enrichment import GeminiEnricher

            enricher = GeminiEnricher()
            async def enrich_and_write() -> tuple[int, int]:
                kept_total = 0
                dropped_total = 0
                async for enriched_batch in enricher.enrich_grants_stream(grants):
                    only_grants = [g for g in enriched_batch if getattr(g, "is_applyable_grant", True)]
                    kept_total += len(only_grants)
                    dropped_total += len(enriched_batch) - len(only_grants)
                    if only_grants:
                        db.upsert_many(only_grants, chunk_size=chunk_size)
                return kept_total, dropped_total

            kept, dropped = asyncio.run(enrich_and_write())
        except RuntimeError as exc:
            click.echo(f"  Skipping enrichment (and filtering): {exc}")
            kept = len(grants)
            dropped = 0
            db.upsert_many(grants, chunk_size=chunk_size)
        except Exception:
            click.echo("  Enrichment failed (see logs). Proceeding without filtering.")
            structlog.get_logger().error("enrichment failed", exc_info=True)
            kept = len(grants)
            dropped = 0
            db.upsert_many(grants, chunk_size=chunk_size)

        click.echo(f"  -> Keeping {kept} applyable grants; dropped {dropped} non-grant rows.")
        total_upserted += kept

    click.echo(f"\nCrawling complete. {total_crawled} total rows crawled.")

    if enrich_after and not no_enrich:
        click.echo(f"\n{'='*60}")
        click.echo("  Enriching crawled rows from DB (enrich-after)")
        click.echo(f"{'='*60}")
        _run_enrich_for_sources(db, tuple(requested_sources), limit=None)

    click.echo(f"\nDone. {total_upserted} total grants upserted. Database: {ctx.obj['db_path']}")
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


@cli.command("enrich")
@click.option("--source", "sources", type=click.Choice(SOURCES), multiple=True, help="Only enrich these sources (default: all).")
@click.option("--limit", type=int, default=None, help="Max number of rows to enrich (default: all).")
@click.pass_context
def enrich(ctx: click.Context, sources: tuple[str, ...], limit: int | None) -> None:
    """Enrich existing DB rows with Gemini, without crawling.

    This is useful for retrying enrichment after quota/rate-limit issues.
    Non-grant rows (is_applyable_grant=False) are removed from the DB.
    """
    db = GrantRepository(ctx.obj["db_path"])
    try:
        _run_enrich_for_sources(db, sources, limit)
    finally:
        db.close()


def _run_enrich_for_sources(db: GrantRepository, sources: tuple[str, ...], limit: int | None) -> None:
    # Keep CKAN URLs up to date before enrichment (helps downstream detail refresh).
    esdc_grants = db.get_grants(source="esdc")
    bf_grants = db.get_grants(source="benefits-finder")
    esdc_lookup = build_url_lookup(esdc_grants, source="esdc")
    bf_lookup = build_url_lookup(bf_grants, source="benefits-finder")
    ckan_grants = db.get_ckan_grants_without_url()
    if ckan_grants:
        updated_urls = resolve_ckan_urls(ckan_grants, esdc_lookup, bf_lookup)
        if updated_urls:
            db.upsert_many(updated_urls)
        click.echo(f"Resolved CKAN URLs for {len(updated_urls)} of {len(ckan_grants)} rows.")

    sources_to_enrich = sources if sources else SOURCES
    unenriched = db.get_unenriched_grants_for_sources(sources_to_enrich)
    if limit is not None:
        unenriched = unenriched[:limit]

    if not unenriched:
        click.echo("No rows to enrich.")
        return

    try:
        from grant_tracker.enrichment import GeminiEnricher

        enricher = GeminiEnricher()
    except RuntimeError as exc:
        click.echo(f"Skipping enrichment: {exc}")
        return

    click.echo(f"Enriching {len(unenriched)} rows (sources: {', '.join(sources_to_enrich)})...")

    async def enrich_and_apply() -> tuple[int, int, int]:
        kept_total = 0
        dropped_total = 0
        deleted_total = 0

        async for enriched_batch in enricher.enrich_grants_stream(unenriched):
            only_grants = [g for g in enriched_batch if getattr(g, "is_applyable_grant", True)]
            dropped = [g for g in enriched_batch if not getattr(g, "is_applyable_grant", True)]

            if only_grants:
                db.upsert_many(only_grants)
                kept_total += len(only_grants)

            if dropped:
                dropped_total += len(dropped)
                deleted_total += db.delete_by_source_keys([(g.source, g.source_id) for g in dropped])

        return kept_total, dropped_total, deleted_total

    kept, dropped, deleted = asyncio.run(enrich_and_apply())
    if dropped:
        click.echo(f"Dropped {dropped} non-grant rows (deleted {deleted} from DB).")
    click.echo(f"Enriched and kept {kept} applyable grants.")


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
            async def enrich_and_write() -> int:
                enriched_count = 0
                async for enriched_batch in enricher.enrich_grants_stream(updated):
                    db.upsert_many(enriched_batch)
                    enriched_count += sum(1 for g in enriched_batch if g.enriched)
                return enriched_count

            enriched_count = asyncio.run(enrich_and_write())
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


@cli.command()
@click.option("--host", default="0.0.0.0", help="Bind host.")
@click.option("--port", default=8000, type=int, help="Bind port.")
@click.option("--reload", "use_reload", is_flag=True, help="Enable auto-reload for development.")
def web(host: str, port: int, use_reload: bool) -> None:
    """Start the web application."""
    import uvicorn
    uvicorn.run(
        "grant_tracker.web.app:create_app",
        factory=True,
        host=host,
        port=port,
        reload=use_reload,
    )


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
