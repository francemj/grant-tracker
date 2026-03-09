"""LLM enrichment layer using Gemini 2.5 Flash.

Takes raw scraped grant text and produces clean, structured fields
via the Google GenAI SDK with structured output.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time

import click
import structlog
from google import genai

from grant_tracker.models import EnrichedFields, Grant

logger = structlog.get_logger()

MODEL = "gemini-2.5-flash"
BATCH_SIZE = 10
REQUEST_INTERVAL = 7.0  # seconds between API calls (keeps us under 10 RPM)
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 30.0
MAX_RETRY_DELAY = 120.0

SYSTEM_PROMPT = """\
You are a Canadian government grants analyst. You will receive raw scraped \
information about one or more grant programs. For each program, extract \
structured fields accurately.

Rules:
- funding_min / funding_max should be the actual program-level award range \
in CAD dollars. Ignore per-employee or per-unit caps — report the total \
amount an organization can receive. If only a maximum is stated, set \
funding_min to null. If amounts are unclear, set both to null.
- eligibility should list who can apply as concise bullet points \
(e.g. "- Not-for-profit organizations\\n- Indigenous organizations"). \
Ignore navigation text and boilerplate.
- contact_info should contain only an email address or phone number. \
Ignore generic "contact us" links.
- deadline should be the application deadline as a readable date string. \
Set to null for continuous intake or if not found.
- relevance_score: 1.0 = directly targets non-profits, 0.5 = non-profits \
are eligible among others, 0.0 = clearly not relevant to non-profits.
- funding_level: infer from the organization name and context.
"""


def _parse_retry_delay(error_text: str) -> float | None:
    """Extract retry delay from Gemini 429 error message."""
    match = re.search(r"retry in ([\d.]+)s", error_text, re.IGNORECASE)
    if match:
        return float(match.group(1))
    return None


class GeminiEnricher:
    def __init__(self) -> None:
        api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            raise RuntimeError(
                "Set GEMINI_API_KEY or GOOGLE_API_KEY environment variable"
            )
        self.client = genai.Client(api_key=api_key)
        self.log = logger.bind(enricher="gemini")
        self._last_request_time = 0.0

    async def _wait_for_rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        remaining = REQUEST_INTERVAL - elapsed
        if remaining > 0:
            await asyncio.sleep(remaining)
        self._last_request_time = time.monotonic()

    async def enrich_grants(self, grants: list[Grant]) -> list[Grant]:
        if not grants:
            return grants

        self.log.info("starting enrichment", total=len(grants))

        batches = [
            grants[i : i + BATCH_SIZE]
            for i in range(0, len(grants), BATCH_SIZE)
        ]

        total_batches = len(batches)
        results: list[Grant] = []

        for i, batch in enumerate(batches):
            click.echo(f"  Enriching batch {i + 1}/{total_batches} ({len(batch)} grants)...")
            enriched_batch = await self._enrich_batch_with_retry(batch, i)
            results.extend(enriched_batch)

        enriched_count = sum(1 for g in results if g.enriched)
        self.log.info("enrichment complete", enriched=enriched_count, total=len(results))
        return results

    async def _enrich_batch_with_retry(
        self, batch: list[Grant], batch_index: int
    ) -> list[Grant]:
        delay = INITIAL_RETRY_DELAY

        for attempt in range(MAX_RETRIES + 1):
            try:
                await self._wait_for_rate_limit()
                return await self._enrich_batch(batch)
            except Exception as exc:
                error_text = str(exc)
                is_rate_limit = "429" in error_text or "RESOURCE_EXHAUSTED" in error_text

                if not is_rate_limit or attempt == MAX_RETRIES:
                    self.log.error(
                        "batch failed, keeping raw data",
                        batch=batch_index,
                        attempt=attempt + 1,
                        error=error_text,
                    )
                    return batch

                retry_delay = _parse_retry_delay(error_text) or delay
                retry_delay = min(retry_delay, MAX_RETRY_DELAY)
                self.log.warning(
                    "rate limited, retrying",
                    batch=batch_index,
                    attempt=attempt + 1,
                    retry_in=f"{retry_delay:.0f}s",
                )
                click.echo(f"    Rate limited — waiting {retry_delay:.0f}s before retry...")
                await asyncio.sleep(retry_delay)
                delay = min(delay * 2, MAX_RETRY_DELAY)

        return batch  # unreachable, but satisfies type checker

    async def _enrich_batch(self, batch: list[Grant]) -> list[Grant]:
        prompt = self._build_prompt(batch)

        response = await asyncio.to_thread(
            self.client.models.generate_content,
            model=MODEL,
            contents=prompt,
            config={
                "response_mime_type": "application/json",
                "response_schema": list[EnrichedFields],
                "system_instruction": SYSTEM_PROMPT,
                "thinking_config": {"thinking_budget": 0},
            },
        )

        try:
            parsed_list = json.loads(response.text)
        except (json.JSONDecodeError, ValueError) as exc:
            self.log.warning("failed to parse LLM response", error=str(exc))
            return batch

        if not isinstance(parsed_list, list) or len(parsed_list) != len(batch):
            self.log.warning(
                "LLM returned wrong number of results",
                expected=len(batch),
                got=len(parsed_list) if isinstance(parsed_list, list) else "not a list",
            )
            return self._best_effort_apply(batch, parsed_list)

        return self._apply_enrichments(batch, parsed_list)

    def _build_prompt(self, batch: list[Grant]) -> str:
        parts = []
        for i, grant in enumerate(batch):
            text = grant.raw_text or grant.description
            parts.append(
                f"=== GRANT {i + 1} ===\n"
                f"Title: {grant.title}\n"
                f"Organization: {grant.organization}\n"
                f"URL: {grant.url}\n"
                f"Status: {grant.status or 'Unknown'}\n"
                f"Source: {grant.source}\n\n"
                f"Raw text:\n{text[:6000]}\n"
            )

        return (
            f"Extract structured information for the following {len(batch)} "
            f"Canadian government grant program(s). Return a JSON array with "
            f"exactly {len(batch)} objects, one per grant, in the same order.\n\n"
            + "\n".join(parts)
        )

    def _apply_enrichments(
        self, batch: list[Grant], parsed: list[dict]
    ) -> list[Grant]:
        results = []
        for grant, fields_dict in zip(batch, parsed):
            try:
                fields = EnrichedFields(**fields_dict)
                grant = self._merge(grant, fields)
            except Exception:
                self.log.warning("failed to validate enrichment", title=grant.title, exc_info=True)
            results.append(grant)
        return results

    def _best_effort_apply(
        self, batch: list[Grant], parsed: object
    ) -> list[Grant]:
        if not isinstance(parsed, list):
            return batch

        results = []
        for i, grant in enumerate(batch):
            if i < len(parsed) and isinstance(parsed[i], dict):
                try:
                    fields = EnrichedFields(**parsed[i])
                    grant = self._merge(grant, fields)
                except Exception:
                    pass
            results.append(grant)
        return results

    def _merge(self, grant: Grant, fields: EnrichedFields) -> Grant:
        grant.title = fields.title or grant.title
        grant.description = fields.description or grant.description
        grant.funding_min = fields.funding_min
        grant.funding_max = fields.funding_max
        grant.eligibility = fields.eligibility
        grant.deadline = fields.deadline
        grant.contact_info = fields.contact_info
        grant.funding_level = fields.funding_level
        grant.relevance_score = fields.relevance_score
        grant.enriched = True
        grant.raw_text_hash = grant.compute_raw_text_hash()
        return grant
