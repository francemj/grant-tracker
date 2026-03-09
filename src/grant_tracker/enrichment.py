"""LLM enrichment layer using Gemini 2.0 Flash.

Takes raw scraped grant text and produces clean, structured fields
via the Google GenAI SDK with structured output.
"""

from __future__ import annotations

import asyncio
import json
import os

import structlog
from google import genai

from grant_tracker.models import EnrichedFields, Grant

logger = structlog.get_logger()

MODEL = "gemini-2.0-flash"
BATCH_SIZE = 5
MAX_CONCURRENT = 5

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


class GeminiEnricher:
    def __init__(self) -> None:
        api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            raise RuntimeError(
                "Set GEMINI_API_KEY or GOOGLE_API_KEY environment variable"
            )
        self.client = genai.Client(api_key=api_key)
        self.log = logger.bind(enricher="gemini")

    async def enrich_grants(self, grants: list[Grant]) -> list[Grant]:
        if not grants:
            return grants

        self.log.info("starting enrichment", total=len(grants))

        batches = [
            grants[i : i + BATCH_SIZE]
            for i in range(0, len(grants), BATCH_SIZE)
        ]

        semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        results: list[Grant] = []

        async def process_batch(batch: list[Grant]) -> list[Grant]:
            async with semaphore:
                return await self._enrich_batch(batch)

        tasks = [process_batch(batch) for batch in batches]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(batch_results):
            if isinstance(result, Exception):
                self.log.error("batch failed, keeping raw data", batch=i, error=str(result))
                results.extend(batches[i])
            else:
                results.extend(result)

        enriched_count = sum(1 for g in results if g.enriched)
        self.log.info("enrichment complete", enriched=enriched_count, total=len(results))
        return results

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
