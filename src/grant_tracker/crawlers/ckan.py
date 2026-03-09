"""Crawler for the CKAN Proactive Disclosure – Grants and Contributions API.

Queries awarded grants to non-profits, then aggregates by program name to
produce one Grant record per program with derived funding ranges.

API: https://open.canada.ca/data/en/api/3/action/datastore_search
Resource: 1d15a62f-5656-49ad-8c88-f40ce689d831
"""

from __future__ import annotations

import re
from collections import defaultdict

from grant_tracker.crawlers.base import BaseCrawler
from grant_tracker.models import FundingLevel, Grant

API_BASE = "https://open.canada.ca/data/en/api/3/action/datastore_search"
RESOURCE_ID = "1d15a62f-5656-49ad-8c88-f40ce689d831"

# Non-profit recipient type code in the CKAN schema
RECIPIENT_TYPE_NONPROFIT = "N"

PAGE_SIZE = 10000
# Only look at agreements that started recently
MIN_START_DATE = "2023-01-01"


class CKANCrawler(BaseCrawler):
    name = "ckan"

    def __init__(self, *, max_pages: int = 50, delay: float = 0) -> None:
        super().__init__(delay=delay)
        self.max_pages = max_pages

    async def crawl(self) -> list[Grant]:
        programs: dict[str, _ProgramAccumulator] = defaultdict(_ProgramAccumulator)

        async with self._make_client() as client:
            offset = 0
            total_fetched = 0

            for _ in range(self.max_pages):
                params = {
                    "resource_id": RESOURCE_ID,
                    "limit": PAGE_SIZE,
                    "offset": offset,
                    "sort": "agreement_start_date desc",
                    "filters": f'{{"recipient_type":"{RECIPIENT_TYPE_NONPROFIT}"}}',
                }
                resp = await self._get(client, API_BASE + "?" + _urlencode(params))
                data = resp.json()

                if not data.get("success"):
                    self.log.error("CKAN API returned failure", response=data)
                    break

                records = data["result"]["records"]
                if not records:
                    break

                page_has_recent = False
                for rec in records:
                    start_date = rec.get("agreement_start_date") or ""
                    if start_date < MIN_START_DATE:
                        continue

                    page_has_recent = True
                    prog_name = (rec.get("prog_name_en") or "").strip()
                    if not prog_name:
                        continue

                    acc = programs[prog_name]
                    acc.prog_name = prog_name
                    acc.add_record(rec)

                total_fetched += len(records)
                self.log.info("fetched page", offset=offset, records=len(records), total=total_fetched)

                # Sorted desc by date — if no records on this page met the
                # date threshold, all subsequent pages will be even older.
                if not page_has_recent:
                    self.log.info("reached records older than cutoff, stopping", cutoff=MIN_START_DATE)
                    break

                total = data["result"].get("total", 0)
                offset += PAGE_SIZE
                if offset >= total:
                    break

                await self._throttle()

        grants = [acc.to_grant() for acc in programs.values()]
        self.log.info("crawl complete", programs=len(grants))
        return grants


class _ProgramAccumulator:
    """Accumulates individual award records for a single program."""

    def __init__(self) -> None:
        self.prog_name: str = ""
        self.org_titles: set[str] = set()
        self.purposes: set[str] = set()
        self.descriptions: set[str] = set()
        self.amounts: list[int] = []
        self.latest_start: str = ""
        self.record_count: int = 0
        self.expected_results: set[str] = set()

    def add_record(self, rec: dict) -> None:
        self.record_count += 1

        org = (rec.get("owner_org_title") or "").split("|")[0].strip()
        if org:
            self.org_titles.add(org)

        purpose = (rec.get("prog_purpose_en") or "").strip()
        if purpose:
            self.purposes.add(purpose)

        desc = (rec.get("description_en") or "").strip()
        if desc:
            self.descriptions.add(desc)

        val_str = rec.get("agreement_value") or ""
        val = _parse_amount(val_str)
        if val is not None and val > 0:
            self.amounts.append(val)

        expected = (rec.get("expected_results_en") or "").strip()
        if expected:
            self.expected_results.add(expected)

        start = rec.get("agreement_start_date") or ""
        if start > self.latest_start:
            self.latest_start = start

    def to_grant(self) -> Grant:
        description = ""
        if self.purposes:
            description = max(self.purposes, key=len)
        elif self.descriptions:
            description = max(self.descriptions, key=len)

        funding_min = min(self.amounts) if self.amounts else None
        funding_max = max(self.amounts) if self.amounts else None

        org = ", ".join(sorted(self.org_titles)[:3])
        if len(self.org_titles) > 3:
            org += f" (+{len(self.org_titles) - 3} more)"

        source_id = re.sub(r"\s+", "-", self.prog_name.lower())[:200]

        raw_parts = [f"Program: {self.prog_name}"]
        if self.purposes:
            raw_parts.append(f"Purpose: {max(self.purposes, key=len)}")
        if self.descriptions:
            raw_parts.append(f"Description: {max(self.descriptions, key=len)}")
        if self.expected_results:
            raw_parts.append(f"Expected results: {max(self.expected_results, key=len)}")
        if self.amounts:
            raw_parts.append(
                f"Award amounts (CAD): min=${min(self.amounts):,}, "
                f"max=${max(self.amounts):,}, "
                f"count={len(self.amounts)} awards"
            )
        raw_parts.append(f"Organizations: {org}")
        raw_text = "\n".join(raw_parts)

        return Grant(
            title=self.prog_name,
            organization=org or "Government of Canada",
            url="",
            description=description[:4000],
            funding_min=funding_min,
            funding_max=funding_max,
            funding_level=FundingLevel.FEDERAL,
            source="ckan",
            source_id=source_id,
            status=f"Active ({self.record_count} recent awards to non-profits)",
            raw_text=raw_text[:10000],
        )


def _parse_amount(val: str) -> int | None:
    if not val:
        return None
    cleaned = val.replace(",", "").replace("$", "").strip()
    try:
        return int(float(cleaned))
    except (ValueError, TypeError):
        return None


def _urlencode(params: dict[str, object]) -> str:
    from urllib.parse import urlencode
    return urlencode(params)
