"""Crawler for the Innovation Canada Business Benefits Finder XLSX dataset.

Downloads the latest snapshot from the Open Government Portal and parses
program descriptions. Provides broad catalog coverage but no funding amounts,
deadlines, or eligibility criteria.

Dataset: https://open.canada.ca/data/en/dataset/4e75337e-70d0-4ed7-92d1-3b85192ec6b1
"""

from __future__ import annotations

import io
import re
from enum import Enum

import openpyxl

from grant_tracker.crawlers.base import BaseCrawler
from grant_tracker.models import FundingLevel, Grant

CKAN_DATASET_ID = "4e75337e-70d0-4ed7-92d1-3b85192ec6b1"
CKAN_PACKAGE_URL = (
    f"https://open.canada.ca/data/en/api/3/action/package_show?id={CKAN_DATASET_ID}"
)

# Column indices (0-based) in the XLSX
COL_TITLE_EN = 0
COL_SHORT_DESC_EN = 2
COL_LONG_DESC_EN = 4
COL_ORG_EN = 6
COL_ORG_URL_EN = 8

# Row 1 = English headers, Row 2 = French header translations, Row 3+ = data
DATA_START_ROW = 3

FEDERAL_ORG_PREFIX = "Government of Canada"


class FederalFilter(str, Enum):
    FEDERAL_ONLY = "federal_only"
    ALL = "all"


class BenefitsFinderCrawler(BaseCrawler):
    name = "benefits-finder"

    def __init__(
        self,
        *,
        federal_filter: FederalFilter = FederalFilter.FEDERAL_ONLY,
        delay: float = 0,
    ) -> None:
        super().__init__(delay=delay)
        self.federal_filter = federal_filter

    async def crawl(self) -> list[Grant]:
        xlsx_url = await self._resolve_latest_xlsx_url()
        self.log.info("downloading XLSX", url=xlsx_url)

        async with self._make_client() as client:
            resp = await self._get(client, xlsx_url)

        wb = openpyxl.load_workbook(io.BytesIO(resp.content), read_only=True)
        ws = wb.active

        grants: list[Grant] = []
        for row in ws.iter_rows(min_row=DATA_START_ROW, values_only=True):
            grant = self._parse_row(row)
            if grant is not None:
                grants.append(grant)

        wb.close()
        self.log.info("crawl complete", grants=len(grants))
        return grants

    async def _resolve_latest_xlsx_url(self) -> str:
        """Fetch the CKAN package metadata to find the most recent XLSX resource."""
        async with self._make_client() as client:
            resp = await self._get(client, CKAN_PACKAGE_URL)
            data = resp.json()

        resources = data["result"]["resources"]
        xlsx_resources = [
            r for r in resources
            if (r.get("format") or "").upper() in ("XLSX", "XLS")
               or r.get("url", "").endswith(".xlsx")
        ]

        if not xlsx_resources:
            raise RuntimeError("No XLSX resource found in CKAN dataset")

        # Pick the most recently created/modified resource
        xlsx_resources.sort(key=lambda r: r.get("created", ""), reverse=True)
        return xlsx_resources[0]["url"]

    def _parse_row(self, row: tuple) -> Grant | None:
        title = (row[COL_TITLE_EN] or "").strip() if len(row) > COL_TITLE_EN else ""
        if not title:
            return None

        org = (row[COL_ORG_EN] or "").strip() if len(row) > COL_ORG_EN else ""

        if self.federal_filter == FederalFilter.FEDERAL_ONLY:
            if not org.startswith(FEDERAL_ORG_PREFIX):
                return None
            funding_level = FundingLevel.FEDERAL
        else:
            funding_level = _infer_funding_level(org)

        short_desc = (row[COL_SHORT_DESC_EN] or "").strip() if len(row) > COL_SHORT_DESC_EN else ""
        long_desc = (row[COL_LONG_DESC_EN] or "").strip() if len(row) > COL_LONG_DESC_EN else ""
        description = long_desc or short_desc

        url = (row[COL_ORG_URL_EN] or "").strip() if len(row) > COL_ORG_URL_EN else ""

        source_id = re.sub(r"\s+", "-", title.lower())[:200]

        raw_text = f"Title: {title}\nOrganization: {org}\n"
        if short_desc:
            raw_text += f"Short description: {short_desc}\n"
        if long_desc:
            raw_text += f"Long description: {long_desc}\n"

        return Grant(
            title=title,
            organization=org,
            url=url,
            description=description,
            funding_level=funding_level,
            source="benefits-finder",
            source_id=source_id,
            status="Snapshot (may be outdated)",
            raw_text=raw_text[:5000],
        )


def _infer_funding_level(org: str) -> FundingLevel:
    if org.startswith(FEDERAL_ORG_PREFIX):
        return FundingLevel.FEDERAL

    provincial_keywords = [
        "Government of Alberta", "Government of Ontario",
        "Government of British Columbia", "Government of Manitoba",
        "Government of Saskatchewan", "Government of Nova Scotia",
        "Government of New Brunswick", "Government of Newfoundland",
        "Government of Prince Edward Island",
        "Government of Quebec", "Gouvernement du Québec",
        "Government of Northwest Territories",
        "Government of Nunavut", "Government of Yukon",
    ]
    for kw in provincial_keywords:
        if kw in org:
            return FundingLevel.PROVINCIAL

    return FundingLevel.PRIVATE
