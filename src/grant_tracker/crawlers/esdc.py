"""Crawler for the ESDC Funding Programs listing page.

Source: https://www.canada.ca/en/employment-social-development/services/funding/programs.html

Parses the server-rendered HTML listing of ~89 grant programs,
then optionally follows detail links for eligibility and contact info.
"""

from __future__ import annotations

import re

from bs4 import BeautifulSoup, Tag

from grant_tracker.crawlers.base import BaseCrawler
from grant_tracker.models import FundingLevel, Grant

LISTING_URL = (
    "https://www.canada.ca/en/employment-social-development/services/funding/programs.html"
)


class ESDCCrawler(BaseCrawler):
    name = "esdc"

    def __init__(self, *, fetch_details: bool = True, delay: float = 1.5) -> None:
        super().__init__(delay=delay)
        self.fetch_details = fetch_details

    async def crawl(self) -> list[Grant]:
        grants: list[Grant] = []

        async with self._make_client() as client:
            resp = await self._get(client, LISTING_URL)
            soup = BeautifulSoup(resp.text, "lxml")

            cards = soup.select("li.tagfilter-item")
            self.log.info("found program cards", count=len(cards))

            for card in cards:
                grant = self._parse_card(card)
                if grant is None:
                    continue

                if self.fetch_details and grant.url:
                    try:
                        await self._throttle()
                        detail_resp = await self._get(client, grant.url)
                        self._enrich_from_detail(grant, detail_resp.text)
                    except Exception:
                        self.log.warning("failed to fetch detail page", url=grant.url, exc_info=True)

                if not grant.raw_text:
                    grant.raw_text = card.get_text(" ", strip=True)[:5000]

                grants.append(grant)

        self.log.info("crawl complete", grants=len(grants))
        return grants

    def _parse_card(self, card: Tag) -> Grant | None:
        program_h4 = card.select_one("h4.small.text-muted")
        program_name = program_h4.get_text(strip=True) if program_h4 else ""

        link_tag = card.select_one("p.h4 a")
        if link_tag is None:
            return None
        title = link_tag.get_text(strip=True)
        url = link_tag.get("href", "")
        if isinstance(url, list):
            url = url[0]
        if url and not url.startswith("http"):
            url = "https://www.canada.ca" + url

        desc_p = card.select_one("p.h4 + p")
        description = desc_p.get_text(strip=True) if desc_p else ""

        funding_div = card.select_one("div.bg-info p.h4")
        funding_text = funding_div.get_text(strip=True) if funding_div else ""
        funding_min, funding_max = _parse_funding(funding_text)

        status = None
        deadline = None
        # The status li contains an icon <span> (empty) and a text <span>.
        # We need the text span — select all and pick the one with content.
        status_spans = card.select("li.text-success span") or card.select("ul.list-unstyled li span")
        for span in status_spans:
            raw = span.get_text(strip=True)
            if not raw:
                continue
            if "accepting" in raw.lower():
                status = raw
                deadline = _extract_deadline(raw)
                break
            if "not accepting" in raw.lower():
                status = raw
                break

        if status is None:
            tags = card.get("data-wb-tags", "")
            if isinstance(tags, list):
                tags = " ".join(tags)
            if "open" in tags:
                status = "Accepting applications"
            elif "closed" in tags:
                status = "Not accepting applications"

        source_id = _make_source_id(program_name, title)

        return Grant(
            title=title,
            organization=program_name or "ESDC",
            url=url,
            description=description,
            deadline=deadline,
            funding_min=funding_min,
            funding_max=funding_max,
            funding_level=FundingLevel.FEDERAL,
            source="esdc",
            source_id=source_id,
            status=status,
        )

    def _enrich_from_detail(self, grant: Grant, html: str) -> None:
        soup = BeautifulSoup(html, "lxml")
        main = soup.find("main") or soup
        text = main.get_text(" ", strip=True)

        grant.raw_text = text[:15000]

        eligibility = _extract_who_can_apply(text)
        if eligibility:
            grant.eligibility = eligibility

        contact = _extract_contact(text)
        if contact:
            grant.contact_info = contact


def _parse_funding(text: str) -> tuple[int | None, int | None]:
    """Extract min/max dollar amounts from a funding description string."""
    if not text:
        return None, None

    amounts = re.findall(r"\$\s*([\d,]+(?:\.\d+)?)", text)
    if not amounts:
        amount_words = re.findall(r"([\d,]+(?:\.\d+)?)\s*(?:million|Million)", text)
        parsed = []
        for a in amount_words:
            val = float(a.replace(",", "")) * 1_000_000
            parsed.append(int(val))
        if parsed:
            return min(parsed), max(parsed)
        return None, None

    parsed = []
    for a in amounts:
        val = a.replace(",", "")
        parsed.append(int(float(val)))

    if "million" in text.lower():
        parsed = [v * 1_000_000 if v < 1000 else v for v in parsed]

    if len(parsed) == 1:
        return None, parsed[0]
    return min(parsed), max(parsed)


def _extract_deadline(status_text: str) -> str | None:
    """Pull deadline info from status text like 'Accepting applications from Jan 16 until March 12, 2026'."""
    m = re.search(r"until\s+(.+?)(?:\s*$)", status_text, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    m = re.search(r"from\s+(.+?)(?:\s*$)", status_text, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return None


def _extract_who_can_apply(text: str) -> str | None:
    m = re.search(r"Who can apply\s*:?\s*(.+?)(?:Check your eligibility|Start questionnaire|Sections|$)", text, re.DOTALL)
    if m:
        snippet = m.group(1).strip()
        snippet = re.sub(r"\s+", " ", snippet)
        return snippet[:2000] if snippet else None
    return None


def _extract_contact(text: str) -> str | None:
    m = re.search(r"(?:Help and contact information|Contact information|General information)\s*(.+?)(?:Related links|Page details|Date modified|$)", text, re.DOTALL | re.IGNORECASE)
    if m:
        snippet = m.group(1).strip()
        snippet = re.sub(r"\s+", " ", snippet)
        return snippet[:2000] if snippet else None
    return None


def _make_source_id(program_name: str, title: str) -> str:
    raw = f"{program_name}|{title}" if program_name else title
    return re.sub(r"\s+", "-", raw.lower().strip())[:200]
