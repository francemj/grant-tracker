"""Fetch live detail page content for grants that have a URL.

Used by refresh-details to update raw_text from program pages (e.g. Benefits Finder,
CKAN after URL resolution). Same extraction pattern as ESDC detail pages.
"""

from __future__ import annotations

import asyncio

import httpx
import structlog
from bs4 import BeautifulSoup

from grant_tracker.crawlers.base import USER_AGENT

logger = structlog.get_logger()
MAX_TEXT_LENGTH = 15_000


async def fetch_detail_text(
    url: str,
    *,
    timeout: float = 30.0,
    delay: float = 1.0,
) -> str | None:
    """Fetch URL and return main-body text, or None on error.

    Uses the same extraction as ESDC: main or full soup, get_text, truncated.
    Logs and returns None on HTTP errors, timeout, or parse failure so one
    bad URL does not stop the batch.
    """
    if delay > 0:
        await asyncio.sleep(delay)

    try:
        async with httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT},
            timeout=timeout,
            follow_redirects=True,
        ) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            html = resp.text
    except (httpx.HTTPError, httpx.TimeoutException) as exc:
        logger.warning("detail fetch failed", url=url, error=str(exc))
        return None

    try:
        soup = BeautifulSoup(html, "lxml")
        main = soup.find("main") or soup
        text = main.get_text(" ", strip=True)
        return text[:MAX_TEXT_LENGTH] if text else None
    except Exception as exc:
        logger.warning("detail parse failed", url=url, error=str(exc))
        return None
