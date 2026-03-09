from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod

import httpx
import structlog

from grant_tracker.models import Grant

logger = structlog.get_logger()

USER_AGENT = "grant-tracker/0.1 (+https://github.com/grant-tracker; Canadian grant aggregator)"
DEFAULT_TIMEOUT = 30.0
DEFAULT_DELAY = 1.5  # seconds between requests to government sites


class BaseCrawler(ABC):
    """Abstract base for all grant crawlers."""

    name: str = "base"

    def __init__(self, *, delay: float = DEFAULT_DELAY) -> None:
        self.delay = delay
        self.log = logger.bind(crawler=self.name)

    @abstractmethod
    async def crawl(self) -> list[Grant]:
        ...

    def _make_client(self, **kwargs: object) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT},
            timeout=DEFAULT_TIMEOUT,
            follow_redirects=True,
            **kwargs,
        )

    async def _throttle(self) -> None:
        if self.delay > 0:
            await asyncio.sleep(self.delay)

    async def _get(self, client: httpx.AsyncClient, url: str) -> httpx.Response:
        self.log.debug("fetching", url=url)
        resp = await client.get(url)
        resp.raise_for_status()
        return resp
