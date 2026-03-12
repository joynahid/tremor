"""
Article crawler — fetches and extracts text from Signal URLs.

Uses trafilatura for content extraction (strips nav, ads, footers).
Runs async with concurrency control to avoid hammering servers.

Skips:
  - HN discussion URLs (no article, just comments)
  - Reddit self-posts (body already in signal metadata if needed)
  - URLs that time out or return non-200
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import httpx
import trafilatura

from tremor.domain.models import CrawledContent, Signal, SourceType

MAX_CONCURRENT = 5       # parallel fetches
FETCH_TIMEOUT = 10.0     # seconds per request
MAX_BODY_CHARS = 8000    # truncate very long articles

SKIP_SOURCES = {SourceType.REDDIT}  # Reddit self-posts have no external article

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


class Crawler:
    def __init__(self, max_concurrent: int = MAX_CONCURRENT) -> None:
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def crawl_signals(self, signals: list[Signal]) -> dict[str, CrawledContent]:
        """
        Crawl all signals concurrently.
        Returns a dict of signal_id → CrawledContent.
        Signals without a crawlable URL are silently skipped.
        """
        targets = [s for s in signals if self._should_crawl(s)]

        async with httpx.AsyncClient(
            timeout=FETCH_TIMEOUT,
            headers=HEADERS,
            follow_redirects=True,
        ) as client:
            tasks = [self._crawl_one(client, signal) for signal in targets]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        crawled: dict[str, CrawledContent] = {}
        for signal, result in zip(targets, results):
            if isinstance(result, CrawledContent):
                crawled[signal.id] = result
            else:
                crawled[signal.id] = CrawledContent(
                    signal_id=signal.id,
                    url=signal.url or "",
                    title=None,
                    description=None,
                    body=None,
                    error=str(result),
                )
        return crawled

    async def _crawl_one(self, client: httpx.AsyncClient, signal: Signal) -> CrawledContent:
        async with self.semaphore:
            url = signal.url or ""
            try:
                resp = await client.get(url)
                resp.raise_for_status()
                html = resp.text
            except Exception as exc:
                return CrawledContent(
                    signal_id=signal.id,
                    url=url,
                    title=None,
                    description=None,
                    body=None,
                    error=f"fetch error: {exc}",
                )

            return _extract(signal.id, url, html)

    def _should_crawl(self, signal: Signal) -> bool:
        if not signal.url:
            return False
        # Skip HN discussion-only links (no external article)
        if signal.source == SourceType.HACKERNEWS and "news.ycombinator.com/item" in signal.url:
            return False
        # Reddit self-posts link back to Reddit — skip
        if signal.source in SKIP_SOURCES and "reddit.com" in signal.url:
            return False
        return True


def _extract(signal_id: str, url: str, html: str) -> CrawledContent:
    """Run trafilatura extraction on raw HTML."""
    # include_comments=False, include_tables=False keeps output clean
    body = trafilatura.extract(
        html,
        include_comments=False,
        include_tables=False,
        no_fallback=False,
        favor_precision=True,
    )

    # Also extract metadata (title, description) separately
    meta = trafilatura.extract_metadata(html)
    title = meta.title if meta else None
    description = meta.description if meta else None

    if body and len(body) > MAX_BODY_CHARS:
        body = body[:MAX_BODY_CHARS] + "…"

    return CrawledContent(
        signal_id=signal_id,
        url=url,
        title=title,
        description=description,
        body=body,
        crawled_at=datetime.now(timezone.utc),
        error=None if body else "trafilatura returned no content",
    )
