"""
Hacker News adapter.

Uses the official HN Firebase REST API (no auth required):
  https://hacker-news.firebaseio.com/v0/

Fetches top N stories and normalizes each into a Signal.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from tremor.domain.models import Signal, SourceType
from .base import BaseAdapter

HN_BASE = "https://hacker-news.firebaseio.com/v0"
DEFAULT_FETCH_COUNT = 50      # top 50 stories
MAX_CONCURRENT_ITEMS = 10     # parallel item fetches


class HackerNewsAdapter(BaseAdapter):
    source_type = SourceType.HACKERNEWS

    def __init__(self, fetch_count: int = DEFAULT_FETCH_COUNT, **kwargs) -> None:
        super().__init__(**kwargs)
        self.fetch_count = fetch_count

    async def fetch_raw(self) -> list[dict]:
        # Step 1: get ordered list of top story IDs
        resp = await self._client.get(f"{HN_BASE}/topstories.json")
        resp.raise_for_status()
        story_ids: list[int] = resp.json()[: self.fetch_count]

        # Step 2: fetch each item in parallel (batched to avoid hammering the API)
        items = []
        for batch_start in range(0, len(story_ids), MAX_CONCURRENT_ITEMS):
            batch = story_ids[batch_start : batch_start + MAX_CONCURRENT_ITEMS]
            tasks = [self._fetch_item(item_id) for item_id in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, dict):
                    items.append(r)
        return items

    async def _fetch_item(self, item_id: int) -> dict:
        resp = await self._client.get(f"{HN_BASE}/item/{item_id}.json")
        resp.raise_for_status()
        return resp.json()

    def normalize(self, raw: dict) -> Signal:
        # HN 'Ask HN' and 'Show HN' have no external URL — use the HN item URL
        url = raw.get("url") or f"https://news.ycombinator.com/item?id={raw['id']}"

        return Signal(
            source=SourceType.HACKERNEWS,
            source_id=str(raw["id"]),
            title=raw.get("title", ""),
            url=url,
            author=raw.get("by"),
            score=raw.get("score", 0),
            comment_count=raw.get("descendants", 0),
            created_at=datetime.fromtimestamp(raw["time"], tz=timezone.utc),
            fetched_at=datetime.now(timezone.utc),
            tags=_extract_tags(raw.get("title", "")),
            metadata={
                "type": raw.get("type"),          # "story" | "job" | "ask" | "show"
                "kids": raw.get("kids", []),      # top-level comment IDs
            },
        )


def _extract_tags(title: str) -> list[str]:
    """
    Lightweight tag extraction from HN title prefixes.
    HN convention: 'Ask HN:', 'Show HN:', 'Tell HN:', 'Launch HN:'
    """
    tags = []
    title_lower = title.lower()
    for prefix in ("ask hn", "show hn", "tell hn", "launch hn"):
        if title_lower.startswith(prefix):
            tags.append(prefix.replace(" ", "_"))
            break
    return tags
