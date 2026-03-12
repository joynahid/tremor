"""
Reddit adapter.

Uses Reddit's public JSON API — no OAuth needed for read-only public subreddits.
Appends .json to any subreddit URL.

Usage:
    RedditAdapter(subreddits=["programming", "devops", "MachineLearning"])
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from tremor.domain.models import Signal, SourceType
from .base import BaseAdapter

REDDIT_BASE = "https://www.reddit.com"
DEFAULT_SUBREDDITS = ["programming", "devops", "MachineLearning", "startups"]
DEFAULT_LIMIT = 25   # posts per subreddit (max 100)

# Reddit blocks default httpx UA; spoof a browser UA
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; tremor-trend-bot/0.1; +https://github.com/byteskript/tremor)"
}


class RedditAdapter(BaseAdapter):
    source_type = SourceType.REDDIT

    def __init__(
        self,
        subreddits: list[str] = DEFAULT_SUBREDDITS,
        limit: int = DEFAULT_LIMIT,
        sort: str = "hot",   # hot | new | rising | top
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.subreddits = subreddits
        self.limit = limit
        self.sort = sort

    async def fetch_raw(self) -> list[dict]:
        tasks = [self._fetch_subreddit(sub) for sub in self.subreddits]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        items = []
        for sub, result in zip(self.subreddits, results):
            if isinstance(result, list):
                items.extend(result)
            else:
                print(f"[reddit] failed to fetch r/{sub}: {result}")
        return items

    async def _fetch_subreddit(self, subreddit: str) -> list[dict]:
        url = f"{REDDIT_BASE}/r/{subreddit}/{self.sort}.json?limit={self.limit}"
        resp = await self._client.get(url, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()
        posts = data["data"]["children"]
        # Tag each post with the subreddit it came from before normalizing
        for post in posts:
            post["data"]["_subreddit_fetched"] = subreddit
        return [post["data"] for post in posts]

    def normalize(self, raw: dict) -> Signal:
        created_utc = raw.get("created_utc", 0)

        return Signal(
            source=SourceType.REDDIT,
            source_id=raw["id"],
            title=raw.get("title", ""),
            url=raw.get("url"),
            author=raw.get("author"),
            score=raw.get("score", 0),
            comment_count=raw.get("num_comments", 0),
            created_at=datetime.fromtimestamp(created_utc, tz=timezone.utc),
            fetched_at=datetime.now(timezone.utc),
            tags=[raw.get("_subreddit_fetched", raw.get("subreddit", ""))],
            metadata={
                "subreddit": raw.get("subreddit"),
                "upvote_ratio": raw.get("upvote_ratio"),
                "flair": raw.get("link_flair_text"),
                "is_self": raw.get("is_self", False),  # text post vs link
                "permalink": f"https://reddit.com{raw.get('permalink', '')}",
            },
        )
