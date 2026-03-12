"""
Twitter / X adapter.

Twitter API v2 requires a Bearer Token (Basic tier is free but rate-limited).
  https://developer.twitter.com/en/docs/twitter-api

This adapter searches recent tweets using query strings.
Typical queries for tech trends:
    "AI coding lang:en -is:retweet"
    "from:OpenAI OR from:Google OR from:Cloudflare"

Set TWITTER_BEARER_TOKEN in environment.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

from tremor.domain.models import Signal, SourceType
from .base import BaseAdapter

TWITTER_API_BASE = "https://api.twitter.com/2"
DEFAULT_MAX_RESULTS = 50  # 10–100 per request on Basic tier


class TwitterAdapter(BaseAdapter):
    source_type = SourceType.TWITTER

    def __init__(
        self,
        queries: list[str],
        bearer_token: str | None = None,
        max_results: int = DEFAULT_MAX_RESULTS,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.queries = queries
        self.bearer_token = bearer_token or os.getenv("TWITTER_BEARER_TOKEN")
        self.max_results = max_results

        if not self.bearer_token:
            raise ValueError(
                "TwitterAdapter requires a bearer token. "
                "Set TWITTER_BEARER_TOKEN env var or pass bearer_token=..."
            )

    def _auth_headers(self) -> dict:
        return {"Authorization": f"Bearer {self.bearer_token}"}

    async def fetch_raw(self) -> list[dict]:
        all_tweets: list[dict] = []
        for query in self.queries:
            tweets = await self._search(query)
            # Tag each tweet with the query that found it
            for t in tweets:
                t["_query"] = query
            all_tweets.extend(tweets)
        return all_tweets

    async def _search(self, query: str) -> list[dict]:
        params = {
            "query": query,
            "max_results": self.max_results,
            "tweet.fields": "created_at,public_metrics,author_id",
            "expansions": "author_id",
            "user.fields": "username",
        }
        resp = await self._client.get(
            f"{TWITTER_API_BASE}/tweets/search/recent",
            params=params,
            headers=self._auth_headers(),
        )
        resp.raise_for_status()
        body = resp.json()

        tweets = body.get("data", [])
        # Attach username from the includes.users expansion
        users = {u["id"]: u["username"] for u in body.get("includes", {}).get("users", [])}
        for tweet in tweets:
            tweet["_username"] = users.get(tweet.get("author_id"), "unknown")

        return tweets

    def normalize(self, raw: dict) -> Signal:
        metrics = raw.get("public_metrics", {})
        created_raw = raw.get("created_at", "")
        created_at = (
            datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
            if created_raw
            else datetime.now(timezone.utc)
        )

        # Score = likes + retweets (both are explicit positive signals)
        score = metrics.get("like_count", 0) + metrics.get("retweet_count", 0)
        comment_count = metrics.get("reply_count", 0)

        tweet_url = f"https://twitter.com/i/web/status/{raw['id']}"

        return Signal(
            source=SourceType.TWITTER,
            source_id=raw["id"],
            title=raw.get("text", "")[:280],  # tweet text as title
            url=tweet_url,
            author=raw.get("_username"),
            score=score,
            comment_count=comment_count,
            created_at=created_at,
            fetched_at=datetime.now(timezone.utc),
            tags=[raw.get("_query", "")],
            metadata={
                "like_count": metrics.get("like_count", 0),
                "retweet_count": metrics.get("retweet_count", 0),
                "reply_count": metrics.get("reply_count", 0),
                "quote_count": metrics.get("quote_count", 0),
                "author_id": raw.get("author_id"),
            },
        )
