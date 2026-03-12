"""
Core domain models for Tremor.

All adapters normalize into these structures.
The scoring and pipeline layers only ever touch these — never raw platform data.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class SourceType(str, Enum):
    HACKERNEWS = "hackernews"
    REDDIT = "reddit"
    TWITTER = "twitter"
    GITHUB = "github"
    PRODUCT_HUNT = "product_hunt"


@dataclass
class Signal:
    """
    Normalized unit of discussion from any platform.

    Every adapter produces these. Nothing downstream knows about
    HN items, Reddit posts, or tweets — only Signals.
    """

    source: SourceType
    source_id: str         # platform's native ID
    title: str
    url: str | None
    author: str | None
    score: int             # upvotes / likes / stars / points
    comment_count: int     # replies / descendants / comments
    created_at: datetime
    fetched_at: datetime
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def id(self) -> str:
        return f"{self.source.value}:{self.source_id}"

    @property
    def age_hours(self) -> float:
        delta = self.fetched_at - self.created_at
        return max(delta.total_seconds() / 3600, 0.01)


@dataclass
class CrawledContent:
    """
    Extracted article text for a Signal's URL.

    Populated by the crawler after a Signal is collected.
    Fed into the content generator for richer post angles.
    """

    signal_id: str
    url: str
    title: str | None         # page <title> or og:title
    description: str | None   # meta description or og:description
    body: str | None          # main article text (trafilatura extraction)
    crawled_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    error: str | None = None  # set if crawl failed

    @property
    def has_content(self) -> bool:
        return bool(self.body and len(self.body) > 100)

    @property
    def summary(self) -> str:
        """First 1000 chars of body — enough context for the LLM prompt."""
        if not self.body:
            return self.description or ""
        return self.body[:1000]


@dataclass
class VelocityScore:
    signal_id: str
    score_velocity: float
    comment_velocity: float
    combined: float
    computed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class TrendCluster:
    """
    A group of Signals that share a common topic theme.
    Optionally enriched with crawled content per signal.
    """

    id: str
    keywords: list[str]
    signals: list[Signal]
    platforms: set[SourceType]
    peak_velocity: float
    first_seen: datetime
    last_updated: datetime
    crawled: dict[str, CrawledContent] = field(default_factory=dict)  # signal_id → content

    @property
    def cross_platform_score(self) -> int:
        return len(self.platforms)

    @property
    def signal_count(self) -> int:
        return len(self.signals)

    @property
    def crawled_count(self) -> int:
        return sum(1 for c in self.crawled.values() if c.has_content)
