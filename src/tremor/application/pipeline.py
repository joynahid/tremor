"""
The Tremor pipeline — orchestrates fetch → score → crawl → store.
"""

from __future__ import annotations

import httpx

from tremor.adapters.base import BaseAdapter
from tremor.domain.models import CrawledContent, Signal, VelocityScore
from tremor.domain.scoring import compute_velocity
from tremor.infrastructure.crawler import Crawler
from tremor.infrastructure.store import SignalStore


class PipelineResult:
    def __init__(
        self,
        signals: list[Signal],
        scores: list[VelocityScore],
        trending: list[tuple[Signal, VelocityScore]],
        crawled: dict[str, CrawledContent],
    ) -> None:
        self.signals = signals
        self.scores = scores
        self.trending = trending
        self.crawled = crawled

    @property
    def signal_count(self) -> int:
        return len(self.signals)

    @property
    def trending_count(self) -> int:
        return len(self.trending)

    @property
    def crawled_count(self) -> int:
        return sum(1 for c in self.crawled.values() if c.has_content)


class TremorPipeline:
    def __init__(
        self,
        adapters: list[BaseAdapter],
        store: SignalStore,
        trending_threshold: float = 50.0,
        crawl: bool = False,          # opt-in — adds latency but enriches content gen
        crawl_top_n: int = 20,        # only crawl top N signals by velocity
    ) -> None:
        self.adapters = adapters
        self.store = store
        self.trending_threshold = trending_threshold
        self.crawl = crawl
        self.crawl_top_n = crawl_top_n

    async def run(self) -> PipelineResult:
        # 1. Fetch signals from all adapters
        all_signals: list[Signal] = []
        async with httpx.AsyncClient(timeout=20.0) as client:
            for adapter in self.adapters:
                adapter._client = client
                try:
                    signals = await adapter.fetch()
                    all_signals.extend(signals)
                    print(f"[pipeline] {adapter.source_type.value}: {len(signals)} signals")
                except Exception as exc:
                    print(f"[pipeline] {adapter.source_type.value} fetch failed: {exc}")

        # 2. Persist raw signals
        await self.store.upsert_many(all_signals)

        # 3. Score velocity
        scores = [compute_velocity(s) for s in all_signals]
        await self.store.save_velocities(scores)

        # 4. Rank and filter trending
        ranked = sorted(zip(all_signals, scores), key=lambda x: x[1].combined, reverse=True)
        trending = [(s, v) for s, v in ranked if v.combined >= self.trending_threshold]

        # 5. Optional: crawl top N signal URLs for article content
        crawled: dict[str, CrawledContent] = {}
        if self.crawl and all_signals:
            top_signals = [s for s, _ in ranked[: self.crawl_top_n]]
            print(f"[pipeline] crawling {len(top_signals)} signal URLs...")
            crawler = Crawler()
            crawled = await crawler.crawl_signals(top_signals)
            successes = sum(1 for c in crawled.values() if c.has_content)
            print(f"[pipeline] crawled {successes}/{len(crawled)} URLs successfully")
            await self.store.save_crawled_many(list(crawled.values()))

        return PipelineResult(
            signals=all_signals,
            scores=scores,
            trending=trending,
            crawled=crawled,
        )
