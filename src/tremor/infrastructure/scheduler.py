"""
Pipeline scheduler — runs the full pipeline on a configurable interval.

Designed to run as a long-lived process or be called from a cron job.
Writes a run log to the DB and emits structured output each cycle.

Usage (long-lived process):
    uv run tremor schedule --interval 360   # every 6 hours

Usage (single shot, great for cron):
    uv run tremor run
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

from tremor.adapters.hackernews import HackerNewsAdapter
from tremor.adapters.reddit import RedditAdapter
from tremor.application.clustering import cluster_signals
from tremor.application.pipeline import TremorPipeline
from tremor.infrastructure.store import SignalStore


class Scheduler:
    def __init__(
        self,
        store: SignalStore,
        interval_minutes: int = 360,
        trending_threshold: float = 50.0,
        subreddits: list[str] | None = None,
        on_cycle_complete=None,    # async callback(result, clusters)
    ) -> None:
        self.store = store
        self.interval_seconds = interval_minutes * 60
        self.trending_threshold = trending_threshold
        self.subreddits = subreddits or ["programming", "devops", "MachineLearning"]
        self.on_cycle_complete = on_cycle_complete
        self._running = False

    def _build_adapters(self):
        return [
            HackerNewsAdapter(),
            RedditAdapter(subreddits=self.subreddits),
        ]

    async def run_once(self):
        """Execute a single pipeline cycle. Returns (result, clusters)."""
        adapters = self._build_adapters()
        pipeline = TremorPipeline(
            adapters=adapters,
            store=self.store,
            trending_threshold=self.trending_threshold,
        )
        result = await pipeline.run()
        clusters = cluster_signals(result.signals)
        return result, clusters

    async def start(self) -> None:
        """Run the scheduler loop indefinitely."""
        self._running = True
        print(f"[scheduler] starting — interval={self.interval_seconds // 60}m")

        while self._running:
            cycle_start = datetime.now(timezone.utc)
            print(f"\n[scheduler] cycle starting at {cycle_start.isoformat()}")

            try:
                result, clusters = await self.run_once()
                print(
                    f"[scheduler] cycle done — {result.signal_count} signals, "
                    f"{len(clusters)} clusters, {result.trending_count} trending"
                )
                if self.on_cycle_complete:
                    await self.on_cycle_complete(result, clusters)
            except Exception as exc:
                print(f"[scheduler] cycle failed: {exc}")

            print(f"[scheduler] next cycle in {self.interval_seconds // 60} minutes")
            await asyncio.sleep(self.interval_seconds)

    def stop(self) -> None:
        self._running = False
