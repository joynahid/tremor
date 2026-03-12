"""
BaseAdapter — the contract every platform adapter must fulfill.

To add a new source:
  1. Subclass BaseAdapter
  2. Set source_type
  3. Implement fetch_raw() → list of raw platform dicts
  4. Implement normalize(raw) → Signal

The pipeline calls fetch() which composes the two.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import httpx

from tremor.domain.models import Signal, SourceType


class BaseAdapter(ABC):
    source_type: SourceType

    def __init__(self, client: httpx.AsyncClient | None = None) -> None:
        # Adapters can share a client (passed in) or own one
        self._client = client
        self._owns_client = client is None

    async def __aenter__(self) -> "BaseAdapter":
        if self._owns_client:
            self._client = httpx.AsyncClient(timeout=15.0)
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._owns_client and self._client:
            await self._client.aclose()

    @abstractmethod
    async def fetch_raw(self) -> list[dict]:
        """Fetch raw data from the platform. Returns a list of raw dicts."""

    @abstractmethod
    def normalize(self, raw: dict) -> Signal:
        """Convert a single raw platform item into a normalized Signal."""

    async def fetch(self) -> list[Signal]:
        """Full fetch: raw → normalized. This is what the pipeline calls."""
        raw_items = await self.fetch_raw()
        signals = []
        for item in raw_items:
            try:
                signals.append(self.normalize(item))
            except Exception as exc:
                # Skip malformed items without crashing the whole fetch
                print(f"[{self.source_type}] normalize failed: {exc} — item: {item.get('id')}")
        return signals
