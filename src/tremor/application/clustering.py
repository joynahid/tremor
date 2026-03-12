"""
Keyword-based signal clustering.

Groups signals that share meaningful keywords into TrendClusters.
No ML required — fast, deterministic, and explainable.

Algorithm:
  1. Extract keywords from each signal title (strip stop words, short tokens)
  2. Build an inverted index: keyword → [signal_ids]
  3. For each signal, find other signals sharing >= MIN_SHARED_KEYWORDS
  4. Union-Find to merge overlapping groups into clusters
  5. Score each cluster by peak velocity and cross-platform breadth
"""

from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from datetime import datetime, timezone

from tremor.domain.models import Signal, SourceType, TrendCluster
from tremor.domain.scoring import compute_velocity

MIN_KEYWORD_LENGTH = 3
MIN_SHARED_KEYWORDS = 1   # lower = more aggressive clustering
MIN_CLUSTER_SIZE = 1      # single signals can still be a cluster

STOP_WORDS = {
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "shall", "can", "need", "ought",
    "to", "of", "in", "for", "on", "with", "at", "by", "from", "up",
    "about", "into", "through", "during", "before", "after",
    "then", "once", "here", "there", "when", "where", "why", "how",
    "all", "both", "each", "few", "more", "most", "other", "some", "such",
    "no", "nor", "not", "only", "same", "so", "than", "too", "very",
    "just", "but", "and", "or", "if", "as", "this", "that", "it", "its",
    "using", "use", "used", "uses", "get", "got", "make", "made", "makes",
    "show", "ask", "tell", "launch", "now", "new", "old", "what", "which",
    "who", "i", "you", "he", "she", "we", "they", "vs", "via", "per",
    "my", "our", "your", "their", "has", "also", "can", "still", "after",
}


def extract_keywords(title: str) -> set[str]:
    """Extract meaningful keywords from a signal title."""
    tokens = re.findall(r"[a-zA-Z0-9\+\#\.]+", title.lower())
    return {
        t for t in tokens
        if len(t) >= MIN_KEYWORD_LENGTH and t not in STOP_WORDS
    }


class UnionFind:
    """Simple union-find for merging overlapping keyword groups."""

    def __init__(self, n: int) -> None:
        self.parent = list(range(n))

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, x: int, y: int) -> None:
        self.parent[self.find(x)] = self.find(y)

    def groups(self, n: int) -> dict[int, list[int]]:
        result: dict[int, list[int]] = defaultdict(list)
        for i in range(n):
            result[self.find(i)].append(i)
        return dict(result)


def cluster_signals(signals: list[Signal]) -> list[TrendCluster]:
    """
    Group signals into TrendClusters based on keyword overlap.
    Returns clusters sorted by peak velocity descending.
    """
    if not signals:
        return []

    keywords_per_signal = [extract_keywords(s.title) for s in signals]

    # Inverted index: keyword → set of signal indices
    inverted: dict[str, set[int]] = defaultdict(set)
    for idx, kws in enumerate(keywords_per_signal):
        for kw in kws:
            inverted[kw].add(idx)

    # Union-Find: merge signals that share enough keywords
    uf = UnionFind(len(signals))
    for kw, signal_indices in inverted.items():
        indices = list(signal_indices)
        # Any two signals sharing this keyword → candidate for merging
        # We'll refine by shared keyword count below
        for i in range(len(indices)):
            for j in range(i + 1, len(indices)):
                a, b = indices[i], indices[j]
                shared = keywords_per_signal[a] & keywords_per_signal[b]
                if len(shared) >= MIN_SHARED_KEYWORDS:
                    uf.union(a, b)

    # Build cluster objects
    groups = uf.groups(len(signals))
    now = datetime.now(timezone.utc)
    clusters: list[TrendCluster] = []

    for _, member_indices in groups.items():
        if len(member_indices) < MIN_CLUSTER_SIZE:
            continue

        group_signals = [signals[i] for i in member_indices]

        # Collect all keywords across the group
        all_keywords: dict[str, int] = defaultdict(int)
        for s in group_signals:
            for kw in extract_keywords(s.title):
                all_keywords[kw] += 1

        # Top keywords by frequency
        top_keywords = [
            kw for kw, _ in sorted(all_keywords.items(), key=lambda x: -x[1])
        ][:10]

        platforms = {s.source for s in group_signals}
        velocities = [compute_velocity(s).combined for s in group_signals]
        peak_velocity = max(velocities)
        first_seen = min(s.created_at for s in group_signals)

        cluster_id = hashlib.md5(
            "|".join(sorted(top_keywords[:3])).encode()
        ).hexdigest()[:12]

        clusters.append(
            TrendCluster(
                id=cluster_id,
                keywords=top_keywords,
                signals=group_signals,
                platforms=platforms,
                peak_velocity=round(peak_velocity, 2),
                first_seen=first_seen,
                last_updated=now,
            )
        )

    # Sort: cross-platform first, then by peak velocity
    clusters.sort(key=lambda c: (c.cross_platform_score, c.peak_velocity), reverse=True)
    return clusters
