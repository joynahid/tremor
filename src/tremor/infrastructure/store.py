"""
SQLite store for persisting signals and velocity scores.

Uses aiosqlite for async access. Schema is intentionally flat —
the domain models are the source of truth, this is just a durable log.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import aiosqlite

from tremor.domain.models import CrawledContent, Signal, SourceType, VelocityScore

DEFAULT_DB_PATH = Path("tremor.db")

CREATE_SIGNALS = """
CREATE TABLE IF NOT EXISTS signals (
    id          TEXT PRIMARY KEY,
    source      TEXT NOT NULL,
    source_id   TEXT NOT NULL,
    title       TEXT NOT NULL,
    url         TEXT,
    author      TEXT,
    score       INTEGER NOT NULL DEFAULT 0,
    comment_count INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL,
    fetched_at  TEXT NOT NULL,
    tags        TEXT NOT NULL DEFAULT '[]',
    metadata    TEXT NOT NULL DEFAULT '{}'
)
"""

CREATE_VELOCITY = """
CREATE TABLE IF NOT EXISTS velocity_scores (
    signal_id        TEXT NOT NULL,
    score_velocity   REAL NOT NULL,
    comment_velocity REAL NOT NULL,
    combined         REAL NOT NULL,
    computed_at      TEXT NOT NULL,
    PRIMARY KEY (signal_id, computed_at)
)
"""

CREATE_IDX_VELOCITY = """
CREATE INDEX IF NOT EXISTS idx_velocity_combined
    ON velocity_scores (combined DESC)
"""

CREATE_CRAWLED = """
CREATE TABLE IF NOT EXISTS crawled_content (
    signal_id   TEXT PRIMARY KEY,
    url         TEXT NOT NULL,
    title       TEXT,
    description TEXT,
    body        TEXT,
    crawled_at  TEXT NOT NULL,
    error       TEXT
)
"""

CREATE_DRAFTS = """
CREATE TABLE IF NOT EXISTS drafts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cluster_id      TEXT NOT NULL,
    keywords        TEXT NOT NULL,
    angles_json     TEXT NOT NULL,
    recommended     INTEGER NOT NULL DEFAULT 0,
    raw_response    TEXT,
    generated_at    TEXT NOT NULL
)
"""


class SignalStore:
    def __init__(self, db_path: Path = DEFAULT_DB_PATH) -> None:
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        self._db = await aiosqlite.connect(self.db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(
            f"{CREATE_SIGNALS}; {CREATE_VELOCITY}; {CREATE_IDX_VELOCITY}; {CREATE_CRAWLED}; {CREATE_DRAFTS};"
        )
        await self._db.commit()

    async def close(self) -> None:
        if self._db:
            await self._db.close()

    async def __aenter__(self) -> "SignalStore":
        await self.connect()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def upsert_signal(self, signal: Signal) -> None:
        await self._db.execute(
            """
            INSERT OR REPLACE INTO signals
                (id, source, source_id, title, url, author, score,
                 comment_count, created_at, fetched_at, tags, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                signal.id,
                signal.source.value,
                signal.source_id,
                signal.title,
                signal.url,
                signal.author,
                signal.score,
                signal.comment_count,
                signal.created_at.isoformat(),
                signal.fetched_at.isoformat(),
                json.dumps(signal.tags),
                json.dumps(signal.metadata),
            ),
        )
        await self._db.commit()

    async def upsert_many(self, signals: list[Signal]) -> None:
        await self._db.executemany(
            """
            INSERT OR REPLACE INTO signals
                (id, source, source_id, title, url, author, score,
                 comment_count, created_at, fetched_at, tags, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    s.id,
                    s.source.value,
                    s.source_id,
                    s.title,
                    s.url,
                    s.author,
                    s.score,
                    s.comment_count,
                    s.created_at.isoformat(),
                    s.fetched_at.isoformat(),
                    json.dumps(s.tags),
                    json.dumps(s.metadata),
                )
                for s in signals
            ],
        )
        await self._db.commit()

    async def save_velocity(self, v: VelocityScore) -> None:
        await self._db.execute(
            """
            INSERT OR REPLACE INTO velocity_scores
                (signal_id, score_velocity, comment_velocity, combined, computed_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (v.signal_id, v.score_velocity, v.comment_velocity, v.combined, v.computed_at.isoformat()),
        )
        await self._db.commit()

    async def save_velocities(self, scores: list[VelocityScore]) -> None:
        await self._db.executemany(
            """
            INSERT OR REPLACE INTO velocity_scores
                (signal_id, score_velocity, comment_velocity, combined, computed_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (v.signal_id, v.score_velocity, v.comment_velocity, v.combined, v.computed_at.isoformat())
                for v in scores
            ],
        )
        await self._db.commit()

    async def get_top_signals(
        self,
        limit: int = 20,
        source: SourceType | None = None,
        min_combined: float = 0.0,
    ) -> list[tuple[Signal, VelocityScore]]:
        """Return signals joined with their latest velocity score, ranked by velocity."""
        where_clauses = ["v.combined >= ?"]
        params: list = [min_combined]

        if source:
            where_clauses.append("s.source = ?")
            params.append(source.value)

        where = " AND ".join(where_clauses)

        rows = await self._db.execute_fetchall(
            f"""
            SELECT s.*, v.score_velocity, v.comment_velocity, v.combined, v.computed_at
            FROM signals s
            JOIN (
                SELECT signal_id, MAX(computed_at) as latest
                FROM velocity_scores
                GROUP BY signal_id
            ) latest_v ON s.id = latest_v.signal_id
            JOIN velocity_scores v ON v.signal_id = s.id AND v.computed_at = latest_v.latest
            WHERE {where}
            ORDER BY v.combined DESC
            LIMIT ?
            """,
            [*params, limit],
        )

        results = []
        for row in rows:
            signal = _row_to_signal(row)
            velocity = VelocityScore(
                signal_id=row["id"],
                score_velocity=row["score_velocity"],
                comment_velocity=row["comment_velocity"],
                combined=row["combined"],
                computed_at=datetime.fromisoformat(row["computed_at"]),
            )
            results.append((signal, velocity))
        return results

    async def save_crawled(self, content: CrawledContent) -> None:
        await self._db.execute(
            """
            INSERT OR REPLACE INTO crawled_content
                (signal_id, url, title, description, body, crawled_at, error)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                content.signal_id,
                content.url,
                content.title,
                content.description,
                content.body,
                content.crawled_at.isoformat(),
                content.error,
            ),
        )
        await self._db.commit()

    async def save_crawled_many(self, contents: list[CrawledContent]) -> None:
        await self._db.executemany(
            """
            INSERT OR REPLACE INTO crawled_content
                (signal_id, url, title, description, body, crawled_at, error)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (c.signal_id, c.url, c.title, c.description, c.body, c.crawled_at.isoformat(), c.error)
                for c in contents
            ],
        )
        await self._db.commit()

    async def get_crawled(self, signal_ids: list[str]) -> dict[str, CrawledContent]:
        """Load crawled content for a list of signal IDs."""
        if not signal_ids:
            return {}
        placeholders = ",".join("?" * len(signal_ids))
        rows = await self._db.execute_fetchall(
            f"SELECT * FROM crawled_content WHERE signal_id IN ({placeholders})",
            signal_ids,
        )
        return {row["signal_id"]: _row_to_crawled(row) for row in rows}

    async def save_draft(self, suggestion) -> int:
        """Persist a ContentSuggestion. Returns the row id."""
        angles_json = json.dumps([
            {"hook": a.hook, "body": a.body_outline, "why": a.why_it_works, "tags": a.platform_tags}
            for a in suggestion.angles
        ], ensure_ascii=False)
        cursor = await self._db.execute(
            """
            INSERT INTO drafts (cluster_id, keywords, angles_json, recommended, raw_response, generated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                suggestion.cluster_id,
                json.dumps(suggestion.cluster_keywords, ensure_ascii=False),
                angles_json,
                suggestion.recommended_index,
                suggestion.raw_response,
                suggestion.generated_at.isoformat(),
            ),
        )
        await self._db.commit()
        return cursor.lastrowid

    async def get_drafts(self, limit: int = 20) -> list[dict]:
        rows = await self._db.execute_fetchall(
            "SELECT * FROM drafts ORDER BY generated_at DESC LIMIT ?", [limit]
        )
        results = []
        for row in rows:
            results.append({
                "id": row["id"],
                "cluster_id": row["cluster_id"],
                "keywords": json.loads(row["keywords"]),
                "angles": json.loads(row["angles_json"]),
                "recommended": row["recommended"],
                "raw_response": row["raw_response"],
                "generated_at": row["generated_at"],
            })
        return results


def _row_to_signal(row: aiosqlite.Row) -> Signal:
    return Signal(
        source=SourceType(row["source"]),
        source_id=row["source_id"],
        title=row["title"],
        url=row["url"],
        author=row["author"],
        score=row["score"],
        comment_count=row["comment_count"],
        created_at=datetime.fromisoformat(row["created_at"]),
        fetched_at=datetime.fromisoformat(row["fetched_at"]),
        tags=json.loads(row["tags"]),
        metadata=json.loads(row["metadata"]),
    )


def _row_to_crawled(row: aiosqlite.Row) -> CrawledContent:
    return CrawledContent(
        signal_id=row["signal_id"],
        url=row["url"],
        title=row["title"],
        description=row["description"],
        body=row["body"],
        crawled_at=datetime.fromisoformat(row["crawled_at"]),
        error=row["error"],
    )
