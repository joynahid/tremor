"""
Content generator — turns a TrendCluster into Byteskript post angles.

Uses Google Gemini to generate:
  - 3 post angle options (hook + body outline)
  - A recommended angle based on engagement potential
  - Bangla-friendly framing hints

Requires GOOGLE_API_KEY in environment.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

from google import genai

from tremor.domain.models import TrendCluster

MODEL = "gemini-3-flash-preview"
MAX_SIGNALS_IN_PROMPT = 5


@dataclass
class PostAngle:
    hook: str
    body_outline: str
    why_it_works: str
    platform_tags: str


@dataclass
class ContentSuggestion:
    cluster_id: str
    cluster_keywords: list[str]
    angles: list[PostAngle]
    recommended_index: int
    raw_response: str = ""
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def recommended(self) -> PostAngle:
        return self.angles[self.recommended_index]


class ContentGenerator:
    def __init__(
        self,
        api_key: str | None = None,
        audience: str = "Bangladeshi developers and tech enthusiasts",
    ) -> None:
        self.client = genai.Client(api_key=api_key or os.environ["GOOGLE_API_KEY"])
        self.audience = audience

    def generate(self, cluster: TrendCluster) -> ContentSuggestion:
        prompt = self._build_prompt(cluster)
        response = self.client.models.generate_content(model=MODEL, contents=prompt)
        raw = response.text
        return self._parse_response(cluster, raw)

    def _build_prompt(self, cluster: TrendCluster) -> str:
        top_signals = sorted(cluster.signals, key=lambda s: s.score, reverse=True)[:MAX_SIGNALS_IN_PROMPT]
        platforms = ", ".join(p.value for p in cluster.platforms)

        signal_lines_parts = []
        for s in top_signals:
            line = f"  [{s.source.value}] {s.title} (score={s.score}, comments={s.comment_count})"
            crawled = cluster.crawled.get(s.id)
            if crawled and crawled.has_content:
                snippet = crawled.summary[:400].replace("\n", " ")
                line += f"\n    Article excerpt: {snippet}"
            signal_lines_parts.append(line)
        signal_lines = "\n".join(signal_lines_parts)

        return f"""You are a content strategist for Byteskript, a Bengali tech media page.

Audience: {self.audience}
Goal: Create Facebook/LinkedIn posts that explain a tech trend in a way that sparks discussion.

A trend cluster has been detected across these platforms: {platforms}
Peak velocity score: {cluster.peak_velocity}
Cross-platform signals: {cluster.signal_count} posts

Top signals in this cluster:
{signal_lines}

Core keywords: {', '.join(cluster.keywords[:8])}

Generate exactly 3 post angle options separated by ---
For each angle use exactly this format (no markdown, no bold, no numbering):

HOOK: [compelling opening line]
BODY: [2-3 sentence content direction]
WHY: [one sentence on why this generates comments]
TAGS: [4-6 hashtags]

After the 3 angles add:
RECOMMENDED: [1, 2, or 3] [one sentence reason]

Each hook must differ in style: one stat-based, one question-based, one controversy-based."""

    def _parse_response(self, cluster: TrendCluster, raw: str) -> ContentSuggestion:
        # Strip markdown bold markers Gemini sometimes adds
        cleaned = re.sub(r"\*+", "", raw).strip()

        # Split on --- or *** separators between angles
        blocks = re.split(r"\n\s*[-*]{3,}\s*\n", cleaned)

        angles: list[PostAngle] = []
        recommended_index = 0
        recommended_block = ""

        for block in blocks:
            if "RECOMMENDED:" in block:
                # May be appended to the last angle block — split it off
                parts = re.split(r"\n(?=RECOMMENDED:)", block, maxsplit=1)
                block = parts[0]
                if len(parts) > 1:
                    recommended_block = parts[1]
            _try_parse_angle(block, angles)

        if not recommended_block:
            # Try extracting RECOMMENDED from end of raw if not found in blocks
            m = re.search(r"RECOMMENDED:\s*(\d)", cleaned)
            if m:
                recommended_index = int(m.group(1)) - 1
        else:
            m = re.search(r"RECOMMENDED:\s*(\d)", recommended_block)
            if m:
                recommended_index = int(m.group(1)) - 1

        if not angles:
            angles = [PostAngle(hook=raw[:200], body_outline=raw, why_it_works="", platform_tags="")]

        recommended_index = max(0, min(recommended_index, len(angles) - 1))

        return ContentSuggestion(
            cluster_id=cluster.id,
            cluster_keywords=cluster.keywords[:8],
            angles=angles,
            recommended_index=recommended_index,
            raw_response=raw,
        )


def _try_parse_angle(block: str, angles: list[PostAngle]) -> None:
    hook_m = re.search(r"HOOK:\s*(.+)", block)
    if not hook_m:
        return
    body_m = re.search(r"BODY:\s*([\s\S]+?)(?=\nWHY:|\nTAGS:|$)", block)
    why_m  = re.search(r"WHY:\s*([\s\S]+?)(?=\nTAGS:|$)", block)
    tags_m = re.search(r"TAGS:\s*(.+)", block)

    angles.append(PostAngle(
        hook=hook_m.group(1).strip(),
        body_outline=body_m.group(1).strip() if body_m else "",
        why_it_works=why_m.group(1).strip() if why_m else "",
        platform_tags=tags_m.group(1).strip() if tags_m else "",
    ))
