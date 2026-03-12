"""
Velocity scoring — the core algorithm.

Operates entirely on normalized Signal objects.
No platform-specific logic lives here.
"""

from __future__ import annotations

from datetime import datetime, timezone

from .models import Signal, VelocityScore

# Weight: score/upvotes matters more than raw comment count
# because comments can be negative (arguments), but upvotes are explicit approval
SCORE_WEIGHT = 0.7
COMMENT_WEIGHT = 0.3


def compute_velocity(signal: Signal) -> VelocityScore:
    score_v = signal.score / signal.age_hours
    comment_v = signal.comment_count / signal.age_hours
    combined = (score_v * SCORE_WEIGHT) + (comment_v * COMMENT_WEIGHT)

    return VelocityScore(
        signal_id=signal.id,
        score_velocity=round(score_v, 2),
        comment_velocity=round(comment_v, 2),
        combined=round(combined, 2),
        computed_at=datetime.now(timezone.utc),
    )


def rank_signals(signals: list[Signal]) -> list[tuple[Signal, VelocityScore]]:
    """Return signals sorted by combined velocity, highest first."""
    scored = [(s, compute_velocity(s)) for s in signals]
    return sorted(scored, key=lambda x: x[1].combined, reverse=True)


def is_trending(velocity: VelocityScore, threshold: float = 50.0) -> bool:
    """
    Simple threshold gate. A signal is 'trending' if its combined
    velocity exceeds the threshold.

    Default of 50.0 is calibrated for HN. Each adapter can override
    this with a source-appropriate threshold.
    """
    return velocity.combined >= threshold
