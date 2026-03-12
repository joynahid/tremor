# tremor

Developer trend intelligence pipeline. Detects what engineers are discussing across platforms before it goes mainstream.

## How it works

```
Hacker News + Reddit
       ↓
  fetch signals
       ↓
 velocity scoring
       ↓
 keyword clustering
       ↓
  crawl articles
       ↓
  generate drafts
```

Velocity = `score / age_hours`. Signals that spike fast, across multiple platforms simultaneously, are the strongest indicator of an emerging trend.

## Setup

```bash
uv sync
export GOOGLE_API_KEY=your_key_here
```

Get a free key at [aistudio.google.com](https://aistudio.google.com).

## Commands

```bash
# Fetch signals, score, display trending
tremor run

# Show trend clusters grouped by topic
tremor clusters

# Generate post angles for top clusters
tremor draft --top-n 3 --crawl

# View saved drafts
tremor history

# Run on a schedule (default 6h)
tremor schedule --interval 360
```

## Options

```bash
tremor draft --top-n 5
tremor draft --no-reddit
tremor draft --subreddits "rust,golang,devops"
tremor draft --threshold 20
tremor top --limit 20
```

## Architecture

```
src/tremor/
├── domain/
│   ├── models.py        Signal, VelocityScore, TrendCluster, CrawledContent
│   └── scoring.py       velocity calculation
├── adapters/
│   ├── base.py          BaseAdapter ABC
│   ├── hackernews.py    HN Firebase API
│   ├── reddit.py        Reddit public JSON API
│   └── twitter.py       Twitter API v2 (requires bearer token)
├── application/
│   ├── pipeline.py      orchestration
│   ├── clustering.py    Union-Find keyword clustering
│   └── content.py       Gemini-powered post angle generation
├── infrastructure/
│   ├── store.py         SQLite persistence
│   ├── crawler.py       article text extraction (trafilatura)
│   └── scheduler.py     interval runner
└── interfaces/
    └── cli.py           Typer CLI
```

## Adding a new source

1. Subclass `BaseAdapter` in `adapters/`
2. Set `source_type`
3. Implement `fetch_raw()` and `normalize()`

Everything downstream works automatically.

## Stack

- Python 3.11+ / [uv](https://github.com/astral-sh/uv)
- httpx, aiosqlite, trafilatura, google-genai, typer, rich
