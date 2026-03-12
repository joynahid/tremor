"""
Tremor CLI.

Commands:
    tremor run          — run the pipeline once, show clusters + trending
    tremor top          — query DB for top signals by velocity
    tremor clusters     — run pipeline and show trend clusters
    tremor draft        — run pipeline, cluster, and generate Byteskript post angles
    tremor schedule     — run pipeline on a repeat interval (long-lived process)
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from tremor.adapters.hackernews import HackerNewsAdapter
from tremor.adapters.reddit import RedditAdapter
from tremor.application.clustering import cluster_signals
from tremor.application.content import ContentGenerator
from tremor.application.pipeline import TremorPipeline
from tremor.domain.models import SourceType, TrendCluster
from tremor.infrastructure.scheduler import Scheduler
from tremor.infrastructure.store import SignalStore

app = typer.Typer(help="Tremor — developer trend intelligence pipeline", add_completion=False)
console = Console()

# ── shared option defaults ────────────────────────────────────────────────────

_DB = typer.Option(Path("tremor.db"), help="SQLite DB path")
_THRESHOLD = typer.Option(50.0, help="Minimum velocity score to flag as trending")
_SUBREDDITS = typer.Option("programming,devops,MachineLearning", help="Comma-separated subreddits")


# ── commands ──────────────────────────────────────────────────────────────────

@app.command()
def run(
    hn: bool = typer.Option(True, help="Include Hacker News"),
    reddit: bool = typer.Option(True, help="Include Reddit"),
    subreddits: str = _SUBREDDITS,
    threshold: float = _THRESHOLD,
    db: Path = _DB,
) -> None:
    """Fetch signals and display trending topics with velocity scores."""
    asyncio.run(_run(hn, reddit, subreddits.split(","), threshold, db))


@app.command()
def top(
    limit: int = typer.Option(20, help="Number of results"),
    source: str = typer.Option("", help="Filter: hackernews | reddit | twitter"),
    db: Path = _DB,
) -> None:
    """Query the DB for top signals by velocity score."""
    asyncio.run(_top(limit, source or None, db))


@app.command()
def clusters(
    hn: bool = typer.Option(True),
    reddit: bool = typer.Option(True),
    subreddits: str = _SUBREDDITS,
    threshold: float = _THRESHOLD,
    db: Path = _DB,
) -> None:
    """Fetch signals, cluster by topic, and display trend groups."""
    asyncio.run(_clusters(hn, reddit, subreddits.split(","), threshold, db))


@app.command()
def draft(
    hn: bool = typer.Option(True),
    reddit: bool = typer.Option(True),
    subreddits: str = _SUBREDDITS,
    threshold: float = _THRESHOLD,
    top_n: int = typer.Option(3, help="Generate drafts for top N clusters"),
    crawl: bool = typer.Option(False, help="Crawl article URLs for richer content generation"),
    db: Path = _DB,
) -> None:
    """Fetch signals, cluster, and generate Byteskript post angles via Gemini."""
    asyncio.run(_draft(hn, reddit, subreddits.split(","), threshold, top_n, crawl, db))


@app.command()
def history(
    limit: int = typer.Option(10, help="Number of past drafts to show"),
    db: Path = _DB,
) -> None:
    """View previously generated post angle drafts."""
    asyncio.run(_history(limit, db))


@app.command()
def schedule(
    interval: int = typer.Option(360, help="Interval in minutes between pipeline runs"),
    subreddits: str = _SUBREDDITS,
    threshold: float = _THRESHOLD,
    db: Path = _DB,
) -> None:
    """Run the pipeline repeatedly on an interval (long-lived process)."""
    asyncio.run(_schedule(interval, subreddits.split(","), threshold, db))


# ── async implementations ─────────────────────────────────────────────────────

async def _fetch_and_cluster(use_hn, use_reddit, subreddits, threshold, db, crawl=False):
    adapters = []
    if use_hn:
        adapters.append(HackerNewsAdapter())
    if use_reddit:
        adapters.append(RedditAdapter(subreddits=subreddits))
    if not adapters:
        console.print("[red]No adapters enabled.[/red]")
        raise typer.Exit(1)

    async with SignalStore(db) as store:
        pipeline = TremorPipeline(
            adapters=adapters,
            store=store,
            trending_threshold=threshold,
            crawl=crawl,
        )
        console.print(
            f"[bold cyan]tremor[/bold cyan] running ({len(adapters)} sources"
            + (", crawling URLs" if crawl else "") + ")..."
        )
        result = await pipeline.run()

    cluster_list = cluster_signals(result.signals)
    # Attach crawled content to each cluster
    for cluster in cluster_list:
        for signal in cluster.signals:
            if signal.id in result.crawled:
                cluster.crawled[signal.id] = result.crawled[signal.id]

    return result, cluster_list


async def _run(use_hn, use_reddit, subreddits, threshold, db):
    result, cluster_list = await _fetch_and_cluster(use_hn, use_reddit, subreddits, threshold, db)
    console.print(
        f"\n[green]Done.[/green] {result.signal_count} signals · "
        f"[bold]{result.trending_count} trending[/bold] · "
        f"[bold]{len(cluster_list)} clusters[/bold]\n"
    )
    if result.trending:
        _print_signals(result.trending)
    else:
        console.print("[yellow]No trending signals at this threshold.[/yellow]")


async def _top(limit, source_filter, db):
    src = SourceType(source_filter) if source_filter else None
    async with SignalStore(db) as store:
        results = await store.get_top_signals(limit=limit, source=src)
    if not results:
        console.print("[yellow]No signals in DB yet. Run 'tremor run' first.[/yellow]")
        return
    _print_signals(results)


async def _clusters(use_hn, use_reddit, subreddits, threshold, db):
    result, cluster_list = await _fetch_and_cluster(use_hn, use_reddit, subreddits, threshold, db)
    console.print(
        f"\n[green]Done.[/green] {result.signal_count} signals → [bold]{len(cluster_list)} clusters[/bold]\n"
    )
    _print_clusters(cluster_list)


async def _draft(use_hn, use_reddit, subreddits, threshold, top_n, crawl, db):
    result, cluster_list = await _fetch_and_cluster(use_hn, use_reddit, subreddits, threshold, db, crawl=crawl)
    console.print(
        f"\n[green]Done.[/green] {result.signal_count} signals · {len(cluster_list)} clusters"
        + (f" · {result.crawled_count} articles crawled" if crawl else "") + "\n"
    )

    if not cluster_list:
        console.print("[yellow]No clusters to draft from.[/yellow]")
        return

    generator = ContentGenerator()
    targets = cluster_list[:top_n]

    async with SignalStore(db) as store:
        for i, cluster in enumerate(targets, 1):
            console.print(f"\n[bold cyan]-- Cluster {i}/{len(targets)}: {', '.join(cluster.keywords[:5])}[/bold cyan]")
            console.print(
                f"   {cluster.signal_count} signals · platforms: {', '.join(p.value for p in cluster.platforms)} · "
                f"velocity: {cluster.peak_velocity}"
            )

            try:
                suggestion = generator.generate(cluster)
                _print_post_angles(suggestion)
                draft_id = await store.save_draft(suggestion)
                console.print(f"   [dim]saved as draft #{draft_id}[/dim]")
            except Exception as exc:
                console.print(f"[red]Draft generation failed: {exc}[/red]")


async def _history(limit: int, db: Path) -> None:
    async with SignalStore(db) as store:
        drafts = await store.get_drafts(limit=limit)

    if not drafts:
        console.print("[yellow]No drafts yet. Run 'tremor draft' first.[/yellow]")
        return

    for d in drafts:
        console.print(
            f"\n[bold cyan]Draft #{d['id']}[/bold cyan]  "
            f"[dim]{d['generated_at'][:19]}[/dim]  "
            f"keywords: [bold]{', '.join(d['keywords'][:5])}[/bold]"
        )
        rec = d["recommended"]
        for j, angle in enumerate(d["angles"]):
            marker = "[green]RECOMMENDED[/green]" if j == rec else f"Option {j+1}"
            console.print(
                Panel(
                    f"[bold yellow]{angle['hook']}[/bold yellow]\n\n"
                    f"{angle['body']}\n\n"
                    f"[dim]Why:[/dim] {angle['why']}\n"
                    f"[dim]Tags:[/dim] {angle['tags']}",
                    title=marker,
                    expand=False,
                )
            )


async def _schedule(interval, subreddits, threshold, db):
    async with SignalStore(db) as store:
        scheduler = Scheduler(
            store=store,
            interval_minutes=interval,
            trending_threshold=threshold,
            subreddits=subreddits,
            on_cycle_complete=_on_cycle,
        )
        try:
            await scheduler.start()
        except KeyboardInterrupt:
            console.print("\n[yellow]Scheduler stopped.[/yellow]")


async def _on_cycle(result, clusters):
    console.print(
        f"  [green]↑[/green] {result.signal_count} signals · "
        f"{len(clusters)} clusters · "
        f"{result.trending_count} trending"
    )
    if clusters:
        console.print(f"  Top cluster: [bold]{', '.join(clusters[0].keywords[:4])}[/bold] "
                      f"(vel={clusters[0].peak_velocity})")


# ── display helpers ───────────────────────────────────────────────────────────

def _print_signals(ranked: list) -> None:
    table = Table(show_header=True, header_style="bold magenta", box=None, padding=(0, 1))
    table.add_column("#", style="dim", width=3)
    table.add_column("Source", width=12)
    table.add_column("Vel", justify="right", width=7)
    table.add_column("Score", justify="right", width=6)
    table.add_column("Cmts", justify="right", width=5)
    table.add_column("Age h", justify="right", width=6)
    table.add_column("Title")

    for i, (signal, velocity) in enumerate(ranked, 1):
        table.add_row(
            str(i),
            signal.source.value,
            f"[bold]{velocity.combined:.0f}[/bold]",
            str(signal.score),
            str(signal.comment_count),
            f"{signal.age_hours:.1f}",
            signal.title[:90],
        )
    console.print(table)


def _print_clusters(cluster_list: list[TrendCluster]) -> None:
    for i, c in enumerate(cluster_list, 1):
        platforms = " + ".join(p.value for p in c.platforms)
        header = Text()
        header.append(f"{i}. ", style="dim")
        header.append(", ".join(c.keywords[:6]), style="bold white")
        header.append(f"  [{platforms}]", style="cyan")
        header.append(f"  vel={c.peak_velocity}  signals={c.signal_count}", style="dim")
        console.print(header)
        for s in sorted(c.signals, key=lambda x: x.score, reverse=True)[:3]:
            console.print(f"     [dim]{s.source.value}[/dim]  {s.title[:80]}")
        console.print()


def _print_post_angles(suggestion) -> None:
    for j, angle in enumerate(suggestion.angles, 1):
        marker = "[green]★ RECOMMENDED[/green]" if j - 1 == suggestion.recommended_index else f"Option {j}"
        console.print(
            Panel(
                f"[bold yellow]{angle.hook}[/bold yellow]\n\n"
                f"{angle.body_outline}\n\n"
                f"[dim]Why it works:[/dim] {angle.why_it_works}\n"
                f"[dim]Tags:[/dim] {angle.platform_tags}",
                title=marker,
                expand=False,
            )
        )
