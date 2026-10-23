"""Command-line entry points. One sub-command per pipeline stage.

straitflow ingest   --source fake|entsoe|esios --start 2024-01-01 --end 2024-02-01
straitflow backfill --source entsoe --start 2019-01-01       (month by month, resumable)
straitflow status                                            (what is in the lake)
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl
import typer
from loguru import logger

from straitflow.config import settings
from straitflow.ingest.base import Connector
from straitflow.store.parquet import ParquetLake
from straitflow.store.validate import check_long_frame

app = typer.Typer(no_args_is_help=True, add_completion=False)


def _connector(source: str) -> Connector:
    if source == "fake":
        from straitflow.ingest.fake import FakeConnector

        return FakeConnector()
    raise typer.BadParameter(f"unknown source {source!r} (entsoe and esios arrive in week 2)")


def _parse(d: str) -> datetime:
    return datetime.fromisoformat(d).replace(tzinfo=UTC)


@app.command()
def ingest(
    source: str = typer.Option("fake", help="fake | entsoe | esios"),
    start: str = typer.Option(None, help="ISO date, UTC. Default: 3 days ago."),
    end: str = typer.Option(None, help="ISO date, UTC, exclusive. Default: now."),
) -> None:
    """Fetch one period and merge it into the lake."""
    t1 = _parse(end) if end else datetime.now(tz=UTC).replace(minute=0, second=0, microsecond=0)
    t0 = _parse(start) if start else t1 - timedelta(days=3)
    conn = _connector(source)
    logger.info("ingest {} {} -> {}", source, t0, t1)
    df = conn.fetch(t0, t1)
    logger.info("quality: {}", check_long_frame(df))
    ParquetLake(settings.raw_dir).write(df)


@app.command()
def backfill(
    source: str = typer.Option("fake"),
    start: str = typer.Option("2019-01-01"),
    end: str = typer.Option(None),
) -> None:
    """Month-by-month ingestion; safe to interrupt and relaunch (idempotent writes)."""
    t_end = _parse(end) if end else datetime.now(tz=UTC)
    conn = _connector(source)
    lake = ParquetLake(settings.raw_dir)
    t0 = _parse(start)
    while t0 < t_end:
        t1 = min((t0.replace(day=1) + timedelta(days=32)).replace(day=1), t_end)
        logger.info("backfill {} {} -> {}", source, t0.date(), t1.date())
        df = conn.fetch(t0, t1)
        check_long_frame(df)
        lake.write(df)
        t0 = t1


@app.command()
def status() -> None:
    """Summarise the lake: series, coverage, last extraction, revisions."""
    lake = ParquetLake(settings.raw_dir)
    df = lake.read()
    if df.is_empty():
        typer.echo("lake is empty")
        raise typer.Exit()
    summary = df.group_by("series").agg(
        pl.col("ts_utc").min().alias("first"),
        pl.col("ts_utc").max().alias("last"),
        pl.len().alias("rows"),
        pl.col("extracted_at").max().alias("last_extracted"),
    )
    typer.echo(summary.sort("series"))
    rev = settings.raw_dir / "_revisions.parquet"
    if rev.exists():
        typer.echo(f"revisions journaled: {pl.read_parquet(rev).height}")


if __name__ == "__main__":
    app()
