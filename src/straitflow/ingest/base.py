"""Common contract for every data connector.

A connector turns a public source into a tidy long table with exactly these columns:

    ts_utc        datetime[UTC]  start of the hour the value refers to
    series        str            e.g. "flow_es_ma_physical_mw", "price_da_es_eur_mwh"
    value         float
    source        str            e.g. "entsoe", "esios", "openmeteo", "fake"
    extracted_at  datetime[UTC]  when we pulled it (needed to detect revisions and to build
        the `available_at` column of every feature later on)

Keeping one long schema for every source makes storage, validation, revision detection
and DuckDB views identical for all of them.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import UTC, datetime, timedelta

import polars as pl

LONG_SCHEMA: dict[str, pl.DataType] = {
    "ts_utc": pl.Datetime("us", "UTC"),
    "series": pl.String(),
    "value": pl.Float64(),
    "source": pl.String(),
    "extracted_at": pl.Datetime("us", "UTC"),
}

# Publication lag per series family: how long after the hour `ts_utc` the FIRST value is
# normally public. Used to reconstruct what was known at a past instant when the lake was
# back-filled in bulk (then `extracted_at` says nothing about availability). Week-1 defaults,
# to be measured against the real sources and recorded in docs/data_dictionary.md.
PUBLICATION_LAG: dict[str, timedelta] = {
    "flow_": timedelta(hours=2),  # ENTSO-E physical flows: ~1-2 h after the hour
    "price_da_": timedelta(hours=-11),  # day-ahead price: published D-1 ~13:00 CET
    "load_": timedelta(hours=2),
    "gen_": timedelta(hours=2),
    "weather_": timedelta(hours=-6),  # a forecast for ts is available before ts
}
DEFAULT_LAG = timedelta(hours=24)


def publication_lag(series: str) -> timedelta:
    """Compute publication lag of the series

    Args:
        series (str): series of prices

    Returns:
        timedelta: publication lag
    """
    for prefix, lag in PUBLICATION_LAG.items():
        if series.startswith(prefix):
            return lag
    return DEFAULT_LAG


def now_utc() -> datetime:
    return datetime.now(tz=UTC).replace(microsecond=0)


def empty_long() -> pl.DataFrame:
    return pl.DataFrame(schema=LONG_SCHEMA)


class Connector(ABC):
    """Fetch [start, end) at hourly resolution and return a LONG_SCHEMA frame."""

    name: str

    @abstractmethod
    def fetch(self, start: datetime, end: datetime) -> pl.DataFrame: ...

    @staticmethod
    def conform(df: pl.DataFrame) -> pl.DataFrame:
        """Cast to LONG_SCHEMA, drop nulls, sort. Raises if a column is missing."""
        missing = set(LONG_SCHEMA) - set(df.columns)
        if missing:
            raise ValueError(f"connector output misses columns: {sorted(missing)}")
        return (
            df.select(list(LONG_SCHEMA))
            .cast(LONG_SCHEMA)  # type: ignore[arg-type]
            .drop_nulls(["ts_utc", "series", "value"])
            .sort(["series", "ts_utc"])
        )
