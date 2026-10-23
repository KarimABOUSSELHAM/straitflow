"""Data quality gates, run on every ingestion. A failing gate aborts the job loudly.

Checks completeness of the hourly grid, physical bounds
(interconnection capacity), monotone and unique timestamps, UTC timezone.
"""

from __future__ import annotations

from datetime import datetime

import polars as pl

from straitflow.config import settings


class DataQualityError(RuntimeError):
    pass


def check_long_frame(df: pl.DataFrame, max_gap_share: float = 0.05) -> dict[str, float]:
    """Validate a LONG_SCHEMA frame. Returns a small report; raises on hard failures.

    Args:
        df (pl.DataFrame): DataFrame to validate. Must have columns:
            * ts_utc (datetime, UTC)
            * series (str)
            * value (float)
        max_gap_share (float): Maximum allowed share of missing hours per series.

    Returns:
        dict[str, float]: Report of gap shares per series.
    Raises:
        DataQualityError: If the DataFrame fails any of the validation checks.
    """
    if df.is_empty():
        raise DataQualityError("empty frame")
    dtype = df.schema["ts_utc"]
    if not isinstance(dtype, pl.Datetime):
        raise DataQualityError(f"ts_utc must be a datetime column, got {dtype}")
    if dtype.time_zone != "UTC":
        raise DataQualityError(f"ts_utc must be UTC, got {dtype.time_zone}")
    dup = df.group_by(["series", "ts_utc"]).len().filter(pl.col("len") > 1)
    if dup.height:
        raise DataQualityError(f"{dup.height} duplicated (series, ts_utc) keys")

    report: dict[str, float] = {}
    cap = settings.interconnection_capacity_mw
    for name, g in df.group_by("series", maintain_order=True):
        s = str(name[0])
        g = g.sort("ts_utc")
        if s.startswith("flow_") and s.endswith("_mw"):
            out = g.filter(pl.col("value").abs() > cap * 1.05)  # 5 % tolerance for metering noise
            if out.height:
                raise DataQualityError(
                    f"{s}: {out.height} values beyond interconnection capacity {cap} MW"
                )
        if s.startswith("price_") and g.filter(pl.col("value") < -500).height:
            raise DataQualityError(f"{s}: implausible negative prices")
        t0, t1 = g["ts_utc"].min(), g["ts_utc"].max()
        if not isinstance(t0, datetime) or not isinstance(t1, datetime):
            raise DataQualityError(f"{s}: cannot read the time range")
        expected = int((t1 - t0).total_seconds() / 3600) + 1
        gap_share = 1 - g.height / expected
        report[f"{s}.gap_share"] = round(gap_share, 4)
        if gap_share > max_gap_share:
            raise DataQualityError(f"{s}: {gap_share:.1%} of hours missing (> {max_gap_share:.0%})")
    return report
