"""Parquet lake: raw/<source>/<series>/<YYYY-MM>.parquet, append-only with revision tracking.

Rules
-----
* Idempotent: re-ingesting the same period never duplicates a (ts_utc, series) pair.
* Revisions are kept, not overwritten: if a value for an existing hour changes, the new
    row is appended with its own `extracted_at`, and the change is logged to
    raw/_revisions.parquet. Readers take the latest `extracted_at` per hour by default
    (the "current" view) or can reconstruct what was known at any point in time.
* Every written file is hashed; the hash and write time go to raw/_manifest.parquet so
    that any model run can cite the exact data version it used.
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path

import polars as pl
from loguru import logger

from straitflow.ingest.base import LONG_SCHEMA, empty_long, now_utc, publication_lag

KEY = ["ts_utc", "series"]


def _file_hash(path: Path) -> str:
    """Private: 16-char hex digest of the file contents.

    Args:
        path (Path): path to the file to hash

    Returns:
        str: 16-character hex digest of the file contents
    """
    return hashlib.sha256(path.read_bytes()).hexdigest()[:16]


class ParquetLake:
    """Implement a data lake on top of parquet files using polars"""

    def __init__(self, root: Path) -> None:
        """Initialize a ParquetLake instance.

        Args:
            root (Path): root directory for the lake, e.g., "raw".
            he lake will create this directory if it does not exist.
        """
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ write
    def write(self, df: pl.DataFrame) -> dict[str, int]:
        """Merge `df` into the lake.

        Args:
            df (pl.DataFrame): DataFrame to merge into the lake. Must have columns:
                * ts_utc (datetime, UTC)
                * series (str)
                * value (float)

        Returns:
            dict[str, int]: Statistics of the merge operation, including counts of new
            revised, and unchanged rows.
        """
        if df.is_empty():
            return {"new": 0, "revised": 0, "unchanged": 0}
        df = df.with_columns(pl.col("ts_utc").dt.strftime("%Y-%m").alias("_month"))
        stats = {"new": 0, "revised": 0, "unchanged": 0}
        revisions: list[pl.DataFrame] = []
        for (source, series, month), part in df.group_by(
            ["source", "series", "_month"], maintain_order=True
        ):
            path = self.root / str(source) / str(series) / f"{month}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            incoming = part.drop("_month")
            if path.exists():
                existing = pl.read_parquet(path)
                current = self._latest(existing)
                joined = incoming.join(
                    current.select(KEY + ["value"]).rename({"value": "old"}), on=KEY, how="left"
                )
                new = joined.filter(pl.col("old").is_null())
                changed = joined.filter(
                    pl.col("old").is_not_null() & (pl.col("old") != pl.col("value"))
                )
                stats["new"] += new.height
                stats["revised"] += changed.height
                stats["unchanged"] += joined.height - new.height - changed.height
                if changed.height:
                    revisions.append(changed.select(KEY + ["old", "value", "extracted_at"]))
                to_add = pl.concat([new, changed]).drop("old")
                if to_add.is_empty():
                    continue
                merged = pl.concat([existing, to_add]).sort(KEY + ["extracted_at"])
            else:
                stats["new"] += incoming.height
                merged = incoming.sort(KEY + ["extracted_at"])
            merged.write_parquet(path, compression="zstd")
            self._manifest_add(path, merged.height)
        if revisions:
            rev = pl.concat(revisions)
            rev_path = self.root / "_revisions.parquet"
            if rev_path.exists():
                rev = pl.concat([pl.read_parquet(rev_path), rev])
            rev.write_parquet(rev_path)
            logger.warning("{} revised values detected and journaled", stats["revised"])
        logger.info("lake write: {}", stats)
        return stats

    def _manifest_add(self, path: Path, rows: int) -> None:
        """Stores metadata about the written parquet file in a manifest file.

        Args:
            path (Path): Path to the written parquet file.
            rows (int): Number of rows written to the parquet file.
        """
        entry = pl.DataFrame(
            {
                "path": [str(path.relative_to(self.root))],
                "sha256_16": [_file_hash(path)],
                "rows": [rows],
                "written_at": [now_utc()],
            }
        ).with_columns(pl.col("written_at").dt.replace_time_zone("UTC"))
        mpath = self.root / "_manifest.parquet"
        if mpath.exists():
            entry = pl.concat([pl.read_parquet(mpath), entry])
        entry.write_parquet(mpath)

    # ------------------------------------------------------------------- read
    @staticmethod
    def _latest(df: pl.DataFrame) -> pl.DataFrame:
        """Latest extracted_at per (ts_utc, series)."""
        return df.sort("extracted_at").group_by(KEY, maintain_order=True).last()

    def read(
        self,
        series: str | list[str] | None = None,
        as_of: datetime | None = None,
    ) -> pl.DataFrame:
        """Current view of the lake, or the view as it was known at `as_of` (point-in-time).

        A row is visible at `as_of` when BOTH hold:
          * its value was public: ts_utc + publication_lag(series) <= as_of
            (this is what matters for bulk back-filled history, whose extracted_at is "today");
          * we had extracted it: extracted_at <= as_of, OR it is the first version we hold
            of that hour (a bulk back-fill is assumed to carry the originally published value).
        Revisions (later extracted_at for the same hour) are therefore hidden until extracted.
        The returned frame carries `published_at` so feature builders can set `available_at`.

        Args:
            series (str | list[str] | None): Series to filter by. If None, all series are returned.
            as_of (datetime | None): Point-in-time to view the lake.
            If None, the current view is returned.

        Returns:
            pl.DataFrame: DataFrame with the latest values for each (ts_utc, series) pair
            filtered by series and as_of if provided.
            The DataFrame includes a 'published_at' column indicating when the data was published.
        """
        files = list(self.root.glob("*/*/*.parquet"))
        if not files:
            return empty_long().with_columns(
                pl.lit(None).cast(pl.Datetime("us", "UTC")).alias("published_at")
            )
        df = pl.concat([pl.read_parquet(f) for f in files], how="vertical_relaxed")
        if series is not None:
            wanted = [series] if isinstance(series, str) else series
            df = df.filter(pl.col("series").is_in(wanted))
        lag = pl.col("series").map_elements(
            lambda s: int(publication_lag(s).total_seconds() * 1_000_000), return_dtype=pl.Int64
        )
        df = df.with_columns(
            (pl.col("ts_utc") + pl.duration(microseconds=lag)).alias("published_at")
        )
        if as_of is not None:
            first_extraction = pl.col("extracted_at") == pl.col("extracted_at").min().over(KEY)
            df = df.filter(
                (pl.col("published_at") <= as_of)
                & ((pl.col("extracted_at") <= as_of) | first_extraction)
            )
        return self._latest(df).select([*LONG_SCHEMA, "published_at"]).sort(KEY)

    def manifest(self) -> pl.DataFrame:
        """Read _manifest.parquet and return its contents as a DataFrame.

        Returns:
            pl.DataFrame: DataFrame containing the manifest of written parquet files
            including their paths, SHA256 hashes, row counts, and write timestamps.
            If the manifest file does not exist, an empty DataFrame is returned.
        """
        p = self.root / "_manifest.parquet"
        return pl.read_parquet(p) if p.exists() else pl.DataFrame()
