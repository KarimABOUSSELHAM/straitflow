"""DuckDB over the Parquet lake: the feature store without a server.

Views
-----
raw_all      every row ever ingested (with revisions)
raw_current  latest extracted_at per (series, ts_utc)
raw_wide     one column per series, hourly grid (what models consume); the series list is
            read from the lake at connect time because DuckDB views need an explicit PIVOT list
"""

from __future__ import annotations

from pathlib import Path

import duckdb


def connect(raw_root: Path, db_path: Path | None = None) -> duckdb.DuckDBPyConnection:
    """Connect to a DuckDB instance over the Parquet lake.

    Args:
        raw_root (Path): root directory of the Parquet lake, e.g., "raw"
        db_path (Path | None): optional path to a persistent DuckDB database file; if
            None, an in-memory database is used

    Returns:
        duckdb.DuckDBPyConnection: DuckDB connection with the following views:
            * raw_all: every row ever ingested (with revisions)
            * raw_current: latest extracted_at per (series, ts_utc)
            * raw_wide: one column per series, hourly grid (what models consume); the
                series list is read from the lake at connect time because
                DuckDB views need an explicit PIVOT list
    """
    con = duckdb.connect(str(db_path) if db_path else ":memory:")
    glob = str(raw_root / "*" / "*" / "*.parquet").replace("\\", "/")
    con.execute(f"CREATE OR REPLACE VIEW raw_all AS SELECT * FROM read_parquet('{glob}')")
    con.execute(
        """
        CREATE OR REPLACE VIEW raw_current AS
        SELECT ts_utc, series, value, source, extracted_at
        FROM (
            SELECT *,
                row_number() OVER (PARTITION BY series, ts_utc ORDER BY extracted_at DESC) AS rn
            FROM raw_all
        ) WHERE rn = 1
        """
    )
    # DuckDB views need an explicit PIVOT list, so we read the series list from the lake
    series = [
        r[0] for r in con.execute("SELECT DISTINCT series FROM raw_current ORDER BY 1").fetchall()
    ]
    # Create the raw_wide view if there are any series; otherwise, leave it empty
    if series:
        quoted = ", ".join(f"'{s}'" for s in series)
        con.execute(
            f"""
            CREATE OR REPLACE VIEW raw_wide AS
            PIVOT raw_current ON series IN ({quoted}) USING first(value)
            GROUP BY ts_utc ORDER BY ts_utc
            """
        )
    return con
