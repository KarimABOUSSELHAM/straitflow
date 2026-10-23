from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from straitflow.ingest.fake import FakeConnector
from straitflow.store.duck import connect
from straitflow.store.validate import DataQualityError, check_long_frame


def test_quality_report_is_clean_on_fake_data():
    """Test the quality report is clean when fake data is ingested"""
    df = FakeConnector().fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 8, tzinfo=UTC))
    assert all(v == 0 for v in check_long_frame(df).values())


def test_quality_gate_rejects_out_of_capacity():
    """Test that the quality gate rejects data that exceeds interconnection capacity"""
    df = FakeConnector().fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 2, tzinfo=UTC))
    bad = df.with_columns(
        pl.when(pl.col("series").str.starts_with("flow"))
        .then(9_999.0)
        .otherwise(pl.col("value"))
        .alias("value")
    )
    with pytest.raises(DataQualityError):
        check_long_frame(bad)


def test_write_is_idempotent(lake):
    """Test that writing the same data twice does not create duplicates"""
    df = FakeConnector().fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 3, tzinfo=UTC))
    s1 = lake.write(df)
    s2 = lake.write(df)
    assert s1["new"] == df.height and s2["new"] == 0 and s2["revised"] == 0
    assert lake.read().height == df.height


def test_revisions_are_journaled_and_point_in_time_read_works(lake):
    """Test that revisions are journaled and point-in-time reads work"""
    t0, t1 = datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 2, tzinfo=UTC)
    first = FakeConnector(seed=1).fetch(t0, t1)
    lake.write(first)
    later = first.with_columns(
        (pl.col("value") + 10).alias("value"),
        (pl.col("extracted_at") + timedelta(days=1)).alias("extracted_at"),
    )
    assert lake.write(later)["revised"] == first.height
    assert (lake.root / "_revisions.parquet").exists()
    cur = lake.read(series="flow_es_ma_physical_mw").sort("ts_utc")
    old = lake.read(series="flow_es_ma_physical_mw", as_of=first["extracted_at"][0]).sort("ts_utc")
    assert (cur["value"] - old["value"]).round(6).unique().to_list() == [10.0]


def test_duckdb_wide_view(filled_lake):
    con = connect(filled_lake.root)
    cols = [c[0] for c in con.execute("DESCRIBE raw_wide").fetchall()]
    assert "flow_es_ma_physical_mw" in cols and "price_da_es_eur_mwh" in cols
    assert con.execute("SELECT count(*) FROM raw_wide").fetchone()[0] == 45 * 24
