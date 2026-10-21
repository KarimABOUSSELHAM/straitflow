from datetime import UTC, datetime

from straitflow.ingest.base import LONG_SCHEMA
from straitflow.ingest.fake import FakeConnector


def test_fake_connector_respects_the_contract():
    df = FakeConnector().fetch(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 8, tzinfo=UTC))
    assert list(df.columns) == list(LONG_SCHEMA)
    assert df.height == 7 * 24 * 2  # two series, hourly, one week
    assert df.schema["ts_utc"].time_zone == "UTC"
    assert df["series"].unique().sort().to_list() == [
        "flow_es_ma_physical_mw",
        "price_da_es_eur_mwh",
    ]


def test_fake_connector_is_deterministic():
    a = FakeConnector(seed=3).fetch(
        datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 2, tzinfo=UTC)
    )
    b = FakeConnector(seed=3).fetch(
        datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 2, tzinfo=UTC)
    )
    assert a.drop("extracted_at").equals(b.drop("extracted_at"))
