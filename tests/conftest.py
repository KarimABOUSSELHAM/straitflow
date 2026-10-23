from datetime import UTC, datetime

import pytest

from straitflow.ingest.fake import FakeConnector
from straitflow.store.parquet import ParquetLake


@pytest.fixture
def lake(tmp_path):
    return ParquetLake(tmp_path / "raw")


@pytest.fixture
def filled_lake(lake):  # pylint: disable=redefined-outer-name
    df = FakeConnector(seed=1).fetch(
        datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 2, 15, tzinfo=UTC)
    )
    lake.write(df)
    return lake
