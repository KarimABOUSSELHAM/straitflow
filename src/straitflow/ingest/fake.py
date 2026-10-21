"""Fake connector: lets the whole pipeline run end to end on day 1, before any API token.

The synthetic flow has a daily shape, a weekly shape, noise, and is clipped to the
interconnection capacity so that validation rules are exercised for real.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import polars as pl

from straitflow.config import settings
from straitflow.ingest.base import Connector, now_utc


class FakeConnector(Connector):
    name = "fake"

    def __init__(self, seed: int = 0) -> None:
        self.rng = np.random.default_rng(seed)

    def fetch(self, start: datetime, end: datetime) -> pl.DataFrame:
        n = int((end - start) / timedelta(hours=1))
        ts = [start + timedelta(hours=i) for i in range(n)]
        hour = np.array([t.hour for t in ts])
        dow = np.array([t.weekday() for t in ts])
        cap = settings.interconnection_capacity_mw
        daily = 450 + 250 * np.sin((hour - 6) / 24 * 2 * np.pi)
        weekly = np.where(dow >= 5, -120, 0)
        noise = self.rng.normal(0, 80, n)
        flow = np.clip(daily + weekly + noise, -cap, cap)
        price = np.clip(
            60 + 30 * np.sin((hour - 8) / 24 * 2 * np.pi) + self.rng.normal(0, 10, n), 0, None
        )
        extracted = now_utc()
        frames = []
        for series, values in (
            ("flow_es_ma_physical_mw", flow),
            ("price_da_es_eur_mwh", price),
        ):
            frames.append(
                pl.DataFrame(
                    {
                        "ts_utc": ts,
                        "series": series,
                        "value": values.astype(float),
                        "source": self.name,
                        "extracted_at": extracted,
                    }
                ).with_columns(
                    pl.col("ts_utc").dt.replace_time_zone("UTC"),
                    pl.col("extracted_at").dt.replace_time_zone("UTC"),
                )
            )
        return self.conform(pl.concat(frames))
