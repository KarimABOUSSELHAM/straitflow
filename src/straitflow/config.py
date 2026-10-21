"""Central configuration. Every parameter comes from the environment or a .env file.

The principle is :no constant is hard-coded in a service;
everything is a pydantic-settings field with a documented default.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="STRAITFLOW_", env_file=".env", extra="ignore")

    # --- Paths -------------------------------------------------------------
    data_dir: Path = Field(default=Path("data"), description="Root of the Parquet lake.")

    # --- API tokens (empty => the connector falls back to the fake source) ----
    entsoe_token: str = Field(default="", description="ENTSO-E Transparency API token.")
    esios_token: str = Field(default="", description="ESIOS (REE) API token.")

    # --- Domain --------------------------------------------------------------
    zone_es: str = "ES"
    zone_ma: str = "MA"
    # Physical capacity of the ES-MA interconnection, MW (two submarine cables).
    # Used as a hard physical bound in data validation. Source to confirm in week 2
    # (REE/ENTSO-E NTC publications) and record in docs/data_dictionary.md.
    interconnection_capacity_mw: float = 1_400.0

    # --- Forecasting ---------------------------------------------------------
    horizons_h: list[int] = Field(default=[24, 48, 72])
    quantiles: list[float] = Field(default=[0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95])
    issue_time_utc: str = Field(
        default="07:00",
        description="Daily forecast issue time (UTC). Features must be known by then.",
    )

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def features_dir(self) -> Path:
        return self.data_dir / "features"


settings = Settings()
