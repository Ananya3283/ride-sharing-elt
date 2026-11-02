"""Settings read from environment variables (see .env.example)."""

import os
import re
from dataclasses import dataclass

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


@dataclass(frozen=True)
class Settings:
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    lake_bucket: str
    warehouse_host: str
    warehouse_port: int
    warehouse_db: str
    warehouse_user: str
    warehouse_password: str
    sample_fraction: float

    @property
    def jdbc_url(self) -> str:
        return (
            f"jdbc:postgresql://{self.warehouse_host}:{self.warehouse_port}/{self.warehouse_db}"
            "?reWriteBatchedInserts=true"
        )


def load_settings() -> Settings:
    fraction = float(os.environ.get("SAMPLE_FRACTION", "0.05"))
    if not 0 < fraction <= 1:
        raise ValueError(f"SAMPLE_FRACTION must be in (0, 1], got {fraction}")
    return Settings(
        s3_endpoint=os.environ.get("S3_ENDPOINT", "http://localhost:9000"),
        s3_access_key=os.environ["S3_ACCESS_KEY"],
        s3_secret_key=os.environ["S3_SECRET_KEY"],
        lake_bucket=os.environ.get("LAKE_BUCKET", "ride-lake"),
        warehouse_host=os.environ.get("WAREHOUSE_HOST", "localhost"),
        warehouse_port=int(os.environ.get("WAREHOUSE_PORT", "5433")),
        warehouse_db=os.environ.get("WAREHOUSE_DB", "warehouse"),
        warehouse_user=os.environ["POSTGRES_USER"],
        warehouse_password=os.environ["POSTGRES_PASSWORD"],
        sample_fraction=fraction,
    )


def validate_month(month: str) -> str:
    """Return month if it looks like YYYY-MM, else raise."""
    if not MONTH_PATTERN.match(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")
    return month


def raw_trips_key(month: str) -> str:
    year, mm = validate_month(month).split("-")
    return f"raw/fhvhv/year={year}/month={mm}/fhvhv_tripdata_{month}.parquet"


def staged_trips_prefix(month: str) -> str:
    year, mm = validate_month(month).split("-")
    return f"staged/fhvhv/year={year}/month={mm}/"


RAW_ZONES_KEY = "raw/zones/taxi_zone_lookup.csv"


def source_trips_url(month: str) -> str:
    return f"{TLC_BASE_URL}/trip-data/fhvhv_tripdata_{validate_month(month)}.parquet"


SOURCE_ZONES_URL = f"{TLC_BASE_URL}/misc/taxi_zone_lookup.csv"
