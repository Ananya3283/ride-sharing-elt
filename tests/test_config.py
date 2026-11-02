from datetime import date

import pytest

from pipeline.config import raw_trips_key, source_trips_url, staged_trips_prefix, validate_month
from pipeline.transform import month_bounds


@pytest.mark.parametrize("month", ["2026-01", "2025-12"])
def test_validate_month_accepts_yyyy_mm(month):
    assert validate_month(month) == month


@pytest.mark.parametrize("month", ["2026-13", "2026-1", "26-01", "2026/01", ""])
def test_validate_month_rejects_bad_input(month):
    with pytest.raises(ValueError):
        validate_month(month)


def test_lake_keys_are_hive_partitioned():
    assert raw_trips_key("2026-06") == "raw/fhvhv/year=2026/month=06/fhvhv_tripdata_2026-06.parquet"
    assert staged_trips_prefix("2026-06") == "staged/fhvhv/year=2026/month=06/"


def test_source_url():
    assert source_trips_url("2026-06").endswith("/trip-data/fhvhv_tripdata_2026-06.parquet")


def test_month_bounds_handles_december():
    assert month_bounds("2025-12") == (date(2025, 12, 1), date(2026, 1, 1))
    assert month_bounds("2026-06") == (date(2026, 6, 1), date(2026, 7, 1))
