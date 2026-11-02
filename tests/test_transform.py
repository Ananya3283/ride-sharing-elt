from datetime import datetime

import pytest

from pipeline.transform import clean_trips

MONTH = "2026-06"

# Schema of the TLC source files (minus columns we don't use), so nulls never break type inference.
RAW_SCHEMA = """
    hvfhs_license_num string, dispatching_base_num string, request_datetime timestamp_ntz,
    pickup_datetime timestamp_ntz, dropoff_datetime timestamp_ntz, PULocationID int, DOLocationID int,
    trip_miles double, trip_time bigint, base_passenger_fare double, tolls double, bcf double,
    sales_tax double, congestion_surcharge double, airport_fee double, tips double, driver_pay double,
    shared_request_flag string, shared_match_flag string, wav_request_flag string, wav_match_flag string
"""


def raw_row(**overrides):
    row = {
        "hvfhs_license_num": "HV0003",
        "dispatching_base_num": "B03404",
        "request_datetime": datetime(2026, 6, 10, 8, 0),
        "pickup_datetime": datetime(2026, 6, 10, 8, 5),
        "dropoff_datetime": datetime(2026, 6, 10, 8, 25),
        "PULocationID": 161,
        "DOLocationID": 132,
        "trip_miles": 4.2,
        "trip_time": 1200,
        "base_passenger_fare": 25.5,
        "tolls": 0.0,
        "bcf": 0.7,
        "sales_tax": 2.2,
        "congestion_surcharge": 2.75,
        "airport_fee": None,
        "tips": 3.0,
        "driver_pay": 19.1,
        "shared_request_flag": "N",
        "shared_match_flag": "N",
        "wav_request_flag": "Y",
        "wav_match_flag": "N",
    }
    row.update(overrides)
    return row


@pytest.fixture
def make_df(spark):
    def _make(rows):
        return spark.createDataFrame(rows, schema=RAW_SCHEMA)

    return _make


def test_good_row_is_kept_and_standardised(make_df):
    out = clean_trips(make_df([raw_row()]), MONTH).collect()
    assert len(out) == 1
    row = out[0]
    assert row.pickup_location_id == 161
    assert row.trip_seconds == 1200
    assert row.black_car_fund == 0.7
    assert row.airport_fee == 0.0  # null fee filled
    assert row.cbd_congestion_fee == 0.0  # column missing in older files
    assert row.is_wav_request is True and row.is_shared_request is False
    assert len(row.trip_id) == 64
    assert row.source_month == MONTH
    assert str(row.pickup_date) == "2026-06-10"


@pytest.mark.parametrize(
    "bad",
    [
        {"pickup_datetime": datetime(2026, 5, 31, 23, 59)},  # previous month
        {"pickup_datetime": datetime(2026, 7, 1, 0, 0), "dropoff_datetime": datetime(2026, 7, 1, 0, 20)},
        {"dropoff_datetime": datetime(2026, 6, 10, 8, 0)},  # ends before it starts
        {"trip_miles": -1.0},
        {"trip_miles": 1000.0},
        {"trip_time": 0},
        {"base_passenger_fare": -5.0},
        {"driver_pay": -2.0},
        {"PULocationID": None},
    ],
)
def test_invalid_rows_are_rejected(make_df, bad):
    assert clean_trips(make_df([raw_row(), raw_row(**bad)]), MONTH).count() == 1


def test_exact_duplicates_are_removed(make_df):
    assert clean_trips(make_df([raw_row(), raw_row(), raw_row(trip_miles=5.0)]), MONTH).count() == 2


def test_trip_id_is_deterministic(make_df):
    first = clean_trips(make_df([raw_row()]), MONTH).first().trip_id
    second = clean_trips(make_df([raw_row()]), MONTH).first().trip_id
    assert first == second
