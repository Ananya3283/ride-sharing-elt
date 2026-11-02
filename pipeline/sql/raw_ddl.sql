-- Raw layer of the warehouse. Loaded by pipeline/load.py (and by CI from fixtures); modelled by dbt.
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.fhvhv_trips (
    trip_id               CHAR(64)          NOT NULL,
    hvfhs_license_num     TEXT              NOT NULL,
    dispatching_base_num  TEXT,
    request_datetime      TIMESTAMP,
    pickup_datetime       TIMESTAMP         NOT NULL,
    dropoff_datetime      TIMESTAMP         NOT NULL,
    pickup_location_id    INTEGER           NOT NULL,
    dropoff_location_id   INTEGER           NOT NULL,
    trip_miles            DOUBLE PRECISION,
    trip_seconds          BIGINT,
    base_passenger_fare   DOUBLE PRECISION,
    tolls                 DOUBLE PRECISION,
    black_car_fund        DOUBLE PRECISION,
    sales_tax             DOUBLE PRECISION,
    congestion_surcharge  DOUBLE PRECISION,
    airport_fee           DOUBLE PRECISION,
    cbd_congestion_fee    DOUBLE PRECISION,
    tips                  DOUBLE PRECISION,
    driver_pay            DOUBLE PRECISION,
    is_shared_request     BOOLEAN,
    is_shared_match       BOOLEAN,
    is_wav_request        BOOLEAN,
    is_wav_match          BOOLEAN,
    pickup_date           DATE              NOT NULL,
    source_month          CHAR(7)           NOT NULL,
    _loaded_at            TIMESTAMP         NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS fhvhv_trips_source_month_idx ON raw.fhvhv_trips (source_month);

-- Landing table for one month in flight. Spark writes here; load.py then swaps the month into
-- raw.fhvhv_trips in a single transaction, so readers never see a half-loaded month.
CREATE TABLE IF NOT EXISTS raw.fhvhv_trips_incoming (LIKE raw.fhvhv_trips INCLUDING DEFAULTS);

CREATE TABLE IF NOT EXISTS raw.taxi_zones (
    location_id   INTEGER PRIMARY KEY,
    borough       TEXT,
    zone          TEXT,
    service_zone  TEXT,
    _loaded_at    TIMESTAMP NOT NULL DEFAULT now()
);
