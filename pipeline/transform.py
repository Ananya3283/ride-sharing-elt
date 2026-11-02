"""Transform (lake side): standardise, validate and de-duplicate raw trips with PySpark.

Business modelling happens later in dbt. Spark's job here is the heavy, row-level work on
~20M rows a month: enforce a schema, drop records that can't be real trips, give each trip
a stable id, and write partitioned Parquet to the staged zone.
"""

import logging
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.config import Settings, raw_trips_key, staged_trips_prefix, validate_month

log = logging.getLogger(__name__)

# source column -> (target column, Spark type)
COLUMNS = {
    "hvfhs_license_num": ("hvfhs_license_num", "string"),
    "dispatching_base_num": ("dispatching_base_num", "string"),
    "request_datetime": ("request_datetime", "timestamp"),
    "pickup_datetime": ("pickup_datetime", "timestamp"),
    "dropoff_datetime": ("dropoff_datetime", "timestamp"),
    "PULocationID": ("pickup_location_id", "int"),
    "DOLocationID": ("dropoff_location_id", "int"),
    "trip_miles": ("trip_miles", "double"),
    "trip_time": ("trip_seconds", "bigint"),
    "base_passenger_fare": ("base_passenger_fare", "double"),
    "tolls": ("tolls", "double"),
    "bcf": ("black_car_fund", "double"),
    "sales_tax": ("sales_tax", "double"),
    "congestion_surcharge": ("congestion_surcharge", "double"),
    "airport_fee": ("airport_fee", "double"),
    "cbd_congestion_fee": ("cbd_congestion_fee", "double"),  # only exists from Jan 2025
    "tips": ("tips", "double"),
    "driver_pay": ("driver_pay", "double"),
}
FLAG_COLUMNS = {
    "shared_request_flag": "is_shared_request",
    "shared_match_flag": "is_shared_match",
    "wav_request_flag": "is_wav_request",
    "wav_match_flag": "is_wav_match",
}
FEE_COLUMNS = [
    "tolls",
    "black_car_fund",
    "sales_tax",
    "congestion_surcharge",
    "airport_fee",
    "cbd_congestion_fee",
    "tips",
]
TRIP_ID_PARTS = [
    "hvfhs_license_num",
    "dispatching_base_num",
    "request_datetime",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_location_id",
    "dropoff_location_id",
    "trip_miles",
]
MAX_TRIP_MILES = 300
MAX_TRIP_SECONDS = 6 * 3600


def month_bounds(month: str) -> tuple[date, date]:
    year, mm = map(int, validate_month(month).split("-"))
    start = date(year, mm, 1)
    end = date(year + (mm == 12), mm % 12 + 1, 1)
    return start, end


def standardise(raw: DataFrame) -> DataFrame:
    """Rename to snake_case, cast types, turn Y/N flags into booleans, fill missing fees."""
    cols = []
    for src, (dst, dtype) in COLUMNS.items():
        expr = F.col(src) if src in raw.columns else F.lit(None)
        cols.append(expr.cast(dtype).alias(dst))
    for src, dst in FLAG_COLUMNS.items():
        expr = F.upper(F.trim(F.col(src))) == "Y" if src in raw.columns else F.lit(None)
        cols.append(expr.cast("boolean").alias(dst))
    df = raw.select(*cols)
    return df.fillna(0.0, subset=FEE_COLUMNS)


def validate(df: DataFrame, month: str) -> DataFrame:
    """Keep only rows that can be real trips that started in the target month."""
    start, end = month_bounds(month)
    return df.where(
        F.col("hvfhs_license_num").isNotNull()
        & F.col("pickup_datetime").isNotNull()
        & F.col("dropoff_datetime").isNotNull()
        & F.col("pickup_location_id").isNotNull()
        & F.col("dropoff_location_id").isNotNull()
        & (F.col("pickup_datetime") >= F.lit(start).cast("timestamp"))
        & (F.col("pickup_datetime") < F.lit(end).cast("timestamp"))
        & (F.col("dropoff_datetime") > F.col("pickup_datetime"))
        & F.col("trip_miles").between(0, MAX_TRIP_MILES)
        & F.col("trip_seconds").between(1, MAX_TRIP_SECONDS)
        & (F.col("base_passenger_fare") >= 0)
        & (F.col("driver_pay") >= 0)
    )


def add_trip_id(df: DataFrame) -> DataFrame:
    """Deterministic id from the fields that identify a trip, then drop exact repeats."""
    key = F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in TRIP_ID_PARTS])
    return df.withColumn("trip_id", F.sha2(key, 256)).dropDuplicates(["trip_id"])


def clean_trips(raw: DataFrame, month: str) -> DataFrame:
    df = add_trip_id(validate(standardise(raw), month))
    return df.withColumn("pickup_date", F.to_date("pickup_datetime")).withColumn("source_month", F.lit(month))


def transform_month(month: str, settings: Settings, spark: SparkSession) -> dict:
    src = f"s3a://{settings.lake_bucket}/{raw_trips_key(month)}"
    dst = f"s3a://{settings.lake_bucket}/{staged_trips_prefix(month)}"

    raw = spark.read.parquet(src)
    if settings.sample_fraction < 1:
        raw = raw.sample(fraction=settings.sample_fraction, seed=42)
    raw = raw.cache()
    raw_rows = raw.count()

    cleaned = clean_trips(raw, month)
    cleaned.repartition(8).write.mode("overwrite").parquet(dst)

    staged_rows = spark.read.parquet(dst).count()
    raw.unpersist()
    rejected = raw_rows - staged_rows
    log.info("%s: %d sampled rows, %d staged, %d rejected", month, raw_rows, staged_rows, rejected)
    return {"month": month, "raw_rows": raw_rows, "staged_rows": staged_rows, "rejected_rows": rejected}
