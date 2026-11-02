"""Load: copy staged trips and the zone lookup from the lake into the warehouse raw schema.

Spark writes the month into a landing table over several parallel connections. One Postgres
transaction then deletes the month from raw.fhvhv_trips and inserts it from the landing table.
Re-running a month never duplicates rows, and a failed run never leaves a month half-loaded.
"""

import csv
import io
import logging

from pyspark.sql import SparkSession

from pipeline.config import RAW_ZONES_KEY, Settings, staged_trips_prefix, validate_month
from pipeline.lake import s3_client
from pipeline.warehouse import connect, ensure_raw_schema

log = logging.getLogger(__name__)

TRIPS_TABLE = "raw.fhvhv_trips"
INCOMING_TABLE = "raw.fhvhv_trips_incoming"


def load_trips(month: str, settings: Settings, spark: SparkSession) -> dict:
    validate_month(month)
    conn = connect(settings)
    try:
        ensure_raw_schema(conn)
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE {INCOMING_TABLE}")
        conn.commit()
    finally:
        conn.close()

    staged = spark.read.parquet(f"s3a://{settings.lake_bucket}/{staged_trips_prefix(month)}")
    (
        staged.write.format("jdbc")
        .option("url", settings.jdbc_url)
        .option("dbtable", INCOMING_TABLE)
        .option("user", settings.warehouse_user)
        .option("password", settings.warehouse_password)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", 10_000)
        .option("numPartitions", 4)
        .mode("append")
        .save()
    )

    conn = connect(settings)
    try:
        with conn.cursor() as cur:  # one transaction: readers see the old month or the new one
            cur.execute(f"DELETE FROM {TRIPS_TABLE} WHERE source_month = %s", (month,))
            replaced = cur.rowcount
            cur.execute(
                f"INSERT INTO {TRIPS_TABLE} SELECT * FROM {INCOMING_TABLE} WHERE source_month = %s",
                (month,),
            )
            loaded = cur.rowcount
            cur.execute(f"TRUNCATE {INCOMING_TABLE}")
        conn.commit()
    finally:
        conn.close()
    log.info("Swapped in %s: %d rows replaced by %d", month, replaced, loaded)
    return {"month": month, "replaced_rows": replaced, "loaded_rows": loaded}


def load_zones(settings: Settings) -> int:
    body = s3_client(settings).get_object(Bucket=settings.lake_bucket, Key=RAW_ZONES_KEY)["Body"]
    rows = [
        (int(r["LocationID"]), r["Borough"], r["Zone"], r["service_zone"])
        for r in csv.DictReader(io.StringIO(body.read().decode("utf-8")))
    ]
    conn = connect(settings)
    try:
        ensure_raw_schema(conn)
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO raw.taxi_zones (location_id, borough, zone, service_zone)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (location_id) DO UPDATE
                SET borough = EXCLUDED.borough, zone = EXCLUDED.zone,
                    service_zone = EXCLUDED.service_zone, _loaded_at = now()
                """,
                rows,
            )
        conn.commit()
    finally:
        conn.close()
    log.info("Upserted %d zones", len(rows))
    return len(rows)
