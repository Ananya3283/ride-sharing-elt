"""Postgres warehouse connection and raw-layer DDL."""

from pathlib import Path

import psycopg2

from pipeline.config import Settings

RAW_DDL = (Path(__file__).parent / "sql" / "raw_ddl.sql").read_text()


def connect(settings: Settings):
    return psycopg2.connect(
        host=settings.warehouse_host,
        port=settings.warehouse_port,
        dbname=settings.warehouse_db,
        user=settings.warehouse_user,
        password=settings.warehouse_password,
    )


def ensure_raw_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(RAW_DDL)
    conn.commit()
