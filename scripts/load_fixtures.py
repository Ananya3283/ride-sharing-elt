"""Load the small CSV fixtures in ci/fixtures into the raw schema (used by CI to run dbt build)."""

from pathlib import Path

from pipeline.config import load_settings
from pipeline.warehouse import connect, ensure_raw_schema

FIXTURES = Path(__file__).resolve().parents[1] / "ci" / "fixtures"
TABLES = {
    "raw.taxi_zones": ("taxi_zones.csv", "location_id, borough, zone, service_zone"),
    "raw.fhvhv_trips": ("fhvhv_trips.csv", None),
}


def main() -> None:
    conn = connect(load_settings())
    try:
        ensure_raw_schema(conn)
        with conn.cursor() as cur:
            for table, (filename, columns) in TABLES.items():
                cur.execute(f"TRUNCATE {table}")
                path = FIXTURES / filename
                header = columns or path.open().readline().strip()
                with path.open() as f:
                    cur.copy_expert(f"COPY {table} ({header}) FROM STDIN WITH CSV HEADER", f)
                print(f"{table}: {cur.rowcount} rows")
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
