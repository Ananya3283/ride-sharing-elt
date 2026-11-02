# Ride-Sharing ELT Pipeline and Data Warehouse

[![CI](https://github.com/Ananya3283/ride-sharing-elt/actions/workflows/ci.yml/badge.svg)](https://github.com/Ananya3283/ride-sharing-elt/actions/workflows/ci.yml)
![Python 3.12](https://img.shields.io/badge/python-3.12-3776AB?logo=python&logoColor=white)
![PySpark 3.5](https://img.shields.io/badge/PySpark-3.5-E25A1C?logo=apachespark&logoColor=white)
![Airflow 3](https://img.shields.io/badge/Airflow-3.2-017CEE?logo=apacheairflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-1.12-FF694B?logo=dbt&logoColor=white)
![Postgres 16](https://img.shields.io/badge/Postgres-16-4169E1?logo=postgresql&logoColor=white)
![License: MIT](https://img.shields.io/badge/license-MIT-green)

A monthly batch ELT pipeline for **NYC Uber and Lyft trips** (TLC High Volume For-Hire Vehicle
data, ~20M trips a month). It lands raw files in an S3-compatible data lake, cleans them with
**PySpark**, loads them into **PostgreSQL**, and models a **star schema with dbt**. **Airflow**
orchestrates it, everything runs in **Docker**, and **GitHub Actions** tests every push.

```
NYC TLC (Parquet, ~500 MB/month)
   │  extract      Python → lake raw zone                    s3://ride-lake/raw/...
   ▼
S3-compatible lake (RustFS locally; AWS S3 by config)
   │  transform    PySpark: schema, validation, dedupe, ids  s3://ride-lake/staged/...
   │  load         Spark JDBC → Postgres raw.fhvhv_trips (month replaced as a unit)
   ▼
PostgreSQL warehouse
   │  dbt          staging views → fct_trips + dim_company, dim_zone, dim_date
   ▼               → mart_daily_company_metrics, guarded by 31 data tests
BI / SQL analysis
```

## Results from a real run

Two months (June and July 2026) at the default 5% sample, run through Airflow in Docker on a
laptop (12 cores, 8 GB for Docker):

| | |
|---|---|
| Trips in the warehouse | 2,083,221 (June 1,037,871; July 1,045,350) |
| dbt data tests | 31 of 31 passing |
| Runtime for one month | ~4 min: extract 115 s, Spark transform 56 s, load 38 s, dbt run 24 s, dbt test 15 s |
| Retries | The June download from TLC timed out once; Airflow's retry finished it |

What the June star schema shows:

| Company | Trips (sample) | Avg miles | Avg trip | Avg wait for pickup | Avg rider fare (before tips) | Driver pay as % of fare |
|---|---|---|---|---|---|---|
| Uber | 761,995 | 5.08 | 20.7 min | 5.9 min | $35.52 | 64% |
| Lyft | 275,876 | 4.76 | 19.7 min | 5.5 min | $32.44 | 63% |

A full design walkthrough, with the reasoning behind each choice, is in
[docs/WALKTHROUGH.md](docs/WALKTHROUGH.md).

## Quick start

Needs Docker Desktop (with WSL 2 on Windows) and ~8 GB RAM for Docker.

```bash
cp .env.example .env                 # local-only credentials
docker compose up -d --build         # first build takes a few minutes
```

| Service | URL | Login |
|---|---|---|
| Airflow | http://localhost:8080 | none (local all-admin mode) |
| Lake console (RustFS) | http://localhost:9001 | `S3_ACCESS_KEY` / `S3_SECRET_KEY` from `.env` |
| Postgres warehouse | `localhost:5433`, db `warehouse` | `POSTGRES_USER` / `POSTGRES_PASSWORD` |

In Airflow, unpause `ride_sharing_elt`, then **Trigger DAG w/ config** with `{"month": "2026-06"}`.
Run it again for other months to build history; re-running a month never duplicates data.

Without Airflow (same code, one step at a time):

```bash
docker compose exec airflow-scheduler python -m pipeline.cli all 2026-06
```

## What each layer does

| Layer | Where | Responsibility |
|---|---|---|
| Extract | `pipeline/extract.py` | Stream the month's Parquet and the zone lookup from TLC into `raw/` (Hive-style `year=/month=` paths). Skips files already present. |
| Transform | `pipeline/transform.py` | Rename to snake_case, cast types, Y/N flags to booleans, fill missing fees with 0, drop impossible trips (outside the month, ends before it starts, negative fares, >300 miles, >6 h), build a SHA-256 `trip_id`, de-duplicate, write Parquet to `staged/`. |
| Load | `pipeline/load.py` | Spark writes staged rows over JDBC into a landing table; one transaction swaps the month into `raw.fhvhv_trips`. Zones are upserted. |
| Model | `dbt/ride_sharing/` | Staging views (derived wait time, rider fare), a star schema and a daily mart. `fct_trips` is incremental and replaces whole months. |
| Orchestrate | `dags/ride_sharing_elt.py` | `resolve_month → extract → spark_transform → load → dbt_run → dbt_test`, monthly, with retries. |
| CI | `.github/workflows/ci.yml` | Ruff lint and format, PySpark unit tests, then `dbt build` twice against a Postgres service loaded with fixtures (the second run checks the incremental path). |

## Warehouse model

```
                dim_date (date_key)
                      │
dim_company ── fct_trips ── dim_zone (pickup_zone_key)
(company_key)   (trip_id) └─ dim_zone (dropoff_zone_key)
                      │
          mart_daily_company_metrics (day × company)
```

`fct_trips` grain: one row per trip. Measures: miles, duration, wait time, base fare, rider fare
before tips, tips, driver pay. Flags: shared ride, wheelchair-accessible request and match.

## Tests

- **Python (pytest, 24 tests):** month validation, lake paths, the truncated-download guard, and every Spark cleaning rule on
  small hand-made DataFrames.
- **dbt (31 tests):** unique and not-null keys, accepted values, referential integrity from the
  fact to every dimension, a custom `non_negative` generic test, and four reconciliation tests
  (fact equals staging per month, the mart adds up to the fact, one row per day and company,
  dropoff after pickup).
- **DAG integrity:** `docker compose exec airflow-scheduler pytest /opt/airflow/tests/test_dag.py`

Local, without Docker: `pip install -r requirements-dev.txt && pytest` (Spark tests need Java 17).

## Design decisions

- **ELT, not ETL.** Spark does only row-level hygiene that every consumer needs. Business logic
  lives in dbt SQL inside the warehouse, where it's versioned, tested and easy to change.
- **Idempotent by month.** Extract skips existing files, transform overwrites its month's folder,
  load swaps the month in with one transaction (never half-loaded), and `fct_trips` uses
  `delete+insert` on `source_month`. Any month can be re-run safely.
- **Deterministic `trip_id`.** The source has no key, so a SHA-256 of the identifying fields gives
  a stable id for de-duplication and the `unique` test.
- **Sampling switch.** `SAMPLE_FRACTION` (default 5%) keeps a laptop run fast; set it to `1.0` for
  full months. The sample is seeded, so re-runs pick the same trips.
- **Timestamps.** TLC times are NYC local wall-clock with no zone. They're kept as-is end to end
  (Spark session and JVM pinned to UTC) so no step shifts them.
- **S3-compatible lake.** RustFS stands in for S3 locally. Pointing `S3_ENDPOINT` and the
  credentials at AWS moves the lake to real S3 without code changes.

## Data source

NYC Taxi and Limousine Commission, [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
(High Volume FHV files, published with about a two-month lag) and the taxi zone lookup.
