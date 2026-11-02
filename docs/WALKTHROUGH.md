# Project walkthrough: how every piece works and why

Read this top to bottom once, then use the interview section to rehearse. File links point at the
code; open them side by side.

---

## 1. The problem in one paragraph

NYC's Taxi and Limousine Commission (TLC) publishes every Uber, Lyft, Via and Juno trip each month
as one Parquet file of about 20 million rows (~500 MB). The raw file is not analysis-ready:
column names are cryptic (`PULocationID`, `bcf`), flags are `"Y"`/`"N"` strings, some fees are
null, a few rows are impossible (trip ends before it starts, negative pay), there's no trip id,
and locations are numbers with no names. The pipeline turns that into a clean, tested **star
schema** that answers business questions such as "how many trips did Uber do per day, what's the
average wait, and what share of the fare goes to the driver?".

## 2. ELT vs ETL, and why this is ELT

- **ETL** = Extract, Transform, then Load. You transform *before* the warehouse, usually in a
  custom program.
- **ELT** = Extract, Load, then Transform *inside* the warehouse, usually with SQL (dbt).

This project is ELT. Spark does only the **technical hygiene** every consumer needs (types,
validity, de-duplication), then the data lands in Postgres. All **business logic** (rider fare,
wait time, the star schema, the daily mart) is SQL in dbt. Why:
1. SQL in the warehouse is easy for analysts to read and change.
2. dbt versions, documents and **tests** every model.
3. If business rules change you re-run dbt on data already loaded. No re-extracting.

**Interview line:** "Spark handles volume and data quality on the lake. dbt handles meaning in the
warehouse."

## 3. The three zones of data

| Zone | Where | What it holds | Why keep it |
|---|---|---|---|
| raw | `s3://ride-lake/raw/fhvhv/year=2026/month=06/...parquet` | the untouched TLC file | You can always reprocess from the source of truth, even if TLC changes or removes the file. |
| staged | `s3://ride-lake/staged/fhvhv/year=2026/month=06/` | cleaned Parquet from Spark | It's a cheap, typed, de-duplicated copy that any tool can read. |
| warehouse | Postgres `raw.*` → `staging.*` → `marts.*` | tables for SQL users | It's what analysts and BI query. |

The `year=2026/month=06` folder style is **Hive partitioning**. Spark, Athena and others can skip
whole folders when you filter by month (*partition pruning*).

---

## 4. Infrastructure: [docker-compose.yml](../docker-compose.yml)

Docker Compose starts six containers that talk over a private network, using service names as
hostnames (`postgres`, `lake`, `airflow-apiserver`).

| Service | Image | Job |
|---|---|---|
| `postgres` | postgres:16 | Two databases: `warehouse` (our data) and `airflow` (Airflow's own metadata). Exposed on host port **5433** so it never clashes with a Postgres you may have installed. |
| `lake` | rustfs/rustfs | S3-compatible object storage. MinIO used to be the default here, but its images were pulled from Docker Hub in 2025; RustFS speaks the same S3 API. |
| `airflow-init` | our image | Runs `airflow db migrate` once to create Airflow's tables, then exits. Everything else waits for it (`service_completed_successfully`). |
| `airflow-apiserver` | our image | Airflow 3's web UI and REST API on port 8080. It also hosts the **execution API** that tasks call to report state. |
| `airflow-scheduler` | our image | Decides what runs when. With `LocalExecutor` it also runs the tasks as subprocesses. |
| `airflow-dag-processor` | our image | Parses the files in `dags/` (a separate service in Airflow 3). |

Key details:
- `x-airflow-common` is a YAML anchor (`&airflow-common`), reused with `<<: *airflow-common`, so
  all Airflow services share one config.
- **Volumes** mount `dags/`, `pipeline/` and `dbt/` from your folder into the containers, so code
  edits apply without rebuilding. `pg-data` and `lake-data` are named volumes, so data survives
  `docker compose down` (use `down -v` to wipe it).
- `SIMPLE_AUTH_MANAGER_ALL_ADMINS=true` turns off the login screen. That's fine locally, never in
  production.
- `healthcheck` on Postgres (`pg_isready`) means Airflow waits until the database really accepts
  connections, not merely until the container has started.

### [docker/airflow/Dockerfile](../docker/airflow/Dockerfile)

Starts from the official Airflow image and adds:
1. **Java 17.** Spark runs on the JVM; PySpark is only a Python remote control for it.
2. **Three jars:**
   - `hadoop-aws` gives Spark the `s3a://` filesystem;
   - `aws-java-sdk-bundle` is the AWS client that `hadoop-aws` uses. Its version must match the
     Hadoop version bundled in PySpark 3.5 (3.3.4), or you get `NoSuchMethodError`;
   - `postgresql` is the JDBC driver Spark uses to write into Postgres.
3. **dbt in its own virtualenv** (`/opt/dbt-venv`). dbt and Airflow both pin many shared
   libraries; installing them together often breaks one of them. Separate venvs avoid that.
4. `requirements.txt` with pinned versions, so a rebuild next year gives the same result.

---

## 5. Configuration: [pipeline/config.py](../pipeline/config.py)

- All settings come from **environment variables** (filled from `.env` by Compose). The same
  code runs locally, in Docker, in CI or in AWS just by changing env vars. This is the
  *12-factor app* principle. Secrets never live in code, and `.env` is gitignored.
- `Settings` is a frozen dataclass: immutable, typed, built once by `load_settings()`.
- `validate_month()` rejects anything that isn't `YYYY-MM` (a regex), so a typo like `2026-6`
  fails fast with a clear message instead of halfway through a Spark job.
- `raw_trips_key()` / `staged_trips_prefix()` are the **only** place lake paths are built.
  Extract, transform and load all call them, so they can never disagree.
- `jdbc_url` adds `reWriteBatchedInserts=true`. The Postgres driver then turns 10,000 single-row
  `INSERT`s into a few multi-row ones, which is several times faster.

## 6. Extract: [pipeline/extract.py](../pipeline/extract.py)

1. `ensure_bucket` creates the bucket if missing (`head_bucket` → `create_bucket`).
2. If the month's file is **already in the lake, skip**. This makes extract **idempotent**:
   running it twice changes nothing. `force=True` overrides.
3. `_download` streams the file **1 MB at a time** to a temp file (`while chunk := resp.read(...)`,
   the walrus operator), so a 500 MB file never sits in RAM.
4. `upload_file` (boto3) does a **multipart upload** automatically for big files.
5. It returns the keys, which Airflow stores as an XCom (task output) so you can see them in the UI.

[pipeline/lake.py](../pipeline/lake.py) wraps boto3. `endpoint_url` is what points boto3 at
RustFS instead of AWS. Remove it (or set it to the AWS endpoint) and the same code talks to real S3.

## 7. Spark transform: [pipeline/transform.py](../pipeline/transform.py)

### The SparkSession ([pipeline/spark.py](../pipeline/spark.py))
- `local[*]` runs Spark in one JVM using all CPU cores. That's fine for one machine; on a cluster
  you'd change only `SPARK_MASTER`.
- `spark.driver.memory=3g`. In local mode the driver *is* the executor, so this is Spark's whole heap.
- `spark.sql.shuffle.partitions=16`. The default is 200, which creates tiny tasks for a
  ~1M-row job. 16 suits a laptop.
- `fs.s3a.*` settings: the endpoint, keys, and `path.style.access=true`. Path-style is needed for
  local S3 servers (`http://lake:9000/bucket/key` instead of `http://bucket.lake:9000/key`).
- **Timezone pinning** (`spark.sql.session.timeZone=UTC` and `-Duser.timezone=UTC`): see
  [section 12](#12-real-bugs-found-while-building-good-interview-stories).

### The four steps of `clean_trips`
1. **`standardise`**
   - Picks only the 22 columns we use, renames them (`PULocationID` → `pickup_location_id`,
     `bcf` → `black_car_fund`) and casts each to an explicit type. You never trust the source's
     types.
   - If a column doesn't exist (`cbd_congestion_fee` started in January 2025) it uses `lit(None)`,
     so older months still work. That's **schema-drift tolerance**.
   - `"Y"`/`"N"` flags become real booleans: `upper(trim(col)) == "Y"`.
   - Null fees become `0.0` (`fillna`). A missing airport fee means there was no airport fee,
     and it keeps sums correct (in SQL, `5 + NULL = NULL`).
2. **`validate`** keeps only rows that can be real trips:
   - required fields present;
   - pickup **inside the target month**. TLC files contain a few stragglers from adjacent
     months; without this filter, re-loading May could duplicate or delete June rows;
   - dropoff after pickup; 0–300 miles; 1 second to 6 hours; base fare ≥ 0; driver pay ≥ 0.
   Each rule is a named constant (`MAX_TRIP_MILES`), so a reviewer can see and question it.
3. **`add_trip_id`**: the source has **no primary key**. We build one by joining the identifying
   fields with `|` and hashing with SHA-256:
   - *deterministic*: the same trip always gets the same id, on every re-run;
   - fixed length (64 hex characters), so it's cheap to index;
   - `dropDuplicates(["trip_id"])` then removes exact repeats.
   *Caveat to mention:* two genuinely different trips with identical fields would collapse into
   one. With timestamps to the second plus locations and miles, that's practically impossible.
4. Adds `pickup_date` (for the date dimension) and `source_month` (for month-level replacement).

### `transform_month`
- Reads `s3a://.../raw/...parquet`. Spark reads Parquet **columnar**, so it only reads the columns
  it needs.
- **Sampling**: `sample(fraction, seed=42)`. The fixed seed means the same rows every run, so
  results are reproducible.
- `cache()` keeps the sampled data in memory, because we use it twice (count it, then clean it).
  Without the cache Spark would re-read the file from S3. (Spark is **lazy**: nothing executes
  until an *action* such as `count()` or `write`.)
- `repartition(8)` → 8 Parquet files of sensible size (avoids the "many tiny files" problem).
- `mode("overwrite")` on the month's folder makes this step idempotent too.
- Counts raw vs staged rows and returns **rejected rows**, a basic data-quality metric visible in
  Airflow.

## 8. Load: [pipeline/load.py](../pipeline/load.py)

- `ensure_raw_schema` runs [pipeline/sql/raw_ddl.sql](../pipeline/sql/raw_ddl.sql) (`CREATE ... IF NOT
  EXISTS`). We define the table ourselves instead of letting Spark create it, so we control
  types (`CHAR(64)`, `NOT NULL`, `DOUBLE PRECISION`), defaults (`_loaded_at DEFAULT now()`) and
  the index on `source_month`.
- **Atomic month swap (load into a landing table, then swap).**
  1. `TRUNCATE raw.fhvhv_trips_incoming` (a landing table with the same columns, created with
     `LIKE raw.fhvhv_trips`).
  2. Spark writes the staged month into the **landing** table over 4 parallel JDBC connections.
  3. **One** Postgres transaction runs
     `DELETE FROM raw.fhvhv_trips WHERE source_month = '2026-06'`, then
     `INSERT INTO raw.fhvhv_trips SELECT * FROM raw.fhvhv_trips_incoming`, then
     `TRUNCATE` the landing table, then `COMMIT`.

  Why: Spark's 4 connections commit **independently**. If one fails, the others have already
  committed, which is a half-loaded month. That really happened while testing (section 12). With
  the swap, a failure in step 2 only touches the landing table; readers of `raw.fhvhv_trips` see
  either the whole old month or the whole new month, never a mix (transaction **atomicity**). It
  also stays **idempotent**: re-running June replaces June and never doubles it ("partition
  overwrite").
- Spark JDBC write: `batchsize=10000` rows per round trip, `numPartitions=4` parallel connections.
- **Zones** are tiny (265 rows), so plain psycopg2 with an **upsert**
  (`INSERT ... ON CONFLICT (location_id) DO UPDATE`): new zones are inserted and renamed zones
  are updated. Idempotent as well.

---

## 9. dbt: [dbt/ride_sharing/](../dbt/ride_sharing/)

dbt (data build tool) compiles `SELECT` statements into `CREATE VIEW` / `CREATE TABLE` in the
warehouse, works out the order from `ref()` / `source()` calls (a DAG), and runs tests.

### Project setup
- [dbt_project.yml](../dbt/ride_sharing/dbt_project.yml): staging models are **views** (always fresh,
  no storage) and marts are **tables** (fast for BI). The seed column types are forced to text.
- [profiles.yml](../dbt/ride_sharing/profiles.yml): connection details via `env_var()`, so no
  passwords are committed.
- [macros/generate_schema_name.sql](../dbt/ride_sharing/macros/generate_schema_name.sql): by default
  dbt would name schemas `analytics_staging`; this override gives clean `staging` and `marts`.
- **Seed** [hvfhs_companies.csv](../dbt/ride_sharing/seeds/hvfhs_companies.csv): a small static
  lookup (HV0003 = Uber, HV0005 = Lyft) kept in git and loaded by `dbt seed`. Seeds are for
  reference data that changes rarely.

### Layers
1. **Sources** ([_sources.yml](../dbt/ride_sharing/models/staging/_sources.yml)) declare the raw
   tables. Models use `{{ source('raw','fhvhv_trips') }}`, so lineage starts at the raw layer.
2. **Staging** (one model per source, light logic):
   - `stg_fhvhv_trips` derives **wait_seconds** (request → pickup, only when the request time
     exists and isn't after pickup) and **rider_fare_before_tips** (base fare + tolls + black car
     fund + sales tax + congestion surcharge + airport fee + CBD congestion fee). It also renames
     `_loaded_at` to `loaded_at`.
   - `stg_taxi_zones` renames `zone` to `zone_name`.
3. **Marts: the star schema.** One fact table in the middle, dimension tables around it.
   - **Fact** = events you measure (a trip). **Dimension** = context you filter and group by
     (who, where, when).
   - `dim_company`: from the seed. Key = licence number.
   - `dim_zone`: borough, zone and service zone, plus an `is_airport` flag.
   - `dim_date`: generated with Postgres `generate_series` from the first to the last trip day.
     It holds year, quarter, month name, ISO day of week and weekend flag. Key = `20260601`-style
     integer (a *smart key*: readable and sorts correctly).
   - `fct_trips`: grain = **one row per trip**, with foreign keys to every dimension, measures
     and flags, plus `pickup_hour` for time-of-day analysis.
   - `mart_daily_company_metrics`: grain = **one row per day per company**, a pre-aggregated table
     a dashboard can read instantly (trips, miles, average wait, fares, tips, driver pay, and
     driver pay as a share of base fare).
   - **Why a star schema?** Simple joins (fact → dimension, one hop), fast aggregations, and it's
     the shape BI tools like Power BI expect.

### Incremental `fct_trips`
```sql
materialized='incremental', unique_key='source_month', incremental_strategy='delete+insert'
... where loaded_at > (select max(loaded_at) from {{ this }})
```
- First run: builds the whole table. Later runs only process rows loaded **since the last run**.
- `delete+insert` on `source_month` deletes every month in the new batch from the fact, then
  inserts it. This mirrors the load step's month replacement, so a re-loaded month is replaced,
  not duplicated, and trips dropped from a re-load don't linger.
- Proof: on the second dbt run in CI, fct_trips does `INSERT 0 0` (nothing new) and all tests
  still pass.
- `indexes` config creates Postgres indexes on `pickup_date_key` and `company_key` (the common
  filters and joins).
- `dbt build --full-refresh` rebuilds from scratch if logic changes.

### The 31 tests (data contracts)
| Type | Examples | Catches |
|---|---|---|
| `unique`, `not_null` | trip_id, every dimension key | duplicates and missing keys |
| `accepted_values` | licence ∈ {HV0002–5}; borough list; pickup_hour 0–23 | a new company or a bad value appearing |
| `relationships` | fct_trips → dim_company, dim_zone (pickup and dropoff), dim_date | orphan foreign keys (referential integrity) |
| custom generic `non_negative` ([tests/generic](../dbt/ride_sharing/tests/generic/non_negative.sql)) | miles, fares, driver pay, trips | negative values |
| singular (SQL that must return 0 rows) | `assert_fct_matches_staging`, `assert_daily_metrics_reconcile`, `assert_daily_metrics_one_row_per_day_company`, `assert_dropoff_after_pickup` | lost or double-counted rows across layers (**reconciliation**) |

A dbt test is just a query for *bad* rows: zero rows means pass. `dbt build` runs seeds, models
and tests in dependency order, so a failing test **stops downstream models** from building on bad
data.

---

## 10. Orchestration: [dags/ride_sharing_elt.py](../dags/ride_sharing_elt.py)

- Airflow 3 **TaskFlow API** (`@dag`, `@task`). Return values pass between tasks automatically as
  XComs.
- `schedule="0 6 5 * *"`: 06:00 UTC on the 5th of each month (cron syntax).
- `catchup=False`: don't run all missed months since `start_date` when first switched on.
- `max_active_runs=1`: two runs never load at the same time.
- `retries=2`, 5 minutes apart: survives a flaky download.
- **Which month?** TLC publishes about two months late, so a scheduled run processes the month
  three months before it runs. A manual run can pass `{"month": "2025-11"}` to **backfill** any
  month.
- Imports are *inside* the task functions. The dag-processor re-parses DAG files every ~30
  seconds, so top-level imports of Spark or boto3 would slow parsing for every DAG.
- Each Spark task creates and stops its own session (`try/finally`), so a failed task never
  leaves a JVM running.
- `@task.bash` runs dbt from its separate venv: `dbt seed` then `dbt run`, then a separate
  `dbt test` task, so the UI shows clearly whether building or testing failed.
- Chain: `extract >> spark_transform >> load >> dbt_run >> dbt_test`.

## 11. Testing and CI: [tests/](../tests/), [.github/workflows/ci.yml](../.github/workflows/ci.yml)

- **pytest unit tests** build tiny DataFrames by hand and call `clean_trips`. They assert that a
  good row survives with the right types and fills, that each invalid case (9 cases, via
  `@pytest.mark.parametrize`) is rejected, that duplicates collapse, and that `trip_id` is
  deterministic. They run in seconds and need no S3 or database, because the logic is pure
  functions on DataFrames (I/O stays in `transform_month`).
- **CI job 1:** Python 3.12 and Java 17 → `ruff check` (lint) → `ruff format --check` (style) →
  `pytest`.
- **CI job 2:** spins up a Postgres **service container**, loads 8 hand-made trips and 7 zones
  ([ci/fixtures](../ci/fixtures/)) through the same DDL as production, runs `dbt build` (all models
  and tests), then runs it **again** to test the incremental path.
- **DAG test** (in the container): no import errors, and the task order is exactly as designed.

---

## 12. Real bugs found while building (good interview stories)

1. **Type inference on nulls.** The first Spark tests failed with `CANNOT_DETERMINE_TYPE`: every
   test row had `airport_fee = None`, so Spark couldn't guess the column's type. *Fix:* give test
   DataFrames an explicit schema that mirrors the source file. *Lesson:* never rely on schema
   inference; declare schemas.
2. **Timezone shift.** A trip at 00:00 on 1 July was *not* rejected from June. TLC timestamps
   have no timezone. Python converted the test's naive datetime using my machine's clock (IST,
   UTC+5:30), so Spark saw 18:30 on 30 June. *Fix:* treat source times as `timestamp_ntz` (no
   zone, which is how Spark really reads these files) and pin both the Spark session and the JVM
   (which the Postgres JDBC driver uses) to UTC. *Lesson:* timestamps without a zone shift silently
   whenever a machine's local timezone leaks in; pin it end to end.
3. **Dead dependency.** The MinIO Docker images were removed from Docker Hub, so the stack moved to
   RustFS, another S3-compatible server. Because all lake access goes through the S3 API
   (`endpoint_url`, `s3a`), the code didn't change.
4. **Half-loaded month.** The first version deleted the month, then appended with Spark. During a
   re-run, the database timed out on some of Spark's 4 connections. One partition had already
   committed, so June held 52,039 of 208,157 rows, and anyone querying would have seen wrong
   totals. *Fix:* the landing-table swap in section 8. The next run reported
   "52039 rows replaced by 208157", and the run after that "208157 replaced by 208157".
   *Lesson:* parallel writers don't share a transaction; make the final step a single atomic
   one.

## 12b. Verified run on real data (June 2026, 1% sample)

| Step | Result |
|---|---|
| Extract | 506 MB TLC file → lake raw zone |
| Spark transform | 208,244 trips sampled → **208,157 staged, 87 rejected** (0.04%) |
| Load | 208,157 rows, all `trip_id`s distinct; re-run replaced the month exactly |
| dbt build | **39/39 passed** (seed, 7 models, 31 tests): fct_trips 208,157 rows, dim_zone 265, dim_date 30, mart 60 (30 days × 2 companies) |

What the data says (good to quote in an interview):
- **Uber 152,782 trips vs Lyft 55,375**, so Uber has ~73% of the market.
- **Average wait ~5.5–6 minutes** (Uber 353 s, Lyft 330 s); average trip ~5 miles.
- **Drivers get ~63–64%** of what riders pay before tips.
- **Top pickup zones are LaGuardia and JFK airports**, then Crown Heights North.
- **Weekends: ~7,500 trips/day vs ~6,700 on weekdays** in the sample (~11% busier).

## 13. Limitations and what you'd do next

- The month swap holds a lock on those rows while it runs. At full volume, partition
  `raw.fhvhv_trips` by month and use `ATTACH PARTITION` for an instant swap.
- Spark runs in local mode inside the Airflow worker → a Spark cluster, or EMR/Databricks/Glue in
  the cloud (`SPARK_MASTER` / submit operator).
- Postgres is a row store. At full volume (250M rows a year) you'd move to a columnar warehouse
  (Redshift, Snowflake, BigQuery) or partition the fact table by month.
- Add dbt **source freshness**, dbt docs hosting, alerting on failures (Slack or email callback),
  and a Power BI dashboard on `mart_daily_company_metrics`.
- Rejected rows are only counted. Writing them to a `quarantine/` zone would make them auditable.

---

## 14. Interview questions you should be able to answer

**Walk me through the pipeline.**
"Monthly batch ELT on NYC Uber/Lyft data, about 20M trips a month. Airflow pulls the month's
Parquet into an S3 raw zone. PySpark enforces the schema, drops impossible trips, builds a SHA-256
trip id, de-duplicates and writes cleaned Parquet to a staged zone. Spark loads it into Postgres
over JDBC, replacing the month so re-runs are idempotent. dbt builds staging views and a star
schema with a trip fact and company, zone and date dimensions, plus a daily mart, guarded by 31
tests. All of it runs in Docker, and GitHub Actions runs lint, the PySpark unit tests and a full
dbt build on fixture data."

**Why both Spark and dbt?** Spark for heavy row-level work on files in the lake (volume, schema,
dedupe); dbt for business modelling in SQL where it's versioned and tested. ELT keeps raw data,
so business rules can change without re-extracting.

**How is it idempotent?** Each step is safe to re-run: extract skips existing files, transform
overwrites the month's folder, load swaps the whole month in one transaction, and the incremental
fact uses delete+insert on `source_month`.

**What happens if a run fails halfway?** Airflow retries the task twice. Every step is idempotent,
so re-running the month is safe. The load writes into a landing table and swaps the month in with
one transaction, so a failure never leaves a half-loaded month. (I learned that the hard way:
see bug 4.)

**How do you handle bad data?** Explicit casts, validation rules in Spark (rejected count logged
per run), then dbt tests: keys, accepted values, referential integrity and reconciliation between
layers. A failing test stops downstream models.

**What's the grain of fct_trips?** One row per trip. The mart's grain is one row per day per
company.

**Why a surrogate trip_id and why SHA-256?** The source has no key. A deterministic hash of
identifying fields is stable across re-runs (unlike an auto-increment) and fixed length.

**Star vs snowflake schema?** Star keeps dimensions denormalised (one hop from the fact):
simpler and faster for BI. Snowflake normalises dimensions further (zone → borough table), which
saves space but adds joins.

**What is an incremental model?** It processes only new data on each run instead of rebuilding
everything, using a filter (`is_incremental()`) and a strategy (delete+insert, merge, append).

**Why `shuffle.partitions=16`?** The default is 200. For a ~1M-row local job, 200 tiny tasks cost
more in overhead than they save.

**Why `cache()` there?** The sampled DataFrame is used by two actions (count, then write).
Without caching, Spark's lazy evaluation would re-read and re-sample the file from S3.

**What would change at 10× scale or in AWS?** Real S3, Spark on EMR/Glue/Databricks, a columnar
warehouse (Redshift or Snowflake), the fact table partitioned by month, a managed Airflow
(MWAA), and secrets in AWS Secrets Manager instead of `.env`.
