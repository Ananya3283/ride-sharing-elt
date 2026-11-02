"""Monthly ELT for NYC high-volume for-hire trips (Uber, Lyft).

extract -> spark_transform -> load -> dbt_run -> dbt_test

TLC publishes each month's file roughly two months late, so a scheduled run processes the
month three months before it runs. Trigger manually with {"month": "YYYY-MM"} to backfill.
"""

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

DBT = "/opt/dbt-venv/bin/dbt"
DBT_ARGS = "--project-dir /opt/airflow/dbt/ride_sharing --profiles-dir /opt/airflow/dbt/ride_sharing"
PUBLISH_LAG_MONTHS = 3


@dag(
    dag_id="ride_sharing_elt",
    schedule="0 6 5 * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    params={"month": Param("", type="string", description="YYYY-MM to process; blank = automatic")},
    tags=["elt", "spark", "dbt"],
)
def ride_sharing_elt():
    @task
    def resolve_month() -> str:
        from pipeline.config import validate_month

        ctx = get_current_context()
        if month := ctx["params"].get("month"):
            return validate_month(month)
        run_after = ctx["dag_run"].run_after or pendulum.now("UTC")
        return pendulum.instance(run_after).subtract(months=PUBLISH_LAG_MONTHS).format("YYYY-MM")

    @task
    def extract(month: str) -> dict:
        from pipeline.config import load_settings
        from pipeline.extract import extract_month

        return extract_month(month, load_settings())

    @task
    def spark_transform(month: str) -> dict:
        from pipeline.config import load_settings
        from pipeline.spark import get_spark
        from pipeline.transform import transform_month

        settings = load_settings()
        spark = get_spark(settings, "ride-sharing-transform")
        try:
            return transform_month(month, settings, spark)
        finally:
            spark.stop()

    @task
    def load(month: str) -> dict:
        from pipeline.config import load_settings
        from pipeline.load import load_trips, load_zones
        from pipeline.spark import get_spark

        settings = load_settings()
        spark = get_spark(settings, "ride-sharing-load")
        try:
            zones = load_zones(settings)
            return {"zones": zones, **load_trips(month, settings, spark)}
        finally:
            spark.stop()

    @task.bash
    def dbt_run() -> str:
        return f"{DBT} seed {DBT_ARGS} && {DBT} run {DBT_ARGS}"

    @task.bash
    def dbt_test() -> str:
        return f"{DBT} test {DBT_ARGS}"

    month = resolve_month()
    extracted = extract(month)
    transformed = spark_transform(month)
    loaded = load(month)
    extracted >> transformed >> loaded >> dbt_run() >> dbt_test()


ride_sharing_elt()
